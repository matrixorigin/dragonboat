//go:build go1.25

// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dragonboat

import (
	"context"
	"errors"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

type drainCloseSM struct {
	sm.IStateMachine
	entered, release chan struct{}
	closes           int
	mode             string
}

func (s *drainCloseSM) Close() error {
	s.closes++
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	switch s.mode {
	case "Goexit":
		runtime.Goexit()
	case "panic":
		panic("native close fatal panic")
	case "error":
		return errors.New("native close fatal error")
	}
	return nil
}

func makeDrainCloseNode(id uint64, user *drainCloseSM) *node {
	cfg := config.Config{ShardID: id, ReplicaID: 1}
	n := &node{shardID: id, replicaID: 1}
	managed := rsm.NewNativeSM(cfg, rsm.NewInMemStateMachine(user), nil)
	n.sm = rsm.NewStateMachine(managed, nil, cfg, n, vfs.NewMem())
	return n
}

// The live scheduler must restore capacity without re-running the interrupted
// callback, including when another concrete generation has the same shard ID.
func TestClosePoolRestoresCapacityAfterLiveGoexit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newCloseWorkerPool(1)
		closed := false
		t.Cleanup(func() {
			if !closed {
				_ = pool.close()
			}
		})
		first, replacement, independent := &drainCloseSM{mode: "Goexit"}, &drainCloseSM{}, &drainCloseSM{}
		firstNode := makeDrainCloseNode(1, first)
		replacementNode := makeDrainCloseNode(1, replacement)
		independentNode := makeDrainCloseNode(2, independent)
		pool.ready <- closeReq{node: firstNode}
		synctest.Wait()
		require.Equal(t, 1, first.closes)
		require.False(t, firstNode.destroyed())
		pool.ready <- closeReq{node: replacementNode}
		pool.ready <- closeReq{node: independentNode}
		synctest.Wait()
		require.Equal(t, 1, replacement.closes)
		require.Equal(t, 1, independent.closes)
		require.True(t, replacementNode.destroyed())
		require.True(t, independentNode.destroyed())
		err := pool.close()
		closed = true
		require.ErrorIs(t, err, ErrCloseInterrupted)
		var diagnostic *closeInterruptedError
		require.ErrorAs(t, err, &diagnostic)
		require.Same(t, firstNode, diagnostic.node)
		require.Equal(t, 1, first.closes, "interrupted callback must not be retried")
	})
}

func TestClosePoolDrainsAcceptedNodesAfterDiagnosticTimeout(t *testing.T) {
	for _, mode := range []string{"normal", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				first := &drainCloseSM{entered: make(chan struct{}), release: make(chan struct{}), mode: mode}
				second := &drainCloseSM{}

				pool := newCloseWorkerPool(1)
				closed := make(chan struct{})
				released := false
				t.Cleanup(func() {
					if !released {
						close(first.release)
					}
					<-closed
				})
				firstNode, secondNode := makeDrainCloseNode(1, first), makeDrainCloseNode(2, second)
				pool.ready <- closeReq{node: firstNode}
				<-first.entered
				pool.ready <- closeReq{node: secondNode}
				var closeErr error
				go func() { defer close(closed); closeErr = pool.close() }()
				// The bubble advances the real production timer without a wall-clock wait.
				time.Sleep(time.Duration(settings.Soft.CloseWorkerTimedWaitSecond)*time.Second + time.Second)
				close(first.release)
				released = true
				<-closed
				require.Equal(t, 1, first.closes)
				require.Equal(t, 1, second.closes, "accepted pending node must not disappear at timeout")
				require.True(t, pool.isIdle())
				require.True(t, secondNode.destroyed())
				if mode == "Goexit" {
					require.ErrorIs(t, closeErr, ErrCloseInterrupted)
					var diagnostic *closeInterruptedError
					require.ErrorAs(t, closeErr, &diagnostic)
					require.Same(t, firstNode, diagnostic.node)
					require.False(t, firstNode.destroyed(), "accounting cannot certify interrupted destruction")
				} else {
					require.NoError(t, closeErr)
					require.True(t, firstNode.destroyed())
				}
			})
		})
	}
}

// Panic and returned callback errors retain the existing fatal policy. Native
// Goexit recovery must not silently convert either into a successful close.
func TestCloseWorkerPreservesFatalCallbacks(t *testing.T) {
	const key = "DRAGONBOAT_CLOSE_FATAL_MODE"
	if mode := os.Getenv(key); mode != "" {
		synctest.Test(t, func(t *testing.T) {
			pool := newCloseWorkerPool(1)
			pool.ready <- closeReq{node: makeDrainCloseNode(1, &drainCloseSM{mode: mode})}
			_ = pool.close()
			t.Fatal("fatal callback unexpectedly returned")
		})
		return
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	for _, mode := range []string{"panic", "error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, executable, "-test.run=^TestCloseWorkerPreservesFatalCallbacks$")
			child.Env = append(os.Environ(), key+"="+mode)
			output, err := child.CombinedOutput()
			require.NoError(t, ctx.Err(), "child process did not terminate")
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			require.Contains(t, string(output), "panic: native close fatal "+mode)
		})
	}
}

func TestNodeHostRetainsInterruptedNodeAfterLaterCloseUnwind(t *testing.T) {
	for _, mode := range []string{"panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			fs := &closeEnvironmentFS{FS: vfs.Default}
			env, err := server.NewEnv(config.NodeHostConfig{NodeHostDir: t.TempDir()}, fs)
			require.NoError(t, err)
			t.Cleanup(func() {
				if fs.lock != nil && fs.lock.calls == 0 {
					_ = env.Close()
				}
			})
			require.NoError(t, env.LockNodeHostDir())
			nh := &NodeHost{env: env}
			owned := newExecEngine(nh, config.EngineConfig{ExecShards: 1, CommitShards: 1, ApplyShards: 1, SnapshotShards: 1, CloseShards: 1}, true, false, env, nil)
			nh.engine = owned
			t.Cleanup(func() {
				select {
				case <-owned.nodeStopper.ShouldStop():
				default:
					_ = owned.close()
				}
			})
			user := &drainCloseSM{mode: "Goexit"}
			interrupted := makeDrainCloseNode(1, user)
			owned.cp.ready <- closeReq{node: interrupted}
			cause := errors.New("later LogDB close interrupted")
			closes := 0
			nh.mu.logdb = &interruptedCloseLogDB{close: func() error {
				closes++
				if mode == "panic" {
					panic(cause)
				}
				runtime.Goexit()
				return nil
			}}
			returned := false
			var recovered any
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				_ = nh.CloseWithError()
				returned = true
			}()
			<-done
			require.False(t, returned)
			if mode == "panic" {
				require.Same(t, cause, recovered)
			} else {
				require.Nil(t, recovered)
			}
			require.Equal(t, 1, user.closes)
			require.False(t, interrupted.destroyed())
			require.Equal(t, 1, closes)
			require.Equal(t, 1, fs.lock.calls)
			cached := nh.CloseWithError()
			require.ErrorIs(t, cached, ErrCloseInterrupted)
			var diagnostic *closeInterruptedError
			require.ErrorAs(t, cached, &diagnostic)
			require.Same(t, interrupted, diagnostic.node)
			require.Equal(t, cached, nh.CloseWithError())
			require.Equal(t, 1, closes)
		})
	}
}

// The bubble cannot finish with an acquired worker still alive. The impossible
// slice length fails before allocation and exercises native constructor unwind.
func TestEngineConstructionJoinsPoolsAfterAllocationPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.EngineConfig{ExecShards: 1, CommitShards: 1, ApplyShards: 1, SnapshotShards: 1, CloseShards: ^uint64(0)}
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_ = newExecEngine(nil, cfg, false, false, nil, nil)
		}()
		require.NotNil(t, recovered)
		_, nativeAllocationPanic := recovered.(runtime.Error)
		require.True(t, nativeAllocationPanic, "must preserve the native allocation panic")
	})
}
