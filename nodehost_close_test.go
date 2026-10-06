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
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/transport"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/goutils/syncutil"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Observe the native environment lock without replacing its cleanup owner.
type closeEnvironmentFS struct {
	vfs.FS
	lock *closeEnvironmentLock
}
type closeEnvironmentLock struct {
	io.Closer
	calls int
}

func (l *closeEnvironmentLock) Close() error { l.calls++; return l.Closer.Close() }
func (f *closeEnvironmentFS) Lock(name string) (io.Closer, error) {
	lock, err := f.FS.Lock(name)
	if err != nil {
		return nil, err
	}
	f.lock = &closeEnvironmentLock{Closer: lock}
	return f.lock, nil
}

type gatedCloseTransport struct {
	transport.ITransport
	entered chan struct{}
	release chan struct{}
	notify  sync.Once
	calls   atomic.Int32
	err     error
	mode    string
}

func (t *gatedCloseTransport) Close() error {
	t.calls.Add(1)
	t.notify.Do(func() { close(t.entered) })
	<-t.release
	switch t.mode {
	case "panic":
		panic(t.err)
	case "Goexit":
		runtime.Goexit()
	}
	return t.err
}
func TestCloseWithErrorCachesCompletedDiagnostics(t *testing.T) {
	diagnostic := errors.New("transport close diagnostic")
	logDBDiagnostic := errors.New("LogDB close diagnostic")
	var logDBCloses int
	child := &gatedCloseTransport{entered: make(chan struct{}), release: make(chan struct{}), err: diagnostic}
	var release sync.Once
	finish := func() { release.Do(func() { close(child.release) }) }
	env, err := server.NewEnv(config.NodeHostConfig{}, vfs.Default)
	require.NoError(t, err)
	nh := &NodeHost{transport: child, env: env}
	nh.mu.logdb = &interruptedCloseLogDB{close: func() error { logDBCloses++; return logDBDiagnostic }}
	first, second := make(chan error, 1), make(chan error, 1)
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		finish()
		for _, done := range []chan struct{}{firstDone, secondDone} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("test close worker did not join")
			}
		}
	})
	go func() { defer close(firstDone); first <- nh.CloseWithError() }()
	go func() { defer close(secondDone); second <- nh.CloseWithError() }()
	select {
	case <-child.entered:
	case <-time.After(time.Second):
		t.Fatal("owned transport close not reached")
	}
	// Session creation remains usable while native teardown is blocked.
	require.Equal(t, uint64(1), nh.GetNoOPSession(1).ShardID)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session, sessionErr := nh.SyncGetSession(ctx, 1)
	require.Nil(t, session)
	require.ErrorIs(t, sessionErr, ErrShardNotFound)
	finish()
	for _, done := range []chan error{first, second} {
		select {
		case err := <-done:
			require.ErrorIs(t, err, diagnostic)
			require.ErrorIs(t, err, logDBDiagnostic)
		case <-time.After(time.Second):
			t.Fatal("owner close did not finish")
		}
	}
	require.ErrorIs(t, nh.CloseWithError(), diagnostic)
	require.ErrorIs(t, nh.CloseWithError(), logDBDiagnostic)
	require.Equal(t, 1, logDBCloses)
	require.Equal(t, int32(1), child.calls.Load())
}
func TestLegacyDuplicateClosePreservesAvailableOwnerLock(t *testing.T) {
	nh := &NodeHost{}
	nh.Close()
	require.PanicsWithValue(t, "NodeHost.Stop called twice", nh.Close)
	require.True(t, nh.mu.TryLock(), "legacy rejection must release the owner mutex")
	nh.mu.Unlock()
	require.NoError(t, nh.CloseWithError())
}

type interruptedCloseLogDB struct {
	raftio.ILogDB
	close func() error
}

func (d *interruptedCloseLogDB) Close() error { return d.close() }

func TestCloseWithErrorCachesInterruptedTeardown(t *testing.T) {
	for _, tc := range []struct{ owner, mode string }{
		{"transport", "panic"}, {"transport", "Goexit"},
		{"LogDB", "panic"}, {"LogDB", "Goexit"},
	} {
		t.Run(tc.owner+"/"+tc.mode, func(t *testing.T) {
			mode := tc.mode
			cause := errors.New("native transport close interrupted")
			child := &gatedCloseTransport{entered: make(chan struct{}), release: make(chan struct{}), err: cause, mode: mode}
			close(child.release)
			fs := &closeEnvironmentFS{FS: vfs.Default}
			env, err := server.NewEnv(config.NodeHostConfig{NodeHostDir: t.TempDir()}, fs)
			require.NoError(t, err)
			t.Cleanup(func() {
				if fs.lock != nil && fs.lock.calls == 0 {
					_ = env.Close()
				}
			})
			require.NoError(t, env.LockNodeHostDir())
			require.NotNil(t, fs.lock)
			nh := &NodeHost{env: env}
			var engineExits atomic.Int32
			var exitsAtLogDBClose int32
			var logDBCloses int
			var ownedEngine *engine
			if tc.owner == "transport" {
				// Use the actual empty engine, with one worker per existing pool.
				// No replicas, sockets, or storage fixture is needed for shutdown.
				ownedEngine = newExecEngine(nh, config.EngineConfig{ExecShards: 1, CommitShards: 1, ApplyShards: 1, SnapshotShards: 1, CloseShards: 1}, true, false, env, nil)
				nh.engine = ownedEngine
				t.Cleanup(func() {
					select {
					case <-ownedEngine.nodeStopper.ShouldStop():
					default:
						_ = ownedEngine.close()
					}
				})
				for _, stopper := range []*syncutil.Stopper{ownedEngine.nodeStopper, ownedEngine.commitStopper, ownedEngine.taskStopper, ownedEngine.wp.poolStopper, ownedEngine.cp.poolStopper} {
					stopper := stopper
					stopper.RunWorker(func() { <-stopper.ShouldStop(); engineExits.Add(1) })
				}
			}
			if tc.owner == "transport" {
				nh.transport = child
				nh.mu.logdb = &interruptedCloseLogDB{close: func() error {
					logDBCloses++
					exitsAtLogDBClose = engineExits.Load()
					return nil
				}}
			} else {
				nh.mu.logdb = &interruptedCloseLogDB{close: child.Close}
			}
			done := make(chan struct{})
			var recovered interface{}
			returned := false
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				_ = nh.CloseWithError()
				returned = true
			}()
			<-done
			if ownedEngine != nil {
				require.Equal(t, 1, logDBCloses)
				require.Equal(t, int32(5), exitsAtLogDBClose, "engine joins must precede LogDB destruction")
				require.Equal(t, int32(5), engineExits.Load(), "every real engine owner must join before outer unwind completes")
			}
			require.Equal(t, 1, fs.lock.calls, "native environment lock must close before unwind completes")
			require.False(t, returned)
			if mode == "panic" {
				require.Same(t, cause, recovered)
			} else {
				require.Nil(t, recovered)
			}
			require.Equal(t, uint64(1), nh.GetNoOPSession(1).ShardID)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			session, sessionErr := nh.SyncGetSession(ctx, 1)
			require.Nil(t, session)
			require.ErrorIs(t, sessionErr, ErrShardNotFound)
			first := nh.CloseWithError()
			require.ErrorIs(t, first, ErrCloseInterrupted, "interrupted teardown cannot be reported as successful")
			require.Equal(t, first, nh.CloseWithError())
			require.Equal(t, int32(1), child.calls.Load(), "consumed destructor must not be retried")
		})
	}
}
