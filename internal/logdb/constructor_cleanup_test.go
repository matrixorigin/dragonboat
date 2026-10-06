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

package logdb

import (
	"errors"
	"runtime"
	"testing"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb/kv"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/stretchr/testify/require"
)

type constructorStore struct {
	kv.IKVStore
	closes     int
	diagnostic error
	mode       string
	located    bool
}

func (s *constructorStore) Close() error {
	s.closes++
	switch s.mode {
	case "panic":
		panic(s.diagnostic)
	case "Goexit":
		runtime.Goexit()
	}
	return s.diagnostic
}

func (s *constructorStore) IterateValue(_ []byte, _ []byte, _ bool, op func([]byte, []byte) (bool, error)) error {
	if s.located {
		_, err := op(nil, nil)
		return err
	}
	return nil
}

func TestShardedConstructorClosesBeforeFormatReopen(t *testing.T) {
	for _, mode := range []string{"clean", "error", "panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			diagnostic := errors.New("old format close diagnostic")
			old := &constructorStore{located: true, mode: mode}
			if mode != "clean" {
				old.diagnostic = diagnostic
			}
			next := &constructorStore{}
			cfg := config.NodeHostConfig{}
			cfg.Expert.FS = vfs.NewMemFS()
			cfg.Expert.LogDB = config.GetTinyMemLogDBConfig()
			cfg.Expert.LogDB.Shards = 1
			cfg.Expert.Engine.ExecShards = 1
			calls := 0
			factory := func(config.LogDBConfig, kv.LogDBCallback, string, string, vfs.IFS) (kv.IKVStore, error) {
				calls++
				if calls == 1 {
					return old, nil
				}
				if old.closes != 1 {
					panic("replacement acquired before old owner retired")
				}
				return next, nil
			}
			var owner *ShardedDB
			var err error
			var recovered any
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				owner, err = OpenShardedDB(cfg, nil, []string{"/format"}, nil, false, true, factory)
				returned = true
			}()
			<-done
			if owner != nil {
				t.Cleanup(func() { require.NoError(t, owner.Close()); require.Equal(t, 1, next.closes) })
			}
			require.Equal(t, 1, old.closes, "consumed old owner must not be closed again during unwind")
			if mode == "clean" {
				require.True(t, returned)
				require.Nil(t, recovered)
				require.NoError(t, err)
				require.NotNil(t, owner)
				require.Equal(t, 2, calls)
				require.Zero(t, next.closes)
			} else {
				require.Nil(t, owner)
				require.Equal(t, 1, calls)
				if mode == "error" {
					require.True(t, returned)
					require.ErrorIs(t, err, diagnostic)
					require.Nil(t, recovered)
				} else {
					require.False(t, returned)
					if mode == "panic" {
						require.Same(t, diagnostic, recovered)
					} else {
						require.Nil(t, recovered)
					}
				}
			}
		})
	}
}

func TestShardedConstructorUnwindsEarlierStores(t *testing.T) {
	for _, mode := range []string{"error", "panic", "Goexit", "error with close diagnostic"} {
		t.Run(mode, func(t *testing.T) {
			cause := errors.New("next shard acquisition failed")
			diagnostic := errors.New("completed close diagnostic")
			store := &constructorStore{}
			if mode == "error with close diagnostic" {
				store.diagnostic = diagnostic
			}
			cfg := config.NodeHostConfig{}
			cfg.Expert.FS = vfs.NewMemFS()
			cfg.Expert.LogDB = config.GetTinyMemLogDBConfig()
			cfg.Expert.LogDB.Shards = 2
			calls := 0
			factory := func(config.LogDBConfig, kv.LogDBCallback, string, string, vfs.IFS) (kv.IKVStore, error) {
				calls++
				if calls == 1 {
					return store, nil
				}
				switch mode {
				case "panic":
					panic(cause)
				case "Goexit":
					runtime.Goexit()
				}
				return nil, cause
			}
			done := make(chan struct{})
			var result *ShardedDB
			var err error
			var recovered any
			returned := false
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				result, err = OpenShardedDB(cfg, nil, []string{"/first", "/second"}, nil, false, false, factory)
				returned = true
			}()
			<-done
			require.Equal(t, 2, calls)
			require.Equal(t, 1, store.closes)
			require.Nil(t, result)
			switch mode {
			case "panic":
				require.Same(t, cause, recovered)
				require.False(t, returned)
			case "Goexit":
				require.Nil(t, recovered)
				require.False(t, returned)
			default:
				require.Nil(t, recovered)
				require.True(t, returned)
				require.ErrorIs(t, err, cause)
				if store.diagnostic != nil {
					require.ErrorIs(t, err, diagnostic)
				}
			}
		})
	}
}

type constructorBatch struct {
	kv.IWriteBatch
	destroys   int
	mode       string
	diagnostic error
}

func (b *constructorBatch) Destroy() {
	b.destroys++
	switch b.mode {
	case "panic":
		panic(b.diagnostic)
	case "Goexit":
		runtime.Goexit()
	}
}

func TestShardedCloseRetiresSiblingsAndContextAfterInterruptedStore(t *testing.T) {
	for _, mode := range []string{"clean", "error", "panic", "Goexit", "batch panic", "batch Goexit"} {
		t.Run(mode, func(t *testing.T) {
			cause := errors.New("first store close interrupted")
			nativeMode := mode
			batchMode := ""
			if mode == "batch panic" {
				nativeMode, batchMode = "panic", "panic"
			}
			if mode == "batch Goexit" {
				nativeMode, batchMode = "Goexit", "Goexit"
			}
			first := &constructorStore{mode: nativeMode}
			if batchMode != "" {
				first.mode = "clean"
			}
			if mode != "clean" && batchMode == "" {
				first.diagnostic = cause
			}
			second := &constructorStore{}
			cfg := config.NodeHostConfig{}
			cfg.Expert.FS = vfs.NewMemFS()
			cfg.Expert.LogDB = config.GetTinyMemLogDBConfig()
			cfg.Expert.LogDB.Shards = 2
			cfg.Expert.Engine.ExecShards = 2
			calls := 0
			factory := func(config.LogDBConfig, kv.LogDBCallback, string, string, vfs.IFS) (kv.IKVStore, error) {
				calls++
				if calls == 1 {
					return first, nil
				}
				return second, nil
			}
			owner, err := OpenShardedDB(cfg, nil, []string{"/first", "/second"}, nil, false, false, factory)
			require.NoError(t, err)
			t.Cleanup(func() {
				select {
				case <-owner.stopper.ShouldStop():
				default:
					owner.stopper.Stop()
				}
				if second.closes == 0 {
					_ = second.Close()
				}
			})
			batch := &constructorBatch{mode: batchMode, diagnostic: cause}
			ctx := owner.ctxs[0].(*context)
			ctx.wb = batch
			nextBatch := &constructorBatch{}
			nextContext := owner.ctxs[1].(*context)
			nextContext.wb = nextBatch
			done := make(chan struct{})
			returned := false
			var recovered any
			var closeErr error
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				closeErr = owner.Close()
				returned = true
			}()
			<-done
			require.Equal(t, 2, calls)
			require.Equal(t, 1, first.closes)
			require.Equal(t, 1, second.closes, "remaining acquired store cannot be abandoned")
			require.Equal(t, 1, batch.destroys, "native context cleanup must follow interrupted store close")
			if batchMode == "" {
				require.Nil(t, ctx.val)
			}
			require.Equal(t, 1, nextBatch.destroys, "remaining context cannot be abandoned")
			require.Nil(t, nextContext.val)
			if mode == "clean" {
				require.True(t, returned)
				require.NoError(t, closeErr)
				require.Nil(t, recovered)
			} else if mode == "error" {
				require.True(t, returned)
				require.ErrorIs(t, closeErr, cause)
				require.Nil(t, recovered)
			} else {
				require.False(t, returned)
				if nativeMode == "panic" {
					require.Same(t, cause, recovered)
				} else {
					require.Nil(t, recovered)
				}
			}
		})
	}
}
