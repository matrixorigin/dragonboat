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

package tan

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lni/goutils/syncutil"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type syncBorrowFile struct {
	vfs.File
	block            atomic.Bool
	closes           atomic.Int32
	entered, release chan struct{}
}

func (f *syncBorrowFile) Sync() error {
	if f.block.CompareAndSwap(true, false) {
		close(f.entered)
		<-f.release
	}
	return f.File.Sync()
}
func (f *syncBorrowFile) Close() error { f.closes.Add(1); return f.File.Close() }

func TestSyncHoldsMutationLockUntilNativeBorrowCompletes(t *testing.T) {
	for _, action := range []string{"Close", "rotation"} {
		t.Run(action, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/sync-borrow", 0700))
			owner, err := open(1, 1, "owner", "/sync-borrow", &Options{FS: mem, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
			require.NoError(t, err)
			// Join unrelated initial obsolete-file work before measuring the Sync
			// borrow. Otherwise its brief mutex hold can hide a missing Sync lock.
			owner.stopper.Stop()
			owner.stopper = syncutil.NewStopper()
			file := &syncBorrowFile{File: owner.mu.logFile, entered: make(chan struct{}), release: make(chan struct{})}
			owner.mu.logFile = file
			file.block.Store(true)
			syncDone, actionDone := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(file.release) }) }
			syncStarted, actionStarted := false, false
			t.Cleanup(func() {
				release()
				if syncStarted {
					<-syncDone
				}
				if actionStarted {
					<-actionDone
				}
				if owner.closed.Load() == nil {
					_ = owner.close()
				}
			})
			var syncErr, actionErr error
			var syncPanic any
			syncStarted = true
			go func() {
				defer close(syncDone)
				defer func() { syncPanic = recover() }()
				syncErr = owner.sync()
			}()
			<-file.entered
			if owner.mu.TryLock() {
				owner.mu.Unlock()
				t.Fatal("native Sync borrow must hold the mutation lock")
			}
			started := make(chan struct{})
			actionStarted = true
			go func() {
				defer close(actionDone)
				close(started)
				if action == "Close" {
					actionErr = owner.close()
				} else {
					owner.mu.Lock()
					defer owner.mu.Unlock()
					actionErr = owner.switchToNewLog()
				}
			}()
			<-started
			require.Zero(t, file.closes.Load(), "borrowed native descriptor cannot retire while Sync is held")
			select {
			case <-actionDone:
				t.Fatal("retirement completed before active Sync borrow released")
			default:
			}
			release()
			<-syncDone
			<-actionDone
			require.Nil(t, syncPanic)
			require.NoError(t, syncErr)
			require.NoError(t, actionErr)
			require.Equal(t, int32(1), file.closes.Load())
			if action == "rotation" {
				require.NoError(t, owner.close())
			}
		})
	}
}
