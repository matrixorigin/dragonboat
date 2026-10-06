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
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type acquisitionSyncFS struct {
	vfs.FS
	t         *testing.T
	mode      string
	cause     error
	armed     bool
	logCount  int
	failAfter int
	log       *observedConstructorFile
}
type acquisitionSyncDir struct {
	vfs.File
	fs     *acquisitionSyncFS
	closes int
}

func (d *acquisitionSyncDir) Close() error { d.closes++; return d.File.Close() }
func (d *acquisitionSyncDir) Sync() error {
	if d.fs.armed && d.fs.log != nil && d.fs.logCount >= d.fs.failAfter {
		switch d.fs.mode {
		case "panic":
			panic(d.fs.cause)
		case "Goexit":
			runtime.Goexit()
		}
		return d.fs.cause
	}
	return d.File.Sync()
}
func (f *acquisitionSyncFS) OpenDir(name string) (vfs.File, error) {
	file, err := f.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	owned := &acquisitionSyncDir{File: file, fs: f}
	f.t.Cleanup(func() {
		if owned.closes == 0 {
			_ = owned.Close()
		}
	})
	return owned, nil
}
func (f *acquisitionSyncFS) Create(name string) (vfs.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	owned := &observedConstructorFile{File: file}
	f.t.Cleanup(func() {
		if owned.closes.Load() == 0 {
			_ = owned.Close()
		}
	})
	kind, _, ok := parseFilename(f.FS, name)
	if ok && kind == fileTypeLog {
		f.logCount++
		f.log = owned
	}
	return owned, nil
}
func TestOpenRetiresLogAfterAcquiredSyncFailure(t *testing.T) {
	for _, mode := range []string{"error", "panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/log-acquisition", 0700))
			cause := errors.New("acquired log directory sync failed")
			fs := &acquisitionSyncFS{FS: mem, t: t, mode: mode, cause: cause, armed: true, failAfter: 1}
			done := make(chan struct{})
			var owner *db
			var err error
			var recovered any
			returned := false
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				owner, err = open(1, 1, "owner", "/log-acquisition", &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("constructor unwind did not join")
			}
			require.NotNil(t, fs.log, "log acquisition boundary reached")
			require.Equal(t, int32(1), fs.log.closes.Load(), "acquired log must be retired before unwind completes")
			require.Nil(t, owner)
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
			}
		})
	}
}

func TestFailedLogRotationRetiresEachAcquiredHandle(t *testing.T) {
	mem := vfs.NewStrictMem()
	t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
	require.NoError(t, mem.MkdirAll("/log-rotation", 0700))
	cause := errors.New("new log directory sync failed")
	fs := &acquisitionSyncFS{FS: mem, t: t, mode: "error", cause: cause, failAfter: 2}
	owner, err := open(1, 1, "owner", "/log-rotation", &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
	require.NoError(t, err)
	var once sync.Once
	var closePanic any
	closeOwner := func() {
		once.Do(func() { defer func() { closePanic = recover(); t.Log("close panic", closePanic) }(); _ = owner.close() })
	}
	t.Cleanup(closeOwner)
	old := fs.log
	fs.armed = true
	owner.mu.Lock()
	err = owner.switchToNewLog()
	owner.mu.Unlock()
	fs.armed = false
	require.ErrorIs(t, err, cause)
	next := fs.log
	require.Same(t, old, owner.mu.logFile, "failed preparation must not transfer published ownership")
	t.Logf("old=%p next=%p published=%p", old, next, owner.mu.logFile)
	require.NotSame(t, old, next, "replacement file acquisition reached")
	require.Equal(t, int32(0), old.closes.Load(), "preparation failure retains old descriptor")
	require.Equal(t, int32(1), next.closes.Load())
	// Preparation did not seal the published segment, so removing the fault
	// must permit a normal retry through the same owner and transition path.
	owner.mu.Lock()
	retryErr := owner.switchToNewLog()
	owner.mu.Unlock()
	require.NoError(t, retryErr)
	published := fs.log
	require.NotSame(t, old, published)
	require.NotSame(t, next, published)
	require.Same(t, published, owner.mu.logFile)
	require.Equal(t, int32(1), old.closes.Load(), "successful retry retires the original descriptor once")
	require.Equal(t, int32(0), published.closes.Load())
	closeOwner()
	require.Nil(t, closePanic, "normal Close must not flush through the retired old file")
	require.Equal(t, int32(1), old.closes.Load(), "retired old handle must not remain owned for a second close")
	require.Equal(t, int32(1), next.closes.Load())
	require.Equal(t, int32(1), published.closes.Load())
}
