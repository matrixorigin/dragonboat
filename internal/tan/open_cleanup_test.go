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
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type openFailureFS struct {
	vfs.FS
	t         *testing.T
	mode      string
	cause     error
	directory *observedConstructorFile
}

func (f *openFailureFS) OpenDir(name string) (vfs.File, error) {
	file, err := f.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	f.directory = &observedConstructorFile{File: file}
	f.t.Cleanup(func() {
		if f.directory.closes.Load() == 0 {
			_ = f.directory.Close()
		}
	})
	return f.directory, nil
}
func (f *openFailureFS) Stat(name string) (os.FileInfo, error) {
	if f.directory != nil {
		switch f.mode {
		case "panic":
			panic(f.cause)
		case "Goexit":
			runtime.Goexit()
		}
		return nil, f.cause
	}
	return f.FS.Stat(name)
}

func TestOpenUnwindsAcquiredDirectory(t *testing.T) {
	for _, mode := range []string{"error", "panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/open-owner", 0700))
			cause := errors.New("current file lookup failed")
			fs := &openFailureFS{FS: mem, t: t, mode: mode, cause: cause}
			done := make(chan struct{})
			var err error
			var owner *db
			var recovered any
			returned := false
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				owner, err = open(1, 1, "owner", "/open-owner", &Options{FS: fs, DisablePrealloc: true})
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("constructor unwind did not join")
			}
			require.NotNil(t, fs.directory, "directory acquisition boundary was reached")
			require.Equal(t, int32(1), fs.directory.closes.Load())
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

func TestCloseReleasesReadStateAfterCompletedDiagnostic(t *testing.T) {
	mem := vfs.NewStrictMem()
	t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
	require.NoError(t, mem.MkdirAll("/close-owner", 0700))
	owner, err := open(1, 1, "owner", "/close-owner", &Options{FS: mem, MaxLogFileSize: 64 * 1024, DisablePrealloc: true})
	require.NoError(t, err)
	var once sync.Once
	var closeErr error
	closeOwner := func() { once.Do(func() { closeErr = owner.close() }) }
	t.Cleanup(closeOwner)
	state := owner.readState.val
	require.Equal(t, int32(1), atomic.LoadInt32(&state.refcnt))
	cause := errors.New("completed log file close diagnostic")
	file := &diagnosticCloseFile{File: owner.mu.logFile, cause: cause}
	owner.mu.logFile = file
	closeOwner()
	require.ErrorIs(t, closeErr, cause)
	require.Equal(t, 1, file.closes)
	require.Zero(t, atomic.LoadInt32(&state.refcnt), "completed close must release its read-state reference despite diagnostics")
}
