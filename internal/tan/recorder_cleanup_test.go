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
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type diagnosticCloseFile struct {
	vfs.File
	cause  error
	closes int
}

func (f *diagnosticCloseFile) Close() error { f.closes++; return errors.Join(f.File.Close(), f.cause) }

type recorderFailureFS struct {
	vfs.FS
	mode  string
	cause error
	file  *diagnosticCloseFile
}

func (f *recorderFailureFS) OpenForAppend(name string) (vfs.File, error) {
	switch f.mode {
	case "open error":
		return nil, f.cause
	case "open panic":
		panic(f.cause)
	case "open Goexit":
		runtime.Goexit()
	case "mkdir error", "create error":
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}
	file, err := f.FS.OpenForAppend(name)
	if err != nil {
		return nil, err
	}
	var diagnostic error
	if f.mode == "close diagnostic" {
		diagnostic = f.cause
	}
	f.file = &diagnosticCloseFile{File: file, cause: diagnostic}
	return f.file, nil
}
func (f *recorderFailureFS) MkdirAll(name string, mode os.FileMode) error {
	if f.mode == "mkdir error" {
		return f.cause
	}
	return f.FS.MkdirAll(name, mode)
}
func (f *recorderFailureFS) Create(name string) (vfs.File, error) {
	if f.mode == "create error" {
		return nil, f.cause
	}
	return f.FS.Create(name)
}
func TestRecorderReleasesOwnershipOnExit(t *testing.T) {
	for _, mode := range []string{"open error", "mkdir error", "create error", "open panic", "open Goexit", "callback error", "callback panic", "callback Goexit", "close diagnostic"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/recorder", 0700))
			seed, err := mem.Create("/recorder/ARCHIVE")
			require.NoError(t, err)
			require.NoError(t, seed.Close())
			cause := errors.New("recorder fault")
			fs := &recorderFailureFS{FS: mem, mode: mode, cause: cause}
			r := newArchiveRecorder("/recorder", fs)
			var recovered any
			var returned bool
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				err = r.withFile(typeWrite, func() error {
					switch mode {
					case "callback error":
						return cause
					case "callback panic":
						panic(cause)
					case "callback Goexit":
						runtime.Goexit()
					}
					return nil
				})
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("recorder exit did not join")
			}
			available := r.mu.TryLock()
			// Rescue the old implementation's retained lock only after observing the oracle.
			r.mu.Unlock()
			require.True(t, available, "failed operation must release recorder lock")
			require.Nil(t, r.mu.file)
			if fs.file != nil {
				require.Equal(t, 1, fs.file.closes)
			}
			switch mode {
			case "open panic", "callback panic":
				require.Same(t, cause, recovered)
				require.False(t, returned)
			case "open Goexit", "callback Goexit":
				require.Nil(t, recovered)
				require.False(t, returned)
			default:
				require.Nil(t, recovered)
				require.True(t, returned)
				require.ErrorIs(t, err, cause)
			}
			fs.mode = "healthy"
			require.NoError(t, r.withFile(typeWrite, func() error { return nil }), "next operation remains usable")
		})
	}
}
