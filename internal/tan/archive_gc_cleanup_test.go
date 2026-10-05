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
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type gcTempFile struct {
	vfs.File
	mode   string
	cause  error
	closes int
	synced bool
}

func (f *gcTempFile) Write(p []byte) (int, error) {
	switch f.mode {
	case "copy error":
		return 0, f.cause
	case "copy panic":
		panic(f.cause)
	case "copy Goexit":
		runtime.Goexit()
	}
	return f.File.Write(p)
}
func (f *gcTempFile) Sync() error {
	if f.mode == "sync error" {
		return f.cause
	}
	f.synced = true
	return f.File.Sync()
}
func (f *gcTempFile) Close() error {
	f.closes++
	err := f.File.Close()
	if f.mode == "close diagnostic" {
		return errors.Join(err, f.cause)
	}
	return err
}

type gcTempFS struct {
	vfs.FS
	t              *testing.T
	mode           string
	cause          error
	source         *gcSourceFile
	temp           *gcTempFile
	renames        int
	closedAtRename int
	syncedAtRename bool
}

type gcSourceFile struct {
	vfs.File
	cause  error
	closes int
}

func (f *gcSourceFile) Close() error {
	f.closes++
	return errors.Join(f.File.Close(), f.cause)
}

func (f *gcTempFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.Open(name, opts...)
	if err != nil || f.mode != "source close diagnostic" || name != "/gc-owner/ARCHIVE" {
		return file, err
	}
	f.source = &gcSourceFile{File: file, cause: f.cause}
	return f.source, nil
}

func (f *gcTempFS) Create(name string) (vfs.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	if name != "/gc-owner/ARCHIVE.tmp" {
		return file, nil
	}
	f.temp = &gcTempFile{File: file, mode: f.mode, cause: f.cause}
	owned := f.temp
	f.t.Cleanup(func() {
		if owned.closes == 0 {
			_ = owned.Close()
		}
	})
	return owned, nil
}
func (f *gcTempFS) Rename(old, new string) error {
	if f.temp != nil {
		f.renames++
		f.closedAtRename = f.temp.closes
		f.syncedAtRename = f.temp.synced
		if f.mode == "rename error" {
			return f.cause
		}
	}
	return f.FS.Rename(old, new)
}
func readArchiveBytes(t *testing.T, fs vfs.FS) []byte {
	t.Helper()
	file, err := fs.Open("/gc-owner/ARCHIVE")
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	return data
}
func TestGCClosesCopyBeforePublishing(t *testing.T) {
	for _, mode := range []string{"clean", "source close diagnostic", "copy error", "sync error", "close diagnostic", "rename error", "copy panic", "copy Goexit"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/gc-owner", 0700))
			cause := errors.New("archive copy fault")
			fs := &gcTempFS{FS: mem, t: t, mode: mode, cause: cause}
			a := &archiver{ctx: context.Background(), recorder: newArchiveRecorder("/gc-owner", fs)}
			epoch := time.Unix(100, 0)
			require.NoError(t, a.recorder.append(config.RecordItem{FileNum: 1, TS: epoch, FirstLsn: 1}))
			require.NoError(t, a.recorder.append(config.RecordItem{FileNum: 2, TS: epoch.Add(time.Hour), FirstLsn: 2}))
			original := readArchiveBytes(t, mem)
			done := make(chan struct{})
			var recovered any
			returned := false
			var candidates []string
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				candidates = a.gc(epoch.Add(time.Second))
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("GC unwind did not join")
			}
			require.NotNil(t, fs.temp, "temporary copy acquisition reached")
			require.Equal(t, 1, fs.temp.closes)
			after := readArchiveBytes(t, mem)
			if mode == "clean" || mode == "source close diagnostic" {
				require.Equal(t, []string{"/gc-owner/000001.log", "/gc-owner/000001.index"}, candidates)
				require.Equal(t, original[len(original)/2:], after, "only expired first record removed")
				require.Equal(t, 1, fs.renames)
				require.Equal(t, 1, fs.closedAtRename)
				require.True(t, fs.syncedAtRename)
				if mode == "source close diagnostic" {
					require.NotNil(t, fs.source)
					require.Equal(t, 1, fs.source.closes)
					require.Nil(t, a.recorder.mu.file)
					require.NoError(t, a.recorder.append(config.RecordItem{FileNum: 3, TS: epoch.Add(2 * time.Hour), FirstLsn: 3}))
					require.Len(t, readArchiveBytes(t, mem), 48)
				}
			} else {
				require.Empty(t, candidates)
				require.Equal(t, original, after, "failed copy must preserve original archive")
			}
			switch mode {
			case "copy panic":
				require.Same(t, cause, recovered)
				require.False(t, returned)
			case "copy Goexit":
				require.Nil(t, recovered)
				require.False(t, returned)
			default:
				require.Nil(t, recovered)
				require.True(t, returned)
			}
		})
	}
}
