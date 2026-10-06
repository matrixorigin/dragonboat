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
	"bytes"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type replayFile struct {
	vfs.File
	mode                string
	cause, errorOnClose error
	reads, closes       int
	readBytes           []byte
}

func (f *replayFile) Read(p []byte) (int, error) {
	f.reads++
	switch f.mode {
	case "read error", "read and close error":
		return 0, f.cause
	case "panic":
		panic(f.cause)
	case "Goexit":
		runtime.Goexit()
	}
	n, err := f.File.Read(p)
	f.readBytes = append(f.readBytes, p[:n]...)
	return n, err
}
func (f *replayFile) Close() error {
	f.closes++
	err := f.File.Close()
	switch f.mode {
	case "close panic":
		panic(f.errorOnClose)
	case "close Goexit":
		runtime.Goexit()
	}
	return errors.Join(err, f.errorOnClose)
}

type replayFS struct {
	vfs.FS
	t                 *testing.T
	mode              string
	cause, diagnostic error
	file              *replayFile
	target            string
	creates, logOpens int
	indexOpens        int
}

func (f *replayFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if strings.HasSuffix(name, ".log") {
		f.logOpens++
	}
	if strings.HasSuffix(name, ".index") {
		f.indexOpens++
	}
	file, err := f.FS.Open(name, opts...)
	if err != nil || !strings.HasSuffix(name, ".index") || (f.target != "" && name != f.target) {
		return file, err
	}
	owned := &replayFile{File: file, mode: f.mode, cause: f.cause}
	if f.mode == "close error" || f.mode == "read and close error" {
		owned.errorOnClose = f.diagnostic
	}
	f.file = owned
	f.t.Cleanup(func() {
		if owned.closes == 0 {
			_ = owned.Close()
		}
	})
	return owned, nil
}
func (f *replayFS) Create(name string) (vfs.File, error) { f.creates++; return f.FS.Create(name) }
func TestIndexReplayRetiresAcquiredFile(t *testing.T) {
	for _, mode := range []string{"clean", "read error", "panic", "Goexit", "close error", "read and close error", "late index truncation"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/index-owner", 0700))
			seed, err := open(1, 1, "owner", "/index-owner", &Options{FS: mem, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
			require.NoError(t, err)
			var once sync.Once
			closeSeed := func() { once.Do(func() { require.NoError(t, seed.close()) }) }
			t.Cleanup(closeSeed)
			_, err = seed.write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 1, Commit: 10}, Snapshot: pb.Snapshot{Index: 3, Term: 1}, EntriesToSave: []pb.Entry{{Index: 10, Term: 1}}}, nil)
			require.NoError(t, err)
			func() { seed.mu.Lock(); defer seed.mu.Unlock(); require.NoError(t, seed.switchToNewLog()) }()
			_, err = seed.write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 2, Commit: 11}, Snapshot: pb.Snapshot{Index: 4, Term: 2}, EntriesToSave: []pb.Entry{{Index: 10, Term: 2}, {Index: 11, Term: 2}}}, nil)
			require.NoError(t, err)
			later := seed.mu.logNum
			if mode == "late index truncation" {
				// Two complete node groups are the minimum for failure after one group
				// has changed entries, state and snapshot; map order is immaterial.
				_, err = seed.write(pb.Update{ShardID: 2, ReplicaID: 1, State: pb.State{Term: 2, Commit: 11}, Snapshot: pb.Snapshot{Index: 4, Term: 2}, EntriesToSave: []pb.Entry{{Index: 10, Term: 2}, {Index: 11, Term: 2}}}, nil)
				require.NoError(t, err)
				func() { seed.mu.Lock(); defer seed.mu.Unlock(); require.NoError(t, seed.switchToNewLog()) }()
			}
			closeSeed()
			target := makeFilename(mem, "/index-owner", fileTypeIndex, later)
			if mode == "late index truncation" {
				file, err := mem.Open(target)
				require.NoError(t, err)
				data := func() []byte {
					defer func() { require.NoError(t, file.Close()) }()
					b, err := io.ReadAll(file)
					require.NoError(t, err)
					return b
				}()
				file, err = mem.Create(target)
				require.NoError(t, err)
				func() {
					defer func() { require.NoError(t, file.Close()) }()
					_, err = file.Write(data[:len(data)-1])
					require.NoError(t, err)
				}()
			}
			cause := errors.New("index read fault")
			diagnostic := errors.New("completed index close diagnostic")
			fs := &replayFS{FS: mem, t: t, mode: mode, cause: cause, diagnostic: diagnostic, target: target}
			var owner *db
			var recovered any
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				owner, err = open(1, 1, "owner", "/index-owner", &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("index replay exit did not join")
			}
			if owner != nil {
				t.Cleanup(func() { require.NoError(t, owner.close()) })
			}
			require.NotNil(t, fs.file, "persisted index acquisition reached")
			require.Equal(t, 1, fs.file.closes)
			require.Positive(t, fs.file.reads, "index read boundary reached")
			require.Zero(t, fs.logOpens, "existing index failure cannot fall back to source replay")
			require.Equal(t, 2, fs.indexOpens, "construction stops at the failing index before later segment loading")
			if mode != "clean" {
				require.Zero(t, fs.creates, "failed native index load must suppress writable log and destructor index publication")
			}
			if mode == "close error" || mode == "late index truncation" {
				decoded := newNodeStates()
				decodeErr := decoded.load(io.NopCloser(bytes.NewReader(fs.file.readBytes)))
				if mode == "close error" {
					require.NoError(t, decodeErr)
				} else {
					require.Error(t, decodeErr)
				}
				node := decoded.getIndex(1, 1)
				require.Len(t, node.entries.entries, 1)
				require.Equal(t, uint64(10), node.entries.entries[0].start)
				require.Equal(t, uint64(11), node.entries.entries[0].end)
				require.Equal(t, later, node.entries.entries[0].fileNum)
				require.Equal(t, uint64(4), node.snapshot.start)
				completeStates := 0
				for _, decodedNode := range decoded.indexes {
					if decodedNode.state.fileNum == later {
						require.Equal(t, uint64(11), decodedNode.state.start)
						completeStates++
					}
				}
				require.Equal(t, 1, completeStates, "at least one complete node group must be applied before the failure")
				if mode == "late index truncation" {
					require.Len(t, decoded.indexes, 2)
					other := decoded.getIndex(2, 1)
					require.Len(t, other.entries.entries, 1)
					require.Equal(t, uint64(10), other.entries.entries[0].start)
					require.Equal(t, uint64(11), other.entries.entries[0].end)
					require.Equal(t, later, other.entries.entries[0].fileNum)
					require.Equal(t, uint64(4), other.snapshot.start)
				}

			}
			switch mode {
			case "clean":
				require.Nil(t, recovered)
				require.True(t, returned)
				require.NoError(t, err)
				require.NotNil(t, owner)
			case "panic":
				require.Same(t, cause, recovered)
				require.False(t, returned)
				require.Nil(t, owner)
			case "Goexit":
				require.Nil(t, recovered)
				require.False(t, returned)
				require.Nil(t, owner)
			default:
				require.Nil(t, recovered)
				require.True(t, returned)
				require.Nil(t, owner)
				if mode != "close error" && mode != "late index truncation" {
					require.ErrorIs(t, err, cause)
				}
				if mode == "close error" || mode == "read and close error" {
					require.ErrorIs(t, err, diagnostic)
				}
			}
		})
	}
}

// Use the real log reader and consuming native descriptor close; no DB or
// worker fixture is needed to distinguish repairable bytes from close failure.
type logCloseDiagnosticFS struct {
	vfs.FS
	file       *replayFile
	cause      error
	target     string
	failBorrow int
	closeMode  string
	files      []*replayFile
	creates    int
}

func (f *logCloseDiagnosticFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	native, err := f.FS.Open(name, opts...)
	if err != nil || (f.target != "" && name != f.target) {
		return native, err
	}
	cause := f.cause
	if f.failBorrow != 0 && len(f.files)+1 != f.failBorrow {
		cause = nil
	}
	mode := "close error"
	if cause != nil && f.closeMode != "" {
		mode = f.closeMode
	}
	f.file = &replayFile{File: native, mode: mode, errorOnClose: cause}
	f.files = append(f.files, f.file)
	return &seekableReplayFile{replayFile: f.file, seeker: native.(io.Seeker)}, nil
}

func (f *logCloseDiagnosticFS) Create(name string) (vfs.File, error) {
	f.creates++
	return f.FS.Create(name)
}

type seekableReplayFile struct {
	*replayFile
	seeker io.Seeker
}

func (f *seekableReplayFile) Seek(offset int64, whence int) (int64, error) {
	return f.seeker.Seek(offset, whence)
}
func TestLogReadDoesNotHideCloseFailureBehindInvalidTail(t *testing.T) {
	cause := errors.New("consumed log close failed")
	update := pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 1, Commit: 1}}
	data := pb.MustMarshalTo(&update, make([]byte, update.SizeUpperLimit()))
	valid, _ := writeTestLog(t, data)
	for _, tc := range []struct {
		name        string
		data        []byte
		closeErr    error
		wantCalls   int
		invalidTail bool
	}{
		{"invalid tail", make([]byte, recyclableHeaderSize), cause, 0, true},
		{"close wraps invalid record", make([]byte, recyclableHeaderSize), errors.Join(cause, ErrInvalidChunk), 0, true},
		{"clean EOF", nil, cause, 0, false},
		{"callback stop", valid, cause, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := vfs.Default
			dir := t.TempDir()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			source, err := mem.Create(makeFilename(mem, dir, fileTypeLog, 1))
			require.NoError(t, err)
			func() {
				defer func() { require.NoError(t, source.Close()) }()
				_, err = source.Write(tc.data)
				require.NoError(t, err)
			}()
			fs := &logCloseDiagnosticFS{FS: mem, cause: tc.closeErr}
			t.Cleanup(func() {
				if fs.file != nil && fs.file.closes == 0 {
					_ = fs.file.Close()
				}
			})
			owner := &db{dirname: dir, opts: &Options{FS: fs}}
			calls := 0
			err = owner.readLog(indexEntry{fileNum: 1}, func(u pb.Update, _ int64) bool { calls++; require.Equal(t, update, u); return false })
			require.Equal(t, tc.wantCalls, calls)
			require.Equal(t, 1, fs.file.closes)
			if tc.invalidTail {
				require.ErrorIs(t, err, ErrZeroedChunk)
			}
			require.ErrorIs(t, err, cause, "consuming Close diagnostic must survive the read result")
			require.False(t, IsInvalidRecord(err), "close failure must prevent destructive repair")
		})
	}
}

func TestRecoverySourceCloseFailureSuppressesRepairPublication(t *testing.T) {
	for _, tc := range []struct {
		name   string
		borrow int
		mode   string
	}{
		{"initial replay", 1, "close error"}, {"copy replay", 2, "close error"}, {"copy Close panic", 2, "close panic"}, {"copy Close Goexit", 2, "close Goexit"},
	} {
		name, borrow := tc.name, tc.borrow
		t.Run(name, func(t *testing.T) {
			native := vfs.Default
			dir := t.TempDir()
			opts := &Options{FS: native, DisablePrealloc: true, MaxLogFileSize: 128 * 1024}
			seed, err := open(1, 1, "seed", dir, opts)
			require.NoError(t, err)
			var once sync.Once
			closeSeed := func() { once.Do(func() { require.NoError(t, seed.close()) }) }
			t.Cleanup(closeSeed)
			for i := uint64(1); i <= 2; i++ {
				_, err = seed.write(pb.Update{ShardID: 1, ReplicaID: 1, EntriesToSave: []pb.Entry{{Index: i, Term: 1, Cmd: []byte("tail")}}}, nil)
				require.NoError(t, err)
			}
			number := seed.mu.logNum
			closeSeed()
			sourceName := makeFilename(native, dir, fileTypeLog, number)
			data, err := os.ReadFile(sourceName)
			require.NoError(t, err)
			damaged := data[:len(data)-1]
			require.NoError(t, os.WriteFile(sourceName, damaged, 0600))
			indexName := makeFilename(native, dir, fileTypeIndex, number)
			require.NoError(t, native.Remove(indexName))
			cause := errors.New("consumed source Close fault")
			fs := &logCloseDiagnosticFS{FS: native, cause: cause, target: sourceName, failBorrow: borrow, closeMode: tc.mode}
			t.Cleanup(func() {
				for _, file := range fs.files {
					if file.closes == 0 {
						_ = file.Close()
					}
				}
			})
			var owner *db
			var recovered any
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				owner, err = open(1, 1, "recovery", dir, &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 128 * 1024})
				returned = true
			}()
			<-done
			if owner != nil {
				t.Cleanup(func() { require.NoError(t, owner.close()) })
			}
			if tc.mode == "close error" {
				require.True(t, returned)
				require.Nil(t, recovered)
				require.ErrorIs(t, err, cause)
				require.ErrorIs(t, err, ErrInvalidChunk)
			} else {
				require.False(t, returned)
				if tc.mode == "close panic" {
					require.Same(t, cause, recovered)
				} else {
					require.Nil(t, recovered)
				}
			}
			require.Nil(t, owner)
			require.Len(t, fs.files, borrow)
			for _, file := range fs.files {
				require.Equal(t, 1, file.closes)
			}
			require.Equal(t, borrow-1, fs.creates, "only the copy replay may acquire a temporary replacement")
			after, err := os.ReadFile(sourceName)
			require.NoError(t, err)
			require.Equal(t, damaged, after)
			_, err = native.Stat(indexName)
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = native.Stat(makeFilename(native, dir, fileTypeLogTemp, number))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}
