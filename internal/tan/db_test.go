// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4/internal/fileutil"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func runTanTest(t *testing.T, opts *Options, tf func(t *testing.T, d *db), fs vfs.FS) {
	defer leaktest.AfterTest(t)()
	if opts == nil {
		opts = &Options{
			MaxManifestFileSize: MaxManifestFileSize,
			MaxLogFileSize:      MaxLogFileSize,
			FS:                  fs,
		}
	} else if opts.FS == nil {
		panic("fs not specified")
	}
	defer vfs.ReportLeakedFD(opts.FS, t)
	dirname := "/Users/lni/db-dir"
	require.NoError(t, fileutil.MkdirAll(dirname, opts.FS))
	db, err := open(1, 1, dirname, dirname, opts)
	require.NoError(t, err)
	defer func() {
		plog.Infof("going to close")
		db.close()
	}()
	tf(t, db)
}

func TestOpenNewDB(t *testing.T) {
	fs := vfs.NewMem()
	tf := func(t *testing.T, db *db) {
		rs, err := db.getRaftState(2, 3, 200)
		require.Equal(t, raftio.ErrNoSavedLog, err)
		require.Equal(t, raftio.RaftState{}, rs)
		require.Equal(t, uint64(0), rs.EntryCount)
		require.Equal(t, uint64(0), rs.FirstIndex)
		ss, err := db.getSnapshot(2, 3)
		require.NoError(t, err)
		require.True(t, pb.IsEmptySnapshot(ss))
		var entries []pb.Entry
		entries, size, err := db.getEntries(2, 3, entries, 0, 0, 100, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, 0, len(entries))
		require.Equal(t, uint64(0), size)
	}
	runTanTest(t, nil, tf, fs)
}

func TestBasicDBReadWrite(t *testing.T) {
	for _, testSize := range []uint64{0, 1, 1024, 16 * 1024, blockSize, blockSize * 3} {
		size := testSize
		fs := vfs.NewMem()
		tf := func(t *testing.T, db *db) {
			var cmd []byte
			if size > 0 {
				cmd = make([]byte, size)
			}
			u1 := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				State: pb.State{
					Commit: 100,
					Term:   5,
					Vote:   3,
				},
				Snapshot: pb.Snapshot{
					Index: 100,
					Term:  5,
				},
				EntriesToSave: []pb.Entry{
					{Index: 101, Term: 5, Cmd: cmd},
					{Index: 102, Term: 5, Cmd: cmd},
					{Index: 103, Term: 5, Cmd: cmd},
				},
			}
			u2 := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				State: pb.State{
					Commit: 200,
					Term:   10,
					Vote:   6,
				},
				Snapshot: pb.Snapshot{
					Index: 200,
					Term:  10,
				},
				EntriesToSave: []pb.Entry{
					{Index: 201, Term: 10, Cmd: cmd},
					{Index: 202, Term: 10, Cmd: cmd},
					{Index: 203, Term: 10, Cmd: cmd},
				},
			}
			buf := make([]byte, 1024)
			_, err := db.write(u1, buf)
			require.NoError(t, err)
			_, err = db.write(u2, buf)
			require.NoError(t, err)

			require.Equal(t, u2.State, db.mu.nodeStates.getState(2, 3))
			rs, err := db.getRaftState(2, 3, 200)
			require.NoError(t, err)
			require.Equal(t, u2.State, rs.State)
			require.Equal(t, uint64(201), rs.FirstIndex)
			require.Equal(t, uint64(3), rs.EntryCount)

			snapshot, err := db.getSnapshot(2, 3)
			require.NoError(t, err)
			require.Equal(t, u2.Snapshot, snapshot)

			var result []pb.Entry
			entries, _, err := db.getEntries(2, 3, result, 0, 201, 203, math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, 2, len(entries))
			require.Equal(t, size, uint64(len(entries[0].Cmd)))
		}
		runTanTest(t, nil, tf, fs)
	}
}

func TestEntryOverwrite(t *testing.T) {
	type entry struct {
		index uint64
		term  uint64
	}
	type entryRange struct {
		start uint64
		end   uint64
		term  uint64
	}

	tests := []struct {
		input    []entryRange
		low      uint64
		high     uint64
		expected []entry
	}{
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}},
			101, 102,
			[]entry{{101, 5}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}},
			102, 105,
			[]entry{{102, 10}, {103, 10}, {104, 10}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}},
			103, 105,
			[]entry{{103, 10}, {104, 10}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}},
			101, 105,
			[]entry{{101, 5}, {102, 10}, {103, 10}, {104, 10}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}, {100, 105, 15}},
			100, 105,
			[]entry{{100, 15}, {101, 15}, {102, 15}, {103, 15}, {104, 15}},
		},
		{
			[]entryRange{{101, 105, 5}, {102, 104, 10}, {103, 105, 15}},
			101, 105,
			[]entry{{101, 5}, {102, 10}, {103, 15}, {104, 15}},
		},
	}

	buf := make([]byte, 1024)
	for testIdx, tt := range tests {
		idx := testIdx
		input := tt.input
		expected := tt.expected
		low := tt.low
		high := tt.high
		func() {
			fs := vfs.NewMem()
			tf := func(t *testing.T, db *db) {
				for _, ir := range input {
					u := pb.Update{ShardID: 2, ReplicaID: 3}
					for j := ir.start; j <= ir.end; j++ {
						u.EntriesToSave = append(u.EntriesToSave, pb.Entry{Index: j, Term: ir.term})
					}
					_, err := db.write(u, buf)
					require.NoError(t, err)
				}
			}
			runTanTest(t, nil, tf, fs)

			tf = func(t *testing.T, db *db) {
				var result []pb.Entry
				entries, _, err := db.getEntries(2, 3, result, 0, low, high, 1024)
				require.NoError(t, err)
				require.Equal(t, len(expected), len(entries))
				for j, r := range expected {
					require.Equalf(t, r.index, entries[j].Index, "idx: %d, j: %d", idx, j)
					require.Equalf(t, r.term, entries[j].Term, "idx: %d, j: %d", idx, j)
				}
			}
			runTanTest(t, nil, tf, fs)
		}()
	}
}

func TestEmptyEntryUpdate(t *testing.T) {
	fs := vfs.NewMem()
	tf := func(t *testing.T, db *db) {
		u1 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			EntriesToSave: []pb.Entry{
				{Index: 1, Term: 5},
			},
		}
		u2 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			Snapshot: pb.Snapshot{
				Index: 1,
				Term:  5,
			},
		}
		u3 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			State: pb.State{
				Commit: 1,
				Term:   5,
			},
		}
		u4 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			EntriesToSave: []pb.Entry{
				{Index: 2, Term: 5},
			},
		}
		buf := make([]byte, 1024)
		_, err1 := db.write(u1, buf)
		require.NoError(t, err1)
		_, err2 := db.write(u2, buf)
		require.NoError(t, err2)
		_, err3 := db.write(u3, buf)
		require.NoError(t, err3)
		_, err4 := db.write(u4, buf)
		require.NoError(t, err4)
		var result []pb.Entry
		entries, _, err := db.getEntries(2, 3, result, 0, 1, 3, 10240)
		require.NoError(t, err)
		require.Equal(t, 2, len(entries))
	}
	runTanTest(t, nil, tf, fs)
}

func TestSnapshotUpdate(t *testing.T) {
	fs := vfs.NewMem()
	tf := func(t *testing.T, db *db) {
		u1 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			Snapshot: pb.Snapshot{
				Index: 100,
				Term:  5,
			},
		}
		u2 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			Snapshot: pb.Snapshot{
				Index: 90,
				Term:  5,
			},
		}
		u3 := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			Snapshot: pb.Snapshot{
				Index: 80,
				Term:  5,
			},
		}
		buf := make([]byte, 1024)
		_, err1 := db.write(u1, buf)
		require.NoError(t, err1)
		_, err2 := db.write(u2, buf)
		require.NoError(t, err2)
		_, err3 := db.write(u3, buf)
		require.NoError(t, err3)
		snapshot, err := db.getSnapshot(2, 3)
		require.NoError(t, err)
		require.Equal(t, uint64(100), snapshot.Index)
	}
	runTanTest(t, nil, tf, fs)
}

func TestLogRotation(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxLogFileSize:      1024,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	tf := func(t *testing.T, db *db) {
		logNum := db.mu.logNum
		fn := makeFilename(opts.FS, db.dirname, fileTypeLog, logNum)
		_, err := opts.FS.Stat(fn)
		require.NoError(t, err)
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			State: pb.State{
				Commit: 100,
				Term:   5,
				Vote:   3,
			},
			Snapshot: pb.Snapshot{
				Index: 100,
				Term:  5,
			},
			EntriesToSave: []pb.Entry{
				{Index: 0, Term: 5},
			},
		}
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(100); i++ {
			u.EntriesToSave[0].Index = i
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		require.NotEqual(t, logNum, db.mu.logNum)
		fn = makeFilename(opts.FS, db.dirname, fileTypeLog, db.mu.logNum)
		_, err = opts.FS.Stat(fn)
		require.NoError(t, err)
		// check we can query across multiple logs
		var result []pb.Entry
		entries, _, err := db.getEntries(2, 3, result, 0, 1, 100, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, 99, len(entries))
		for i := uint64(1); i < uint64(100); i++ {
			require.Equal(t, i, entries[i-1].Index)
		}
		// don't assume when the rotation happened, just check again to make sure the
		// db is accessible by writes
		_, err = db.write(u, buf)
		require.NoError(t, err)
	}
	runTanTest(t, opts, tf, fs)
}

func TestDBRestart(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxLogFileSize:      1024,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	var logNum fileNum
	tf := func(t *testing.T, db *db) {
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			State: pb.State{
				Commit: 100,
				Term:   5,
				Vote:   3,
			},
			EntriesToSave: []pb.Entry{
				{Index: 0, Term: 5},
			},
		}
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(100); i++ {
			u.EntriesToSave[0].Index = i
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		logNum = db.mu.logNum
	}
	runTanTest(t, opts, tf, fs)

	tf = func(t *testing.T, db *db) {
		require.NotEqual(t, logNum, db.mu.logNum)
		// this will write an entry with index 100 term 6
		// query should return this entry
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			State: pb.State{
				Commit: 100,
				Term:   5,
				Vote:   3,
			},
			Snapshot: pb.Snapshot{
				Index: 100,
				Term:  5,
			},
			EntriesToSave: []pb.Entry{
				{Index: 100, Term: 6},
			},
		}
		buf := make([]byte, 1024)
		_, err := db.write(u, buf)
		require.NoError(t, err)
		var result []pb.Entry
		entries, _, err := db.getEntries(2, 3, result, 0, 1, 100, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, 99, len(entries))
		for i := uint64(1); i < uint64(100); i++ {
			require.Equal(t, i, entries[i-1].Index)
		}
		require.Equal(t, uint64(5), entries[len(entries)-1].Term)
		snapshot, err := db.getSnapshot(2, 3)
		require.NoError(t, err)
		require.Equal(t, uint64(100), snapshot.Index)

		rs, err := db.getRaftState(2, 3, 1)
		require.NoError(t, err)
		require.Equal(t, u.State, rs.State)
		require.Equal(t, uint64(2), rs.FirstIndex)
		require.Equal(t, uint64(99), rs.EntryCount)
	}
	runTanTest(t, opts, tf, fs)
}

func TestDBConcurrentAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	defer vfs.ReportLeakedFD(fs, t)
	opts := &Options{
		MaxLogFileSize:      1,
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	dirname := "db-dir"
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	db, err := open(1, 1, dirname, dirname, opts)
	require.NoError(t, err)
	defer db.close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(1000); i++ {
			u := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				State: pb.State{
					Commit: i,
					Term:   6,
				},
				Snapshot: pb.Snapshot{
					Index: i,
					Term:  6,
				},
				EntriesToSave: []pb.Entry{
					{Index: i, Term: 6},
				},
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(1000); i++ {
			if i%uint64(10) == 0 {
				require.NoError(t, db.removeAll(2, 3))
			} else if i%uint64(11) == 0 {
				if err := db.removeEntries(2, 3, i); err != nil {
					if err != ErrNoState {
						t.Errorf("failed to remove entries %v", err)
					}
				}
			} else {
				u := pb.Update{
					ShardID:   2,
					ReplicaID: 3,
					State: pb.State{
						Commit: i,
						Term:   5,
					},
					Snapshot: pb.Snapshot{
						Index: i,
						Term:  5,
					},
					EntriesToSave: []pb.Entry{
						{Index: i, Term: 5},
					},
				}
				_, err := db.write(u, buf)
				require.NoError(t, err)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			var result []pb.Entry
			_, _, err := db.getEntries(2, 3, result, 0, 1, 100, math.MaxUint64)
			require.NoError(t, err)
			_, err = db.getSnapshot(2, 3)
			require.NoError(t, err)
			_, err = db.getRaftState(2, 3, 1)
			require.True(t, err == nil || errors.Is(err, raftio.ErrNoSavedLog))
		}
	}()
	wg.Wait()
}

func TestDBIndexIsSavedOnClose(t *testing.T) {
	fs := vfs.NewMem()
	var logNum fileNum
	var index []indexEntry
	var dirname string
	tf := func(t *testing.T, db *db) {
		buf := make([]byte, 1024)
		for i := uint64(1); i <= uint64(10); i++ {
			u := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				EntriesToSave: []pb.Entry{
					{Index: i, Term: 6},
				},
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		dirname = db.dirname
		logNum = db.mu.logNum
		index = db.mu.nodeStates.getIndex(2, 3).currEntries.entries
	}
	runTanTest(t, nil, tf, fs)
	fn := makeFilename(fs, dirname, fileTypeIndex, logNum)
	_, err := fs.Stat(fn)
	require.NoError(t, err)
	require.True(t, len(index) > 0)
}

func TestRebuildIndex(t *testing.T) {
	fs := vfs.NewMem()
	var logNum, laterLogNum fileNum
	var savedIndex index
	var snapshot indexEntry
	dirname := "/Users/lni/db-dir"
	run := func(check func(*testing.T, *db)) {
		opts := &Options{MaxManifestFileSize: MaxManifestFileSize, MaxLogFileSize: MaxLogFileSize, FS: fs}
		require.NoError(t, fileutil.MkdirAll(dirname, fs))
		owner, err := open(1, 1, dirname, dirname, opts)
		require.NoError(t, err)
		defer func() { require.NoError(t, owner.close(), "healthy fixture close must publish its index") }()
		check(t, owner)
	}
	defer vfs.ReportLeakedFD(fs, t)
	tf := func(t *testing.T, db *db) {
		dirname = db.dirname
		buf := make([]byte, 1024)
		// Two entries on each side of the compaction boundary retain the
		// prefix/suffix behavior without 1000 identical record writes.
		for i := uint64(499); i <= uint64(502); i++ {
			u := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				State:     pb.State{Term: 10, Commit: i},
				EntriesToSave: []pb.Entry{
					{Index: i, Term: 10},
				},
				Snapshot: pb.Snapshot{Index: 300, Term: 10},
			}
			// Preserve physical record-block crossing without 1000 small writes.
			if i == 499 {
				u.EntriesToSave[0].Cmd = bytes.Repeat([]byte("x"), 2*blockSize+17)
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		require.NoError(t, db.removeEntries(2, 3, 500))
		logNum = db.mu.logNum
		savedIndex = db.mu.nodeStates.getIndex(2, 3).entries
		require.Len(t, savedIndex.entries, 4)
		retained := 0
		for _, entry := range savedIndex.entries {
			require.Equal(t, logNum, entry.fileNum)
			if entry.start > 500 {
				retained++
				require.Greater(t, entry.pos, int64(blockSize), "retained suffix locators must seek beyond the first physical block")
			}
		}
		require.Equal(t, 2, retained)

		snapshot = db.mu.nodeStates.getIndex(2, 3).snapshot
		func() {
			db.mu.Lock()
			defer db.mu.Unlock()
			require.NoError(t, db.switchToNewLog())
			laterLogNum = db.mu.logNum
			node := db.mu.nodeStates.getIndex(2, 3)
			require.Empty(t, node.currEntries.entries, "new segment cannot inherit prior entries")
			require.Zero(t, node.currEntries.compactedTo, "new segment cannot inherit prior compaction record")
			require.Equal(t, uint64(500), node.entries.compactedTo, "cumulative compaction boundary must survive rotation")
		}()
	}
	run(tf)

	readSource := func() []byte {
		file, err := fs.Open(makeFilename(fs, dirname, fileTypeLog, logNum))
		require.NoError(t, err)
		defer func() { require.NoError(t, file.Close()) }()
		data, err := io.ReadAll(file)
		require.NoError(t, err)
		return data
	}
	sourceBytes := readSource()
	for _, number := range []fileNum{logNum, laterLogNum} {
		_, err := fs.Stat(makeFilename(fs, dirname, fileTypeLog, number))
		require.NoError(t, err)
		_, err = fs.Stat(makeFilename(fs, dirname, fileTypeIndex, number))
		require.NoError(t, err)
	}
	fn := makeFilename(fs, dirname, fileTypeIndex, logNum)
	require.NoError(t, fs.RemoveAll(fn))

	tf = func(t *testing.T, db *db) {
		state, err := db.getRaftState(2, 3, 500)
		require.NoError(t, err)
		require.Equal(t, raftio.RaftState{State: pb.State{Term: 10, Commit: 502}, FirstIndex: 501, EntryCount: 2}, state)
		entries, _, err := db.getEntries(2, 3, nil, 0, 501, 503, ^uint64(0))
		require.NoError(t, err)
		require.Equal(t, []pb.Entry{{Index: 501, Term: 10}, {Index: 502, Term: 10}}, entries)
		ss, err := db.getSnapshot(2, 3)
		require.NoError(t, err)
		require.Equal(t, pb.Snapshot{Index: 300, Term: 10}, ss)
		require.Equal(t, savedIndex, db.mu.nodeStates.getIndex(2, 3).entries)
		require.Equal(t, snapshot, db.mu.nodeStates.getIndex(2, 3).snapshot)
		require.Equal(t, uint64(500), db.mu.nodeStates.compactedTo(2, 3))
		require.Equal(t, sourceBytes, readSource(), "rebuilding derived index cannot alter intact source bytes")
		_, err = fs.Stat(fn)
		require.NoError(t, err, "missing historical index must be rebuilt")
		_, err = db.write(pb.Update{ShardID: 2, ReplicaID: 3, State: pb.State{Term: 10, Commit: 503}, EntriesToSave: []pb.Entry{{Index: 503, Term: 10, Cmd: []byte("after-recovery")}}}, nil)
		require.NoError(t, err)
		require.NoError(t, db.sync())
	}
	run(tf)
	run(func(t *testing.T, owner *db) {
		entries, _, err := owner.getEntries(2, 3, nil, 0, 501, 504, ^uint64(0))
		require.NoError(t, err)
		require.Equal(t, []pb.Entry{{Index: 501, Term: 10}, {Index: 502, Term: 10}, {Index: 503, Term: 10, Cmd: []byte("after-recovery")}}, entries)
		state, err := owner.getRaftState(2, 3, 500)
		require.NoError(t, err)
		require.Equal(t, raftio.RaftState{State: pb.State{Term: 10, Commit: 503}, FirstIndex: 501, EntryCount: 3}, state)
	})
}

func TestRebuildLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.Default
	defer vfs.ReportLeakedFD(fs, t)
	opts := &Options{
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	dirname := "db-dir"
	require.NoError(t, fs.RemoveAll(dirname))
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	defer func() {
		require.NoError(t, fs.RemoveAll(dirname))
	}()
	db, err := open(1, 1, dirname, dirname, opts)
	require.NoError(t, err)
	buf := make([]byte, 1024)
	for i := uint64(1); i <= uint64(20); i++ {
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			EntriesToSave: []pb.Entry{
				{Index: i, Term: 5, Cmd: make([]byte, 32)},
			},
		}
		_, err := db.write(u, buf)
		require.NoError(t, err)
	}
	logNum := db.mu.logNum
	require.NoError(t, db.close())
	logFn := makeFilename(fs, dirname, fileTypeLog, logNum)
	idxFn := makeFilename(fs, dirname, fileTypeIndex, logNum)
	lf, err := os.OpenFile(logFn, os.O_RDWR, 0755)
	require.NoError(t, err)
	fi, err := lf.Stat()
	require.NoError(t, err)
	// truncate the file, remove the index
	require.NoError(t, lf.Truncate(fi.Size()-16))
	require.NoError(t, lf.Close())
	require.NoError(t, fs.RemoveAll(idxFn))
	db, err = open(1, 1, dirname, dirname, opts)
	require.NoError(t, err)
	var result []pb.Entry
	result, _, err = db.getEntries(2, 3, result, 0, 1, 21, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, 19, len(result))
	require.Equal(t, uint64(19), result[len(result)-1].Index)
	require.NoError(t, db.close())
	lf, err = os.Open(logFn)
	require.NoError(t, err)
	fi2, err := lf.Stat()
	require.NoError(t, err)
	require.Equal(t, fi.Size()*19/20, fi2.Size())
}

func TestGetEntriesWithMaxSize(t *testing.T) {
	fs := vfs.NewMem()
	opts := &Options{
		MaxManifestFileSize: MaxManifestFileSize,
		MaxLogFileSize:      1024,
		FS:                  fs,
	}
	tf := func(t *testing.T, db *db) {
		cmd := make([]byte, 128)
		buf := make([]byte, 1024)
		for i := 0; i < 128; i++ {
			u := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				EntriesToSave: []pb.Entry{
					{Index: 1 + uint64(i), Term: 5, Cmd: cmd},
				},
			}
			_, err := db.write(u, buf)
			require.NoError(t, err)
		}
		entries, _, err := db.getEntries(2, 3, nil, 0, 1, 128, 128)
		require.NoError(t, err)
		require.Equal(t, 1, len(entries))
	}
	runTanTest(t, opts, tf, fs)
}

func TestWriteWithInvalidState(t *testing.T) {
	for _, testSize := range []uint64{0, 1, 1024, 16 * 1024, blockSize, blockSize * 3} {
		size := testSize
		fs := vfs.NewMem()
		tf := func(t *testing.T, db *db) {
			var cmd []byte
			if size > 0 {
				cmd = make([]byte, size)
			}
			u1 := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				State: pb.State{
					Commit: 100,
					Term:   5,
					Vote:   3,
				},
				EntriesToSave: []pb.Entry{
					{Index: 101, Term: 5, Cmd: cmd},
					{Index: 102, Term: 5, Cmd: cmd},
					{Index: 103, Term: 5, Cmd: cmd},
				},
			}
			u2 := pb.Update{
				ShardID:   2,
				ReplicaID: 3,
				State: pb.State{
					Commit: 0,
					Term:   0,
					Vote:   0,
				},
			}
			buf := make([]byte, 1024)
			_, err := db.write(u1, buf)
			require.NoError(t, err)
			sync, err := db.write(u2, buf)
			require.NoError(t, err)
			// sync should be false.
			require.False(t, sync)

			// state should be the same as u1, because u2 is invalid.
			require.Equal(t, u1.State, db.mu.nodeStates.getState(2, 3))
		}
		runTanTest(t, nil, tf, fs)
	}
}

func TestLoadArchivedNodeStates(t *testing.T) {
	fs := vfs.NewMem()
	tf1 := func(t *testing.T, db *db) {
		_, err := db.loadArchivedNodeStates()
		require.Error(t, err)
	}
	runTanTest(t, nil, tf1, fs)

	opts := &Options{
		MaxManifestFileSize: MaxManifestFileSize,
		MaxLogFileSize:      32,
		FS:                  fs,
		archiveIO:           newMockArchiveIO(fs, "db-dir"),
	}
	tf2 := func(t *testing.T, db *db) {
		buf := make([]byte, 1024)
		hs := pb.State{
			Term:   2,
			Vote:   3,
			Commit: 100,
		}
		for i := uint64(0); i < 10; i++ {
			e2 := pb.Entry{
				Term:  2,
				Index: i + 1,
				Type:  pb.ApplicationEntry,
				Cmd:   []byte("test data 2"),
			}
			ud := pb.Update{
				EntriesToSave: []pb.Entry{e2},
				State:         hs,
				ShardID:       1,
				ReplicaID:     1,
			}
			_, err := db.write(ud, buf)
			require.NoError(t, err)
		}
		require.NoError(t, db.removeEntries(1, 1, 100))

		timeout := time.NewTimer(time.Second * 3)
		defer timeout.Stop()
		ticker := time.NewTicker(time.Millisecond * 10)
		defer ticker.Stop()
		for {
			select {
			case <-timeout.C:
				panic("failed to get lsn by ts")

			case <-ticker.C:
				ns, err := db.loadArchivedNodeStates()
				require.NoError(t, err)
				ies, ok := ns.query(1, 1, 1, 100)
				require.True(t, ok)
				if len(ies) == 9 {
					return
				}
			}
		}
	}
	runTanTest(t, opts, tf2, fs)
}

// TestRebuildLogTornTail covers the other shape a crash leaves at the tail of
// the last log: the final chunk's header reached disk but the end of its
// payload did not, so the file keeps its length and the chunk fails its
// checksum.  Like a truncated tail, the torn record must be dropped on open.
func TestRebuildLogTornTail(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.Default
	defer vfs.ReportLeakedFD(fs, t)
	opts := &Options{
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	dirname := "db-dir-torn"
	require.NoError(t, fs.RemoveAll(dirname))
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	defer func() {
		require.NoError(t, fs.RemoveAll(dirname))
	}()
	db, err := open(1, 1, dirname, dirname, opts)
	require.NoError(t, err)
	buf := make([]byte, 1024)
	for i := uint64(1); i <= uint64(20); i++ {
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			EntriesToSave: []pb.Entry{
				{Index: i, Term: 5, Cmd: make([]byte, 32)},
			},
		}
		_, err := db.write(u, buf)
		require.NoError(t, err)
	}
	logNum := db.mu.logNum
	require.NoError(t, db.close())
	logFn := makeFilename(fs, dirname, fileTypeLog, logNum)
	idxFn := makeFilename(fs, dirname, fileTypeIndex, logNum)
	lf, err := os.OpenFile(logFn, os.O_RDWR, 0755)
	require.NoError(t, err)
	fi, err := lf.Stat()
	require.NoError(t, err)
	// Lose the last 16 bytes of the final record without shortening the file:
	// they read back as zeroes, as unwritten pages of a preallocated or
	// size-extended file do after a crash.
	_, err = lf.WriteAt(make([]byte, 16), fi.Size()-16)
	require.NoError(t, err)
	require.NoError(t, lf.Close())
	require.NoError(t, fs.RemoveAll(idxFn))

	db, err = open(1, 1, dirname, dirname, opts)
	require.NoError(t, err)
	var result []pb.Entry
	result, _, err = db.getEntries(2, 3, result, 0, 1, 21, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, 19, len(result))
	require.Equal(t, uint64(19), result[len(result)-1].Index)
	require.NoError(t, db.close())
}

// TestRebuildLogMidStreamCorruption is the counterpart of the torn tail: a
// record in the middle of the last log fails its checksum while the records
// after it are intact.  That is corruption, not a crash tail, and open must
// refuse rather than drop the durable records that follow.
func TestRebuildLogMidStreamCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.Default
	defer vfs.ReportLeakedFD(fs, t)
	opts := &Options{
		MaxManifestFileSize: MaxManifestFileSize,
		FS:                  fs,
	}
	dirname := "db-dir-midstream"
	require.NoError(t, fs.RemoveAll(dirname))
	require.NoError(t, fs.MkdirAll(dirname, 0700))
	defer func() {
		require.NoError(t, fs.RemoveAll(dirname))
	}()
	db, err := open(1, 1, dirname, dirname, opts)
	require.NoError(t, err)
	buf := make([]byte, 1024)
	for i := uint64(1); i <= uint64(20); i++ {
		u := pb.Update{
			ShardID:   2,
			ReplicaID: 3,
			EntriesToSave: []pb.Entry{
				{Index: i, Term: 5, Cmd: make([]byte, 32)},
			},
		}
		_, err := db.write(u, buf)
		require.NoError(t, err)
	}
	logNum := db.mu.logNum
	require.NoError(t, db.close())
	logFn := makeFilename(fs, dirname, fileTypeLog, logNum)
	idxFn := makeFilename(fs, dirname, fileTypeIndex, logNum)
	lf, err := os.OpenFile(logFn, os.O_RDWR, 0755)
	require.NoError(t, err)
	fi, err := lf.Stat()
	require.NoError(t, err)
	// The 20 records are the same size; flip the last payload byte of the 10th.
	pos := fi.Size()*10/20 - 1
	b := make([]byte, 1)
	_, err = lf.ReadAt(b, pos)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = lf.WriteAt(b, pos)
	require.NoError(t, err)
	require.NoError(t, lf.Close())
	require.NoError(t, fs.RemoveAll(idxFn))

	_, err = open(1, 1, dirname, dirname, opts)
	require.ErrorIs(t, err, ErrCRCMismatch)
}

// faultyTempLogFS injects failures into the temporary log rebuildLog writes,
// and passes everything else through.
type faultyTempLogFS struct {
	vfs.FS
	tempLog string
	// writeLimit fails writes to the temporary log once this many bytes have
	// been written (e.g. ENOSPC part-way through the copy); negative disables.
	writeLimit int
	failSync   bool
	closeMode  string
	t          *testing.T
	closes     int
	renames    int
}

func (fs *faultyTempLogFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil || name != fs.tempLog {
		return f, err
	}
	owned := &faultyFile{File: f, fs: fs}
	if fs.t != nil {
		fs.t.Cleanup(func() {
			if owned.closes == 0 {
				require.NoError(fs.t, owned.Close())
			}
		})
	}
	return owned, nil
}

type faultyFile struct {
	vfs.File
	fs      *faultyTempLogFS
	written int
	closes  int
}

func (f *faultyFile) Write(p []byte) (int, error) {
	if limit := f.fs.writeLimit; limit >= 0 && f.written+len(p) > limit {
		n, _ := f.File.Write(p[:limit-f.written])
		f.written += n
		return n, vfs.ErrInjected
	}
	n, err := f.File.Write(p)
	f.written += n
	return n, err
}

func (f *faultyFile) Sync() error {
	if f.fs.failSync {
		return vfs.ErrInjected
	}
	return f.File.Sync()
}

func (fs *faultyTempLogFS) Rename(old, new string) error {
	fs.renames++
	return fs.FS.Rename(old, new)
}

func (f *faultyFile) Close() error {
	f.closes++
	f.fs.closes++
	err := f.File.Close()
	switch f.fs.closeMode {
	case "error":
		return errors.Wrap(vfs.ErrInjected, "completed replay Close")
	case "panic":
		panic(vfs.ErrInjected)
	case "Goexit":
		runtime.Goexit()
	}
	return err
}

func TestRebuildLogConsumesCompletedCloseOnce(t *testing.T) {
	for _, mode := range []string{"clean", "error", "panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/replay-close", 0700))
			fs := &faultyTempLogFS{FS: mem, t: t, writeLimit: -1, closeMode: mode}
			owner, err := open(1, 1, "replay", "/replay-close", &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, owner.close()) })
			_, err = owner.write(pb.Update{ShardID: 2, ReplicaID: 3, EntriesToSave: []pb.Entry{{Term: 5, Index: 1, Cmd: []byte("retained")}}}, nil)
			require.NoError(t, err)
			require.NoError(t, owner.sync())
			fs.tempLog = makeFilename(mem, "/replay-close", fileTypeLogTemp, owner.mu.logNum)
			fs.renames = 0
			done := make(chan struct{})
			var recovered any
			returned := false
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				err = owner.rebuildLog(owner.mu.logNum)
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("replay consuming Close did not unwind")
			}
			require.Equal(t, 1, fs.closes, "completed native Close consumes descriptor even on abnormal return")
			if mode == "panic" {
				require.False(t, returned)
				require.Same(t, vfs.ErrInjected, recovered)
			} else if mode == "Goexit" {
				require.False(t, returned)
				require.Nil(t, recovered)
			} else {
				require.True(t, returned)
				require.Nil(t, recovered)
				if mode == "error" {
					require.ErrorIs(t, err, vfs.ErrInjected)
				} else {
					require.NoError(t, err)
				}
			}
			if mode == "clean" {
				require.Equal(t, 1, fs.renames)
			} else {
				require.Zero(t, fs.renames, "interrupted or failed Close must not publish copy")
			}
		})
	}
}

// Review: rebuildLog must publish the copied log only after the copy is
// complete and durable.  A write or sync failure while repairing a torn tail
// must leave the original log byte-for-byte intact, leave no temporary log,
// and let a retried open recover.
func TestRebuildLogFailureKeepsOriginal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		writeLimit int
		failSync   bool
	}{
		{"write fails at once", 0, false},
		{"write fails part-way", 100, false},
		{"sync fails", -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer leaktest.AfterTest(t)()
			fs := vfs.Default
			defer vfs.ReportLeakedFD(fs, t)
			opts := &Options{MaxManifestFileSize: MaxManifestFileSize, FS: fs}
			dirname := "db-dir-rebuild-fault"
			require.NoError(t, fs.RemoveAll(dirname))
			require.NoError(t, fs.MkdirAll(dirname, 0700))
			defer func() {
				require.NoError(t, fs.RemoveAll(dirname))
			}()
			db, err := open(1, 1, dirname, dirname, opts)
			require.NoError(t, err)
			buf := make([]byte, 1024)
			for i := uint64(1); i <= uint64(20); i++ {
				u := pb.Update{
					ShardID:       2,
					ReplicaID:     3,
					EntriesToSave: []pb.Entry{{Index: i, Term: 5, Cmd: make([]byte, 32)}},
				}
				_, err := db.write(u, buf)
				require.NoError(t, err)
			}
			logNum := db.mu.logNum
			require.NoError(t, db.close())
			logFn := makeFilename(fs, dirname, fileTypeLog, logNum)
			tmpFn := makeFilename(fs, dirname, fileTypeLogTemp, logNum)
			// A torn tail, so open repairs the log through rebuildLog.
			lf, err := os.OpenFile(logFn, os.O_RDWR, 0755)
			require.NoError(t, err)
			fi, err := lf.Stat()
			require.NoError(t, err)
			_, err = lf.WriteAt(make([]byte, 16), fi.Size()-16)
			require.NoError(t, err)
			require.NoError(t, lf.Close())
			require.NoError(t, fs.RemoveAll(makeFilename(fs, dirname, fileTypeIndex, logNum)))
			original, err := os.ReadFile(logFn)
			require.NoError(t, err)

			faulty := &faultyTempLogFS{FS: fs, t: t, tempLog: tmpFn, writeLimit: tc.writeLimit, failSync: tc.failSync}
			_, err = open(1, 1, dirname, dirname, &Options{MaxManifestFileSize: MaxManifestFileSize, FS: faulty})
			require.ErrorIs(t, err, vfs.ErrInjected)

			after, err := os.ReadFile(logFn)
			require.NoError(t, err)
			require.Equal(t, original, after, "the original log survives a failed repair")
			_, err = os.Stat(tmpFn)
			require.True(t, os.IsNotExist(err), "the partial copy is removed")

			// Retry with a healthy file system: the repair completes.
			db, err = open(1, 1, dirname, dirname, opts)
			require.NoError(t, err)
			var result []pb.Entry
			result, _, err = db.getEntries(2, 3, result, 0, 1, 21, math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, 19, len(result))
			require.NoError(t, db.close())
		})
	}
}

// A later owned generation proves the historical source was sealed. Missing
// derived indexes do not authorize truncating any record in that source.
func TestRecoveryRejectsNonTailOrSealedCorruption(t *testing.T) {
	for _, mode := range []string{"truncated final record", "final payload CRC", "newest middle zero header", "historical tail with orphan", "newest tail with orphan"} {
		t.Run(mode, func(t *testing.T) {
			fs := vfs.Default
			t.Cleanup(func() { vfs.ReportLeakedFD(fs, t) })
			dir := t.TempDir()
			require.NoError(t, fs.MkdirAll(dir, 0700))
			opts := &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 128 * 1024}
			seed, err := open(1, 1, "seed", dir, opts)
			require.NoError(t, err)
			var closeOnce sync.Once
			closeSeed := func() { closeOnce.Do(func() { require.NoError(t, seed.close()) }) }
			t.Cleanup(closeSeed)
			count := uint64(2)
			if mode == "newest middle zero header" {
				count = 3
			}
			for i := uint64(1); i <= count; i++ {
				_, err = seed.write(pb.Update{ShardID: 1, ReplicaID: 1, EntriesToSave: []pb.Entry{{Index: i, Term: 1, Cmd: []byte("sealed-payload")}}}, nil)
				require.NoError(t, err)
			}
			old := seed.mu.logNum
			if mode != "newest middle zero header" && mode != "newest tail with orphan" {
				func() { seed.mu.Lock(); defer seed.mu.Unlock(); require.NoError(t, seed.switchToNewLog()) }()
			}
			closeSeed()
			sourceName := makeFilename(fs, dir, fileTypeLog, old)
			readSource := func() []byte {
				file, err := fs.Open(sourceName)
				require.NoError(t, err)
				defer func() { require.NoError(t, file.Close()) }()
				data, err := io.ReadAll(file)
				require.NoError(t, err)
				return data
			}
			damaged := readSource()
			if mode == "newest middle zero header" {
				reader := newReader(bytes.NewReader(damaged), old)
				first, err := reader.next()
				require.NoError(t, err)
				_, err = io.Copy(io.Discard, first)
				require.NoError(t, err)
				secondOffset := int(reader.offset())
				for i := 0; i < legacyHeaderSize; i++ {
					damaged[secondOffset+i] = 0
				}
			} else if mode == "truncated final record" || strings.Contains(mode, "with orphan") {
				damaged = damaged[:len(damaged)-1]
			} else {
				damaged[len(damaged)-1] ^= 0xff
			}
			func() {
				file, err := fs.Create(sourceName)
				require.NoError(t, err)
				defer func() { require.NoError(t, file.Close()) }()
				_, err = file.Write(damaged)
				require.NoError(t, err)
				require.NoError(t, file.Sync())
			}()
			if strings.Contains(mode, "with orphan") {
				func() {
					file, err := fs.Create(makeFilename(fs, dir, fileTypeLog, old+100))
					require.NoError(t, err)
					defer func() { require.NoError(t, file.Close()) }()
					_, err = file.Write([]byte("not manifest-owned"))
					require.NoError(t, err)
				}()
			}
			indexName := makeFilename(fs, dir, fileTypeIndex, old)
			require.NoError(t, fs.Remove(indexName))
			owner, err := open(1, 1, "recovery", dir, opts)
			if owner != nil {
				t.Cleanup(func() { require.NoError(t, owner.close()) })
			}
			if mode == "newest tail with orphan" {
				require.NoError(t, err, "higher orphan cannot revoke newest owned tail repair")
				entries, _, err := owner.getEntries(1, 1, nil, 0, 1, 3, ^uint64(0))
				require.NoError(t, err)
				require.Equal(t, []pb.Entry{{Index: 1, Term: 1, Cmd: []byte("sealed-payload")}}, entries)
				return
			}
			require.Error(t, err, "sealed source corruption cannot be repaired as newest tail")
			require.Nil(t, owner)
			if mode == "newest middle zero header" {
				require.ErrorIs(t, err, ErrCorruptChunk)
			}
			require.Equal(t, damaged, readSource(), "failed recovery must preserve original corrupted evidence")
			_, statErr := fs.Stat(indexName)
			require.ErrorIs(t, statErr, os.ErrNotExist, "failed replay cannot publish derived index")
			_, statErr = fs.Stat(makeFilename(fs, dir, fileTypeLogTemp, old))
			require.ErrorIs(t, statErr, os.ErrNotExist, "rejected corruption cannot leave a replacement log")
		})
	}
}

type reverseRecoveryFS struct{ vfs.FS }

func (f reverseRecoveryFS) List(name string) ([]string, error) {
	names, err := f.FS.List(name)
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	return names, err
}

func TestMixedRecoveryPreservesChronologicalOverwriteAndCompaction(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing []int
	}{
		{"alternating", []int{0, 2}}, {"all missing", []int{0, 1, 2, 3}}, {"newest missing", []int{3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := vfs.NewMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(fs, t) })
			const dir = "/mixed-recovery"
			require.NoError(t, fs.MkdirAll(dir, 0700))
			opts := &Options{FS: reverseRecoveryFS{fs}, DisablePrealloc: true, MaxLogFileSize: 128 * 1024}
			seed, err := open(1, 1, "seed", dir, opts)
			require.NoError(t, err)
			var seedOnce sync.Once
			closeSeed := func() { seedOnce.Do(func() { require.NoError(t, seed.close()) }) }
			t.Cleanup(closeSeed)
			logs := []fileNum{seed.mu.logNum}
			write := func(u pb.Update) { _, err := seed.write(u, nil); require.NoError(t, err) }
			rotate := func() {
				func() { seed.mu.Lock(); defer seed.mu.Unlock(); require.NoError(t, seed.switchToNewLog()) }()
				logs = append(logs, seed.mu.logNum)
			}
			write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 1, Commit: 2}, Snapshot: pb.Snapshot{Index: 1, Term: 1}, EntriesToSave: []pb.Entry{{Index: 1, Term: 1, Cmd: []byte("a1")}, {Index: 2, Term: 1, Cmd: []byte("old2")}}})
			write(pb.Update{ShardID: 2, ReplicaID: 1, State: pb.State{Term: 1, Commit: 1}, Snapshot: pb.Snapshot{Index: 1, Term: 1}, EntriesToSave: []pb.Entry{{Index: 1, Term: 1, Cmd: []byte("other1")}}})
			rotate()
			write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 2, Commit: 3}, Snapshot: pb.Snapshot{Index: 2, Term: 2}, EntriesToSave: []pb.Entry{{Index: 2, Term: 2, Cmd: []byte("new2")}, {Index: 3, Term: 2, Cmd: []byte("b3")}}})
			rotate()
			require.NoError(t, seed.removeEntries(1, 1, 1)) // This segment has no entries, only a compaction record.
			rotate()
			write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 2, Commit: 4}, EntriesToSave: []pb.Entry{{Index: 4, Term: 2, Cmd: []byte("d4")}}})
			write(pb.Update{ShardID: 2, ReplicaID: 1, State: pb.State{Term: 2, Commit: 2}, EntriesToSave: []pb.Entry{{Index: 2, Term: 2, Cmd: []byte("other2")}}})
			closeSeed()
			require.Len(t, logs, 4)
			for _, i := range tc.missing {
				require.NoError(t, fs.Remove(makeFilename(fs, dir, fileTypeIndex, logs[i])))
			}
			check := func(owner *db, fresh bool) {
				expected := []pb.Entry{{Index: 2, Term: 2, Cmd: []byte("new2")}, {Index: 3, Term: 2, Cmd: []byte("b3")}, {Index: 4, Term: 2, Cmd: []byte("d4")}}
				end, commit := uint64(5), uint64(4)
				if fresh {
					expected = append(expected, pb.Entry{Index: 5, Term: 2, Cmd: []byte("fresh5")})
					end, commit = 6, 5
				}
				entries, _, err := owner.getEntriesWithMultiplexed(1, 1, nil, 0, 2, end, ^uint64(0))
				require.NoError(t, err)
				require.Equal(t, expected, entries)
				state, err := owner.getRaftState(1, 1, 1)
				require.NoError(t, err)
				require.Equal(t, raftio.RaftState{State: pb.State{Term: 2, Commit: commit}, FirstIndex: 2, EntryCount: end - 2}, state)
				snapshot, err := owner.getSnapshot(1, 1)
				require.NoError(t, err)
				require.Equal(t, pb.Snapshot{Index: 2, Term: 2}, snapshot)
				require.Equal(t, uint64(1), owner.mu.nodeStates.compactedTo(1, 1))
				entries, _, err = owner.getEntriesWithMultiplexed(2, 1, nil, 0, 1, 3, ^uint64(0))
				require.NoError(t, err)
				require.Equal(t, []pb.Entry{{Index: 1, Term: 1, Cmd: []byte("other1")}, {Index: 2, Term: 2, Cmd: []byte("other2")}}, entries)
				state, err = owner.getRaftState(2, 1, 0)
				require.NoError(t, err)
				require.Equal(t, raftio.RaftState{State: pb.State{Term: 2, Commit: 2}, FirstIndex: 1, EntryCount: 2}, state)
				snapshot, err = owner.getSnapshot(2, 1)
				require.NoError(t, err)
				require.Equal(t, pb.Snapshot{Index: 1, Term: 1}, snapshot)
			}
			run := func(fresh bool, action func(*db)) {
				owner, err := open(1, 1, "recovery", dir, opts)
				require.NoError(t, err)
				defer func() { require.NoError(t, owner.close()) }()
				check(owner, fresh)
				if action != nil {
					action(owner)
				}
			}
			run(false, func(owner *db) {
				// Decode each rebuilt disk index separately, comparing to literal
				// segment contents rather than the cumulative in-memory accumulator.
				ranges := [4][2][2]uint64{{{1, 2}, {1, 1}}, {{2, 3}, {0, 0}}, {{0, 0}, {0, 0}}, {{4, 4}, {2, 2}}}
				for _, segment := range tc.missing {
					func() {
						file, err := fs.Open(makeFilename(fs, dir, fileTypeIndex, logs[segment]))
						require.NoError(t, err)
						defer func() { require.NoError(t, file.Close()) }()
						decoded := newNodeStates()
						require.NoError(t, decoded.load(file))
						for shard := uint64(1); shard <= 2; shard++ {
							node := decoded.getIndex(shard, 1)
							span := ranges[segment][shard-1]
							if span[0] == 0 {
								require.Empty(t, node.entries.entries)
							} else {
								require.Len(t, node.entries.entries, 1)
								entry := node.entries.entries[0]
								require.Equal(t, span[0], entry.start)
								require.Equal(t, span[1], entry.end)
								require.Equal(t, logs[segment], entry.fileNum)
							}
							marker := uint64(0)
							if segment == 2 && shard == 1 {
								marker = 1
							}
							require.Equal(t, marker, node.entries.compactedTo, "compaction records belong only to their physical segment")
						}
					}()
				}
				_, err := owner.write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 2, Commit: 5}, EntriesToSave: []pb.Entry{{Index: 5, Term: 2, Cmd: []byte("fresh5")}}}, nil)
				require.NoError(t, err)
				require.NoError(t, owner.sync())
			})
			run(true, nil)
		})
	}
}
