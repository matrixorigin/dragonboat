// Copyright 2026 Matrix Origin
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
	"fmt"
	"math"
	"testing"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/logdb"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

// Bound repeated opens, not elapsed time: the broken implementation must fail
// deterministically without leaving a spinning reader goroutine behind.
type readProgressFS struct {
	vfs.FS
	filename string
	opens    int
	limit    int
}

func (fs *readProgressFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if name == fs.filename {
		fs.opens++
		if fs.opens > fs.limit {
			return nil, fmt.Errorf("test read-open limit exceeded")
		}
	}
	return fs.FS.Open(name, opts...)
}

func TestIndexedReadWithoutProgress(t *testing.T) {
	for _, tc := range []struct {
		name      string
		low       uint64
		stateOnly bool
		opens     int
	}{
		{"index_at_eof", 2, false, 1},
		{"valid_prefix_then_eof", 1, false, 2},
		{"state_only_at_indexed_position", 2, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(leaktest.AfterTest(t))
			fs := &readProgressFS{FS: vfs.NewMem(), limit: tc.opens}
			t.Cleanup(func() { vfs.ReportLeakedFD(fs.FS, t) })
			cfg := config.NodeHostConfig{Expert: config.ExpertConfig{FS: fs}}
			require.NoError(t, cfg.Prepare())
			dirs := []string{"read-progress"}
			ldb, err := CreateTan(cfg, nil, dirs, nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				if ldb != nil {
					require.NoError(t, ldb.Close())
				}
			})
			update := pb.Update{
				ShardID: 1, ReplicaID: 1,
				State:         pb.State{Term: 1, Commit: 2},
				EntriesToSave: []pb.Entry{{Index: 1, Term: 1, Cmd: []byte("durable")}},
			}
			require.NoError(t, ldb.SaveRaftState([]pb.Update{update}, 1))
			d, err := ldb.getDB(1, 1)
			require.NoError(t, err)
			logNum, offset := d.mu.logNum, d.mu.offset
			filename := makeFilename(fs, d.dirname, fileTypeLog, logNum)
			// Model a persisted index claiming an entry that the log does not
			// contain. This injects the reported bad input; it does not claim
			// to explain how the original missing bytes were produced.
			update.EntriesToSave = []pb.Entry{{Index: 2, Term: 1}}
			d.updateIndex(update, offset, logNum)
			if tc.stateOnly {
				update.EntriesToSave = nil
				update.State.Term = 2
				require.NoError(t, ldb.SaveRaftState([]pb.Update{update}, 1))
			}
			require.NoError(t, ldb.Close())
			ldb = nil
			stat, err := fs.Stat(filename)
			require.NoError(t, err)
			if !tc.stateOnly {
				require.Equal(t, offset, stat.Size())
			}
			// Exercise the persisted index and the public LogDB boundary after
			// reopening, not just an in-memory callback.
			ldb, err = CreateTan(cfg, nil, dirs, nil)
			require.NoError(t, err)
			fs.filename = filename
			entries, size, err := ldb.IterateEntries(nil, 0, 1, 1, tc.low, 3, math.MaxUint64)
			require.Error(t, err)
			require.Contains(t, err.Error(), "no progress")
			require.Contains(t, err.Error(), "index 2")
			require.Nil(t, entries, "a valid prefix must not hide the missing entry")
			require.Zero(t, size)
			require.Equal(t, tc.opens, fs.opens)
			fs.opens = 0
			reader := logdb.NewLogReader(1, 1, ldb)
			reader.SetRange(1, 2)
			entries, err = reader.Entries(tc.low, 3, math.MaxUint64)
			require.Error(t, err)
			require.Contains(t, err.Error(), "no progress")
			require.Nil(t, entries)
			require.Equal(t, tc.opens, fs.opens)
			fs.filename = ""
			// The failure must not damage readable entries or leave Close blocked.
			entries, _, err = ldb.IterateEntries(nil, 0, 1, 1, 1, 2, math.MaxUint64)
			require.NoError(t, err)
			require.Equal(t, []pb.Entry{{Index: 1, Term: 1, Cmd: []byte("durable")}}, entries)
			// A size cap can legitimately stop before the missing record and
			// before advancing passed. Do not turn this into a storage error.
			entries, _, err = ldb.IterateEntries(nil, 0, 1, 1, 1, 3, 1)
			require.NoError(t, err)
			require.Equal(t, []pb.Entry{{Index: 1, Term: 1, Cmd: []byte("durable")}}, entries)
		})
	}
}
