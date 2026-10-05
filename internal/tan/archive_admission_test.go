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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type countingArchiveDelete struct {
	config.ArchiveIO
	calls atomic.Int32
}

func (a *countingArchiveDelete) Delete(context.Context, string, ...string) error {
	a.calls.Add(1)
	return nil
}
func TestClosedDatabaseRejectsGCBeforePublication(t *testing.T) {
	mem := vfs.NewStrictMem()
	t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
	require.NoError(t, mem.MkdirAll("/archive-admission", 0700))
	aio := &countingArchiveDelete{}
	owner, err := open(1, 1, "owner", "/archive-admission", &Options{FS: mem, archiveIO: aio, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			closed = true
			require.NoError(t, owner.close())
		}
	})
	now := time.Now()
	require.NoError(t, owner.archiver.recorder.append(config.RecordItem{FileNum: 98, TS: now.Add(-time.Hour), FirstLsn: 98}))
	require.NoError(t, owner.archiver.recorder.append(config.RecordItem{FileNum: 99, TS: now.Add(2 * time.Hour), FirstLsn: 99}))
	closed = true
	require.NoError(t, owner.close())
	read := func() []byte {
		f, err := mem.Open("/archive-admission/ARCHIVE")
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		b, err := io.ReadAll(f)
		require.NoError(t, err)
		return b
	}
	before := read()
	require.GreaterOrEqual(t, len(before), 48)
	owner.gcArchive(now.Add(time.Hour))
	require.Equal(t, before, read(), "rejected job must not remove archive metadata")
	require.Zero(t, aio.calls.Load(), "rejected job must not call remote Delete")
}
