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
	"context"
	stderrors "errors"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/lni/dragonboat/v4/config"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/goutils/syncutil"
)

// open opens the tan db located in the folder called dirname.
func open(shardID, replicaID uint64, name string, dirname string, opts *Options) (_ *db, err error) {
	opts = opts.EnsureDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	d := &db{
		ctx:              ctx,
		cancel:           cancel,
		name:             name,
		closedCh:         make(chan struct{}),
		opts:             opts,
		dirname:          dirname,
		deleteObsoleteCh: make(chan struct{}, 1),
		stopper:          syncutil.NewStopper(),
	}
	completed := false
	defer func() {
		if !completed {
			if cleanupErr := d.close(); cleanupErr != nil {
				if err != nil {
					err = stderrors.Join(err, cleanupErr)
				} else {
					plog.Errorf("Tan constructor rollback: %v", cleanupErr)
				}
			}
		}
	}()
	d.archiver = newArchiver(ctx, opts.archiveIO, dirname, opts.FS)
	d.mu.versions = &versionSet{}
	d.mu.nodeStates = newNodeStates()

	d.mu.Lock()
	defer d.mu.Unlock()

	d.dataDir, err = opts.FS.OpenDir(dirname)
	if err != nil {
		return nil, err
	}
	currentName := makeFilename(opts.FS, dirname, fileTypeCurrent, 0)
	if _, err := opts.FS.Stat(currentName); oserror.IsNotExist(err) {
		// Create the DB if it did not already exist.
		plog.Infof("%s creating a new tan db", d.id())
		if err := d.mu.versions.create(dirname, opts, d.dataDir, &d.mu.Mutex); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, errors.Wrapf(err, "tan: database %q", dirname)
	} else {
		// Load the version set.
		plog.Infof("%s loading an existing tan db", d.id())
		if err := d.mu.versions.load(dirname, opts, &d.mu.Mutex); err != nil {
			return nil, err
		}
		if err := d.mu.versions.currentVersion().checkConsistency(dirname, opts.FS); err != nil {
			return nil, err
		}
	}

	ls, err := opts.FS.List(d.dirname)
	if err != nil {
		return nil, err
	}
	plog.Infof("%s on disk files %v", d.id(), ls)
	currentVersion := d.mu.versions.currentVersion()
	for _, filename := range ls {
		ft, fn, ok := parseFilename(opts.FS, filename)
		if !ok {
			continue
		}
		if d.mu.versions.nextFileNum <= fn {
			d.mu.versions.nextFileNum = fn + 1
		}
		switch ft {
		case fileTypeLogTemp, fileTypeBootstrapTemp, fileTypeIndexTemp, fileTypeTemp:
			if err := opts.FS.Remove(opts.FS.PathJoin(dirname, filename)); err != nil {
				return nil, err
			}
		}
	}
	logs := make([]fileNum, 0, len(currentVersion.files))
	for number := range currentVersion.files {
		logs = append(logs, number)
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i] < logs[j] })
	for i, number := range logs {
		if err := func() (err error) {
			file, err := opts.FS.Open(makeFilename(opts.FS, dirname, fileTypeIndex, number))
			if oserror.IsNotExist(err) {
				return d.rebuildLogAndIndex(number, i == len(logs)-1)
			}
			if err != nil {
				return err
			}
			defer func() { err = stderrors.Join(err, file.Close()) }()
			return d.mu.nodeStates.load(file)
		}(); err != nil {
			return nil, err
		}
	}

	if err := d.createNewLog(); err != nil {
		return nil, err
	}

	// indexes are populated when d.mu.state.load() is called above
	for _, index := range d.mu.nodeStates.indexes {
		if index.entries.compactedTo > 0 {
			if err := d.compactionLocked(index); err != nil {
				return nil, err
			}
		}
	}

	d.stopper.RunWorker(func() {
		d.deleteObsoleteWorkerMain()
	})
	d.stopper.RunWorker(func() {
		d.startArchiver()
	})
	d.scanObsoleteFiles(ls)
	d.notifyDeleteObsoleteWorker()
	completed = true
	return d, nil
}

func (d *db) id() string {
	return d.name
}

func (d *db) createNewLog() (err error) {
	if err := d.checkWritableLocked(); err != nil {
		return err
	}
	logNum := d.mu.versions.getNextFileNum()
	logName := makeFilename(d.opts.FS, d.dirname, fileTypeLog, logNum)
	logFile, err := d.opts.FS.Create(logName)
	if err != nil {
		return err
	}
	owned, manifestAttempted := true, false
	defer func() {
		if owned && !manifestAttempted {
			err = stderrors.Join(err, d.opts.FS.Remove(logName))
		}
	}()
	defer func() {
		if owned {
			err = stderrors.Join(err, logFile.Close())
		}
	}()
	if err := prealloc(logFile, d.opts.MaxLogFileSize+indexBlockSize, d.opts.DisablePrealloc); err != nil {
		return err
	}
	if err := logFile.Sync(); err != nil {
		return err
	}
	if err := d.dataDir.Sync(); err != nil {
		return err
	}
	nextWriter := newWriter(logFile)
	oldFile := d.mu.logFile
	if oldFile != nil {
		if err := d.sealCurrentLogLocked(); err != nil {
			return err
		}
		if err := d.saveIndex(); err != nil {
			return err
		}
	}
	ve := versionEdit{newFiles: []newFileEntry{{meta: &fileMetadata{fileNum: logNum}}}}
	manifestAttempted = true
	if err := d.applyVersionEditLocked(&ve); err != nil {
		return err
	}
	d.mu.logFile, d.mu.logWriter = logFile, nextWriter
	d.mu.logNum, d.mu.offset = logNum, 0
	owned = false
	if oldFile != nil {
		defer func() { err = stderrors.Join(err, oldFile.Close()) }()
	}
	d.mu.nodeStates.retireCurrentEntries()
	d.updateReadStateLocked(nil)
	d.archiver.addItem(config.RecordItem{FileNum: uint64(logNum), TS: time.Now(), FirstLsn: d.mu.lsn + 1})
	return nil
}

func (d *db) rebuildLogAndIndex(logNum fileNum, allowTailRepair bool) (err error) {
	plog.Infof("%s rebuildLogAndIndex, logNum %d", d.id(), logNum)
	f := func(u pb.Update, offset int64) bool {
		d.updateIndex(u, offset, logNum)
		return true
	}
	if err := d.readLog(indexEntry{fileNum: logNum}, f); err != nil {
		if !allowTailRepair || !IsInvalidRecord(err) {
			return err
		}
		if err := d.rebuildLog(logNum); err != nil {
			return err
		}
	} else {
		// Valid bytes may still be an unsynced suffix from the preceding run.
		// The rebuilt index must not become durable before its source log.
		if err := func() (err error) {
			name := makeFilename(d.opts.FS, d.dirname, fileTypeLog, logNum)
			file, err := d.opts.FS.Open(name)
			if err != nil {
				return err
			}
			defer func() { err = stderrors.Join(err, file.Close()) }()
			return file.Sync()
		}(); err != nil {
			return err
		}
	}
	if err := d.mu.nodeStates.save(d.dirname, d.dataDir, logNum, d.opts.FS); err != nil {
		return err
	}
	d.mu.nodeStates.retireCurrentEntries()
	return nil
}

func (d *db) rebuildLog(logNum fileNum) (err error) {
	// it is possible to have the last log file to contain a corrupted chunk or
	// block on the tail, e.g. power got cut after partially written chunk or
	// block. for those situations, the log file itself is totally fine, we just
	// need to remove the final chunk or block.
	// in theory, we should be able to just truncate the log file to the last
	// reported offset. however, for simplicity, let's just copy the log and skip
	// the last broken chunk or block.
	//
	// The copy replaces the original log only once it is complete and durable:
	// every record copied, the writer finished, the file synced and closed. Any
	// failure before that keeps the original log untouched, removes the copy
	// and returns the error, so open can simply be retried. (open also removes
	// a temporary log left behind by a crash during the copy.)
	fn := makeFilename(d.opts.FS, d.dirname, fileTypeLogTemp, logNum)
	ln := makeFilename(d.opts.FS, d.dirname, fileTypeLog, logNum)
	f, err := d.opts.FS.Create(fn)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if published {
			return
		}
		if f != nil {
			err = stderrors.Join(err, f.Close())
		}
		err = stderrors.Join(err, d.opts.FS.Remove(fn))
	}()
	w := newWriter(f)
	buf := make([]byte, defaultBufferSize)
	var herr error
	var newOffset int64
	h := func(u pb.Update, offset int64) bool {
		sz := u.SizeUpperLimit()
		if sz > len(buf) {
			buf = make([]byte, sz)
		}
		data := pb.MustMarshalTo(&u, buf)
		updatedOffset, err := w.writeRecord(data)
		if err != nil {
			herr = err
			return false
		}
		if newOffset != offset {
			plog.Panicf("offset changed, %d, %d", offset, newOffset)
		}
		newOffset = updatedOffset
		return true
	}
	if err := d.readLog(indexEntry{fileNum: logNum}, h); err != nil {
		if !IsInvalidRecord(err) {
			return err
		}
	}
	if herr != nil {
		return herr
	}
	if err := w.close(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	closing := f
	f = nil
	cerr := closing.Close()
	if cerr != nil {
		return cerr
	}
	if err := d.opts.FS.Rename(fn, ln); err != nil {
		return err
	}
	published = true
	return d.dataDir.Sync()
}

func (d *db) saveIndex() (err error) {
	completed := false
	defer func() { d.finishPersistenceLocked(err, completed) }()
	err = d.mu.nodeStates.save(d.dirname, d.dataDir, d.mu.logNum, d.opts.FS)
	completed = true
	return err
}

func (d *db) close() (err error) {
	d.cancel()
	d.stopper.Stop()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	d.closed.Store(errors.WithStack(ErrClosed))
	close(d.closedCh)

	// Arm each remaining owner independently before persistence callbacks.
	if d.readState.val != nil {
		defer d.readState.val.unrefLocked()
	}
	if d.dataDir != nil {
		defer func() {
			file := d.dataDir
			d.dataDir = nil
			err = stderrors.Join(err, file.Close())
		}()
	}
	if d.mu.logFile != nil {
		defer func() {
			file := d.mu.logFile
			d.mu.logFile, d.mu.logWriter = nil, nil
			err = stderrors.Join(err, file.Close())
		}()
	}
	if d.mu.versions != nil {
		defer func() { err = stderrors.Join(err, d.mu.versions.close()) }()
	}
	if d.mu.persistenceErr != nil {
		return d.mu.persistenceErr
	}
	if d.mu.logWriter != nil {
		if err := d.sealCurrentLogLocked(); err != nil {
			return err
		}
	}
	if d.readState.val != nil {
		return d.saveIndex()
	}
	return nil
}
