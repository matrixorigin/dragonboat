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

package dragonboat_test

import (
	"errors"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type observedDirectoryLock struct {
	io.Closer
	calls atomic.Int32
}

func (l *observedDirectoryLock) Close() error { l.calls.Add(1); return l.Closer.Close() }

type observedConstructorFS struct {
	vfs.FS
	t     *testing.T
	locks []*observedDirectoryLock
}

func (f *observedConstructorFS) Lock(name string) (io.Closer, error) {
	lock, err := f.FS.Lock(name)
	if err != nil {
		return nil, err
	}
	l := &observedDirectoryLock{Closer: lock}
	f.locks = append(f.locks, l)
	f.t.Cleanup(func() {
		if l.calls.Load() == 0 {
			_ = l.Close()
		}
	})
	return l, nil
}

type constructorUnwindFactory struct {
	mode   string
	cause  error
	called bool
}

func (*constructorUnwindFactory) Name() string { return "constructor-unwind" }
func (f *constructorUnwindFactory) Create(config.NodeHostConfig, config.LogDBCallback, []string, []string) (raftio.ILogDB, error) {
	f.called = true
	switch f.mode {
	case "error":
		return nil, f.cause
	case "string panic":
		panic("constructor-unwind-literal")
	case "error panic":
		panic(f.cause)
	case "Goexit":
		runtime.Goexit()
	}
	panic("invalid unwind case")
}
func TestNodeHostConstructorUnwindOwnership(t *testing.T) {
	for _, mode := range []string{"error", "string panic", "error panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			fs := &observedConstructorFS{FS: mem, t: t}
			factory := &constructorUnwindFactory{mode: mode, cause: errors.New("constructor-acquisition")}
			cfg := config.NodeHostConfig{NodeHostDir: "/constructor-unwind", RTTMillisecond: 10, RaftAddress: "127.0.0.1:1", Expert: config.ExpertConfig{FS: fs, LogDBFactory: factory}}
			done := make(chan struct{})
			var value any
			var result error
			var returned bool
			go func() {
				defer close(done)
				defer func() { value = recover() }()
				nh, err := dragonboat.NewNodeHost(cfg)
				result = err
				returned = true
				if nh != nil {
					t.Cleanup(nh.Close)
				}
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("constructor unwind did not terminate")
			}
			require.True(t, factory.called)
			require.NotEmpty(t, fs.locks)
			t.Logf("mode=%s called=%v returned=%v recovered=%T:%v lock_closes=%d", mode, factory.called, returned, value, value, fs.locks[0].calls.Load())
			for _, lock := range fs.locks {
				require.Equal(t, int32(1), lock.calls.Load(), "owned directory lock must close before unwind completes")
			}
			switch mode {
			case "error":
				require.True(t, returned)
				require.ErrorIs(t, result, factory.cause)
				require.Nil(t, value)
			case "string panic":
				require.False(t, returned)
				require.Equal(t, "constructor-unwind-literal", value)
			case "error panic":
				require.False(t, returned)
				require.Same(t, factory.cause, value)
			case "Goexit":
				require.False(t, returned)
				require.Nil(t, value)
			}
		})
	}
}
