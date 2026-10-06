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

package tee

import (
	"errors"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/stretchr/testify/require"
	"runtime"
	"testing"
)

type closeOnlyLogDB struct {
	raftio.ILogDB
	calls int
	err   error
	mode  string
}

func (d *closeOnlyLogDB) Close() error {
	d.calls++
	switch d.mode {
	case "panic":
		panic(d.err)
	case "Goexit":
		runtime.Goexit()
	}
	return d.err
}
func TestCloseCompletesBothStoresWithDiagnostics(t *testing.T) {
	first, second := errors.New("first store close diagnostic"), errors.New("second store close diagnostic")
	for _, tc := range []struct {
		name          string
		first, second error
	}{{"clean", nil, nil}, {"first diagnostic", first, nil}, {"second diagnostic", nil, second}, {"both diagnostics", first, second}} {
		t.Run(tc.name, func(t *testing.T) {
			left, right := &closeOnlyLogDB{err: tc.first}, &closeOnlyLogDB{err: tc.second}
			owner := MakeTeeLogDB(left, right)
			err := owner.Close()
			require.Equal(t, 1, left.calls)
			require.Equal(t, 1, right.calls)
			if tc.first == nil && tc.second == nil {
				require.NoError(t, err)
			}
			if tc.first != nil {
				require.ErrorIs(t, err, tc.first)
			}
			if tc.second != nil {
				require.ErrorIs(t, err, tc.second)
			}
		})
	}
}

func TestCloseCompletesSecondStoreAfterFirstInterruption(t *testing.T) {
	for _, mode := range []string{"panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			cause := errors.New("first close interrupted")
			left, right := &closeOnlyLogDB{mode: mode, err: cause}, &closeOnlyLogDB{}
			owner := MakeTeeLogDB(left, right)
			done := make(chan struct{})
			var recovered interface{}
			returned := false
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				_ = owner.Close()
				returned = true
			}()
			<-done
			// Rescue follows the ownership oracle; it cannot make a missed Close pass.
			defer func() {
				if right.calls == 0 {
					_ = right.Close()
				}
			}()
			require.Equal(t, 1, left.calls)
			require.Equal(t, 1, right.calls)
			require.False(t, returned)
			if mode == "panic" {
				require.Same(t, cause, recovered)
			} else {
				require.Nil(t, recovered)
			}
		})
	}
}
