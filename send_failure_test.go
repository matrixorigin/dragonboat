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

package dragonboat

import (
	"testing"

	pb "github.com/lni/dragonboat/v4/raftpb"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestPendingProposalSendFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		uncertain bool
	}{
		{name: "not sent"},
		{name: "delivery unknown", uncertain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pending, _ := getPendingProposal(false)
			request, err := pending.propose(getBlankTestSession(), []byte("payload"), 100)
			if err != nil {
				t.Fatal(err)
			}
			pending.sendFailed(request.clientID, request.seriesID, request.key+1, tc.uncertain)
			select {
			case <-request.ResultC():
				t.Fatal("unrelated key completed the proposal")
			default:
			}
			pending.sendFailed(request.clientID, request.seriesID, request.key, tc.uncertain)
			result := <-request.ResultC()
			if tc.uncertain && !result.Timeout() || !tc.uncertain && !result.Dropped() {
				t.Fatalf("unexpected send failure result: %v", result)
			}
			if countPendingProposal(pending) != 0 {
				t.Fatal("failed proposal remains pending")
			}
			pending.applied(request.clientID, request.seriesID, request.key, sm.Result{}, false)
			pending.sendFailed(request.clientID, request.seriesID, request.key, tc.uncertain)
			pending.close()
			select {
			case <-request.ResultC():
				t.Fatal("proposal completed twice")
			default:
			}
		})
	}
}

func TestMessageHandlerReportsOnlyLocalForwardedProposal(t *testing.T) {
	pending, queue := getPendingProposal(false)
	request, err := pending.propose(getBlankTestSession(), []byte("payload"), 100)
	if err != nil {
		t.Fatal(err)
	}
	entries := queue.get(false)
	if len(entries) != 1 {
		t.Fatalf("got %d queued entries", len(entries))
	}
	host := &NodeHost{}
	host.mu.shards.Store(uint64(100), &node{
		replicaID:        120,
		pendingProposals: pending,
	})
	handler := newNodeHostMessageHandler(host)
	msg := pb.Message{Type: pb.Propose, ShardID: 100, From: 121, Entries: entries}
	handler.HandleSendFailure(msg, false)
	select {
	case <-request.ResultC():
		t.Fatal("another replica completed the local proposal")
	default:
	}
	msg.From = 120
	handler.HandleSendFailure(msg, true)
	if result := <-request.ResultC(); !result.Timeout() {
		t.Fatalf("uncertain delivery returned %v", result)
	}
	if countPendingProposal(pending) != 0 {
		t.Fatal("send failure left the proposal pending")
	}
}
