// Copyright 2026 Boeger IT Sh.p.k
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ringwire

import (
	"slices"
	"strings"
	"testing"
)

// TestAdminCommandsAreTheCommandsRingAdminAccepts holds AdminCommands to the codec: every
// command it lists encodes and decodes, a command it does not list is refused, and the
// refusal names every listed command. AdminCommands is what a table that must cover the
// wire (ringvec's rows, a driver's dispatch) reads, so a command the codec accepts without
// appearing here would leave such a table silently incomplete.
func TestAdminCommandsAreTheCommandsRingAdminAccepts(t *testing.T) {
	zeroSlot := int32(0)
	requests := map[string]RingAdminRequest{
		AdminCreateDatabase:   {Cmd: AdminCreateDatabase, Name: "app"},
		AdminCreateBucket:     {Cmd: AdminCreateBucket, Database: "app", Name: "User", Fields: []RingAdminField{}},
		AdminAlterBucket:      {Cmd: AdminAlterBucket, Database: "app", Bucket: "User", Add: []RingAdminField{{Name: "Age", Type: "int"}}, Drop: []string{}},
		AdminDropBucket:       {Cmd: AdminDropBucket, Database: "app", Bucket: "User"},
		AdminDescribeBucket:   {Cmd: AdminDescribeBucket, Database: "app", Bucket: "User"},
		AdminShowBuckets:      {Cmd: AdminShowBuckets, Database: "app"},
		AdminShowDatabases:    {Cmd: AdminShowDatabases},
		AdminDescribeDatabase: {Cmd: AdminDescribeDatabase, Database: "app"},
		AdminDescribeNode:     {Cmd: AdminDescribeNode},
		AdminSeedingStatus:    {Cmd: AdminSeedingStatus},
		AdminSeedingRetry:     {Cmd: AdminSeedingRetry, Database: "app", Slot: &zeroSlot},
		AdminSeedingForget:    {Cmd: AdminSeedingForget, Database: "app", Slot: &zeroSlot},
		AdminJoin:             {Cmd: AdminJoin, NodeID: "node-b", DataAddr: "node-b.example:4433"},
		AdminPromote:          {Cmd: AdminPromote, NodeID: "node-b"},
		AdminLeave:            {Cmd: AdminLeave, NodeID: "node-b"},
		AdminRemoveNode:       {Cmd: AdminRemoveNode, NodeID: "node-b"},
		AdminReplaceNode:      {Cmd: AdminReplaceNode, NodeID: "node-b", DataAddr: "node-b.example:4433"},
	}
	commands := AdminCommands()
	if len(commands) != len(requests) {
		t.Fatalf("AdminCommands lists %d commands and this test has %d requests: add a request for every listed command", len(commands), len(requests))
	}
	seen := make(map[string]bool, len(commands))
	for _, cmd := range commands {
		if seen[cmd] {
			t.Errorf("AdminCommands lists %q twice", cmd)
		}
		seen[cmd] = true
		req, ok := requests[cmd]
		if !ok {
			t.Fatalf("AdminCommands lists %q and this test has no request for it: add one", cmd)
		}
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Errorf("%s is listed and its request is refused: %v", cmd, err)
			continue
		}
		if _, err := DecodeRingAdmin(raw); err != nil {
			t.Errorf("%s is listed and its own encoding is not decoded: %v", cmd, err)
		}
	}
	for cmd := range requests {
		if !slices.Contains(commands, cmd) {
			t.Errorf("this test has a request for %q, which AdminCommands does not list", cmd)
		}
	}
	_, err := EncodeRingAdmin(RingAdminRequest{Cmd: "drop_database"})
	if err == nil {
		t.Fatal("drop_database encoded, and it is not an admin command")
	}
	for _, cmd := range commands {
		if !strings.Contains(err.Error(), cmd) {
			t.Errorf("the refusal of drop_database (%q) does not name the listed command %q", err, cmd)
		}
	}
}
