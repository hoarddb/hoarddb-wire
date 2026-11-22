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

// SeedState is where one slot of one database stands on this node, as the operator reads it.
// The wire carries the lowercase word; the CLI prints it upper-cased.
type SeedState string

// The states of a SeedRow.
const (
	// SeedPlacing means the slot is in this node's copy set and its pull is queued.
	SeedPlacing SeedState = "placing"
	// SeedSeeding means a pull is applying chunks.
	SeedSeeding SeedState = "seeding"
	// SeedStalled means no chunk was applied for replication.seed_idle_seconds.
	SeedStalled SeedState = "stalled"
	// SeedReady means this node holds a full, attested copy and serves the slot.
	SeedReady SeedState = "ready"
	// SeedRetired means the slot left this node's copy set and its range is kept until the
	// deletion gate passes. Only this state can be forgotten.
	SeedRetired SeedState = "retired"
	// SeedRetiring means the deletion gate passed and the range delete has started. It can never
	// be cancelled, so an operator waits: neither retry nor forget applies (the machine's SlotRetiring).
	SeedRetiring SeedState = "retiring"
)

// SeedStates is every state a row can be in, in the order a pull goes through them.
var SeedStates = []SeedState{SeedPlacing, SeedSeeding, SeedStalled, SeedReady, SeedRetired, SeedRetiring}
