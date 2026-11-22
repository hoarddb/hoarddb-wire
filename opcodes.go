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

// RequestIDLen is the fixed size of the request ID field in the frame header.
// Each request ID is a 16-byte UUID v4, used for 0-RTT deduplication and
// request correlation.
const RequestIDLen = 16

// The opcode byte of each ring frame. They are plain numbers here: the server's
// transport package and a driver's frame code each give them their own opcode
// type, and both read the number from this one declaration.
const (
	// The entry-ring client opcodes.

	// OpRingList is a ListRequest (BSON); a read.
	OpRingList = 0xDA
	// OpRingCount is a CountRequest (BSON); a read.
	OpRingCount = 0xDB
	// OpRingKeyed is a keyed request: client prologue, 40-byte header, one class form.
	OpRingKeyed = 0xDC
	// OpRingAdmin is one BSON admin command (create_database, create_bucket, describe_bucket,
	// show_buckets and the others of the Admin constants). It is not a read: the catalog changes.
	OpRingAdmin = 0xDD
	// OpRingTopology is a TopologyRequest (BSON); a read.
	OpRingTopology = 0xDE
	// OpRingReply is the success reply to all five ring requests, matched by request id.
	OpRingReply = 0xDF
	// OpRingBatch is a batch of write operations: one client prologue, a count byte, then
	// that many RING_KEYED write bodies. It is not a read: it writes. It is answered by
	// OpRingReply with one answer per operation.
	OpRingBatch = 0xE0
	// OpRingGetMany reads many documents in one frame: one client prologue, a count byte, then
	// that many RING_KEYED get or get_fields bodies. It is a read. It is answered by OpRingReply
	// with one answer per key in request order.
	OpRingGetMany = 0xE1
)
