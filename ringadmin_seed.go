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
	"bytes"
	"encoding/binary"
	"slices"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The cmd values of the seeding commands.
const (
	// AdminSeedingStatus lists the slot pulls of this node.
	AdminSeedingStatus = "seeding_status"
	// AdminSeedingRetry restarts the pull of one slot.
	AdminSeedingRetry = "seeding_retry"
	// AdminSeedingForget removes the retired record of one slot and keeps its range.
	AdminSeedingForget = "seeding_forget"
)

// The bounds of the seeding commands.
const (
	// SeedingMaxPage is the ceiling of a seeding_status page. A worst-case row is about 2.3
	// KiB, so a full page is under 2.5 MiB and always fits one frame.
	SeedingMaxPage = 1000
	// SeedingMaxRefusal bounds the refusal text of a row, in bytes.
	SeedingMaxRefusal = 512
	// SeedingCursorMax is the longest cursor: two bytes of slot and a database name.
	SeedingCursorMax = 2 + MaxName
)

// RingAdminSeedRow is one slot of one database in a seeding_status reply. Every member is
// always written: a text with no value is the empty string and a number with none is 0.
type RingAdminSeedRow struct {
	// Database and Slot name the pull.
	Database string
	Slot     int32
	// State is one of placing, seeding, stalled, ready and retired.
	State string
	// Source is the node id the pull reads from, or empty.
	Source string
	// BytesDone, BytesKnown, Entries and IdleMS are the counters of SeedRow, with the idle time in milliseconds.
	BytesDone, BytesKnown, Entries, IdleMS int64
	// Refusal is the last refusal text of the source, or empty.
	Refusal string
}

// RingAdminSeeding is the reply of the three seeding commands. A seeding_status reply sets
// Rows (non-nil, possibly empty), More and Cursor; a seeding_retry or seeding_forget reply
// sets Database and Slot, the slot it acted on, and leaves the rest zero.
type RingAdminSeeding struct {
	Rows   []RingAdminSeedRow
	More   bool
	Cursor []byte
	// Database and Slot answer seeding_retry and seeding_forget.
	Database string
	Slot     int32
}

type adminSeedingStatusDoc struct {
	Cmd      string `bson:"cmd"`
	Database string `bson:"database,omitempty"`
	Slot     *int32 `bson:"slot,omitempty"`
	Limit    *int32 `bson:"limit,omitempty"`
	Cursor   []byte `bson:"cursor,omitempty"`
}

type adminSeedingActionDoc struct {
	Cmd      string `bson:"cmd"`
	Database string `bson:"database"`
	Slot     int32  `bson:"slot"`
	// Anyway is left out unless true, so a retry and a plain forget send the document every
	// earlier build sent.
	Anyway bool `bson:"anyway,omitempty"`
}

type adminSeedRowDoc struct {
	Database   string `bson:"database"`
	Slot       int32  `bson:"slot"`
	State      string `bson:"state"`
	Source     string `bson:"source"`
	BytesDone  int64  `bson:"bytes_done"`
	BytesKnown int64  `bson:"bytes_known"`
	Entries    int64  `bson:"entries"`
	IdleMS     int64  `bson:"idle_ms"`
	Refusal    string `bson:"refusal"`
}

type adminSeedingStatusReplyDoc struct {
	Cmd    string            `bson:"cmd"`
	Rows   []adminSeedRowDoc `bson:"rows"`
	More   bool              `bson:"more,omitempty"`
	Cursor []byte            `bson:"cursor,omitempty"`
}

type adminSeedingActionReplyDoc struct {
	Cmd      string `bson:"cmd"`
	Database string `bson:"database"`
	Slot     int32  `bson:"slot"`
}

// unmarshalSeeding unpacks a seeding document into doc. The size and depth were checked by the caller.
func unmarshalSeeding(raw []byte, doc any) error {
	if err := bson.Unmarshal(raw, doc); err != nil {
		return Refusal("a seeding document is not the document of its command (its members, each of its stated type): %v", err)
	}
	return nil
}

func isSeedingCmd(cmd string) bool {
	return cmd == AdminSeedingStatus || cmd == AdminSeedingRetry || cmd == AdminSeedingForget
}

func checkSeedingSlot(cmd string, slot int32) error {
	if slot < 0 || slot >= Slots {
		return Refusal("%s: slot is %d and a slot is 0 to %d. Send a slot 'status seeding' lists", cmd, slot, Slots-1)
	}
	return nil
}

// checkSeedingRequest checks the members of the three seeding requests.
func checkSeedingRequest(req RingAdminRequest) error {
	if req.Anyway && req.Cmd != AdminSeedingForget {
		return Refusal("%s takes no anyway, which only seeding_forget takes. Remove anyway", req.Cmd)
	}
	if req.Cmd != AdminSeedingStatus {
		takes := "database slot"
		if req.Cmd == AdminSeedingForget {
			takes += " anyway"
		}
		if err := adminStray(req, takes); err != nil {
			return err
		}
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
		if req.Slot == nil {
			return Refusal("%s needs a slot, 0 to %d, and this request has none. Run 'status seeding' to list the slots of this node, then send one of them", req.Cmd, Slots-1)
		}
		return checkSeedingSlot(req.Cmd, *req.Slot)
	}
	if err := adminStray(req, "database slot limit cursor"); err != nil {
		return err
	}
	if req.Database != "" {
		if err := checkAdminName(req.Cmd, "database", req.Database); err != nil {
			return err
		}
	}
	if req.Slot != nil {
		if req.Database == "" {
			return Refusal("seeding_status: a slot needs the database it belongs to, and this request names none. Send database with slot, or leave both out")
		}
		if err := checkSeedingSlot(req.Cmd, *req.Slot); err != nil {
			return err
		}
	}
	if req.Limit != nil && (*req.Limit < 0 || *req.Limit > SeedingMaxPage) {
		return Refusal("seeding_status: limit is %d and a page holds 1 to %d. Leave limit out for the default of %d", *req.Limit, SeedingMaxPage, DefaultPageLimit)
	}
	if len(req.Cursor) > SeedingCursorMax {
		return Refusal("seeding_status: cursor is %d bytes and at most %d are taken. Send the cursor of the previous reply unchanged", len(req.Cursor), SeedingCursorMax)
	}
	return nil
}

func seedingDocOf(req RingAdminRequest) any {
	if req.Cmd == AdminSeedingStatus {
		return adminSeedingStatusDoc{Cmd: req.Cmd, Database: req.Database, Slot: req.Slot, Limit: req.Limit, Cursor: req.Cursor}
	}
	return adminSeedingActionDoc{Cmd: req.Cmd, Database: req.Database, Slot: *req.Slot, Anyway: req.Anyway}
}

// CheckSeedingRow bounds one row of a status reply.
func CheckSeedingRow(index int, row RingAdminSeedRow) error {
	if len(row.Database) == 0 || len(row.Database) > MaxName {
		return Refusal("row %d: database is %d bytes and a name is 1 to %d", index, len(row.Database), MaxName)
	}
	if row.Slot < 0 || row.Slot >= Slots {
		return Refusal("row %d: slot is %d and a slot is 0 to %d", index, row.Slot, Slots-1)
	}
	if !slices.Contains(SeedStates, SeedState(row.State)) {
		return Refusal("row %d: state is %q and a state is one of placing, seeding, stalled, ready, retired or retiring", index, row.State)
	}
	for _, counter := range []struct {
		name  string
		count int64
	}{{"bytes_done", row.BytesDone}, {"bytes_known", row.BytesKnown}, {"entries", row.Entries}, {"idle_ms", row.IdleMS}} {
		if counter.count < 0 {
			return Refusal("row %d: %s is %d and a counter is 0 or more", index, counter.name, counter.count)
		}
	}
	if len(row.Source) > MaxNodeText {
		return Refusal("row %d: source is %d bytes and at most %d are taken", index, len(row.Source), MaxNodeText)
	}
	if len(row.Refusal) > SeedingMaxRefusal {
		return Refusal("row %d: refusal is %d bytes and at most %d are taken", index, len(row.Refusal), SeedingMaxRefusal)
	}
	if !utf8.ValidString(row.Refusal) || !utf8.ValidString(row.Source) {
		return Refusal("row %d: source and refusal are text and must be UTF-8", index)
	}
	return nil
}

// checkSeedingReply checks a seeding reply. The caller has checked that no other member is set.
func checkSeedingReply(cmd string, reply *RingAdminSeeding) error {
	if cmd == AdminSeedingStatus {
		if reply.Rows == nil || reply.Database != "" || reply.Slot != 0 {
			return Refusal("a seeding_status reply is rows, more and cursor, and nothing else")
		}
		if len(reply.Rows) > SeedingMaxPage {
			return Refusal("a seeding_status page holds %d rows and at most %d", len(reply.Rows), SeedingMaxPage)
		}
		if reply.More != (len(reply.Cursor) != 0) || len(reply.Cursor) > SeedingCursorMax {
			return Refusal("a seeding_status reply has a cursor of at most %d bytes exactly when more is true", SeedingCursorMax)
		}
		for i, row := range reply.Rows {
			if err := CheckSeedingRow(i, row); err != nil {
				return err
			}
		}
		return nil
	}
	if reply.Rows != nil || reply.More || len(reply.Cursor) != 0 {
		return Refusal("a %s reply is database and slot, and nothing else", cmd)
	}
	if err := checkAdminName(cmd, "database", reply.Database); err != nil {
		return err
	}
	return checkSeedingSlot(cmd, reply.Slot)
}

func seedingReplyDocOf(cmd string, reply *RingAdminSeeding) any {
	if cmd != AdminSeedingStatus {
		return adminSeedingActionReplyDoc{Cmd: cmd, Database: reply.Database, Slot: reply.Slot}
	}
	rows := make([]adminSeedRowDoc, 0, len(reply.Rows))
	for _, row := range reply.Rows {
		rows = append(rows, adminSeedRowDoc(row))
	}
	return adminSeedingStatusReplyDoc{Cmd: cmd, Rows: rows, More: reply.More, Cursor: reply.Cursor}
}

func unpackSeedingRequest(cmd string, raw []byte) (RingAdminRequest, error) {
	if cmd == AdminSeedingStatus {
		var doc adminSeedingStatusDoc
		if err := unmarshalSeeding(raw, &doc); err != nil {
			return RingAdminRequest{}, err
		}
		return RingAdminRequest{Cmd: cmd, Database: doc.Database, Slot: doc.Slot, Limit: doc.Limit, Cursor: doc.Cursor}, nil
	}
	var doc adminSeedingActionDoc
	if err := unmarshalSeeding(raw, &doc); err != nil {
		return RingAdminRequest{}, err
	}
	return RingAdminRequest{Cmd: cmd, Database: doc.Database, Slot: &doc.Slot, Anyway: doc.Anyway}, nil
}

// countRows counts the elements of the rows array of a validated status document without
// building them, so a reply of many tiny rows is refused before anything is allocated for it.
func countRows(raw []byte) (int, error) {
	value, err := bson.Raw(raw).LookupErr("rows")
	if err != nil {
		return 0, nil // the unpack names the missing member
	}
	array, ok := value.ArrayOK()
	if !ok {
		return 0, nil
	}
	count, at := 0, 4
	for at < len(array) && array[at] != 0 {
		keyEnd := bytes.IndexByte(array[at+1:], 0)
		if keyEnd < 0 || at+1+keyEnd+5 > len(array) {
			return 0, Refusal("a seeding_status reply has a rows array that is cut short")
		}
		size := int(binary.LittleEndian.Uint32(array[at+1+keyEnd+1:]))
		if array[at] != 0x03 || size < 5 || at+1+keyEnd+1+size > len(array) {
			return 0, Refusal("a seeding_status reply has a rows array that holds something other than row documents")
		}
		at += 1 + keyEnd + 1 + size
		if count++; count > SeedingMaxPage {
			return 0, Refusal("a seeding_status page holds more than %d rows and at most %d", SeedingMaxPage, SeedingMaxPage)
		}
	}
	return count, nil
}

func unpackSeedingReply(cmd string, raw []byte) (RingAdminReply, error) {
	if cmd != AdminSeedingStatus {
		var doc adminSeedingActionReplyDoc
		if err := unmarshalSeeding(raw, &doc); err != nil {
			return RingAdminReply{}, err
		}
		return RingAdminReply{Cmd: cmd, Seeding: &RingAdminSeeding{Database: doc.Database, Slot: doc.Slot}}, nil
	}
	if _, err := countRows(raw); err != nil {
		return RingAdminReply{}, err
	}
	var doc adminSeedingStatusReplyDoc
	if err := unmarshalSeeding(raw, &doc); err != nil {
		return RingAdminReply{}, err
	}
	rows := make([]RingAdminSeedRow, 0, len(doc.Rows))
	for _, row := range doc.Rows {
		rows = append(rows, RingAdminSeedRow(row))
	}
	return RingAdminReply{Cmd: cmd, Seeding: &RingAdminSeeding{Rows: rows, More: doc.More, Cursor: doc.Cursor}}, nil
}
