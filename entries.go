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
	"fmt"
)

// ListEntry is one document of a page: its slot, encoded id and BSON body.
type ListEntry struct {
	Slot  uint16
	EncID []byte
	Body  []byte
}

// maxEntryID is the cap of an encoded _id_ in an entries blob: the one id cap, MaxEncodedID.
const maxEntryID = MaxEncodedID

// DecodeEntries parses an entries blob until it ends. Every length is checked
// against the bytes left before anything is allocated, so a hostile blob costs
// no more memory than its own size. It refuses a slot above 8191, an id of no
// bytes or over 1024, a length that is not the shortest uvarint and a truncated
// record.
func DecodeEntries(blob []byte) ([]ListEntry, error) {
	var entries []ListEntry
	rest := blob
	for len(rest) > 0 {
		if len(rest) < 2 {
			return nil, entriesError("record %d is cut off before its slot ends", len(entries))
		}
		entry := ListEntry{Slot: binary.BigEndian.Uint16(rest)}
		if entry.Slot >= Slots {
			return nil, entriesError("record %d names slot %d, and there are %d", len(entries), entry.Slot, Slots)
		}
		rest = rest[2:]
		encID, after, err := takeSized(rest, "id", len(entries))
		if err != nil {
			return nil, err
		}
		if len(encID) == 0 || len(encID) > maxEntryID {
			return nil, entriesError("record %d has an id of %d bytes, want 1 to %d", len(entries), len(encID), maxEntryID)
		}
		body, after, err := takeSized(after, "body", len(entries))
		if err != nil {
			return nil, err
		}
		entry.EncID, entry.Body = bytes.Clone(encID), bytes.Clone(body)
		entries = append(entries, entry)
		rest = after
	}
	return entries, nil
}

// takeSized reads a canonical uvarint length and that many bytes.
func takeSized(raw []byte, what string, record int) (taken, rest []byte, err error) {
	length, n, ok := ReadCanonicalUvarint(raw)
	if !ok {
		return nil, nil, entriesError("record %d has a %s length that is not a shortest uvarint", record, what)
	}
	raw = raw[n:]
	if length > uint64(len(raw)) {
		return nil, nil, entriesError("record %d has a %s of %d bytes and %d are left", record, what, length, len(raw))
	}
	return raw[:length], raw[length:], nil
}

func entriesError(format string, args ...any) error {
	return &Error{Code: CodeInvalidArgument, Message: "a list answer is damaged: " + fmt.Sprintf(format, args...)}
}
