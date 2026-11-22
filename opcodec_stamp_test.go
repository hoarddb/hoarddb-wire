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
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"
)

// Storage-format slice A, unit F5: the record body is [class][flags][header:40][stamp?][form],
// the client body is today's [class][header:40][form] byte for byte, and two decoders read
// them.

// opcodecClientGoldens are EncodeOp's bytes for opcodecEveryClass on the build before the
// flags byte existed (6827a7b3a), one per class: the client body must never move. Update and
// delete end in the one guard/return trailer byte (G1a), 0x00 when neither is used.
var opcodecClientGoldens = map[byte]string{
	OpInsert:    "0100000000000000290000000000000007000000000000000800000000000000030000000000000063507031000000000008020300000070310000020002000000060201000000000003000000051005000000",
	OpUpdate:    "0200000000000000290000000000000007000000000000000800000000000000030000000000000063507031000000030100020000000510010000000200030000000009000400000002000900",
	OpUpsert:    "0300000000000000290000000000000007000000000000000800000000000000030000000000000063507031000000000008020300000070310000010002000000010a",
	OpDelete:    "0400000000000000290000000000000007000000000000000800000000000000030000000000000063507031000000",
	OpStoreVerb: "0500000000000000290000000000000007000000000000000800000000000000030000000000000063507031000000040100000003617267",
	OpClaim:     "0600000000000000290000000000000007000000000000000800000000000000030000000000000063507031000000040005000000000012d6870102030405060708090a0b0c0d0e0f10",
	OpAck:       "070000000000000029000000000000000700000000000000080000000000000003000000000000006350703100000004a0a1a2a3a4a5a6a7a8a9aaabacadaeaf000000000074cbb1",
	OpRelease:   "080000000000000029000000000000000700000000000000080000000000000003000000000000006350703100000004a0a1a2a3a4a5a6a7a8a9aaabacadaeaf000000000074cbb1",
	OpExtend:    "090000000000000029000000000000000700000000000000080000000000000003000000000000006350703100000004a0a1a2a3a4a5a6a7a8a9aaabacadaeaf000000000000006400000000000000c8",
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestClientBodyIsUnchanged(t *testing.T) {
	for _, op := range opcodecEveryClass() {
		body, err := EncodeClientOp(op)
		if err != nil {
			t.Fatalf("class %#02x: EncodeClientOp: %v", op.Class, err)
		}
		if want := mustHex(t, opcodecClientGoldens[op.Class]); !bytes.Equal(body, want) {
			t.Fatalf("class %#02x: client body\n got %x\nwant %x (the bytes of the build before the flags byte)", op.Class, body, want)
		}
		decoded, err := DecodeClientOp(body)
		if err != nil || !reflect.DeepEqual(decoded, op) {
			t.Fatalf("class %#02x: DecodeClientOp = %+v, %v", op.Class, decoded, err)
		}
		if body[0] != op.Class || body[1] != 0 || body[8] != 0x29 {
			t.Fatalf("class %#02x: the client body's second byte is the bucket id's top byte, not a flags byte: %x", op.Class, body[:9])
		}
	}
	stamped := opcodecHeader(Op{Class: OpDelete, Stamp: IndexStamp{Fields: []uint16{2}}})
	if _, err := EncodeClientOp(stamped); err == nil {
		t.Fatal("EncodeClientOp accepted a stamp; a client body never carries one")
	}
}

func TestAWriteWithNoIndexesCostsOneFlagsByte(t *testing.T) {
	for _, op := range opcodecEveryClass() {
		client, err := EncodeClientOp(op)
		if err != nil {
			t.Fatal(err)
		}
		record, err := EncodeOp(op)
		if err != nil {
			t.Fatalf("class %#02x: EncodeOp: %v", op.Class, err)
		}
		want := append([]byte{op.Class, 0x00}, client[1:]...)
		if !bytes.Equal(record, want) {
			t.Fatalf("class %#02x: record body\n got %x\nwant %x: [class][flags 0x00] then the client bytes, no stamp bytes", op.Class, record, want)
		}
		if len(record) != len(client)+1 {
			t.Fatalf("class %#02x: a write with no indexes costs %d bytes over the client body, want 1", op.Class, len(record)-len(client))
		}
		decoded, err := DecodeOp(record)
		if err != nil || !reflect.DeepEqual(decoded, op) {
			t.Fatalf("class %#02x: DecodeOp = %+v, %v", op.Class, decoded, err)
		}
	}
}

func opcodecStamped(class byte) Op {
	op := opcodecEveryClass()[class-1]
	op.Stamp = IndexStamp{
		Fields: []uint16{2, 3, 7},
		Claims: []UniqueClaim{{FieldID: 2, ClaimedAt: 1_700_000_000_000}, {FieldID: 7, ClaimedAt: 1}},
	}
	return op
}

func TestAStampedWriteCostsTwoPlusTwoNPlusTenU(t *testing.T) {
	for _, op := range opcodecEveryClass() {
		stamped := opcodecStamped(op.Class)
		client, err := EncodeClientOp(op)
		if err != nil {
			t.Fatal(err)
		}
		record, err := EncodeOp(stamped)
		if err != nil {
			t.Fatalf("class %#02x: EncodeOp: %v", op.Class, err)
		}
		const n, u = 3, 2
		if got, want := len(record)-len(client), 1+2+2*n+10*u; got != want {
			t.Fatalf("class %#02x: a stamped write costs %d bytes over the client body, want flags 1 + stamp 2 + 2n + 10u = %d", op.Class, got, want)
		}
		wantStamp := []byte{
			n, 0, 2, 0, 3, 0, 7,
			u, 0, 2, 0x00, 0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68, 0x00, 0, 7, 0, 0, 0, 0, 0, 0, 0, 1,
		}
		if record[1] != 0x01 || !bytes.Equal(record[2+opcodecHeaderLen:2+opcodecHeaderLen+len(wantStamp)], wantStamp) {
			t.Fatalf("class %#02x: flags %#02x, after the 40-byte header %x; want flags 0x01 and the stamp %x", op.Class, record[1], record[2+opcodecHeaderLen:2+opcodecHeaderLen+len(wantStamp)], wantStamp)
		}
		if !bytes.Equal(record[2:2+opcodecHeaderLen], client[1:1+opcodecHeaderLen]) {
			t.Fatalf("class %#02x: the header moved: the stamp must follow the 40-byte header", op.Class)
		}
		decoded, err := DecodeOp(record)
		if err != nil || !reflect.DeepEqual(decoded, stamped) {
			t.Fatalf("class %#02x: DecodeOp = %+v, %v; want the stamp back", op.Class, decoded, err)
		}
	}
}

func TestAClientBodyStillDecodesAfterAFlagsByteExists(t *testing.T) {
	// A bucket id below 2^56: the client body's second byte is 0x00, which one decoder for
	// both bodies would read as flags, and then every header field would be off by one.
	op := opcodecHeader(Op{Class: OpDelete})
	op.BucketID = 41
	body, err := EncodeClientOp(op)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeClientOp(body)
	if err != nil {
		t.Fatalf("DecodeClientOp: %v", err)
	}
	for name, got := range map[string][2]any{
		"BucketID":          {decoded.BucketID, op.BucketID},
		"DeclVersion":       {decoded.DeclVersion, op.DeclVersion},
		"SchemaVersionSeen": {decoded.SchemaVersionSeen, op.SchemaVersionSeen},
		"PrevEnd":           {decoded.PrevEnd, op.PrevEnd},
		"EncID":             {string(decoded.EncID), string(op.EncID)},
	} {
		if !reflect.DeepEqual(got[0], got[1]) {
			t.Errorf("%s decoded as %v, want %v: the client decoder read a flags byte that is not there", name, got[0], got[1])
		}
	}
	if !decoded.Stamp.Empty() {
		t.Fatalf("a client body decoded with a stamp %+v", decoded.Stamp)
	}
	prefix, taken, err := DecodeClientOpPrefix(append(body, 0xEE))
	if err != nil || taken != len(body) || prefix.BucketID != 41 {
		t.Fatalf("DecodeClientOpPrefix = %+v, %d, %v", prefix, taken, err)
	}
}

func TestDecodeOpRefusesAReservedFlagBit(t *testing.T) {
	record, err := EncodeOp(opcodecEveryClass()[0])
	if err != nil {
		t.Fatal(err)
	}
	// Only bits that no feature has claimed, so the list holds in any landing order:
	// 0x04, 0x08 (expiry) and 0x10 (aggregates) are known or not by what has merged.
	for _, flags := range []byte{0x02, 0x20, 0x40, 0x80, 0xFE, 0x03} {
		mutant := append([]byte(nil), record...)
		mutant[1] = flags
		_, err := DecodeOp(mutant)
		var refusal *FlagsError
		if !errors.As(err, &refusal) || refusal.Flags != flags {
			t.Errorf("flags %#02x: DecodeOp = %v, want a *FlagsError naming the flags", flags, err)
		}
	}
}

func TestDecodeOpRefusesASetFlagWithAnEmptyStamp(t *testing.T) {
	record, err := EncodeOp(opcodecEveryClass()[3]) // DELETE: header then nothing
	if err != nil {
		t.Fatal(err)
	}
	// flags 0x01 and a stamp of [n=0][u=0]: the only canonical form of "no indexes" is absent.
	mutant := append([]byte(nil), record[:2+opcodecHeaderLen]...)
	mutant[1] = 0x01
	mutant = append(mutant, 0, 0)
	mutant = append(mutant, record[2+opcodecHeaderLen:]...)
	if _, err := DecodeOp(mutant); err == nil {
		t.Fatal("DecodeOp accepted flags 0x01 with an empty stamp: it would not re-encode to the same bytes")
	}
}

// TestOpcodecRecordFormMutationAndTruncation extends the two existing mutation tests to the
// record form: an accepted record body re-encodes identically, and every truncation of a
// stamped body is refused.
func TestOpcodecRecordFormMutationAndTruncation(t *testing.T) {
	var seeds [][]byte
	for _, op := range opcodecEveryClass() {
		for _, candidate := range []Op{op, opcodecStamped(op.Class)} {
			body, err := EncodeOp(candidate)
			if err != nil {
				t.Fatal(err)
			}
			seeds = append(seeds, body)
			for cut := 0; cut < len(body); cut++ {
				if _, err := DecodeOp(body[:cut]); err == nil {
					t.Fatalf("class %#02x: %d of %d record bytes decoded", op.Class, cut, len(body))
				}
			}
			if _, err := DecodeOp(append(append([]byte(nil), body...), 0)); err == nil {
				t.Fatalf("class %#02x: a trailing byte was accepted", op.Class)
			}
		}
	}
	rng := rand.New(rand.NewPCG(31, 31))
	accepted := 0
	for round := 0; round < 100000; round++ {
		mutated := append([]byte{}, seeds[rng.IntN(len(seeds))]...)
		for flips := 1 + rng.IntN(4); flips > 0; flips-- {
			switch rng.IntN(3) {
			case 0:
				mutated[rng.IntN(len(mutated))] = byte(rng.IntN(256))
			case 1:
				mutated = mutated[:rng.IntN(len(mutated)+1)]
			default:
				mutated = append(mutated, byte(rng.IntN(256)))
			}
			if len(mutated) == 0 {
				mutated = []byte{byte(rng.IntN(256))}
			}
		}
		op, err := DecodeOp(mutated)
		if err != nil {
			continue
		}
		accepted++
		again, err := EncodeOp(op)
		if err != nil || !bytes.Equal(again, mutated) {
			t.Fatalf("accepted %x re-encodes as %x (%v)", mutated, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutation was accepted")
	}
}
