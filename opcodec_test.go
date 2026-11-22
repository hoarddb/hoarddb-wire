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

	"github.com/hoarddb/hoarddb-bson/bson"
)

// opcodecSpecBody is the 75-byte body of conv Formats 1: class INSERT, bucket
// id 41, declaration version 7, schema_version_seen 7, prevEnd 0:0, then
// {"_id_": "p1", "Name": "Ada"} with Name as field 2.
const opcodecSpecBody = "01" +
	"0000000000000029" + "0000000000000007" + "0000000000000007" +
	"00000000000000000000000000000000" +
	"5070310000" + "00000008" + "0203000000703100" +
	"0001" + "0002" + "00000009" + "020400000041646100"

func opcodecSpecOp() Op {
	return Op{
		Class: OpInsert, BucketID: 41, DeclVersion: 7, SchemaVersionSeen: 7,
		EncID:   []byte{0x50, 'p', '1', 0, 0},
		IDValue: []byte{0x02, 3, 0, 0, 0, 'p', '1', 0},
		Fields:  []FieldOp{{FieldID: 2, Value: []byte{0x02, 4, 0, 0, 0, 'A', 'd', 'a', 0}}},
	}
}

func opcodecHeader(op Op) Op {
	op.BucketID = 41
	op.DeclVersion = 7
	op.SchemaVersionSeen = 8
	op.PrevEnd = Position{Epoch: 3, Seq: 99}
	op.EncID = []byte{0x50, 'p', '1', 0, 0}
	return op
}

// opcodecEveryClass returns one fully populated Op per class.
func opcodecEveryClass() []Op {
	idValue := []byte{0x02, 3, 0, 0, 0, 'p', '1', 0}
	nonce := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	lease := [16]byte{0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF}
	return []Op{
		opcodecHeader(Op{Class: OpInsert, IDValue: idValue, Fields: []FieldOp{
			{FieldID: 2, Value: []byte{0x02, 1, 0, 0, 0, 0}}, {FieldID: 3, Value: []byte{0x10, 5, 0, 0, 0}}}}),
		opcodecHeader(Op{Class: OpUpdate, Fields: []FieldOp{
			{FieldID: 2, OpCode: 0x01, Value: []byte{0x10, 1, 0, 0, 0}},
			{FieldID: 3, OpCode: 0x02},
			{FieldID: 4, OpCode: 0x09, Value: []byte{0, 9}}}}),
		opcodecHeader(Op{Class: OpUpsert, IDValue: idValue, Fields: []FieldOp{{FieldID: 2, Value: []byte{0x0A}}}}),
		opcodecHeader(Op{Class: OpDelete}),
		opcodecHeader(Op{Class: OpStoreVerb, Fields: []FieldOp{{FieldID: 4, Verb: 1, Value: []byte("arg")}}}),
		opcodecHeader(Op{Class: OpClaim, Fields: []FieldOp{{FieldID: 4}}, Count: 5, Now: 1234567, Nonce: nonce}),
		opcodecHeader(Op{Class: OpAck, Fields: []FieldOp{{FieldID: 4}}, LeaseID: lease, Now: 7654321}),
		opcodecHeader(Op{Class: OpRelease, Fields: []FieldOp{{FieldID: 4}}, LeaseID: lease, Now: 7654321}),
		opcodecHeader(Op{Class: OpExtend, Fields: []FieldOp{{FieldID: 4}}, LeaseID: lease, Now: 100, Until: 200}),
	}
}

func TestEveryOperationClassRoundTrips(t *testing.T) {
	classes := opcodecEveryClass()
	if len(classes) != 9 {
		t.Fatalf("want one Op per class, got %d", len(classes))
	}
	for _, op := range classes {
		body, err := EncodeOp(op)
		if err != nil {
			t.Fatalf("class 0x%02x: encode: %v", op.Class, err)
		}
		if body[0] != op.Class {
			t.Fatalf("class 0x%02x: first body byte is 0x%02x", op.Class, body[0])
		}
		got, err := DecodeOp(body)
		if err != nil {
			t.Fatalf("class 0x%02x: decode: %v", op.Class, err)
		}
		if !reflect.DeepEqual(got, op) {
			t.Fatalf("class 0x%02x: round trip\n got %+v\nwant %+v", op.Class, got, op)
		}
		again, err := EncodeOp(got)
		if err != nil || !bytes.Equal(again, body) {
			t.Fatalf("class 0x%02x: re-encode differs: %v", op.Class, err)
		}
	}
}

func TestPayloadCarriesFieldIDsNeverNames(t *testing.T) {
	if _, ok := reflect.TypeOf(FieldOp{}).FieldByName("Name"); ok {
		t.Fatal("FieldOp carries a name")
	}
	body, err := EncodeOp(opcodecSpecOp())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte("Name")) {
		t.Fatalf("the body carries a field name: %x", body)
	}
	if !bytes.Contains(body, []byte{0x00, 0x02, 0, 0, 0, 9}) {
		t.Fatalf("the body does not carry field id 2 and its length: %x", body)
	}
	bad := opcodecSpecOp()
	bad.Fields[0].FieldID = 0
	if _, err := EncodeOp(bad); err == nil {
		t.Fatal("field id 0 is never allocated and must be refused")
	}
}

func TestOneEntryPerFieldIDIsRefused(t *testing.T) {
	insert := opcodecSpecOp()
	insert.Fields = append(insert.Fields, FieldOp{FieldID: 2, Value: []byte{0x0A}})
	update := opcodecHeader(Op{Class: OpUpdate, Fields: []FieldOp{
		{FieldID: 2, OpCode: 0x03, Value: []byte{1}}, {FieldID: 2, OpCode: 0x01, Value: []byte{2}}}})
	for _, op := range []Op{insert, update} {
		_, err := EncodeOp(op)
		var refusal *Error
		if !errors.As(err, &refusal) || refusal.Code != CodeInvalidArgument {
			t.Fatalf("class 0x%02x: want INVALID_ARGUMENT, got %v", op.Class, err)
		}
	}
	// The decoder refuses the same bytes: two copies must not disagree.
	body, err := hex.DecodeString(opcodecSpecBody)
	if err != nil {
		t.Fatal(err)
	}
	offset := opcodecFieldCountOffset(body)
	dup := append([]byte{}, body[:offset]...)
	dup = append(dup, 0x00, 0x02) // fieldCount 2
	dup = append(dup, body[offset+2:]...)
	dup = append(dup, body[offset+2:]...)
	if _, err := DecodeOp(dup); err == nil {
		t.Fatal("decode accepted a duplicate field id")
	}
}

// opcodecFieldCountOffset is where fieldCount sits in the spec body.
func opcodecFieldCountOffset(body []byte) int { return 1 + 40 + 5 + 4 + 8 }

// TestWriteBodyIsClassPlusPayload pins conventions Formats 1 after the storage-format
// revision: the record body is [class][flags] then the 40-byte header then the form, 76
// bytes for the worked example, and the client body is the same bytes without the flags
// byte, 75.
func TestWriteBodyIsClassPlusPayload(t *testing.T) {
	client, err := hex.DecodeString(opcodecSpecBody)
	if err != nil {
		t.Fatal(err)
	}
	if len(client) != 75 {
		t.Fatalf("spec client body is %d bytes, Formats 1 says 75", len(client))
	}
	want := append([]byte{client[0], 0x00}, client[1:]...)
	if len(want) != 76 {
		t.Fatalf("spec record body is %d bytes, Formats 1 says 76", len(want))
	}
	body, err := EncodeOp(opcodecSpecOp())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("record body\n got %x\nwant %x ([class][flags] then the 40-byte header)", body, want)
	}
	if clientBody, err := EncodeClientOp(opcodecSpecOp()); err != nil || !bytes.Equal(clientBody, client) {
		t.Fatalf("client body\n got %x\nwant %x", clientBody, client)
	}
	op, err := DecodeOp(want)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(op, opcodecSpecOp()) {
		t.Fatalf("decoded %+v", op)
	}
	var asBSON bson.D
	if err := bson.Unmarshal(body, &asBSON); err == nil {
		t.Fatal("the body must be class + payload, not a BSON document")
	}
	for _, class := range []byte{0x00, 0x0A, 0xFF} {
		unknown := append([]byte{class}, want[1:]...)
		_, err := DecodeOp(unknown)
		var refusal *Error
		if !errors.As(err, &refusal) || refusal.Code != CodeInvalidArgument {
			t.Fatalf("class 0x%02x: want INVALID_ARGUMENT, got %v", class, err)
		}
	}
	if _, err := EncodeOp(Op{Class: 0x0A}); err == nil {
		t.Fatal("encode accepted an unknown class")
	}
}

func TestOpcodecRefusesFieldsAClassDoesNotUse(t *testing.T) {
	lease := [16]byte{1}
	cases := map[string]Op{
		"lease on insert":   func() Op { op := opcodecSpecOp(); op.LeaseID = lease; return op }(),
		"count on insert":   func() Op { op := opcodecSpecOp(); op.Count = 1; return op }(),
		"verb on insert":    func() Op { op := opcodecSpecOp(); op.Fields[0].Verb = 1; return op }(),
		"opcode on insert":  func() Op { op := opcodecSpecOp(); op.Fields[0].OpCode = 1; return op }(),
		"id on delete":      opcodecHeader(Op{Class: OpDelete, IDValue: []byte{1}}),
		"now on delete":     opcodecHeader(Op{Class: OpDelete, Now: 1}),
		"until on ack":      opcodecHeader(Op{Class: OpAck, Fields: []FieldOp{{FieldID: 1}}, Until: 1}),
		"fields on delete":  opcodecHeader(Op{Class: OpDelete, Fields: []FieldOp{{FieldID: 1}}}),
		"nonce on ack":      opcodecHeader(Op{Class: OpAck, Fields: []FieldOp{{FieldID: 1}}, Nonce: lease}),
		"until on claim":    opcodecHeader(Op{Class: OpClaim, Fields: []FieldOp{{FieldID: 1}}, Until: 1}),
		"value on ack":      opcodecHeader(Op{Class: OpAck, Fields: []FieldOp{{FieldID: 1, Value: []byte{1}}}}),
		"two fields on ack": opcodecHeader(Op{Class: OpAck, Fields: []FieldOp{{FieldID: 1}, {FieldID: 2}}}),
		"bad verb":          opcodecHeader(Op{Class: OpStoreVerb, Fields: []FieldOp{{FieldID: 1, Verb: VerbInc + 1}}}),
		"bad opcode":        opcodecHeader(Op{Class: OpUpdate, Fields: []FieldOp{{FieldID: 1, OpCode: 0x0A}}}),
		"empty id":          opcodecHeader(Op{Class: OpInsert}),
		"empty encID":       {Class: OpDelete},
	}
	for name, op := range cases {
		if _, err := EncodeOp(op); err == nil {
			t.Errorf("%s: encode accepted", name)
		}
	}
}

func TestOpcodecDecodeRefusesEveryTruncationAndTrailingBytes(t *testing.T) {
	for _, op := range opcodecEveryClass() {
		body, err := EncodeOp(op)
		if err != nil {
			t.Fatal(err)
		}
		for cut := 0; cut < len(body); cut++ {
			if _, err := DecodeOp(body[:cut]); err == nil {
				t.Fatalf("class 0x%02x: %d of %d bytes decoded", op.Class, cut, len(body))
			}
		}
		if _, err := DecodeOp(append(body, 0)); err == nil {
			t.Fatalf("class 0x%02x: trailing byte accepted", op.Class)
		}
	}
}

// TestOpcodecMutatedInputNeverPanicsAndAcceptedInputReEncodesIdentically
// flips bytes of valid bodies 100,000 times. Whatever the decoder accepts must
// encode back to the same bytes, or two encodings would name one operation.
func TestOpcodecMutatedInputNeverPanicsAndAcceptedInputReEncodesIdentically(t *testing.T) {
	rng := rand.New(rand.NewPCG(29, 29))
	var seeds [][]byte
	for _, op := range opcodecEveryClass() {
		body, err := EncodeOp(op)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, body)
	}
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
		if err != nil {
			t.Fatalf("decoded %x but cannot encode it: %v", mutated, err)
		}
		if !bytes.Equal(again, mutated) {
			t.Fatalf("accepted %x re-encodes as %x", mutated, again)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutation was accepted, so the identity half tested nothing")
	}
}

// TestAckAndReleaseCarryTheOwnersNow pins the amended layout of conventions
// section 9.1: after the lease id, ACK and RELEASE end with now:8, stamped by
// the owner before the append, so a deadline is compared with the current
// owner's clock and a replay reads the logged value.
func TestAckAndReleaseCarryTheOwnersNow(t *testing.T) {
	lease := [16]byte{0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF}
	for _, class := range []byte{OpAck, OpRelease} {
		op := opcodecHeader(Op{Class: class, Fields: []FieldOp{{FieldID: 4}}, LeaseID: lease, Now: 0x0102030405060708})
		body, err := EncodeOp(op)
		if err != nil {
			t.Fatal(err)
		}
		tail := append(append([]byte{0x00, 0x04}, lease[:]...), 1, 2, 3, 4, 5, 6, 7, 8)
		if !bytes.HasSuffix(body, tail) {
			t.Fatalf("class 0x%02x ends %x, want fieldID, lease id, now: %x", class, body[len(body)-len(tail):], tail)
		}
	}
}
