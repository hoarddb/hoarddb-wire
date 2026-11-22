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
	"reflect"
	"testing"
)

// guardTestID is the EncodeComponent("p1") form opcodec_test.go uses for its ids.
var guardTestID = []byte{0x50, 'p', '1', 0, 0}

// guardTestUpdate is a plain update of field 2, the base every guard test adds to.
func guardTestUpdate() Op {
	return Op{
		Class: OpUpdate, BucketID: 9, DeclVersion: 7, SchemaVersionSeen: 7,
		EncID:  guardTestID,
		Fields: []FieldOp{{FieldID: 2, OpCode: UpdateSet, Value: []byte{0x10, 1, 0, 0, 0}}},
	}
}

// guardTestDelete is a plain delete of guardTestID.
func guardTestDelete() Op {
	return Op{Class: OpDelete, BucketID: 9, DeclVersion: 7, SchemaVersionSeen: 7, EncID: guardTestID}
}

func TestGuardAndReturnRoundTripOnUpdateAndDelete(t *testing.T) {
	update := guardTestUpdate()
	update.Guard = []GuardTerm{{FieldID: 3, Value: []byte{0x10, 7, 0, 0, 0}}, {FieldID: 5, Value: []byte{0x0A}}}
	update.Return = ReturnBefore
	del := guardTestDelete()
	del.Guard = []GuardTerm{{FieldID: 3, Value: []byte{0x02, 2, 0, 0, 0, 'x', 0}}}
	del.Return = ReturnBefore

	first, err := EncodeClientOp(update)
	if err != nil {
		t.Fatalf("EncodeClientOp(update): %v", err)
	}
	second, err := EncodeClientOp(del)
	if err != nil {
		t.Fatalf("EncodeClientOp(delete): %v", err)
	}
	third, err := EncodeClientOp(guardTestUpdate())
	if err != nil {
		t.Fatalf("EncodeClientOp(plain update): %v", err)
	}
	batch := append(append(append([]byte(nil), first...), second...), third...)

	got, n, err := DecodeClientOpPrefix(batch)
	if err != nil || n != len(first) {
		t.Fatalf("DecodeClientOpPrefix(update) took %d bytes, want %d: %v", n, len(first), err)
	}
	if !reflect.DeepEqual(got.Guard, update.Guard) || got.Return != ReturnBefore {
		t.Fatalf("update decoded as guard %+v return %d", got.Guard, got.Return)
	}
	batch = batch[n:]

	got, n, err = DecodeClientOpPrefix(batch)
	if err != nil || n != len(second) {
		t.Fatalf("DecodeClientOpPrefix(delete) took %d bytes, want %d: %v", n, len(second), err)
	}
	if !reflect.DeepEqual(got.Guard, del.Guard) || got.Return != ReturnBefore {
		t.Fatalf("delete decoded as guard %+v return %d", got.Guard, got.Return)
	}
	batch = batch[n:]

	got, n, err = DecodeClientOpPrefix(batch)
	if err != nil || n != len(third) {
		t.Fatalf("DecodeClientOpPrefix(plain update) took %d bytes, want %d: %v", n, len(third), err)
	}
	if got.Guard != nil || got.Return != ReturnNone {
		t.Fatalf("plain update decoded with guard %+v return %d", got.Guard, got.Return)
	}
}

func TestPlainUpdateAndDeleteAddExactlyOneByte(t *testing.T) {
	update := guardTestUpdate()
	plain, err := EncodeClientOp(update)
	if err != nil {
		t.Fatalf("EncodeClientOp: %v", err)
	}
	update.Guard = []GuardTerm{{FieldID: 3, Value: []byte{0x10, 7, 0, 0, 0}}}
	update.Return = ReturnAfter
	withGuard, err := EncodeClientOp(update)
	if err != nil {
		t.Fatalf("EncodeClientOp(guarded): %v", err)
	}
	if want := len(plain) + 1 + 2 + 4 + 5; len(withGuard) != want { // count, field id, length, value
		t.Fatalf("a one-term guard added %d bytes, want %d", len(withGuard)-len(plain), want-len(plain))
	}
	if back, err := DecodeClientOp(withGuard); err != nil || len(back.Guard) != 1 || back.Return != ReturnAfter {
		t.Fatalf("DecodeClientOp = %+v, %v", back, err)
	}

	// The delete literal is the body the base commit encodes, before the trailer existed.
	const baseDelete = "04000000000000000900000000000000070000000000000007000000000000000000000000000000005070310000"
	base, err := hex.DecodeString(baseDelete)
	if err != nil {
		t.Fatalf("hex: %v", err)
	}
	plainDelete, err := EncodeClientOp(guardTestDelete())
	if err != nil {
		t.Fatalf("EncodeClientOp(delete): %v", err)
	}
	if len(plainDelete) != len(base)+1 || !bytes.Equal(plainDelete[:len(base)], base) || plainDelete[len(base)] != 0 {
		t.Fatalf("plain delete = %x, want the base body %x and one zero trailer byte", plainDelete, base)
	}
}

func TestRecordBodyRefusesAGuard(t *testing.T) {
	guarded := guardTestUpdate()
	guarded.Guard = []GuardTerm{{FieldID: 3, Value: []byte{0x10, 7, 0, 0, 0}}}
	if _, err := EncodeOp(guarded); err == nil {
		t.Fatal("EncodeOp accepted a guard; a record body must never carry one")
	}

	returned := guardTestUpdate()
	returned.Return = ReturnAfter
	record, err := EncodeOp(returned)
	if err != nil {
		t.Fatalf("EncodeOp(return): %v", err)
	}
	if back, err := DecodeOp(record); err != nil || back.Return != ReturnAfter {
		t.Fatalf("DecodeOp = return %d, %v; want the return to reach the record", back.Return, err)
	}

	// A record whose trailer sets the guard bit and carries a guard section is refused.
	plainRecord, err := EncodeOp(guardTestUpdate())
	if err != nil {
		t.Fatalf("EncodeOp(plain): %v", err)
	}
	forged := append([]byte(nil), plainRecord...)
	forged[len(forged)-1] = 0x01 // the guard bit
	forged = append(forged, 1, 0, 3, 0, 0, 0, 5, 0x10, 7, 0, 0, 0)
	if _, err := DecodeOp(forged); err == nil {
		t.Fatal("DecodeOp accepted a guard section in a record body")
	}
}

func TestTrailerRefusesWhatItDoesNotKnow(t *testing.T) {
	plain, err := EncodeClientOp(guardTestUpdate())
	if err != nil {
		t.Fatalf("EncodeClientOp: %v", err)
	}
	head := plain[:len(plain)-1]
	oneTerm := func(fieldID uint16, value ...byte) []byte {
		section := []byte{0x01, 0x01, byte(fieldID >> 8), byte(fieldID), 0, 0, 0, byte(len(value))}
		return append(section, value...)
	}
	manyTerms := []byte{0x01, byte(MaxGuardTerms + 1)}
	for range MaxGuardTerms + 1 {
		manyTerms = append(manyTerms, 0, 3, 0, 0, 0, 1, 0x0A)
	}
	cases := map[string][]byte{
		"bit 3 set":           append(append([]byte(nil), head...), 0x08),
		"bit 7 set":           append(append([]byte(nil), head...), 0x80),
		"return mode 3":       append(append([]byte(nil), head...), 0x06),
		"guard with no terms": append(append([]byte(nil), head...), 0x01, 0x00),
		"guard cut short":     append(append([]byte(nil), head...), 0x01),
		"too many terms":      append(append([]byte(nil), head...), manyTerms...),
		"empty guard value":   append(append([]byte(nil), head...), 0x01, 0x01, 0, 3, 0, 0, 0, 0),
		"guard id 0":          append(append([]byte(nil), head...), oneTerm(0, 0x0A)...),
		"guard id 1":          append(append([]byte(nil), head...), oneTerm(1, 0x0A)...),
		"ids out of order":    append(append(append([]byte(nil), head...), 0x01, 0x02, 0, 5, 0, 0, 0, 1, 0x0A), 0, 3, 0, 0, 0, 1, 0x0A),
		"ids repeated":        append(append(append([]byte(nil), head...), 0x01, 0x02, 0, 3, 0, 0, 0, 1, 0x0A), 0, 3, 0, 0, 0, 1, 0x0A),
	}
	for name, body := range cases {
		if _, err := DecodeClientOp(body); err == nil {
			t.Errorf("%s: DecodeClientOp accepted %x", name, body)
		}
	}
}

func TestGuardOnOtherClassesIsRefused(t *testing.T) {
	insert := opcodecSpecOp()
	if _, err := EncodeClientOp(insert); err != nil {
		t.Fatalf("control: a plain insert must encode: %v", err)
	}
	guard := []GuardTerm{{FieldID: 3, Value: []byte{0x10, 7, 0, 0, 0}}}

	withGuard := insert
	withGuard.Guard = guard
	if _, err := EncodeClientOp(withGuard); err == nil {
		t.Error("an INSERT with a guard encoded; the guard would be dropped silently")
	}
	withReturn := insert
	withReturn.Return = ReturnBefore
	if _, err := EncodeClientOp(withReturn); err == nil {
		t.Error("an INSERT with a return encoded")
	}
	upsert := opcodecSpecOp()
	upsert.Class = OpUpsert
	upsert.Guard = guard
	if _, err := EncodeClientOp(upsert); err == nil {
		t.Error("an UPSERT with a guard encoded")
	}
}

func TestDeleteRefusesReturnAfter(t *testing.T) {
	del := guardTestDelete()
	del.Return = ReturnAfter
	if _, err := EncodeClientOp(del); err == nil {
		t.Fatal("EncodeClientOp accepted a DELETE that returns after; there is no after")
	}

	del.Return = ReturnBefore
	body, err := EncodeClientOp(del)
	if err != nil {
		t.Fatalf("EncodeClientOp(return before): %v", err)
	}
	if back, err := DecodeClientOp(body); err != nil || back.Return != ReturnBefore {
		t.Fatalf("DecodeClientOp = return %d, %v; want before", back.Return, err)
	}

	// A DELETE body whose trailer names return mode 1 (after) is refused on decode.
	plain, err := EncodeClientOp(guardTestDelete())
	if err != nil {
		t.Fatalf("EncodeClientOp(plain delete): %v", err)
	}
	plain[len(plain)-1] = byte(ReturnAfter) << 1
	if _, err := DecodeClientOp(plain); err == nil {
		t.Fatal("DecodeClientOp accepted a DELETE whose trailer says return after")
	}
}
