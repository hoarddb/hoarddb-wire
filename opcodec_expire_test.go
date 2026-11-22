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
	"errors"
	"reflect"
	"testing"
)

// Expiry build E2a: the record body carries the expire items under flags bit 3 and the
// owner's system write under bit 2; a client body carries neither and refuses both.

func TestAWriteWithNoExpiryCostsNoExtraBytes(t *testing.T) {
	for _, op := range opcodecEveryClass() {
		client, err := EncodeClientOp(op)
		if err != nil {
			t.Fatalf("class %#02x: EncodeClientOp: %v", op.Class, err)
		}
		record, err := EncodeOp(op)
		if err != nil {
			t.Fatalf("class %#02x: EncodeOp: %v", op.Class, err)
		}
		if record[1] != 0x00 {
			t.Fatalf("class %#02x: flags are %#02x on a write with no stamp, no expire list and no system flag; want 0x00", op.Class, record[1])
		}
		if len(record) != len(client)+1 {
			t.Fatalf("class %#02x: the record body is %d bytes over the client body, want the flags byte alone", op.Class, len(record)-len(client))
		}
		decoded, err := DecodeOp(record)
		if err != nil || !reflect.DeepEqual(decoded, op) {
			t.Fatalf("class %#02x: DecodeOp = %+v, %v; want the op back", op.Class, decoded, err)
		}
	}
}

func TestExpireItemsRoundTripInARecordBody(t *testing.T) {
	items := []ExpireItem{{FieldID: 7}, {FieldID: 9, Path: []byte("meta.at")}}
	// The whole list after the 40-byte header: [n=2][7][pLen=0][9][pLen=6]["meta.at"].
	const clock = int64(1_700_000_000_000)
	wantList := binary.BigEndian.AppendUint64([]byte{2, 0, 7, 0, 0, 9, 7, 'm', 'e', 't', 'a', '.', 'a', 't'}, uint64(clock))
	for _, class := range []byte{OpDelete, OpStoreVerb} {
		op := opcodecEveryClass()[class-1]
		op.Expire = append([]ExpireItem(nil), items...)
		op.System = true
		op.ExpireNow = clock
		body, err := EncodeOp(op)
		if err != nil {
			t.Fatalf("class %#02x: EncodeOp: %v", class, err)
		}
		if body[1] != 0x0C {
			t.Fatalf("class %#02x: flags are %#02x, want 0x0C (system | expire)", class, body[1])
		}
		gotList := body[2+opcodecHeaderLen : 2+opcodecHeaderLen+len(wantList)]
		if !bytes.Equal(gotList, wantList) {
			t.Fatalf("class %#02x: after the header %x, want %x", class, gotList, wantList)
		}
		decoded, err := DecodeOp(body)
		if err != nil || !reflect.DeepEqual(decoded, op) {
			t.Fatalf("class %#02x: DecodeOp = %+v, %v; want the expire items and system flag back", class, decoded, err)
		}
	}

	// With a stamp, the stamp comes first and the expire list follows it.
	op := opcodecEveryClass()[OpStoreVerb-1]
	op.Stamp = IndexStamp{Fields: []uint16{2}}
	op.Expire = append([]ExpireItem(nil), items...)
	op.ExpireNow = clock
	op.System = true
	body, err := EncodeOp(op)
	if err != nil {
		t.Fatalf("stamped: EncodeOp: %v", err)
	}
	if body[1] != 0x0D {
		t.Fatalf("stamped: flags are %#02x, want 0x0D (stamp | system | expire)", body[1])
	}
	stampLen := 1 + 2*len(op.Stamp.Fields) + 1 + 10*len(op.Stamp.Claims)
	gotList := body[2+opcodecHeaderLen+stampLen : 2+opcodecHeaderLen+stampLen+len(wantList)]
	if !bytes.Equal(gotList, wantList) {
		t.Fatalf("stamped: expire list %x, want %x after the stamp", gotList, wantList)
	}
	decoded, err := DecodeOp(body)
	if err != nil || !reflect.DeepEqual(decoded, op) {
		t.Fatalf("stamped: DecodeOp = %+v, %v; want the stamp, expire items and system flag back", decoded, err)
	}
}

func TestAClientBodyWithTheSystemBitIsRefused(t *testing.T) {
	base := opcodecEveryClass()[OpInsert-1]
	system := base
	system.System = true
	if _, err := EncodeClientOp(system); err == nil {
		t.Fatal("EncodeClientOp accepted the system flag; a client never issues a write to the owner itself")
	}
	expiring := base
	expiring.Expire = []ExpireItem{{FieldID: 4}}
	if _, err := EncodeClientOp(expiring); err == nil {
		t.Fatal("EncodeClientOp accepted expire items; only the ordering owner stamps expiry")
	}

	record, err := EncodeOp(base)
	if err != nil {
		t.Fatal(err)
	}
	mutant := append([]byte(nil), record...)
	mutant[1] = 0x20
	_, err = DecodeOp(mutant)
	var refusal *FlagsError
	if !errors.As(err, &refusal) || refusal.Flags != 0x20 {
		t.Fatalf("DecodeOp of flags 0x20 = %v, want a *FlagsError naming the flags", err)
	}
}

func TestAnExpireClockWithoutItemsIsRefused(t *testing.T) {
	op := opcodecEveryClass()[0]
	op.ExpireNow = 5
	if _, err := EncodeOp(op); err == nil {
		t.Fatal("EncodeOp accepted an expire clock with no expire items: the decoder would read a different op back")
	}
	op.Expire = []ExpireItem{{FieldID: 7}}
	if _, err := EncodeClientOp(op); err == nil {
		t.Fatal("EncodeClientOp accepted expire items and a clock in a client body")
	}
}

func TestTheExpireListIsCanonical(t *testing.T) {
	base := opcodecEveryClass()[0]
	cases := map[string]func(op *Op){
		"unordered ids":     func(op *Op) { op.Expire = []ExpireItem{{FieldID: 9}, {FieldID: 7}} },
		"repeated id":       func(op *Op) { op.Expire = []ExpireItem{{FieldID: 7}, {FieldID: 7}} },
		"items, zero clock": func(op *Op) { op.Expire = []ExpireItem{{FieldID: 7}}; op.ExpireNow = 0 },
	}
	for name, mutate := range cases {
		op := base
		op.ExpireNow = 5
		mutate(&op)
		if _, err := EncodeOp(op); err == nil {
			t.Errorf("%s: EncodeOp accepted a list that two byte strings could name", name)
		}
	}
	// A decoder refuses the same shapes: take a good record and patch the clock to zero.
	op := base
	op.Expire, op.ExpireNow = []ExpireItem{{FieldID: 7}}, 5
	good, err := EncodeOp(op)
	if err != nil {
		t.Fatal(err)
	}
	zeroed := append([]byte(nil), good...)
	offset := 2 + opcodecHeaderLen + 1 + 3
	copy(zeroed[offset:offset+8], make([]byte, 8))
	if _, err := DecodeOp(zeroed); err == nil {
		t.Error("DecodeOp accepted an expire list with a zero clock")
	}
}

// The decoder refuses a zero clock on its own, not only through the validation that follows it: each
// guard is pinned by a test that goes red when that guard alone is removed.
func TestTheExpireDecoderRefusesAZeroClockByItself(t *testing.T) {
	list := []byte{1, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0} // one item (field 7, no path), clock 0
	var op Op
	if err := opcodecDecodeExpire(&opcodecReader{rest: list}, &op); err == nil {
		t.Fatalf("opcodecDecodeExpire accepted a zero clock: %+v", op)
	}
	list[len(list)-1] = 5
	op = Op{}
	if err := opcodecDecodeExpire(&opcodecReader{rest: list}, &op); err != nil || op.ExpireNow != 5 {
		t.Fatalf("a good list: %+v, %v", op, err)
	}
}

// Only the expiry sweep issues a system write, and it issues a delete or a store verb; a record
// with the system bit on any other class is refused with the flags error, like any unknown bit.
func TestTheSystemBitIsRefusedOnEveryClassButDeleteAndStoreVerb(t *testing.T) {
	for _, op := range opcodecEveryClass() {
		record, err := EncodeOp(op)
		if err != nil {
			t.Fatal(err)
		}
		mutant := append([]byte(nil), record...)
		mutant[1] |= opFlagSystem
		_, err = DecodeOp(mutant)
		var refusal *FlagsError
		allowed := op.Class == OpDelete || op.Class == OpStoreVerb
		if allowed && err != nil {
			t.Errorf("class %#02x with the system bit: DecodeOp = %v, want it accepted", op.Class, err)
		}
		if !allowed && (!errors.As(err, &refusal) || refusal.Flags&opFlagSystem == 0) {
			t.Errorf("class %#02x with the system bit: DecodeOp = %v, want a *FlagsError", op.Class, err)
		}
	}
}
