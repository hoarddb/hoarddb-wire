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
	"math"
	"reflect"
	"strings"
	"testing"
)

func bdHex(t *testing.T, text string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(strings.ReplaceAll(text, " ", ""))
	if err != nil {
		t.Fatalf("test vector %q is not hex: %v", text, err)
	}
	return raw
}

// TestReadCanonicalUvarintBoundaries holds the shortest-encoding rule. A length with a
// redundant continuation byte reads as the same number, so two byte strings would name one
// table or one page; the reader takes exactly one.
func TestReadCanonicalUvarintBoundaries(t *testing.T) {
	for _, vector := range []struct {
		name  string
		hex   string
		value uint64
		used  int
		ok    bool
	}{
		{"zero", "00", 0, 1, true},
		{"one", "01", 1, 1, true},
		{"largest of one byte", "7f", 127, 1, true},
		{"smallest of two bytes", "8001", 128, 2, true},
		{"largest of two bytes", "ff7f", 16383, 2, true},
		{"smallest of three bytes", "808001", 16384, 3, true},
		{"1024 as a length", "8008", 1024, 2, true},
		{"largest of nine bytes", "ffffffffffffffff7f", 1<<63 - 1, 9, true},
		{"smallest of ten bytes", "80808080808080808001", 1 << 63, 10, true},
		{"largest value", "ffffffffffffffffff01", math.MaxUint64, 10, true},
		{"bytes after the number are left", "05ffff", 5, 1, true},
		{"bytes after a two-byte number are left", "800100", 128, 2, true},

		{"no bytes", "", 0, 0, false},
		{"cut off after a continuation byte", "80", 0, 0, false},
		{"cut off after nine continuation bytes", "ffffffffffffffffff", 0, 0, false},
		{"zero in two bytes", "8000", 0, 0, false},
		{"one in two bytes", "8100", 0, 0, false},
		{"127 in two bytes", "ff00", 0, 0, false},
		{"zero in three bytes", "808000", 0, 0, false},
		{"128 in three bytes", "808100", 0, 0, false},
		{"1024 in three bytes", "808800", 0, 0, false},
		{"zero in ten bytes", "80808080808080808000", 0, 0, false},
		{"over 64 bits in ten bytes", "ffffffffffffffffff02", 0, 0, false},
		{"eleven bytes", "8080808080808080808001", 0, 0, false},
	} {
		value, used, ok := ReadCanonicalUvarint(bdHex(t, vector.hex))
		if ok != vector.ok || value != vector.value || used != vector.used {
			t.Errorf("%s (%s): ReadCanonicalUvarint = %d, %d, %v; want %d, %d, %v. A number has one encoding, the shortest, and nothing else is read",
				vector.name, vector.hex, value, used, ok, vector.value, vector.used, vector.ok)
		}
	}
}

// TestDecodeEntriesBoundaries holds the entries blob of a list answer to its rules, with the
// decoded values written out: slots 0 to 8191, an id of 1 to 1024 bytes, shortest lengths and
// whole records.
func TestDecodeEntriesBoundaries(t *testing.T) {
	longID := strings.Repeat("50", 1024)
	for _, vector := range []struct {
		name string
		hex  string
		want []ListEntry
	}{
		{"empty blob", "", nil},
		{"slot 0, one-byte id, empty body", "0000 01 10 00", []ListEntry{{Slot: 0, EncID: []byte{0x10}, Body: []byte{}}}},
		{"slot 8191, the last", "1fff 01 10 00", []ListEntry{{Slot: 8191, EncID: []byte{0x10}, Body: []byte{}}}},
		{"an id of 1024 bytes", "0001 8008 " + longID + " 00", []ListEntry{{Slot: 1, EncID: bytes.Repeat([]byte{0x50}, 1024), Body: []byte{}}}},
		{"two records in order", "0005 02 2001 03 aabbcc 1ffe 01 10 01 dd", []ListEntry{
			{Slot: 5, EncID: []byte{0x20, 0x01}, Body: []byte{0xAA, 0xBB, 0xCC}},
			{Slot: 8190, EncID: []byte{0x10}, Body: []byte{0xDD}},
		}},

		{"slot 8192, one past the last", "2000 01 10 00", nil},
		{"slot 8193", "2001 01 10 00", nil},
		{"slot 65535", "ffff 01 10 00", nil},
		{"a second record with slot 8192", "0000 01 10 00 2000 01 10 00", nil},
		{"an id of no bytes", "0000 00 00", nil},
		{"an id of 1025 bytes", "0001 8108 " + longID + "50 00", nil},
		{"an id length in two bytes", "0000 8100 10 00", nil},
		{"a body length in two bytes", "0000 01 10 8000", nil},
		{"cut off inside the slot", "00", nil},
		{"cut off before the id length", "0000", nil},
		{"cut off inside the id", "0000 02 10", nil},
		{"cut off before the body length", "0000 01 10", nil},
		{"cut off inside the body", "0000 01 10 02 aa", nil},
		{"an id length past the bytes left", "0000 ff7f 10 00", nil},
	} {
		raw := bdHex(t, vector.hex)
		entries, err := DecodeEntries(raw)
		refused := vector.want == nil && vector.hex != ""
		switch {
		case refused && err == nil:
			t.Errorf("%s (%s): DecodeEntries took it and returned %d records. A list answer naming a slot that does not exist, or an id no document can have, must be refused", vector.name, vector.hex, len(entries))
		case refused:
			var coded *Error
			if !errors.As(err, &coded) || coded.Code != CodeInvalidArgument {
				t.Errorf("%s: the refusal is %v, want an INVALID_ARGUMENT *Error", vector.name, err)
			}
			if entries != nil {
				t.Errorf("%s: a refused blob returned %d records with its error", vector.name, len(entries))
			}
		case err != nil:
			t.Errorf("%s (%s): DecodeEntries refused it: %v", vector.name, vector.hex, err)
		case !reflect.DeepEqual(entries, vector.want):
			t.Errorf("%s (%s): DecodeEntries = %+v, want %+v", vector.name, vector.hex, entries, vector.want)
		}
	}
}

// TestEncodedComponentLenBoundaries holds the width each tag fixes, as written and inverted:
// one byte short is refused, and bytes after the component are not measured.
func TestEncodedComponentLenBoundaries(t *testing.T) {
	invert := func(raw []byte) []byte {
		out := make([]byte, len(raw))
		for i, b := range raw {
			out[i] = ^b
		}
		return out
	}
	for _, vector := range []struct {
		name  string
		hex   string
		width int // 0 is a refusal
	}{
		{"null", "10", 1},
		{"null and more", "10ffff", 1},
		{"bool", "2001", 2},
		{"bool cut short", "20", 0},
		{"number", "30" + strings.Repeat("80", 17), 18},
		{"number cut short", "30" + strings.Repeat("80", 16), 0},
		{"number and more", "30" + strings.Repeat("80", 18), 18},
		{"datetime", "40" + strings.Repeat("01", 8), 9},
		{"datetime cut short", "40" + strings.Repeat("01", 7), 0},
		{"object id", "70" + strings.Repeat("ab", 12), 13},
		{"object id cut short", "70" + strings.Repeat("ab", 11), 0},
		{"empty string", "500000", 3},
		{"string", "50610000", 4},
		{"string with an escaped zero", "506100ff620000", 7},
		{"string and more", "5061000050", 4},
		{"string with no terminator", "5061", 0},
		{"string ending in half a terminator", "506100", 0},
		{"string with a zero no encoder writes", "50610001620000", 0},
		{"binary", "60de00ffad0000", 7},
		{"binary with no terminator", "60de00ff", 0},
		{"no bytes", "", 0},
		{"tag 0x00", "00", 0},
		{"tag 0x11", "11", 0},
		{"tag 0x80", "80", 0},
	} {
		raw := bdHex(t, vector.hex)
		for _, form := range []struct {
			name    string
			raw     []byte
			measure func([]byte) (int, error)
		}{
			{"as written", raw, EncodedComponentLen},
			{"inverted", invert(raw), EncodedComponentLenDesc},
		} {
			width, err := form.measure(form.raw)
			switch {
			case vector.width == 0 && err == nil:
				t.Errorf("%s, %s (%x): measured %d bytes, want a refusal: a wrong length moves a reader into the next key", vector.name, form.name, form.raw, width)
			case vector.width != 0 && (err != nil || width != vector.width):
				t.Errorf("%s, %s (%x): measured %d (%v), want %d", vector.name, form.name, form.raw, width, err, vector.width)
			}
		}
	}
}

// TestPositionBoundaries holds the two forms of a position at their edges: 16 bytes with the
// epoch first, and epoch:seq in plain decimal up to the largest uint64.
func TestPositionBoundaries(t *testing.T) {
	got, err := DecodePosition(bdHex(t, "0000000000000001 0000000000000002"))
	if want := (Position{Epoch: 1, Seq: 2}); err != nil || got != want {
		t.Errorf("DecodePosition of epoch 1, seq 2 = %v (%v), want %v: the epoch is the first 8 bytes", got, err, want)
	}
	if raw := AppendPosition(nil, Position{Epoch: 1, Seq: 2}); !bytes.Equal(raw, bdHex(t, "0000000000000001 0000000000000002")) {
		t.Errorf("AppendPosition of 1:2 = %x, want the epoch then the seq, 8 bytes each, big-endian", raw)
	}
	for _, vector := range []struct {
		text string
		want Position
		ok   bool
	}{
		{"0:0", Position{}, true},
		{"1:2", Position{Epoch: 1, Seq: 2}, true},
		{"18446744073709551615:18446744073709551615", Position{Epoch: math.MaxUint64, Seq: math.MaxUint64}, true},
		{"18446744073709551616:1", Position{}, false},
		{"1:18446744073709551616", Position{}, false},
		{"-1:1", Position{}, false},
		{"1", Position{}, false},
		{"1:", Position{}, false},
		{":1", Position{}, false},
		{"1:2:3", Position{}, false},
		{"", Position{}, false},
	} {
		got, err := ParsePosition(vector.text)
		if (err == nil) != vector.ok || got != vector.want {
			t.Errorf("ParsePosition(%q) = %v (%v), want %v, taken %v", vector.text, got, err, vector.want, vector.ok)
		}
	}
}
