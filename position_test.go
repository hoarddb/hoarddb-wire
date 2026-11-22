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
	"math"
	"testing"
)

// TestPositionOrderIsLexicographic pins the order every copy uses to decide
// which position is later. The epoch decides first: a seq of 5 in epoch 1 is
// after a seq of 999 in epoch 0, because the seq restarts at 1 each epoch.
func TestPositionOrderIsLexicographic(t *testing.T) {
	cases := []struct {
		name string
		p, q Position
		want bool
	}{
		{"later epoch wins over a larger seq", Position{Epoch: 0, Seq: 999}, Position{Epoch: 1, Seq: 1}, true},
		{"earlier epoch with a huge seq is still earlier", Position{Epoch: 1, Seq: 1}, Position{Epoch: 0, Seq: 999}, false},
		{"same epoch orders by seq", Position{Epoch: 41, Seq: 98212}, Position{Epoch: 41, Seq: 98213}, true},
		{"same epoch, larger seq is not less", Position{Epoch: 41, Seq: 98213}, Position{Epoch: 41, Seq: 98212}, false},
		{"equal positions are not less", Position{Epoch: 42, Seq: 7}, Position{Epoch: 42, Seq: 7}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.p.Less(c.q); got != c.want {
				t.Fatalf("%v.Less(%v) = %v, want %v", c.p, c.q, got, c.want)
			}
		})
	}
}

// TestPositionPrintAndParseRoundTrip pins the one print form, epoch:seq in
// decimal (conv §2 prints 41:98213 everywhere), and that ParsePosition reads
// it back to the same value.
func TestPositionPrintAndParseRoundTrip(t *testing.T) {
	p := Position{Epoch: 41, Seq: 98213}
	if got := p.String(); got != "41:98213" {
		t.Fatalf("String() = %q, want %q", got, "41:98213")
	}
	back, err := ParsePosition(p.String())
	if err != nil {
		t.Fatalf("ParsePosition(%q): %v", p.String(), err)
	}
	if back != p {
		t.Fatalf("round trip = %v, want %v", back, p)
	}

	for _, bad := range []string{"", "41", "41:", ":5", "41:98213:1", "-1:5", "x:5", "4198213"} {
		if _, err := ParsePosition(bad); err == nil {
			t.Errorf("ParsePosition(%q) accepted a malformed position", bad)
		}
	}
}

// TestZeroPositionIsNotARealOne pins the zero value as the "no earlier epoch"
// sentinel: prevEnd is 0:0 on an epoch opener with nothing before it (conv
// §10 rule 5), so 0:0 reports Zero and every other position does not.
func TestZeroPositionIsNotARealOne(t *testing.T) {
	if !(Position{}).Zero() {
		t.Fatal("Position{}.Zero() = false, want true: 0:0 is the no-earlier-epoch sentinel")
	}
	if p, err := ParsePosition("0:0"); err != nil || !p.Zero() {
		t.Fatalf("ParsePosition(\"0:0\") = %v, %v; want the zero position", p, err)
	}
	for _, p := range []Position{{Epoch: 0, Seq: 1}, {Epoch: 1, Seq: 0}, {Epoch: 1, Seq: 1}} {
		if p.Zero() {
			t.Errorf("%v.Zero() = true, want false", p)
		}
	}
}

// wireVectors are the 16-byte forms of Position, computed by hand: the epoch
// then the seq, each 8 bytes big-endian. 41:98213 is 0x29 and 0x17FA5 (98213 =
// 1·65536 + 7·4096 + 15·256 + 10·16 + 5), so its seq bytes are 00 00 00 00 00
// 01 7f a5.
var wireVectors = []struct {
	name string
	p    Position
	hex  string
}{
	{"zero", Position{Epoch: 0, Seq: 0}, "00000000000000000000000000000000"},
	{"one and one", Position{Epoch: 1, Seq: 1}, "00000000000000010000000000000001"},
	{"41:98213", Position{Epoch: 41, Seq: 98213}, "00000000000000290000000000017fa5"},
	{"max uint64 both", Position{Epoch: math.MaxUint64, Seq: math.MaxUint64}, "ffffffffffffffffffffffffffffffff"},
}

// TestPositionWireVectorsAreFrozen pins the 16-byte layout: epoch first, seq
// second, both big-endian. Changing it changes every key and frame on disk and
// on the wire, so the vectors are frozen by hand, not by reading the code.
func TestPositionWireVectorsAreFrozen(t *testing.T) {
	for _, v := range wireVectors {
		t.Run(v.name, func(t *testing.T) {
			if got := hex.EncodeToString(AppendPosition(nil, v.p)); got != v.hex {
				t.Fatalf("AppendPosition(%v) = %s, want %s", v.p, got, v.hex)
			}
			raw, err := hex.DecodeString(v.hex)
			if err != nil {
				t.Fatalf("bad vector %q: %v", v.hex, err)
			}
			got, err := DecodePosition(raw)
			if err != nil {
				t.Fatalf("DecodePosition(%s): %v", v.hex, err)
			}
			if got != v.p {
				t.Fatalf("DecodePosition(%s) = %v, want %v", v.hex, got, v.p)
			}
		})
	}
}

// TestPositionWireRoundTrip pins that DecodePosition reads back exactly what
// AppendPosition wrote, and that AppendPosition extends dst instead of
// overwriting it, so a caller can build a key in one buffer.
func TestPositionWireRoundTrip(t *testing.T) {
	prefix := []byte{0xAA, 0xBB}
	for _, v := range wireVectors {
		out := AppendPosition(prefix, v.p)
		if len(out) != 2+16 || out[0] != 0xAA || out[1] != 0xBB {
			t.Fatalf("AppendPosition(prefix, %v) = %x, want the prefix then 16 bytes", v.p, out)
		}
		back, err := DecodePosition(out[2:])
		if err != nil {
			t.Fatalf("DecodePosition(%v): %v", v.p, err)
		}
		if back != v.p {
			t.Fatalf("round trip of %v = %v", v.p, back)
		}
	}
}

// TestPositionWireSortsLikeLess pins why the encoding is big-endian: a plain
// bytewise compare of two encodings must agree with Position.Less, so a key
// range scan reads one pair in log order.
func TestPositionWireSortsLikeLess(t *testing.T) {
	ps := []Position{
		{Epoch: 0, Seq: 0}, {Epoch: 0, Seq: 1}, {Epoch: 0, Seq: 999},
		{Epoch: 1, Seq: 0}, {Epoch: 1, Seq: 1}, {Epoch: 41, Seq: 98213},
		{Epoch: 41, Seq: 98214}, {Epoch: 42, Seq: 0}, {Epoch: 1 << 32, Seq: 0},
		{Epoch: 0, Seq: math.MaxUint64}, {Epoch: math.MaxUint64, Seq: 0},
		{Epoch: math.MaxUint64, Seq: math.MaxUint64},
	}
	for _, p := range ps {
		for _, q := range ps {
			bp := AppendPosition(nil, p)
			bq := AppendPosition(nil, q)
			want := 0
			switch {
			case p.Less(q):
				want = -1
			case q.Less(p):
				want = 1
			}
			if got := bytes.Compare(bp, bq); got != want {
				t.Fatalf("bytes.Compare(enc(%v), enc(%v)) = %d, want %d (Less order)", p, q, got, want)
			}
		}
	}
}

// TestPositionWireRefusesOtherLengths pins that only exactly 16 bytes decode.
// A 15- or 17-byte value is a corrupt key or frame, and reading its first 16
// bytes as a position would name a different write.
func TestPositionWireRefusesOtherLengths(t *testing.T) {
	for _, n := range []int{0, 15, 17, 26} {
		if _, err := DecodePosition(make([]byte, n)); err == nil {
			t.Errorf("DecodePosition of %d bytes was accepted, want a refusal", n)
		}
	}
}
