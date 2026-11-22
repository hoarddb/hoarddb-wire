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
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// Position is one point in a (slot, database) pair's log: the epoch a record
// was written in and the seq assigned inside that epoch. Copies compare
// positions lexicographically, so a later epoch always wins, and seq restarts
// at 1 in each epoch, which is why the epoch must come first.
// It is printed epoch:seq everywhere, so an operator reads the same text in a
// log line, a CLI row and an error.
type Position struct {
	Epoch, Seq uint64
}

// Less reports whether p comes before q. The epoch decides first and the seq
// only within one epoch: a seq of 999 in epoch 0 is before seq 1 in epoch 1.
func (p Position) Less(q Position) bool {
	if p.Epoch != q.Epoch {
		return p.Epoch < q.Epoch
	}
	return p.Seq < q.Seq
}

// Zero reports whether p is 0:0. That value is the "no earlier epoch"
// sentinel of a prevEnd. It never names a record, because
// the first seq of any epoch is 1.
func (p Position) Zero() bool {
	return p == Position{}
}

// String returns the epoch:seq print form in decimal, for example 41:98213.
func (p Position) String() string {
	return strconv.FormatUint(p.Epoch, 10) + ":" + strconv.FormatUint(p.Seq, 10)
}

// ParsePosition reads the epoch:seq print form back into a Position. Both
// halves are unsigned decimal. An epoch above MaxInt64 parses here and is
// refused at commit, where the BSON bound applies, so this function
// does not decide that refusal.
func ParsePosition(s string) (Position, error) {
	epochText, seqText, ok := strings.Cut(s, ":")
	if !ok {
		return Position{}, fmt.Errorf("position %q: want epoch:seq in decimal, for example 41:98213", s)
	}
	epoch, err := strconv.ParseUint(epochText, 10, 64)
	if err != nil {
		return Position{}, fmt.Errorf("position %q: epoch is not an unsigned decimal: %w", s, err)
	}
	seq, err := strconv.ParseUint(seqText, 10, 64)
	if err != nil {
		return Position{}, fmt.Errorf("position %q: seq is not an unsigned decimal: %w", s, err)
	}
	return Position{Epoch: epoch, Seq: seq}, nil
}

// AppendPosition appends the 16-byte form of p to dst: the epoch then the seq,
// each 8 bytes big-endian. Big-endian is what makes a bytewise compare of two
// encodings agree with Less, so a key range scan reads one pair in log order.
func AppendPosition(dst []byte, p Position) []byte {
	dst = binary.BigEndian.AppendUint64(dst, p.Epoch)
	return binary.BigEndian.AppendUint64(dst, p.Seq)
}

// DecodePosition reads the 16-byte form written by AppendPosition. Any other
// length is refused: a short or long value is a corrupt key or frame, and
// reading its first 16 bytes would name a different write.
func DecodePosition(b []byte) (Position, error) {
	if len(b) != 16 {
		return Position{}, fmt.Errorf("position: want 16 bytes, got %d", len(b))
	}
	return Position{
		Epoch: binary.BigEndian.Uint64(b[0:8]),
		Seq:   binary.BigEndian.Uint64(b[8:16]),
	}, nil
}
