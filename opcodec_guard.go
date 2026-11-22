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
	"math"
)

// GuardTerm is one equality of a guard: the stored value of FieldID must equal Value under
// storedEqual. Value is the type byte then the BSON bytes; a null term (0x0A) also matches an
// absent field.
type GuardTerm struct {
	FieldID uint16
	Value   []byte
}

// Return modes of an Update or Delete. They ride both bodies, so a copy builds the same
// dedupe result the owner did.
const (
	// ReturnNone is today's acknowledgement.
	ReturnNone byte = 0
	// ReturnAfter is the document as applied (Update only).
	ReturnAfter byte = 1
	// ReturnBefore is the document as it was.
	ReturnBefore byte = 2
)

// MaxGuardTerms caps a guard. A guard names a handful of equalities; more is refused so a
// body cannot make the owner read an unbounded number of fields.
const MaxGuardTerms = 16

// The trailer byte that ends every Update and Delete form: bit 0 says a guard section follows
// it, bits 1 and 2 hold the return mode, and every other bit is refused. The trailer is one
// byte on every such body, so a plain update or delete grows by exactly that byte.
const (
	opTrailerGuard  byte = 0x01
	opTrailerReturn byte = 0x06 // mask; the mode sits at bit 1
	opTrailerKnown  byte = opTrailerGuard | opTrailerReturn
)

// opcodecValidateGuard refuses a guard or return on any class but Update and Delete, a return
// mode this build does not know, ReturnAfter on a Delete, and a guard that is not a list of
// equalities on fields 2 and up in ascending order.
func opcodecValidateGuard(op Op) error {
	if len(op.Guard) == 0 && op.Return == ReturnNone {
		return nil
	}
	if op.Class != OpUpdate && op.Class != OpDelete {
		return opcodecError("operation class 0x%02x takes no guard and no return; both belong to UPDATE and DELETE only. Nothing was written; drop Guard and Return", op.Class)
	}
	if op.Return > ReturnBefore {
		return opcodecError("return mode %d is not one of 0 none, 1 after or 2 before", op.Return)
	}
	if op.Class == OpDelete && op.Return == ReturnAfter {
		return opcodecError("DELETE cannot return the document after it is gone; use return before to get the document as it was")
	}
	if len(op.Guard) > MaxGuardTerms {
		return opcodecError("guard has %d terms, more than the %d a guard may name", len(op.Guard), MaxGuardTerms)
	}
	var previous uint16
	for index, term := range op.Guard {
		if term.FieldID < 2 {
			return opcodecError("guard names field id %d; ids 0 and 1 are reserved, so guards name fields from id 2", term.FieldID)
		}
		if index > 0 && term.FieldID <= previous {
			return opcodecError("guard field id %d follows %d; guard terms ascend by field id with no repeats", term.FieldID, previous)
		}
		if len(term.Value) == 0 {
			return opcodecError("guard field %d has no value; a value is its type byte then the BSON bytes", term.FieldID)
		}
		if uint64(len(term.Value)) > math.MaxUint32 {
			return opcodecError("guard field %d value is too long to frame", term.FieldID)
		}
		previous = term.FieldID
	}
	return nil
}

// opcodecAppendTrailer writes [trailer:1] and, when the op carries a guard and record is false,
// [n:1][(fieldID:2 BE ‖ len:4 BE ‖ value) x n]. A record body carries the return but never
// the guard: the guard is checked before the append and is not logged.
func opcodecAppendTrailer(body []byte, op Op, record bool) []byte {
	trailer := op.Return << 1
	guarded := len(op.Guard) != 0 && !record
	if guarded {
		trailer |= opTrailerGuard
	}
	body = append(body, trailer)
	if !guarded {
		return body
	}
	body = append(body, byte(len(op.Guard)))
	for _, term := range op.Guard {
		body = binary.BigEndian.AppendUint16(body, term.FieldID)
		body = binary.BigEndian.AppendUint32(body, uint32(len(term.Value)))
		body = append(body, term.Value...)
	}
	return body
}

// opcodecDecodeTrailer is the inverse of opcodecAppendTrailer. It refuses a bit this build does
// not know, a return mode of 3, a guard of 0 or over MaxGuardTerms terms, and a guard bit in a
// record body; the terms' order, ids and values are checked by opcodecValidate, which every
// decoder runs on the whole op.
func opcodecDecodeTrailer(reader *opcodecReader, op *Op, record bool) error {
	trailer, err := reader.byte("trailer")
	if err != nil {
		return err
	}
	if trailer&^opTrailerKnown != 0 {
		return opcodecError("operation trailer 0x%02x sets a bit this build does not know; the body is from a newer build, so it is refused rather than read as today's", trailer)
	}
	op.Return = (trailer & opTrailerReturn) >> 1
	if trailer&opTrailerGuard == 0 {
		return nil
	}
	if record {
		return opcodecError("a record body never carries a guard; the guard is checked before the append and is not logged")
	}
	count, err := reader.byte("guard count")
	if err != nil {
		return err
	}
	if count == 0 || int(count) > MaxGuardTerms {
		return opcodecError("guard has %d terms, want 1 to %d", count, MaxGuardTerms)
	}
	terms := make([]GuardTerm, 0, count)
	for range count {
		fieldID, err := reader.uint16("guard field id")
		if err != nil {
			return err
		}
		value, err := reader.bytes("guard value")
		if err != nil {
			return err
		}
		terms = append(terms, GuardTerm{FieldID: fieldID, Value: value})
	}
	op.Guard = terms
	return nil
}
