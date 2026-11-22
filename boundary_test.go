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
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
)

// The acceptance rules of the wire, held at their edges. A golden row and its byte mutations
// reach a decoder's limit only by chance: no row carries slot 8192, a 1025-byte id or a count
// of 101. Each row below builds the input at a size n and says which sizes are taken, with
// every bound written as a number, never as the constant under test, so a constant that moves
// and a comparison that slips by one both fail here.
//
// Where a rule has an encoder and a decoder, the decoder's input is built by hand or from the
// wire document, not by the encoder, and the row requires three things: the two agree on
// accept or refuse, the encoder's bytes are the hand-built bytes, and the refusal is
// INVALID_ARGUMENT.

// bdRow is one rule: try(n) is nil for every n from lowest to highest and a refusal outside.
type bdRow struct {
	name            string
	lowest, highest int
	// step is the distance between two sizes the rule can be asked about; 0 means 1.
	step int
	// below says lowest-step can be built and must be refused.
	below bool
	// plain says the refusal is an ordinary error, not a coded *Error: the function is one a
	// caller wraps in its own refusal.
	plain bool
	try   func(n int) error
}

// bdAgree returns the decoder's verdict, or an uncoded error when the encoder's differs.
func bdAgree(encodeErr, decodeErr error) error {
	if (encodeErr == nil) != (decodeErr == nil) {
		return fmt.Errorf("the encoder and the decoder disagree: the encoder says %v and the decoder says %v", encodeErr, decodeErr)
	}
	return decodeErr
}

// bdSame is bdAgree, and also holds the encoder's bytes to the independently built ones.
func bdSame(encoded []byte, encodeErr error, built []byte, decodeErr error) error {
	if encodeErr == nil && !bytes.Equal(encoded, built) {
		return fmt.Errorf("the encoder wrote %d bytes that are not the %d bytes built by hand from the layout", len(encoded), len(built))
	}
	return bdAgree(encodeErr, decodeErr)
}

// bdID is the key component of the string "u1".
var bdID = []byte{0x50, 'u', '1', 0x00, 0x00}

// bdStringID is a string key component of exactly n bytes: the tag, n-3 letters, the terminator.
func bdStringID(n int) []byte {
	id := append([]byte{0x50}, bytes.Repeat([]byte{'a'}, n-3)...)
	return append(id, 0x00, 0x00)
}

func bdU16(n int) []byte { return binary.BigEndian.AppendUint16(nil, uint16(n)) }

func bdU32(n int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(n)) }

func bdJoin(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// bdReadPayload is a RING_KEYED payload of one read, byte by byte: a prologue of prologueLen
// zero bytes, the class, a header of headerLen zero bytes, the id and the class form.
func bdReadPayload(prologueLen, headerLen int, class byte, encID []byte, form ...[]byte) []byte {
	out := make([]byte, prologueLen, prologueLen+1+headerLen+len(encID)+64)
	out = append(out, class)
	out = append(out, make([]byte, headerLen)...)
	out = append(out, encID...)
	return append(out, bdJoin(form...)...)
}

// bdRead holds one read to both sides: the encoder on the struct, the decoder on the bytes.
func bdRead(read RingRead, form ...[]byte) error {
	built := bdReadPayload(20, 40, read.Class, read.EncID, form...)
	encoded, encodeErr := EncodeRingKeyed(RingKeyedRequest{Read: &read})
	_, decodeErr := DecodeRingKeyed(built)
	return bdSame(encoded, encodeErr, built, decodeErr)
}

// bdDecodeOnly is for a size the encoder cannot be asked about: it has no such member.
func bdDecodeOnly[T any](decode func([]byte) (T, error), raw []byte) error {
	_, err := decode(raw)
	return err
}

// bdInsert is an insert whose client body is exactly bodyLen bytes, padded in its one value.
func bdInsert(bodyLen int) (Op, []byte, error) {
	op := Op{
		Class: OpInsert, EncID: bdID, IDValue: []byte{0x02, 3, 0, 0, 0, 'u', '1', 0},
		Fields: []FieldOp{{FieldID: 2, Value: []byte{0x0A}}},
	}
	smallest, err := EncodeClientOp(op)
	if err != nil {
		return Op{}, nil, fmt.Errorf("the smallest insert does not encode: %w", err)
	}
	if bodyLen < len(smallest) {
		return Op{}, nil, fmt.Errorf("an insert body is at least %d bytes and %d were asked for", len(smallest), bodyLen)
	}
	value := make([]byte, bodyLen-len(smallest)+1)
	value[0] = 0x0A
	op.Fields[0].Value = value
	body, err := EncodeClientOp(op)
	if err != nil {
		return Op{}, nil, fmt.Errorf("an insert of %d bytes does not encode: %w", bodyLen, err)
	}
	if len(body) != bodyLen {
		return Op{}, nil, fmt.Errorf("the insert body is %d bytes and %d were asked for", len(body), bodyLen)
	}
	return op, body, nil
}

func bdDeletes(n int) []Op {
	ops := make([]Op, n)
	for i := range ops {
		ops[i] = Op{Class: OpDelete, EncID: bdID}
	}
	return ops
}

func bdGets(n int) []RingRead {
	reads := make([]RingRead, n)
	for i := range reads {
		reads[i] = RingRead{Class: RingClassGet, EncID: bdID}
	}
	return reads
}

func bdNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("f%d", i)
	}
	return names
}

// bdNameList is the get_fields form of names: a count byte, then a length byte and each name.
func bdNameList(names []string) []byte {
	out := []byte{byte(len(names))}
	for _, name := range names {
		out = append(append(out, byte(len(name))), name...)
	}
	return out
}

func bdMarshal(doc any) []byte {
	raw, err := bson.Marshal(doc)
	if err != nil {
		panic(fmt.Sprintf("a test document does not encode: %v", err)) // a test bug, never an input
	}
	return raw
}

func bdAdmin(req RingAdminRequest) error {
	encoded, encodeErr := EncodeRingAdmin(req)
	built := bdMarshal(adminDocOf(req))
	_, decodeErr := DecodeRingAdmin(built)
	return bdSame(encoded, encodeErr, built, decodeErr)
}

func bdAdminReply(reply RingAdminReply) error {
	encoded, encodeErr := EncodeRingAdminReply(reply)
	built := bdMarshal(adminReplyDocOf(reply))
	_, decodeErr := DecodeRingAdminReply(built)
	return bdSame(encoded, encodeErr, built, decodeErr)
}

func bdReply(reply RingReply) error {
	encoded, encodeErr := EncodeRingReply(reply)
	doc, err := ReplyDocWithAnswers(reply)
	if err != nil {
		return fmt.Errorf("the reply document of the test does not build: %w", err)
	}
	built := bdMarshal(doc)
	_, decodeErr := DecodeRingReply(built)
	return bdSame(encoded, encodeErr, built, decodeErr)
}

func bdList(req RingListRequest) error {
	encoded, encodeErr := EncodeRingList(req)
	built := bdMarshal(ringListDocOf(req))
	_, decodeErr := DecodeRingList(built)
	return bdSame(encoded, encodeErr, built, decodeErr)
}

func bdOptions(o RingReadOptions) error {
	encoded, encodeErr := EncodeRingReadOptions(o)
	doc := bdMarshal(ringOptionsDocOf(o))
	built := append(bdU16(len(doc)), doc...)
	_, used, decodeErr := DecodeRingReadOptions(built)
	if decodeErr == nil && used != len(built) {
		return fmt.Errorf("the options decoder took %d of %d bytes", used, len(built))
	}
	return bdSame(encoded, encodeErr, built, decodeErr)
}

// bdSizedOptions is a read options trailer of exactly n bytes, its 2-byte length included.
func bdSizedOptions(n int) (RingReadOptions, error) {
	options := func(pad int) RingReadOptions {
		where := bdMarshal(bson.D{{Key: "a", Value: strings.Repeat("x", pad)}})
		return RingReadOptions{Local: true, Expand: []RingExpandSpec{{Field: "f", Where: where}}}
	}
	smallest := 2 + len(bdMarshal(ringOptionsDocOf(options(0))))
	if n < smallest {
		return RingReadOptions{}, fmt.Errorf("the trailer of the test is at least %d bytes and %d were asked for", smallest, n)
	}
	sized := options(n - smallest)
	if got := 2 + len(bdMarshal(ringOptionsDocOf(sized))); got != n {
		return RingReadOptions{}, fmt.Errorf("the trailer of the test is %d bytes and %d were asked for", got, n)
	}
	return sized, nil
}

func bdSeedRow() RingAdminSeedRow {
	return RingAdminSeedRow{Database: "app", Slot: 7, State: string(SeedSeeding)}
}

func bdInt32(n int) *int32 {
	v := int32(n)
	return &v
}

func bdOrNil(raw []byte) []byte {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

func bdFill(n int, b byte) []byte { return bdOrNil(bytes.Repeat([]byte{b}, n)) }

func bdOK(ok bool) error {
	if ok {
		return nil
	}
	return errors.New("refused")
}

var bdRows = []bdRow{
	// The frame: a prologue of 20 bytes, a header of 40.
	{name: "keyed read: prologue bytes", lowest: 20, highest: 20, below: true, try: func(n int) error {
		return bdDecodeOnly(DecodeRingKeyed, bdReadPayload(n, 40, 0x81, bdID))
	}},
	{name: "keyed read: header bytes", lowest: 40, highest: 40, below: true, try: func(n int) error {
		return bdDecodeOnly(DecodeRingKeyed, bdReadPayload(20, n, 0x81, bdID))
	}},
	{name: "client body: header bytes", lowest: 40, highest: 40, below: true, try: func(n int) error {
		return bdDecodeOnly(DecodeClientOp, bdReadPayload(0, n, 0x04, bdID, []byte{0x00}))
	}},
	{name: "record body: header bytes", lowest: 40, highest: 40, below: true, try: func(n int) error {
		return bdDecodeOnly(DecodeOp, bdJoin([]byte{0x04, 0x00}, make([]byte, n), bdID, []byte{0x00}))
	}},
	{name: "keyed request: payload bytes", lowest: 200, highest: 33554432, try: func(n int) error {
		op, body, err := bdInsert(n - 20)
		if err != nil {
			return err
		}
		built := append(make([]byte, 20, n), body...)
		encoded, encodeErr := EncodeRingKeyed(RingKeyedRequest{Op: op})
		_, decodeErr := DecodeRingKeyed(built)
		return bdSame(encoded, encodeErr, built, decodeErr)
	}},
	{name: "batch: payload bytes the decoder takes", lowest: 200, highest: 33554432, try: func(n int) error {
		_, body, err := bdInsert(n - 21)
		if err != nil {
			return err
		}
		return bdDecodeOnly(DecodeRingBatch, append(append(make([]byte, 20, n), 0x01), body...))
	}},
	// A frame's length counts the opcode byte and the 16-byte request id as well.
	{name: "batch: payload bytes the encoder writes", lowest: 200, highest: 33554415, try: func(n int) error {
		op, _, err := bdInsert(n - 21)
		if err != nil {
			return err
		}
		encoded, err := EncodeRingBatch(RingBatchRequest{Ops: []Op{op}})
		if err == nil && len(encoded) != n {
			return fmt.Errorf("the batch is %d bytes and %d were asked for", len(encoded), n)
		}
		return err
	}},
	{name: "reply: bytes the encoder writes", lowest: 200, highest: 33554415, try: func(n int) error {
		smallest := len(bdMarshal(ReplyDocOf(RingReply{Elements: [][]byte{{0}}})))
		reply := RingReply{Elements: [][]byte{make([]byte, n-smallest+1)}}
		encoded, err := EncodeRingReply(reply)
		if err == nil && len(encoded) != n {
			return fmt.Errorf("the reply is %d bytes and %d were asked for", len(encoded), n)
		}
		return err
	}},

	// Counts of one byte.
	{name: "CheckBatchSize: operations", lowest: 0, highest: 255, below: true, try: CheckBatchSize},
	{name: "CheckRingBatchSize: operations", lowest: 1, highest: 255, below: true, try: CheckRingBatchSize},
	{name: "batch: operations", lowest: 1, highest: 255, below: true, try: func(n int) error {
		encoded, encodeErr := EncodeRingBatch(RingBatchRequest{Ops: bdDeletes(n)})
		switch {
		case n == 0:
			return bdAgree(encodeErr, bdDecodeOnly(DecodeRingBatch, make([]byte, 21)))
		case encodeErr != nil:
			return encodeErr
		}
		decoded, err := DecodeRingBatch(encoded)
		if err == nil && len(decoded.Ops) != n {
			return fmt.Errorf("a batch of %d operations decoded as %d", n, len(decoded.Ops))
		}
		return err
	}},
	{name: "CheckRingGetManySize: keys", lowest: 1, highest: 255, below: true, try: CheckRingGetManySize},
	{name: "get-many: keys", lowest: 1, highest: 255, below: true, try: func(n int) error {
		encoded, encodeErr := EncodeRingGetMany(RingGetManyRequest{Reads: bdGets(n)})
		switch {
		case n == 0:
			return bdAgree(encodeErr, bdDecodeOnly(DecodeRingGetMany, make([]byte, 21)))
		case encodeErr != nil:
			return encodeErr
		}
		decoded, err := DecodeRingGetMany(encoded)
		if err == nil && len(decoded.Reads) != n {
			return fmt.Errorf("a get-many of %d keys decoded as %d", n, len(decoded.Reads))
		}
		return err
	}},
	{name: "batch reply: answers", lowest: 1, highest: 255, below: true, try: func(n int) error {
		answers := make([]RingAnswer, n)
		for i := range answers {
			answers[i] = RingAnswer{Reply: &RingReply{WriteOutcome: OutcomeApplied}}
		}
		return bdReply(RingReply{Outcome: BatchOK, Answers: answers})
	}},

	// The document id.
	{name: "get: id bytes", lowest: 3, highest: 1024, try: func(n int) error {
		return bdRead(RingRead{Class: RingClassGet, EncID: bdStringID(n)})
	}},
	{name: "client body: id bytes", lowest: 3, highest: 1024, try: func(n int) error {
		id := bdStringID(n)
		built := bdReadPayload(0, 40, 0x04, id, []byte{0x00})
		encoded, encodeErr := EncodeClientOp(Op{Class: OpDelete, EncID: id})
		_, decodeErr := DecodeClientOp(built)
		return bdSame(encoded, encodeErr, built, decodeErr)
	}},
	{name: "record body: id bytes", lowest: 3, highest: 1024, try: func(n int) error {
		id := bdStringID(n)
		built := bdJoin([]byte{0x04, 0x00}, make([]byte, 40), id, []byte{0x00})
		encoded, encodeErr := EncodeOp(Op{Class: OpDelete, EncID: id})
		_, decodeErr := DecodeOp(built)
		return bdSame(encoded, encodeErr, built, decodeErr)
	}},
	{name: "entries: id bytes", lowest: 1, highest: 1024, below: true, try: func(n int) error {
		blob := bdJoin([]byte{0x00, 0x05}, binary.AppendUvarint(nil, uint64(n)), bytes.Repeat([]byte{0x50}, n), []byte{0x00})
		return bdDecodeOnly(DecodeEntries, blob)
	}},
	{name: "entries: slot", lowest: 0, highest: 8191, try: func(n int) error {
		return bdDecodeOnly(DecodeEntries, bdJoin(bdU16(n), []byte{0x01, 0x10, 0x00}))
	}},

	// The reads.
	{name: "peek: count", lowest: 1, highest: 100, below: true, try: func(n int) error {
		return bdRead(RingRead{Class: RingClassPeek, EncID: bdID, FieldID: 1, Limit: uint16(n)}, bdU16(1), bdU16(n))
	}},
	{name: "store list: limit", lowest: 1, highest: 1000, below: true, try: func(n int) error {
		return bdRead(RingRead{Class: RingClassStoreList, EncID: bdID, FieldID: 1, Limit: uint16(n)}, bdU16(1), bdU16(n), bdU16(0))
	}},
	{name: "store list: cursor bytes", lowest: 0, highest: 2059, try: func(n int) error {
		after := bdFill(n, 'c')
		return bdRead(RingRead{Class: RingClassStoreList, EncID: bdID, FieldID: 1, Limit: 1, After: after}, bdU16(1), bdU16(1), bdU16(n), after)
	}},
	{name: "aggregate read: limit", lowest: 0, highest: 10000, try: func(n int) error {
		read := RingRead{Class: RingClassAggregate, EncID: bdID, FieldID: 1, Limit: uint16(n), Aggregate: &RingAggregateRead{Kind: 0}}
		return bdRead(read, bdU16(1), []byte{0x00, 0x00, 0x00}, bdU16(n), bdU16(0))
	}},
	{name: "aggregate read: kind", lowest: 0, highest: 3, try: func(n int) error {
		aggregate := &RingAggregateRead{Kind: uint8(n)}
		field, by := "", ""
		if n == 1 || n == 2 || n > 3 {
			field = "f"
		}
		if n == 2 || n == 3 {
			by = "b"
		}
		aggregate.Field, aggregate.By = field, by
		read := RingRead{Class: RingClassAggregate, EncID: bdID, FieldID: 1, Aggregate: aggregate}
		return bdRead(read, bdU16(1), []byte{byte(n), byte(len(field))}, []byte(field), []byte{byte(len(by))}, []byte(by), bdU16(0), bdU16(0))
	}},
	{name: "nearest: k", lowest: 1, highest: 100, below: true, try: func(n int) error {
		read := RingRead{Class: RingClassNearest, EncID: bdID, FieldID: 1, Nearest: &RingNearest{Metric: 1, K: uint8(n), Field: "v", Vector: make([]byte, 4)}}
		return bdRead(read, bdU16(1), []byte{0x01, byte(n), 0x01, 'v'}, bdU16(4), make([]byte, 4))
	}},
	{name: "nearest: metric", lowest: 1, highest: 3, below: true, try: func(n int) error {
		read := RingRead{Class: RingClassNearest, EncID: bdID, FieldID: 1, Nearest: &RingNearest{Metric: byte(n), K: 1, Field: "v", Vector: make([]byte, 4)}}
		return bdRead(read, bdU16(1), []byte{byte(n), 0x01, 0x01, 'v'}, bdU16(4), make([]byte, 4))
	}},
	{name: "nearest: vector bytes", lowest: 4, highest: 16384, step: 4, below: true, try: func(n int) error {
		read := RingRead{Class: RingClassNearest, EncID: bdID, FieldID: 1, Nearest: &RingNearest{Metric: 1, K: 1, Field: "v", Vector: make([]byte, n)}}
		return bdRead(read, bdU16(1), []byte{0x01, 0x01, 0x01, 'v'}, bdU16(n), make([]byte, n))
	}},
	{name: "nearest: name bytes", lowest: 1, highest: 255, below: true, try: func(n int) error {
		name := strings.Repeat("v", n)
		read := RingRead{Class: RingClassNearest, EncID: bdID, FieldID: 1, Nearest: &RingNearest{Metric: 1, K: 1, Field: name, Vector: make([]byte, 4)}}
		if n > 255 { // one byte of length: the decoder cannot be sent it
			_, err := EncodeRingKeyed(RingKeyedRequest{Read: &read})
			return err
		}
		return bdRead(read, bdU16(1), []byte{0x01, 0x01, byte(n)}, []byte(name), bdU16(4), make([]byte, 4))
	}},
	{name: "store_get: key bytes", lowest: 1, highest: 1024, below: true, try: func(n int) error {
		key := strings.Repeat("k", n)
		return bdRead(RingRead{Class: RingClassStoreGet, EncID: bdID, FieldID: 1, Key: key}, bdU16(1), bdU16(n), []byte(key))
	}},
	{name: "get_fields: names", lowest: 1, highest: 64, below: true, try: func(n int) error {
		names := bdNames(n)
		return bdRead(RingRead{Class: RingClassGetFields, EncID: bdID, Fields: names}, bdNameList(names))
	}},
	{name: "get_fields: name bytes", lowest: 1, highest: 255, below: true, try: func(n int) error {
		names := []string{strings.Repeat("n", n)}
		read := RingRead{Class: RingClassGetFields, EncID: bdID, Fields: names}
		if n > 255 {
			_, err := EncodeRingKeyed(RingKeyedRequest{Read: &read})
			return err
		}
		return bdRead(read, bdNameList(names))
	}},
	{name: "CheckFieldNames: names", lowest: 1, highest: 64, below: true, try: func(n int) error {
		return CheckFieldNames("fields", bdNames(n))
	}},
	{name: "CheckRingGetFields: names", lowest: 1, highest: 64, below: true, try: func(n int) error {
		return CheckRingGetFields(bdNames(n))
	}},

	// The read options trailer.
	{name: "read options: expand specs", lowest: 1, highest: 8, try: func(n int) error {
		specs := make([]RingExpandSpec, n)
		for i := range specs {
			specs[i] = RingExpandSpec{Field: fmt.Sprintf("f%d", i)}
		}
		return bdOptions(RingReadOptions{Expand: specs})
	}},
	{name: "read options: trailer bytes", lowest: 200, highest: 4096, try: func(n int) error {
		options, err := bdSizedOptions(n)
		if err != nil {
			return err
		}
		return bdOptions(options)
	}},

	// The write trailer and the stamps.
	{name: "guard: terms", lowest: 1, highest: 16, below: true, try: func(n int) error {
		terms := make([]GuardTerm, n)
		built := bdReadPayload(0, 40, 0x04, bdID, []byte{0x01, byte(n)})
		for i := range terms {
			terms[i] = GuardTerm{FieldID: uint16(2 + i), Value: []byte{0x0A}}
			built = append(built, bdJoin(bdU16(2+i), bdU32(1), []byte{0x0A})...)
		}
		_, decodeErr := DecodeClientOp(built)
		if n == 0 { // no terms is no guard to the encoder; the decoder is sent the guard bit over a count of 0
			return decodeErr
		}
		encoded, encodeErr := EncodeClientOp(Op{Class: OpDelete, EncID: bdID, Guard: terms})
		return bdSame(encoded, encodeErr, built, decodeErr)
	}},
	{name: "return mode", lowest: 0, highest: 2, try: func(n int) error {
		op := Op{Class: OpUpdate, EncID: bdID, Fields: []FieldOp{{FieldID: 2, OpCode: UpdateUnset}}, Return: byte(n)}
		encoded, encodeErr := EncodeClientOp(op)
		if encodeErr != nil {
			return encodeErr
		}
		decoded, err := DecodeClientOp(encoded)
		if err == nil && decoded.Return != byte(n) {
			return fmt.Errorf("return mode %d decoded as %d", n, decoded.Return)
		}
		return err
	}},
	{name: "aggregate stamp: aggregates", lowest: 1, highest: 8, try: func(n int) error {
		stamps := make([]AggregateStamp, n)
		built := bdJoin([]byte{0x05, 0x10}, make([]byte, 40), []byte{byte(n)})
		for i := range stamps {
			stamps[i] = AggregateStamp{AggID: uint8(i + 1), Kind: 1, Field: "f"}
			built = append(built, byte(i+1), 0x01, 0x01, 'f', 0x00)
		}
		op := Op{Class: OpStoreVerb, EncID: bdID, Fields: []FieldOp{{FieldID: 4, Verb: VerbPush, Value: []byte("arg")}}, Aggregates: stamps}
		plain, err := EncodeOp(Op{Class: op.Class, EncID: op.EncID, Fields: op.Fields})
		if err != nil {
			return fmt.Errorf("the unstamped store verb does not encode: %w", err)
		}
		// The stamp sits between the header and the form: the unstamped record gives the form.
		built = append(built, plain[2+40:]...)
		encoded, encodeErr := EncodeOp(op)
		_, decodeErr := DecodeOp(built)
		return bdSame(encoded, encodeErr, built, decodeErr)
	}},
	{name: "index stamp: fields", lowest: 1, highest: 255, try: func(n int) error {
		fields := make([]uint16, n)
		for i := range fields {
			fields[i] = uint16(i + 2)
		}
		encoded, err := EncodeOp(Op{Class: OpDelete, EncID: bdID, Stamp: IndexStamp{Fields: fields}})
		if err != nil {
			return err
		}
		decoded, err := DecodeOp(encoded)
		if err == nil && len(decoded.Stamp.Fields) != n {
			return fmt.Errorf("a stamp of %d fields decoded as %d", n, len(decoded.Stamp.Fields))
		}
		return err
	}},
	{name: "expire list: items", lowest: 1, highest: 255, try: func(n int) error {
		items := make([]ExpireItem, n)
		for i := range items {
			items[i] = ExpireItem{FieldID: uint16(i + 2)}
		}
		encoded, err := EncodeOp(Op{Class: OpDelete, EncID: bdID, Expire: items, ExpireNow: 1700000000000, System: true})
		if err != nil {
			return err
		}
		decoded, err := DecodeOp(encoded)
		if err == nil && len(decoded.Expire) != n {
			return fmt.Errorf("an expire list of %d items decoded as %d", n, len(decoded.Expire))
		}
		return err
	}},

	// The store verb arguments.
	{name: "CheckPopCount", lowest: 1, highest: 100, below: true, try: func(n int) error { return CheckPopCount(int64(n)) }},
	{name: "EncodePopArg: n", lowest: 1, highest: 100, below: true, try: func(n int) error {
		arg, err := EncodePopArg(n)
		if err == nil && !bytes.Equal(arg, bdMarshal(bson.D{{Key: "n", Value: int32(n)}})) {
			return fmt.Errorf("the pop arg for %d is not the document {n: %d}", n, n)
		}
		return err
	}},
	{name: "CheckVerbKey: key bytes", lowest: 1, highest: 1024, below: true, try: func(n int) error {
		return CheckVerbKey("k", strings.Repeat("k", n))
	}},
	{name: "EncodeTakeKeyArg: key bytes", lowest: 1, highest: 1024, below: true, try: func(n int) error {
		_, err := EncodeTakeKeyArg(strings.Repeat("k", n))
		return err
	}},
	{name: "CheckVerbArg: arg bytes", lowest: 14, highest: 1048576, try: func(n int) error {
		arg := bdMarshal(ElementArgDoc{V: make([]byte, n-13)})
		if len(arg) != n {
			return fmt.Errorf("the arg of the test is %d bytes and %d were asked for", len(arg), n)
		}
		return CheckVerbArg(VerbPush, arg)
	}},
	{name: "EncodeElementArg: element bytes", lowest: 1, highest: 1048563, below: true, try: func(n int) error {
		arg, err := EncodeElementArg(make([]byte, n))
		if err == nil && len(arg) != n+13 {
			return fmt.Errorf("an element of %d bytes made an arg of %d, want 13 more", n, len(arg))
		}
		return err
	}},
	{name: "CheckIncrements: paths", lowest: 1, highest: 64, below: true, try: func(n int) error {
		increments := make(bson.D, n)
		for i := range increments {
			increments[i] = bson.E{Key: fmt.Sprintf("p%d", i), Value: int32(1)}
		}
		return CheckIncrements(bdMarshal(increments))
	}},
	{name: "CheckIncrements: path levels", lowest: 1, highest: 3, try: func(n int) error {
		path := strings.TrimSuffix(strings.Repeat("a.", n), ".")
		return CheckIncrements(bdMarshal(bson.D{{Key: path, Value: int32(1)}}))
	}},

	// RING_LIST and RING_COUNT.
	{name: "list: database name bytes", lowest: 1, highest: 1021, below: true, try: func(n int) error {
		return bdList(RingListRequest{Database: strings.Repeat("d", n), Bucket: "b"})
	}},
	{name: "list: bucket name bytes", lowest: 1, highest: 1021, below: true, try: func(n int) error {
		return bdList(RingListRequest{Database: "d", Bucket: strings.Repeat("b", n)})
	}},
	{name: "list: cursor bytes", lowest: 0, highest: 2059, try: func(n int) error {
		return bdList(RingListRequest{Database: "d", Bucket: "b", Cursor: bdFill(n, 'c')})
	}},
	{name: "list: field names", lowest: 1, highest: 64, try: func(n int) error {
		return bdList(RingListRequest{Database: "d", Bucket: "b", Fields: bdNames(n)})
	}},
	{name: "count: database name bytes", lowest: 1, highest: 1021, below: true, try: func(n int) error {
		req := RingCountRequest{Database: strings.Repeat("d", n), Bucket: "b"}
		encoded, encodeErr := EncodeRingCount(req)
		built := bdMarshal(ringCountDocOf(req))
		_, decodeErr := DecodeRingCount(built)
		return bdSame(encoded, encodeErr, built, decodeErr)
	}},

	// RING_ADMIN.
	{name: "create_database: name bytes", lowest: 1, highest: 1021, below: true, try: func(n int) error {
		return bdAdmin(RingAdminRequest{Cmd: AdminCreateDatabase, Name: strings.Repeat("d", n)})
	}},
	{name: "create_bucket: fields", lowest: 0, highest: 999, try: func(n int) error {
		fields := make([]RingAdminField, n)
		for i := range fields {
			fields[i] = RingAdminField{Name: fmt.Sprintf("f%d", i), Type: "string"}
		}
		return bdAdmin(RingAdminRequest{Cmd: AdminCreateBucket, Database: "d", Name: "b", Fields: fields})
	}},
	{name: "create_bucket: field name bytes", lowest: 1, highest: 255, below: true, try: func(n int) error {
		fields := []RingAdminField{{Name: strings.Repeat("f", n), Type: "string"}}
		return bdAdmin(RingAdminRequest{Cmd: AdminCreateBucket, Database: "d", Name: "b", Fields: fields})
	}},
	{name: "seeding_status: limit", lowest: 0, highest: 1000, below: true, try: func(n int) error {
		return bdAdmin(RingAdminRequest{Cmd: AdminSeedingStatus, Limit: bdInt32(n)})
	}},
	{name: "seeding_status: cursor bytes", lowest: 0, highest: 1023, try: func(n int) error {
		return bdAdmin(RingAdminRequest{Cmd: AdminSeedingStatus, Cursor: bdFill(n, 'c')})
	}},
	{name: "seeding_retry: slot", lowest: 0, highest: 8191, below: true, try: func(n int) error {
		return bdAdmin(RingAdminRequest{Cmd: AdminSeedingRetry, Database: "d", Slot: bdInt32(n)})
	}},
	{name: "show_buckets reply: buckets", lowest: 0, highest: 10000, try: func(n int) error {
		buckets := make([]RingAdminListed, n)
		for i := range buckets {
			buckets[i] = RingAdminListed{Name: fmt.Sprintf("b%d", i), BucketID: uint64(i + 1), State: "active"}
		}
		return bdAdminReply(RingAdminReply{Cmd: AdminShowBuckets, Buckets: buckets})
	}},
	{name: "describe_bucket reply: field table entries", lowest: 0, highest: 1000, try: func(n int) error {
		fields := make([]RingAdminReplyField, n)
		for i := range fields {
			fields[i] = RingAdminReplyField{ID: uint16(i + 1), Name: fmt.Sprintf("f%d", i), Type: "string"}
		}
		return bdAdminReply(RingAdminReply{Cmd: AdminDescribeBucket, BucketID: 9, State: "active", Fields: fields})
	}},
	{name: "membership reply: result bytes", lowest: 1, highest: 512, below: true, try: func(n int) error {
		return bdAdminReply(RingAdminReply{Cmd: AdminJoin, Membership: &RingAdminMembership{Epoch: 1, StateSeq: 1, Result: strings.Repeat("r", n)}})
	}},
	{name: "seeding_status reply: rows", lowest: 0, highest: 1000, try: func(n int) error {
		rows := make([]RingAdminSeedRow, n)
		for i := range rows {
			rows[i] = bdSeedRow()
		}
		return bdAdminReply(RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: rows}})
	}},
	{name: "seeding_status reply: cursor bytes", lowest: 1, highest: 1023, try: func(n int) error {
		seeding := &RingAdminSeeding{Rows: []RingAdminSeedRow{bdSeedRow()}, More: true, Cursor: bdFill(n, 'c')}
		return bdAdminReply(RingAdminReply{Cmd: AdminSeedingStatus, Seeding: seeding})
	}},
	{name: "CheckSeedingRow: slot", lowest: 0, highest: 8191, below: true, try: func(n int) error {
		row := bdSeedRow()
		row.Slot = int32(n)
		return CheckSeedingRow(0, row)
	}},
	{name: "CheckSeedingRow: database name bytes", lowest: 1, highest: 1021, below: true, try: func(n int) error {
		row := bdSeedRow()
		row.Database = strings.Repeat("d", n)
		return CheckSeedingRow(0, row)
	}},
	{name: "CheckSeedingRow: source bytes", lowest: 0, highest: 256, try: func(n int) error {
		row := bdSeedRow()
		row.Source = strings.Repeat("s", n)
		return CheckSeedingRow(0, row)
	}},
	{name: "CheckSeedingRow: refusal bytes", lowest: 0, highest: 512, try: func(n int) error {
		row := bdSeedRow()
		row.Refusal = strings.Repeat("r", n)
		return CheckSeedingRow(0, row)
	}},
	{name: "CheckAdminNode: refused peers", lowest: 0, highest: 64, try: func(n int) error {
		peers := make([]string, n)
		for i := range peers {
			peers[i] = "peer"
		}
		return CheckAdminNode(&RingAdminNode{NodeID: "n", Version: "v", DefaultRF: 1, Serving: ServingOK, RefusedPeers: peers})
	}},
	{name: "CheckAdminNode: node id bytes", lowest: 1, highest: 1021, below: true, try: func(n int) error {
		return CheckAdminNode(&RingAdminNode{NodeID: strings.Repeat("n", n), Version: "v", DefaultRF: 1, Serving: ServingOK})
	}},

	// Replies and errors.
	{name: "reply: served_by bytes", lowest: 1, highest: 256, try: func(n int) error {
		return bdReply(RingReply{ServedBy: strings.Repeat("n", n)})
	}},
	{name: "reply: cursor bytes", lowest: 1, highest: 2059, try: func(n int) error {
		return bdReply(RingReply{Cursor: bdFill(n, 'c')})
	}},
	{name: "reply: key bytes", lowest: 1, highest: 1024, below: true, try: func(n int) error {
		return bdReply(RingReply{Elements: [][]byte{{0x05, 0, 0, 0, 0}}, Keys: []string{strings.Repeat("k", n)}})
	}},
	{name: "reply: lease bytes", lowest: 16, highest: 16, below: true, try: func(n int) error {
		return bdReply(RingReply{Lease: bdFill(n, 0xA0)})
	}},
	{name: "error: message bytes", lowest: 1, highest: 65536, below: true, try: func(n int) error {
		refusal := RingError{Code: CodeInvalidArgument, Message: strings.Repeat("m", n)}
		encoded, encodeErr := EncodeRingError(refusal)
		doc, err := ringErrorDocOf(refusal)
		if err != nil {
			return fmt.Errorf("the error document of the test does not build: %w", err)
		}
		built := bdMarshal(doc)
		_, decodeErr := DecodeRingError(built)
		return bdSame(encoded, encodeErr, built, decodeErr)
	}},
	{name: "error: detail node bytes", lowest: 1, highest: 256, below: true, try: func(n int) error {
		refusal := RingError{Code: CodeLeaseLapsed, Message: "m", Detail: &Detail{Node: strings.Repeat("n", n), LapsedMs: 5}}
		encoded, encodeErr := EncodeRingError(refusal)
		doc, err := ringErrorDocOf(refusal)
		if err != nil {
			return fmt.Errorf("the error document of the test does not build: %w", err)
		}
		built := bdMarshal(doc)
		_, decodeErr := DecodeRingError(built)
		return bdSame(encoded, encodeErr, built, decodeErr)
	}},

	// Fixed widths. These functions return an ordinary error or a bool; their callers refuse.
	{name: "DecodePosition: bytes", lowest: 16, highest: 16, below: true, plain: true, try: func(n int) error {
		_, err := DecodePosition(make([]byte, n))
		return err
	}},
	{name: "ReadNumber: int32 bytes", lowest: 4, highest: 4, below: true, plain: true, try: func(n int) error {
		_, ok := ReadNumber(bsontype.Int32, make([]byte, n))
		return bdOK(ok)
	}},
	{name: "ReadNumber: int64 bytes", lowest: 8, highest: 8, below: true, plain: true, try: func(n int) error {
		_, ok := ReadNumber(bsontype.Int64, make([]byte, n))
		return bdOK(ok)
	}},
	{name: "ReadNumber: double bytes", lowest: 8, highest: 8, below: true, plain: true, try: func(n int) error {
		_, ok := ReadNumber(bsontype.Double, make([]byte, n))
		return bdOK(ok)
	}},
}

// TestWireLimitsHoldAtTheirBoundaries asks every rule of bdRows at its lowest size, one step
// under its highest, its highest, and one step past each end.
func TestWireLimitsHoldAtTheirBoundaries(t *testing.T) {
	seen := map[string]bool{}
	for _, row := range bdRows {
		if seen[row.name] {
			t.Fatalf("bdRows names %q twice", row.name)
		}
		seen[row.name] = true
		t.Run(row.name, func(t *testing.T) {
			step := max(row.step, 1)
			taken := map[int]bool{row.lowest: true, row.highest: true}
			if row.highest-step >= row.lowest {
				taken[row.highest-step] = true
			}
			for n := range taken {
				if err := row.try(n); err != nil {
					t.Errorf("%d is refused and the rule takes %d to %d: %v", n, row.lowest, row.highest, err)
				}
			}
			refused := []int{row.highest + step}
			if row.below {
				refused = append(refused, row.lowest-step)
			}
			for _, n := range refused {
				err := row.try(n)
				var coded *Error
				switch {
				case err == nil:
					t.Errorf("%d is taken and the rule takes %d to %d only. A request the other side refuses, or a count it cannot hold, now passes this check", n, row.lowest, row.highest)
				case row.plain:
				case !errors.As(err, &coded):
					t.Errorf("%d is refused with an error that carries no code: %v", n, err)
				case coded.Code != CodeInvalidArgument:
					t.Errorf("%d is refused with %s, want INVALID_ARGUMENT: %v", n, coded.Code, err)
				}
			}
		})
	}
}
