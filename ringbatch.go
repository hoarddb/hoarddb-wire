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
	"errors"
	"fmt"
)

// RingBatchRequest is one RING_BATCH payload: one
// client prologue and 1 to 255 write operations. The prologue is once because a frame is one
// routing decision and one deadline budget.
type RingBatchRequest struct {
	// Prologue is the client prologue of the whole frame.
	Prologue RingPrologue
	// Ops are the write operations in request order; index i is the op's dedupe index.
	Ops []Op
}

// CheckRingBatchSize refuses a batch of n operations that cannot be sent: none, or more than
// 255 (each operation is numbered in one byte). A client calls it as it queues an operation, so
// the refusal comes before anything is sent.
func CheckRingBatchSize(n int) error {
	if n == 0 {
		return Refusal("a batch carries at least one operation and this one has none. Nothing was sent; queue one to 255 write operations")
	}
	return CheckBatchSize(n)
}

// ringBatchOpError says which operation of the batch an error belongs to, keeping its code.
func ringBatchOpError(index int, err error) error {
	var coded *Error
	if errors.As(err, &coded) {
		return &Error{Code: coded.Code, Message: fmt.Sprintf("operation %d of the batch: %s", index, coded.Message)}
	}
	return fmt.Errorf("operation %d of the batch: %w", index, err)
}

// EncodeRingBatch returns the RING_BATCH payload for req: the client prologue, a count byte,
// then each operation as the class byte, the 40-byte header and the class form of a
// RING_KEYED body. It refuses a request DecodeRingBatch would not accept, with
// INVALID_ARGUMENT: no operation, more than 255, or a class that is not a write.
func EncodeRingBatch(req RingBatchRequest) ([]byte, error) {
	if len(req.Ops) == 0 {
		return nil, Refusal("a batch carries at least one operation and this one has none. Nothing was sent; send one to 255 write operations, or use a RING_KEYED for a single read")
	}
	if err := CheckBatchSize(len(req.Ops)); err != nil {
		return nil, err
	}
	out := make([]byte, 0, RingPrologueLen+1+len(req.Ops)*96)
	out = binary.BigEndian.AppendUint64(out, req.Prologue.Epoch)
	out = binary.BigEndian.AppendUint64(out, req.Prologue.StateSeq)
	out = binary.BigEndian.AppendUint32(out, req.Prologue.DeadlineMs)
	out = append(out, byte(len(req.Ops)))
	for index, op := range req.Ops {
		if err := ringCheckWrite(op); err != nil {
			return nil, ringBatchOpError(index, err)
		}
		body, err := EncodeClientOp(op)
		if err != nil {
			return nil, ringBatchOpError(index, err)
		}
		out = append(out, body...)
		// A frame's length counts its opcode and request id as well as the payload.
		if room := MaxSubScanBody - 1 - RequestIDLen; len(out) > room {
			return nil, Refusal("the batch is over %d bytes at operation %d and a frame holds at most %d. Split it into smaller batches", len(out), index, room)
		}
	}
	return out, nil
}

// DecodeRingBatch reads a RING_BATCH payload written by EncodeRingBatch. It never panics:
// the payload is capped at the frame ceiling, each operation is read by the decoder of a
// RING_KEYED body, which checks every length against the bytes left, a count of zero, a read
// class and trailing bytes are refused, and what it accepts encodes back to the same bytes.
// A refusal names the index of the operation where decoding stopped.
func DecodeRingBatch(payload []byte) (RingBatchRequest, error) {
	if len(payload) > MaxSubScanBody {
		return RingBatchRequest{}, Refusal("a batch request is %d bytes and a frame holds at most %d", len(payload), MaxSubScanBody)
	}
	if len(payload) < RingPrologueLen+1 {
		return RingBatchRequest{}, Refusal("a batch request starts with a %d-byte client prologue and a 1-byte count, and this one is %d bytes", RingPrologueLen, len(payload))
	}
	req := RingBatchRequest{Prologue: RingPrologue{
		Epoch:      binary.BigEndian.Uint64(payload[0:8]),
		StateSeq:   binary.BigEndian.Uint64(payload[8:16]),
		DeadlineMs: binary.BigEndian.Uint32(payload[16:20]),
	}}
	count := int(payload[RingPrologueLen])
	if count == 0 {
		return RingBatchRequest{}, Refusal("a batch carries at least one operation and this one says 0. Nothing was done; send one to 255 write operations")
	}
	rest := payload[RingPrologueLen+1:]
	req.Ops = make([]Op, 0, count)
	for index := range count {
		if len(rest) == 0 {
			return RingBatchRequest{}, Refusal("operation %d of the batch is missing: the count says %d operations and the bytes end after %d. Nothing was done; send the count of operations you include", index, count, index)
		}
		if ringClassIsRead(rest[0]) {
			return RingBatchRequest{}, Refusal("operation %d of the batch is a read (class 0x%02x) and a batch carries writes only. Nothing was done; send each read as its own RING_KEYED", index, rest[0])
		}
		if rest[0] < OpInsert || rest[0] > OpExtend {
			return RingBatchRequest{}, ringBatchOpError(index, ClassRefusal(rest[0]))
		}
		op, taken, err := DecodeClientOpPrefix(rest)
		if err != nil {
			return RingBatchRequest{}, ringBatchOpError(index, err)
		}
		if err := ringCheckWrite(op); err != nil {
			return RingBatchRequest{}, ringBatchOpError(index, err)
		}
		req.Ops = append(req.Ops, op)
		rest = rest[taken:]
	}
	if len(rest) != 0 {
		return RingBatchRequest{}, Refusal("the batch says %d operations and %d bytes follow the last one. Nothing was done; send exactly the operations you counted", count, len(rest))
	}
	return req, nil
}
