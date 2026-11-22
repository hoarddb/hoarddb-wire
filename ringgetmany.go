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
	"strings"
)

// MaxGetManyKeys is the most keys one RING_GET_MANY frame carries: the count is one byte, as a
// RING_BATCH's is, and 255 keys of about 56 bytes are a request, not a bottleneck.
const MaxGetManyKeys = 255

// RingGetManyRequest is one RING_GET_MANY payload:
// one client prologue and 1 to 255 reads of class get or get_fields. The keys may name
// different buckets and databases because each body carries its own header.
type RingGetManyRequest struct {
	// Prologue is the client prologue of the whole frame.
	Prologue RingPrologue
	// Reads are the keys in request order; answer i belongs to Reads[i].
	Reads []RingRead
}

// CheckRingGetManySize refuses a frame of n keys that cannot be sent: none, or more than 255.
// A client calls it before it sends, so the refusal comes before anything leaves.
func CheckRingGetManySize(n int) error {
	switch {
	case n == 0:
		return Refusal("RING_GET_MANY carries 0 keys. Send 1 to %d keys, or use Get for one", MaxGetManyKeys)
	case n > MaxGetManyKeys:
		return Refusal("at most %d keys go in one get-many frame and this has %d. Split it into frames of %d or fewer (Bucket.GetMany does this for you)", MaxGetManyKeys, n, MaxGetManyKeys)
	}
	return nil
}

// ringGetManyKeyError says which key of the frame an error belongs to, keeping its code.
func ringGetManyKeyError(index, count int, err error) error {
	text := err.Error()
	var coded *Error
	if errors.As(err, &coded) {
		text = coded.Message
	}
	message := fmt.Sprintf("key %d of %d: %s. Nothing was read", index, count, strings.TrimSuffix(text, "."))
	if coded != nil {
		return &Error{Code: coded.Code, Message: message}
	}
	return fmt.Errorf("%s: %w", message, err)
}

// ringClassPhrase names a class byte the way the get-many refusals do: "an update (class 0x02)".
func ringClassPhrase(class byte) string {
	names := map[byte]string{
		OpInsert: "an insert", OpUpdate: "an update", OpUpsert: "an upsert",
		OpDelete: "a delete", OpStoreVerb: "a store verb", OpClaim: "a claim",
		OpAck: "an ack", OpRelease: "a release", OpExtend: "an extend",
		RingClassPeek: "a peek", RingClassStoreList: "a store list", RingClassStoreCount: "a store count",
		RingClassLeaseGet: "a lease_get", RingClassStoreGet: "a store_get", RingClassNearest: "a nearest",
		RingClassAggregate: "an aggregate read",
	}
	if name, ok := names[class]; ok {
		return fmt.Sprintf("%s (class 0x%02x)", name, class)
	}
	return fmt.Sprintf("class 0x%02x", class)
}

// ringGetManyClassRefusal is the refusal of a key whose class is not get or get_fields.
func ringGetManyClassRefusal(index, count int, class byte) error {
	switch {
	case class >= OpInsert && class <= OpExtend:
		return Refusal("key %d of %d is %s. A get-many reads documents only; send writes in a RING_BATCH (Bucket.In(batch))", index, count, ringClassPhrase(class))
	case ringClassIsRead(class):
		return Refusal("key %d of %d is %s. A get-many reads whole documents (get 0x%02x, get_fields 0x%02x); read a store with StoreList", index, count, ringClassPhrase(class), RingClassGet, RingClassGetFields)
	}
	return Refusal("key %d of %d is %s, which is neither a write (0x01 to 0x09) nor a read (0x81 to 0x84, 0x87 to 0x8B). A get-many reads documents only: send get (0x%02x) or get_fields (0x%02x)", index, count, ringClassPhrase(class), RingClassGet, RingClassGetFields)
}

func ringGetManyExpandRefusal(index, count int) error {
	return Refusal("key %d of %d asks for expand. A get-many does not expand; use GetExpanded for one document, or StoreListExpanded to read a page with the documents its keys name", index, count)
}

// EncodeRingGetMany returns the RING_GET_MANY payload for req: the client prologue, a count
// byte, then each key as a 2-byte big-endian length and the body of a RING_KEYED read. It refuses a request DecodeRingGetMany
// would not accept, with INVALID_ARGUMENT, naming the key.
func EncodeRingGetMany(req RingGetManyRequest) ([]byte, error) {
	count := len(req.Reads)
	if err := CheckRingGetManySize(count); err != nil {
		return nil, err
	}
	out := make([]byte, 0, RingPrologueLen+1+count*72)
	out = binary.BigEndian.AppendUint64(out, req.Prologue.Epoch)
	out = binary.BigEndian.AppendUint64(out, req.Prologue.StateSeq)
	out = binary.BigEndian.AppendUint32(out, req.Prologue.DeadlineMs)
	out = append(out, byte(count))
	for index, read := range req.Reads {
		if read.Class != RingClassGet && read.Class != RingClassGetFields {
			return nil, ringGetManyClassRefusal(index, count, read.Class)
		}
		if len(read.Options.Expand) != 0 {
			return nil, ringGetManyExpandRefusal(index, count)
		}
		body, err := EncodeRingRead(read)
		if err != nil {
			return nil, ringGetManyKeyError(index, count, err)
		}
		out = binary.BigEndian.AppendUint16(out, uint16(len(body))) // a get body is far under 65535 bytes (names, trailer, id are all bounded)
		out = append(out, body...)
		// A frame's length counts its opcode and request id as well as the payload.
		if room := MaxSubScanBody - 1 - RequestIDLen; len(out) > room {
			return nil, Refusal("the get-many frame is over %d bytes at key %d of %d and a frame holds at most %d. Split it into smaller frames", len(out), index, count, room)
		}
	}
	return out, nil
}

// DecodeRingGetMany reads a RING_GET_MANY payload written by EncodeRingGetMany. It never
// panics: the payload is capped at the frame ceiling, each key is read by the decoder of a
// RING_KEYED read body on the bytes its 2-byte length delimits, so it never guesses where a body ends. A count of zero, a length of zero or past the bytes left,
// a class other than get and get_fields, expand and trailing bytes are refused, so what it
// accepts encodes back to the same bytes. A refusal names the key where decoding stopped.
func DecodeRingGetMany(payload []byte) (RingGetManyRequest, error) {
	if len(payload) > MaxSubScanBody {
		return RingGetManyRequest{}, Refusal("a get-many request is %d bytes and a frame holds at most %d", len(payload), MaxSubScanBody)
	}
	if len(payload) < RingPrologueLen+1 {
		return RingGetManyRequest{}, Refusal("a get-many request starts with a %d-byte client prologue and a 1-byte count of keys, and this one is %d bytes", RingPrologueLen, len(payload))
	}
	req := RingGetManyRequest{Prologue: RingPrologue{
		Epoch:      binary.BigEndian.Uint64(payload[0:8]),
		StateSeq:   binary.BigEndian.Uint64(payload[8:16]),
		DeadlineMs: binary.BigEndian.Uint32(payload[16:20]),
	}}
	count := int(payload[RingPrologueLen])
	if err := CheckRingGetManySize(count); err != nil {
		return RingGetManyRequest{}, err
	}
	rest := payload[RingPrologueLen+1:]
	req.Reads = make([]RingRead, 0, count)
	for index := range count {
		if len(rest) < 2 {
			return RingGetManyRequest{}, Refusal("key %d of %d is missing: the count says %d keys and the bytes end after %d. Nothing was read; send the count of keys you include, each as a 2-byte length and its body", index, count, count, index)
		}
		size := int(binary.BigEndian.Uint16(rest))
		rest = rest[2:]
		if size == 0 {
			return RingGetManyRequest{}, Refusal("key %d of %d has a length of 0. Nothing was read; send each key as the 2-byte byte length of its read body and then the body", index, count)
		}
		if size > len(rest) {
			return RingGetManyRequest{}, Refusal("key %d of %d claims %d bytes and %d remain. Nothing was read; send each key as the 2-byte byte length of its read body and then exactly that many bytes", index, count, size, len(rest))
		}
		body := rest[:size]
		rest = rest[size:]
		if body[0] != RingClassGet && body[0] != RingClassGetFields {
			return RingGetManyRequest{}, ringGetManyClassRefusal(index, count, body[0])
		}
		read, err := decodeRingRead(body)
		if err != nil {
			return RingGetManyRequest{}, ringGetManyKeyError(index, count, err)
		}
		if len(read.Options.Expand) != 0 {
			return RingGetManyRequest{}, ringGetManyExpandRefusal(index, count)
		}
		req.Reads = append(req.Reads, read)
	}
	if len(rest) != 0 {
		return RingGetManyRequest{}, Refusal("key %d of %d: %d bytes follow the last key. Nothing was read; send exactly the keys you counted", count-1, count, len(rest))
	}
	return req, nil
}
