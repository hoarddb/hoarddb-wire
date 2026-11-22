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
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// Read classes of RING_KEYED. Reads take no position and write no log record, so
// they sit above the nine write classes (class bytes with the high bit set).
const (
	// RingClassGet reads one document.
	RingClassGet byte = 0x81
	// RingClassPeek reads the elements a pop would take from one store field.
	RingClassPeek byte = 0x82
	// RingClassStoreList reads one page of one store field.
	RingClassStoreList byte = 0x83
	// RingClassStoreCount reads one store field's element count.
	RingClassStoreCount byte = 0x84
	// RingClassLeaseGet reads the messages currently leased under one lease id of a
	// fifo or lifo store field. It is 0x87 because 0x85 and 0x86 name RING_LIST and
	// RING_COUNT in the authorizer's table and are never on the wire.
	RingClassLeaseGet byte = 0x87
	// RingClassStoreGet reads one element of a hashStore, btreeStore or heapStore field
	// by its key.
	RingClassStoreGet byte = 0x88
	// RingClassGetFields reads one document and returns only the named scalar fields and
	// _id_.
	RingClassGetFields byte = 0x89
	// RingClassNearest reads the elements of one store field closest to a query vector. Its
	// form after the field id is [metric:1][k:1][name len:1][name][vector len:2][vector bytes].
	RingClassNearest byte = 0x8A
	// RingClassAggregate reads one maintained aggregate of one store field, or describes them
	// all. It is 0x8B because 0x8A is a nearest. Its form after the field id is
	// [kind:1][fLen:1][field][bLen:1][by][limit:2][afterLen:2][after].
	RingClassAggregate byte = 0x8B
)

// The metric bytes of a RingClassNearest.
const (
	RingMetricCosine byte = 0x01
	RingMetricDot    byte = 0x02
	RingMetricL2     byte = 0x03
)

const (
	// MaxNearestVector is 4 * 4096 bytes: the largest query vector.
	MaxNearestVector = 16384
	// MaxNearestName bounds the element field name, one byte of length.
	MaxNearestName = 255
)

const (
	// RingPrologueLen is the client prologue of a ring request:
	// [epoch:8 BE][state_seq:8 BE][deadline_ms:4 BE].
	RingPrologueLen = 20
	// HeaderLen is the payload header of an operation: bucket id, declaration
	// version, schema_version_seen and prevEnd.
	HeaderLen = 40
	// ringMaxPage bounds a store list limit, the cap server.FieldListRequest already
	// clamps to.
	ringMaxPage = 1000
	// MaxPeek bounds a peek count: the pop limit cap of 100 that the server
	// enforces is the contract.
	MaxPeek = MaxPeekCount
	// MaxAfter bounds a store list cursor. A keyed cursor is the key's
	// EncodeComponent: a tag, the key with each NUL escaped as two bytes, and a
	// 2-byte terminator, so a legal 1024-byte key takes 3 + 2*1024 bytes. A heap
	// cursor adds its 8-byte priority.
	MaxAfter = 8 + 3 + 2*MaxVerbKeyBytes
)

// RingPrologue is the 20 bytes at the head of every RING_KEYED payload.
type RingPrologue struct {
	// Epoch is the epoch of the client's table; 0 asks the node to route.
	Epoch uint64
	// StateSeq is informational; 0 asks the node to route.
	StateSeq uint64
	// DeadlineMs bounds the owner's write-concern wait; 0 means no deadline.
	DeadlineMs uint32
}

// RingRead is the request form of a read class. Which members a class uses:
//
//	get          EncID
//	peek         EncID, FieldID, Limit (how many elements, 1 to 100)
//	store list   EncID, FieldID, Limit (page size, 1 to 1000), After (empty on the first page)
//	store count  EncID, FieldID
//	lease_get    EncID, FieldID, Lease (the 16-byte id a claim returned)
//	store_get    EncID, FieldID, Key (the element key of a hash or btree, the entry id of a heap)
//
// The header members ride with every class. A member a class does not use must be
// zero, so one request has one encoding.
type RingRead struct {
	// Class is one of the RingClass constants.
	Class byte
	// BucketID is the bucket the client resolved the statement against.
	BucketID uint64
	// DeclVersion and SchemaVersionSeen are the versions the client holds.
	DeclVersion, SchemaVersionSeen int64
	// EncID is the document id's EncodeComponent encoding.
	EncID []byte
	// FieldID names the store field.
	FieldID uint16
	// Limit is a peek count or a store list page size.
	Limit uint16
	// After is the store list cursor, exclusive; empty starts at the first element.
	After []byte
	// Lease is the lease id a lease_get names: the 16 bytes a claim returned.
	Lease [16]byte
	// Key is the element key (hash, btree) or entry id (heap) a store_get names.
	Key string
	// Fields are the scalar field names a get_fields returns besides _id_: 1 to 64 names.
	Fields []string
	// Nearest is the query of a nearest; set exactly when Class is RingClassNearest.
	Nearest *RingNearest
	// Options is what a get, a store list and a get_fields carry after the class form, and
	// zero on every other class.
	Options RingReadOptions
	// Aggregate names the aggregate of an aggregate read; set exactly when Class is
	// RingClassAggregate. Limit and After page a by form: limit 0 takes the default of 100,
	// at most 10000.
	Aggregate *RingAggregateRead
}

// RingNearest is the form of a RingClassNearest. Every member is required; the client resolves
// its defaults (k 10, metric cosine, field "Vector") before encoding.
type RingNearest struct {
	Metric byte   // RingMetricCosine, RingMetricDot or RingMetricL2
	K      uint8  // 1 to MaxPeek
	Field  string // 1 to 255 bytes of valid UTF-8, one top-level key of the element
	Vector []byte // 4 to 16384 bytes, a multiple of 4
}

// RingKeyedRequest is one RING_KEYED payload. Read is set for a read class and Op
// (a zero Op) is then unused; otherwise Op carries one of the nine write classes.
// A client never sends prevEnd (the ordering node stamps epoch openers), so
// Op.PrevEnd must be zero; Now and Nonce are the owner's to stamp and the codec
// carries whatever the client wrote there.
type RingKeyedRequest struct {
	// Prologue is the client prologue.
	Prologue RingPrologue
	// Op is the write class and its form.
	Op Op
	// Read is the read class and its form, or nil for a write.
	Read *RingRead
}

// Refusal is an INVALID_ARGUMENT refusal of a ring request, reply or argument.
func Refusal(format string, args ...any) error {
	return &Error{Code: CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

func ringClassIsRead(class byte) bool {
	return class >= RingClassGet && class <= RingClassStoreCount || class >= RingClassLeaseGet && class <= RingClassNearest || class == RingClassAggregate
}

// ClassRefusal is the refusal of a class byte that is neither a write nor a read.
func ClassRefusal(class byte) error {
	return Refusal("request class 0x%02x is neither a write (0x01 to 0x09) nor a read (0x81 to 0x84, 0x87 to 0x8B). Nothing was done; send a class from one of those ranges", class)
}

func ringCheckEncID(encID []byte) error {
	if len(encID) == 0 || len(encID) > MaxEncodedID {
		return Refusal("document id encoding is %d bytes, want 1 to %d", len(encID), MaxEncodedID)
	}
	width, err := EncodedComponentLen(encID)
	if err != nil {
		return Refusal("document id encoding is not an EncodeComponent value: %v", err)
	}
	if width != len(encID) {
		return Refusal("document id encoding is %d bytes but its component is %d", len(encID), width)
	}
	return nil
}

// CheckVerbArg is the part of the STORE_VERB arg rule that needs no store type:
// a pop has no arg or {n}, any other verb has one BSON document of at most 1 MiB that
// re-encodes to the same bytes. The layout per verb and store type is
// decodeVerbArg's, run by the owner once it has resolved the field's type.
func CheckVerbArg(verb byte, arg []byte) error {
	if verb == VerbPop && len(arg) == 0 {
		return nil
	}
	if len(arg) == 0 {
		return Refusal("store verb %d needs a BSON arg and this one is empty. Send the arg document of the verb, for example {v: <element>} for a push or {k: <key>, v: <element>} for a put", verb)
	}
	if len(arg) > MaxVerbArgBytes {
		return Refusal("the arg is %d bytes and at most %d bytes are taken", len(arg), MaxVerbArgBytes)
	}
	if _, err := bson.Validate(arg, bson.Limits{Depth: VerbArgDepth, Size: MaxVerbArgBytes}); err != nil {
		return Refusal("the arg is not a valid BSON document: %v", err)
	}
	var doc bson.D
	if err := bson.Unmarshal(arg, &doc); err != nil {
		return Refusal("the arg is not one BSON document: %v", err)
	}
	again, err := bson.Marshal(doc)
	if err != nil || !bytes.Equal(again, arg) {
		return Refusal("the arg is not one canonical BSON document (trailing bytes, duplicate or reordered fields, or a widened integer)")
	}
	return nil
}

func ringCheckWrite(op Op) error {
	if !op.PrevEnd.Zero() {
		return Refusal("a client request carries prevEnd %s; only the ordering node sets it. Send 0:0", op.PrevEnd)
	}
	if op.Class == OpStoreVerb && len(op.Fields) == 1 {
		return CheckVerbArg(op.Fields[0].Verb, op.Fields[0].Value)
	}
	return nil
}

func ringCheckRead(read RingRead) error {
	if !ringClassIsRead(read.Class) {
		return ClassRefusal(read.Class)
	}
	if read.DeclVersion < 0 || read.SchemaVersionSeen < 0 {
		return Refusal("read class 0x%02x carries a negative version (declaration %d, schema seen %d); versions are int64 counters from 0", read.Class, read.DeclVersion, read.SchemaVersionSeen)
	}
	if err := ringCheckEncID(read.EncID); err != nil {
		return err
	}
	usesFields := read.Class == RingClassGetFields
	usesField := read.Class != RingClassGet && !usesFields
	usesLimit := read.Class == RingClassPeek || read.Class == RingClassStoreList || read.Class == RingClassAggregate
	usesLease := read.Class == RingClassLeaseGet
	usesKey := read.Class == RingClassStoreGet
	maxLimit := uint16(ringMaxPage)
	if read.Class == RingClassPeek {
		maxLimit = MaxPeek
	}
	if err := ringCheckNearest(read); err != nil {
		return err
	}
	if err := CheckAggregate(read); err != nil {
		return err
	}
	switch {
	case usesFields:
		if err := CheckFieldNames("fields", read.Fields); err != nil {
			return err
		}
		if read.FieldID != 0 {
			return Refusal("a get_fields names scalar fields in fields and no store field; drop field id %d", read.FieldID)
		}
	case read.Fields != nil:
		return Refusal("read class 0x%02x names no scalar fields to return; drop fields (only a get_fields, 0x%02x, carries them)", read.Class, RingClassGetFields)
	}
	switch {
	case usesLease && read.Lease == ([16]byte{}):
		return Refusal("a lease_get names the lease id of a claim and this one is all zeros, which is what a claim returns when the field had nothing to lease. There is nothing to lease and nothing to read; claim first and send the 'lease' member of its reply")
	case !usesLease && read.Lease != ([16]byte{}):
		return Refusal("read class 0x%02x names no lease; drop the lease id (only a lease_get, 0x%02x, carries one)", read.Class, RingClassLeaseGet)
	case usesField && read.FieldID == 0:
		return Refusal("read class 0x%02x names a store field and field id 0 is never allocated; send the field's id from 1", read.Class)
	case !usesField && read.FieldID != 0:
		return Refusal("a document get names no store field; drop field id %d", read.FieldID)
	case usesKey && (len(read.Key) < 1 || len(read.Key) > MaxVerbKeyBytes):
		return Refusal("a store_get names the element key (a hashStore or btreeStore) or entry id (a heapStore) to read and this one is %d bytes; it must be 1 to %d", len(read.Key), MaxVerbKeyBytes)
	case usesKey && !utf8.ValidString(read.Key):
		return Refusal("the store_get key is not valid UTF-8. Nothing was read. Send a key and an entry id as text; encode binary values as hex or base64 first")
	case !usesKey && read.Key != "":
		return Refusal("read class 0x%02x names no element key; drop %q (only a store_get, 0x%02x, carries one)", read.Class, read.Key, RingClassStoreGet)
	case !ringClassCarriesOptions(read.Class) && !read.Options.Empty():
		return Refusal("read class 0x%02x takes no options; drop local and expand (only get 0x81, store list 0x83 and get_fields 0x89 carry them)", read.Class)
	case read.Class == RingClassAggregate && read.Limit > AggregateMaxLimit:
		return Refusal("an aggregate read asks for limit %d and it must be 0 (the default 100) to %d", read.Limit, AggregateMaxLimit)
	case usesLimit && read.Class != RingClassAggregate && (read.Limit < 1 || read.Limit > maxLimit):
		return Refusal("read class 0x%02x asks for %s %d and it must be 1 to %d", read.Class, ringLimitName(read.Class), read.Limit, maxLimit)
	case !usesLimit && read.Limit != 0:
		return Refusal("read class 0x%02x takes no count or limit; drop %d", read.Class, read.Limit)
	case read.Class != RingClassStoreList && read.Class != RingClassAggregate && len(read.After) != 0:
		return Refusal("read class 0x%02x takes no cursor; only a store list and an aggregate read page", read.Class)
	case len(read.After) > MaxAfter:
		return Refusal("the store list cursor is %d bytes and at most %d are taken", len(read.After), MaxAfter)
	}
	return checkRingReadOptions(read.Options)
}

// ringCheckNearest holds the rules of the nearest form: present exactly on a nearest, and each
// member inside its bound, so the owner never sees a malformed query.
func ringCheckNearest(read RingRead) error {
	nearest := read.Nearest
	switch {
	case read.Class != RingClassNearest && nearest != nil:
		return Refusal("read class 0x%02x names no nearest query; drop it (only a nearest, 0x%02x, carries one)", read.Class, RingClassNearest)
	case read.Class != RingClassNearest:
		return nil
	case nearest == nil:
		return Refusal("a nearest names a query and this one has none. Send the metric, k, field name and vector")
	case nearest.Metric < RingMetricCosine || nearest.Metric > RingMetricL2:
		return Refusal("a nearest names metric 0x%02x; send 0x01 (cosine), 0x02 (dot) or 0x03 (l2)", nearest.Metric)
	case nearest.K < 1 || nearest.K > MaxPeek:
		return Refusal("a nearest asks for k %d and it must be 1 to %d", nearest.K, MaxPeek)
	case len(nearest.Field) < 1 || len(nearest.Field) > MaxNearestName || !utf8.ValidString(nearest.Field):
		return Refusal("a nearest names its vector field in %d bytes of valid UTF-8 and it must be 1 to %d bytes", len(nearest.Field), MaxNearestName)
	case len(nearest.Vector) < 4 || len(nearest.Vector) > MaxNearestVector || len(nearest.Vector)%4 != 0:
		return Refusal("a nearest vector is %d bytes; send 4 to %d bytes, a multiple of 4 (little-endian float32 values)", len(nearest.Vector), MaxNearestVector)
	}
	for i := 0; i < len(nearest.Vector); i += 4 {
		value := float64(math.Float32frombits(binary.LittleEndian.Uint32(nearest.Vector[i:])))
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return Refusal("a nearest vector holds a NaN or infinite value at element %d; send finite float32 values", i/4)
		}
	}
	return nil
}

// CheckAggregate holds the aggregate form to its rules: present exactly on an aggregate read,
// a kind from 0 to 3, and the paths each kind names and no others.
func CheckAggregate(read RingRead) error {
	aggregate := read.Aggregate
	switch {
	case read.Class != RingClassAggregate && aggregate != nil:
		return Refusal("read class 0x%02x names no aggregate; drop it (only an aggregate read, 0x%02x, carries one)", read.Class, RingClassAggregate)
	case read.Class != RingClassAggregate:
		return nil
	case aggregate == nil:
		return Refusal("an aggregate read names a kind and this one has none. Send 0 (describe), 1 (sum), 2 (sum by) or 3 (count by)")
	case aggregate.Kind > AggregateReadCountBy:
		return Refusal("an aggregate read names kind %d; send 0 (describe), 1 (sum), 2 (sum by) or 3 (count by)", aggregate.Kind)
	case len(aggregate.Field) > 255 || len(aggregate.By) > 255:
		return Refusal("an aggregate path is at most 255 bytes")
	case aggregate.Kind == AggregateReadDescribe && (aggregate.Field != "" || aggregate.By != ""):
		return Refusal("a describe names no field or by; drop them")
	case (aggregate.Kind == AggregateReadSum || aggregate.Kind == AggregateReadSumBy) && aggregate.Field == "":
		return Refusal("a sum names the element field to sum; send its path")
	case (aggregate.Kind == AggregateReadSumBy || aggregate.Kind == AggregateReadCountBy) && aggregate.By == "":
		return Refusal("a by form names the element field to group by; send its path")
	case aggregate.Kind == AggregateReadSum && aggregate.By != "", aggregate.Kind == AggregateReadCountBy && aggregate.Field != "":
		return Refusal("this aggregate kind takes no such path; a sum has no by and a count by has no field")
	}
	return nil
}

func ringLimitName(class byte) string {
	if class == RingClassPeek {
		return "count"
	}
	return "limit"
}

func appendRingHeader(out []byte, bucketID uint64, declVersion, schemaSeen int64) []byte {
	out = binary.BigEndian.AppendUint64(out, bucketID)
	out = binary.BigEndian.AppendUint64(out, uint64(declVersion))
	out = binary.BigEndian.AppendUint64(out, uint64(schemaSeen))
	return AppendPosition(out, Position{})
}

// EncodeRingRead returns the RING_KEYED body of one read, without the client prologue: the
// form a RING_GET_MANY carries per key.
func EncodeRingRead(read RingRead) ([]byte, error) {
	if err := ringCheckRead(read); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 1+HeaderLen+len(read.EncID)+8+len(read.After)+len(read.Lease)+len(read.Key))
	out = append(out, read.Class)
	out = appendRingHeader(out, read.BucketID, read.DeclVersion, read.SchemaVersionSeen)
	out = append(out, read.EncID...)
	if read.Class == RingClassGetFields {
		out = append(out, byte(len(read.Fields)))
		for _, name := range read.Fields {
			out = append(append(out, byte(len(name))), name...)
		}
	} else if read.Class != RingClassGet {
		out = binary.BigEndian.AppendUint16(out, read.FieldID)
		switch read.Class {
		case RingClassLeaseGet:
			out = append(out, read.Lease[:]...)
		case RingClassStoreGet:
			out = binary.BigEndian.AppendUint16(out, uint16(len(read.Key)))
			out = append(out, read.Key...)
		case RingClassPeek:
			out = binary.BigEndian.AppendUint16(out, read.Limit)
		case RingClassStoreList:
			out = binary.BigEndian.AppendUint16(out, read.Limit)
			out = binary.BigEndian.AppendUint16(out, uint16(len(read.After)))
			out = append(out, read.After...)
		case RingClassAggregate:
			aggregate := read.Aggregate
			out = append(out, aggregate.Kind, byte(len(aggregate.Field)))
			out = append(out, aggregate.Field...)
			out = append(append(out, byte(len(aggregate.By))), aggregate.By...)
			out = binary.BigEndian.AppendUint16(out, read.Limit)
			out = binary.BigEndian.AppendUint16(out, uint16(len(read.After)))
			out = append(out, read.After...)
		case RingClassNearest:
			nearest := read.Nearest
			out = append(out, nearest.Metric, nearest.K, byte(len(nearest.Field)))
			out = append(out, nearest.Field...)
			out = binary.BigEndian.AppendUint16(out, uint16(len(nearest.Vector)))
			out = append(out, nearest.Vector...)
		}
	}
	trailer, err := EncodeRingReadOptions(read.Options)
	if err != nil {
		return nil, err
	}
	return append(out, trailer...), nil
}

// EncodeRingKeyed returns the RING_KEYED payload for req: the client prologue, then
// the class byte, the 40-byte header and the class form. It refuses a request
// DecodeRingKeyed would not accept, with INVALID_ARGUMENT.
func EncodeRingKeyed(req RingKeyedRequest) ([]byte, error) {
	var body []byte
	var err error
	switch {
	case req.Read != nil && req.Op.Class != 0:
		return nil, Refusal("a request is a read or a write, not both: Read is set and the write class is 0x%02x", req.Op.Class)
	case req.Read != nil:
		body, err = EncodeRingRead(*req.Read)
	default:
		if err = ringCheckWrite(req.Op); err == nil {
			body, err = EncodeClientOp(req.Op)
		}
	}
	if err != nil {
		return nil, err
	}
	if RingPrologueLen+len(body) > MaxSubScanBody {
		return nil, Refusal("a keyed request would be %d bytes and a frame holds at most %d", RingPrologueLen+len(body), MaxSubScanBody)
	}
	out := make([]byte, 0, RingPrologueLen+len(body))
	out = binary.BigEndian.AppendUint64(out, req.Prologue.Epoch)
	out = binary.BigEndian.AppendUint64(out, req.Prologue.StateSeq)
	out = binary.BigEndian.AppendUint32(out, req.Prologue.DeadlineMs)
	return append(out, body...), nil
}

// DecodeRingKeyed reads a RING_KEYED payload written by EncodeRingKeyed. It never
// panics: the payload is capped at the frame ceiling, every length is checked
// against the bytes left before anything is sliced or allocated, trailing bytes are
// refused, and what it accepts encodes back to the same bytes.
func DecodeRingKeyed(payload []byte) (RingKeyedRequest, error) {
	if len(payload) > MaxSubScanBody {
		return RingKeyedRequest{}, Refusal("a keyed request is %d bytes and a frame holds at most %d", len(payload), MaxSubScanBody)
	}
	if len(payload) < RingPrologueLen {
		return RingKeyedRequest{}, Refusal("a keyed request starts with a %d-byte client prologue and this one is %d bytes", RingPrologueLen, len(payload))
	}
	req := RingKeyedRequest{Prologue: RingPrologue{
		Epoch:      binary.BigEndian.Uint64(payload[0:8]),
		StateSeq:   binary.BigEndian.Uint64(payload[8:16]),
		DeadlineMs: binary.BigEndian.Uint32(payload[16:20]),
	}}
	body := payload[RingPrologueLen:]
	if len(body) == 0 {
		return RingKeyedRequest{}, Refusal("a keyed request has a prologue and no class byte")
	}
	if ringClassIsRead(body[0]) {
		read, err := decodeRingRead(body)
		if err != nil {
			return RingKeyedRequest{}, err
		}
		req.Read = &read
		return req, nil
	}
	if body[0] < OpInsert || body[0] > OpExtend {
		return RingKeyedRequest{}, ClassRefusal(body[0])
	}
	op, err := DecodeClientOp(body)
	if err != nil {
		return RingKeyedRequest{}, err
	}
	if err := ringCheckWrite(op); err != nil {
		return RingKeyedRequest{}, err
	}
	req.Op = op
	return req, nil
}

func decodeRingRead(body []byte) (RingRead, error) {
	if len(body) < 1+HeaderLen {
		return RingRead{}, Refusal("read class 0x%02x needs a %d-byte header and the request has %d bytes after the class", body[0], HeaderLen, len(body)-1)
	}
	read := RingRead{Class: body[0]}
	header := body[1 : 1+HeaderLen]
	read.BucketID = binary.BigEndian.Uint64(header[0:8])
	declVersion := binary.BigEndian.Uint64(header[8:16])
	schemaSeen := binary.BigEndian.Uint64(header[16:24])
	if declVersion > math.MaxInt64 || schemaSeen > math.MaxInt64 {
		return RingRead{}, Refusal("read header carries a version above the int64 range")
	}
	read.DeclVersion, read.SchemaVersionSeen = int64(declVersion), int64(schemaSeen)
	prevEnd, err := DecodePosition(header[24:40])
	if err != nil {
		return RingRead{}, fmt.Errorf("read header prevEnd: %w", err)
	}
	if !prevEnd.Zero() {
		return RingRead{}, Refusal("a client request carries prevEnd %s; only the ordering node sets it. Send 0:0", prevEnd)
	}
	rest := body[1+HeaderLen:]
	width, err := EncodedComponentLen(rest)
	if err != nil {
		return RingRead{}, Refusal("document id encoding does not parse: %v", err)
	}
	read.EncID = bytes.Clone(rest[:width])
	rest = rest[width:]
	if read.Class == RingClassGetFields {
		names, remaining, err := decodeFieldNames(rest)
		if err != nil {
			return RingRead{}, err
		}
		read.Fields, rest = names, remaining
	}
	if read.Class != RingClassGet && read.Class != RingClassGetFields {
		if len(rest) < 2 {
			return RingRead{}, Refusal("read class 0x%02x needs a 2-byte field id and %d bytes remain", read.Class, len(rest))
		}
		read.FieldID, rest = binary.BigEndian.Uint16(rest), rest[2:]
	}
	if read.Class == RingClassLeaseGet {
		if len(rest) < len(read.Lease) {
			return RingRead{}, Refusal("a lease_get needs the 16-byte lease id a claim returned after the field id and %d bytes remain. Send the 'lease' member of the claim's reply", len(rest))
		}
		rest = rest[copy(read.Lease[:], rest):]
	}
	if read.Class == RingClassStoreGet {
		if len(rest) < 2 {
			return RingRead{}, Refusal("a store_get needs a 2-byte key length after the field id and %d bytes remain", len(rest))
		}
		keyLen := uint64(binary.BigEndian.Uint16(rest))
		rest = rest[2:]
		if keyLen > uint64(len(rest)) {
			return RingRead{}, Refusal("the store_get key claims %d bytes and %d remain", keyLen, len(rest))
		}
		read.Key, rest = string(rest[:keyLen]), rest[keyLen:]
	}
	if read.Class == RingClassPeek || read.Class == RingClassStoreList {
		if len(rest) < 2 {
			return RingRead{}, Refusal("read class 0x%02x needs a 2-byte %s and %d bytes remain", read.Class, ringLimitName(read.Class), len(rest))
		}
		read.Limit, rest = binary.BigEndian.Uint16(rest), rest[2:]
	}
	if read.Class == RingClassStoreList {
		if len(rest) < 2 {
			return RingRead{}, Refusal("a store list needs a 2-byte cursor length and %d bytes remain", len(rest))
		}
		afterLen := uint64(binary.BigEndian.Uint16(rest))
		rest = rest[2:]
		if afterLen > uint64(len(rest)) {
			return RingRead{}, Refusal("the store list cursor claims %d bytes and %d remain", afterLen, len(rest))
		}
		if afterLen > 0 {
			read.After = bytes.Clone(rest[:afterLen])
		}
		rest = rest[afterLen:]
	}
	if read.Class == RingClassAggregate {
		aggregate, limit, after, remaining, err := decodeRingAggregate(rest)
		if err != nil {
			return RingRead{}, err
		}
		read.Aggregate, read.Limit, read.After, rest = aggregate, limit, after, remaining
	}
	if read.Class == RingClassNearest {
		nearest, remaining, err := decodeRingNearest(rest)
		if err != nil {
			return RingRead{}, err
		}
		read.Nearest, rest = nearest, remaining
	}
	if ringClassCarriesOptions(read.Class) {
		options, used, err := DecodeRingReadOptions(rest)
		if err != nil {
			return RingRead{}, err
		}
		read.Options, rest = options, rest[used:]
	}
	if len(rest) != 0 && read.Class == RingClassLeaseGet {
		return RingRead{}, Refusal("a lease_get ends with the 16-byte lease id a claim returned and %d bytes follow it. Send the 'lease' member of the claim's reply and nothing after it", len(rest))
	}
	if len(rest) != 0 {
		return RingRead{}, Refusal("read class 0x%02x has %d bytes after its last field", read.Class, len(rest))
	}
	if err := ringCheckRead(read); err != nil {
		return RingRead{}, err
	}
	return read, nil
}

// decodeRingAggregate reads the aggregate form after the field id. Every length is checked
// against the bytes left before a slice is taken.
func decodeRingAggregate(rest []byte) (*RingAggregateRead, uint16, []byte, []byte, error) {
	short := func(what string) error {
		return Refusal("an aggregate read needs %s and %d bytes remain", what, len(rest))
	}
	if len(rest) < 2 {
		return nil, 0, nil, nil, short("a 1-byte kind and a 1-byte field length")
	}
	aggregate := &RingAggregateRead{Kind: rest[0]}
	fieldLen := int(rest[1])
	rest = rest[2:]
	if len(rest) < fieldLen+1 {
		return nil, 0, nil, nil, short("its field path and a by length")
	}
	aggregate.Field = string(rest[:fieldLen])
	byLen := int(rest[fieldLen])
	rest = rest[fieldLen+1:]
	if len(rest) < byLen+4 {
		return nil, 0, nil, nil, short("its by path, a 2-byte limit and a 2-byte cursor length")
	}
	aggregate.By = string(rest[:byLen])
	rest = rest[byLen:]
	limit := binary.BigEndian.Uint16(rest)
	afterLen := int(binary.BigEndian.Uint16(rest[2:]))
	rest = rest[4:]
	if afterLen > len(rest) {
		return nil, 0, nil, nil, Refusal("the aggregate cursor claims %d bytes and %d remain", afterLen, len(rest))
	}
	var after []byte
	if afterLen > 0 {
		after = bytes.Clone(rest[:afterLen])
	}
	return aggregate, limit, after, rest[afterLen:], nil
}

// decodeRingNearest reads the nearest form after the field id. Every length is checked against
// the bytes left before a slice is taken, so a lying length allocates nothing.
func decodeRingNearest(rest []byte) (*RingNearest, []byte, error) {
	if len(rest) < 3 {
		return nil, nil, Refusal("a nearest needs a 1-byte metric, a 1-byte k and a 1-byte name length after the field id and %d bytes remain", len(rest))
	}
	nearest := &RingNearest{Metric: rest[0], K: rest[1]}
	nameLen := int(rest[2])
	rest = rest[3:]
	if nameLen > len(rest) {
		return nil, nil, Refusal("a nearest needs its name after the field id and %d bytes remain", len(rest))
	}
	nearest.Field, rest = string(rest[:nameLen]), rest[nameLen:]
	if len(rest) < 2 {
		return nil, nil, Refusal("a nearest needs a 2-byte vector length after the field id and %d bytes remain", len(rest))
	}
	vectorLen := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if vectorLen > len(rest) {
		return nil, nil, Refusal("a nearest needs its vector after the field id and %d bytes remain", len(rest))
	}
	nearest.Vector, rest = bytes.Clone(rest[:vectorLen]), rest[vectorLen:]
	return nearest, rest, nil
}

// decodeFieldNames reads the names of a get_fields: [count:1] then, per name, [len:1][name].
// The count is at most 64 and every length is checked against the bytes left before a name
// is sliced, so a lying request allocates no more than its own size. What follows the last
// name is returned unread: it is the read options trailer.
func decodeFieldNames(rest []byte) ([]string, []byte, error) {
	if len(rest) < 1 {
		return nil, nil, Refusal("a get_fields needs a 1-byte count of the names in fields after the document id and no bytes remain. Send 1 to %d field names", MaxFields)
	}
	count := int(rest[0]) // a byte: the names are counted against the cap by ringCheckRead
	rest = rest[1:]
	names := make([]string, 0, count)
	for len(names) < count {
		if len(rest) < 1 {
			return nil, nil, Refusal("a get_fields says %d names in fields and the bytes end after %d. Send each name as a 1-byte length and the name", count, len(names))
		}
		nameLen := int(rest[0])
		rest = rest[1:]
		if nameLen > len(rest) {
			return nil, nil, Refusal("a name in fields claims %d bytes and %d remain", nameLen, len(rest))
		}
		names = append(names, string(rest[:nameLen]))
		rest = rest[nameLen:]
	}
	return names, rest, nil
}
