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
	"math"
)

// The nine operation classes. One byte, one meaning, no value
// reused; the ordering node decides the class before it assigns a position.
const (
	// OpInsert stores a new document and refuses an existing one.
	OpInsert byte = 0x01
	// OpUpdate applies field operations to an existing document.
	OpUpdate byte = 0x02
	// OpUpsert has INSERT's form; the apply picks the insert or update path.
	OpUpsert byte = 0x03
	// OpDelete removes a document.
	OpDelete byte = 0x04
	// OpStoreVerb runs push, pop, take or put on one store field.
	OpStoreVerb byte = 0x05
	// OpClaim leases messages from one store field.
	OpClaim byte = 0x06
	// OpAck acknowledges a lease.
	OpAck byte = 0x07
	// OpRelease gives a lease back.
	OpRelease byte = 0x08
	// OpExtend moves a lease's deadline.
	OpExtend byte = 0x09
)

// Store verbs of an OpStoreVerb.
const (
	// VerbPush appends to a store field.
	VerbPush byte = 1
	// VerbPop removes and returns from a store field.
	VerbPop byte = 2
	// VerbTake removes a chosen element of a store field.
	VerbTake byte = 3
	// VerbPut writes a keyed element of a store field.
	VerbPut byte = 4
	// VerbInc adds to the numbers of a keyed element of a hashStore or btreeStore (F5).
	VerbInc byte = 5
)

// Update operation codes of an OpUpdate FieldOp.
const (
	// UpdateSet sets a field.
	UpdateSet byte = 0x01
	// UpdateUnset clears a field.
	UpdateUnset byte = 0x02
	// UpdateInc adds to a number.
	UpdateInc byte = 0x03
	// UpdateMul multiplies a number.
	UpdateMul byte = 0x04
	// UpdatePush appends to an array.
	UpdatePush byte = 0x05
	// UpdateAddToSet appends to an array unless present.
	UpdateAddToSet byte = 0x06
	// UpdateMin keeps the smaller value.
	UpdateMin byte = 0x07
	// UpdateMax keeps the larger value.
	UpdateMax byte = 0x08
	// UpdateRenameTo renames a field; its Value is the target field id, 2 bytes.
	UpdateRenameTo byte = 0x09
)

const (
	opcodecHeaderLen = 40 // bucket id, declaration version, schema_version_seen, prevEnd
	opcodecLeaseLen  = 16
	opcodecMaxEncID  = 1024 // conv §2: encID is 1…1024 bytes
	// opcodecMaxStampList bounds each list of the stamp: its count is one byte.
	opcodecMaxStampList = 255
	// opcodecMaxExpireList bounds the expire list: its count is one byte.
	opcodecMaxExpireList = 255
)

// The flags byte of a record body (storage-format spec §1.2, one table for the index spec,
// the expiry items and the system write too): bit 0 says a stamp follows the 40-byte header,
// bit 2 says the owner issued the write to itself, bit 3 says an expire list follows the
// stamp; bit 1 is reserved for a packed multi-operation body and bits 4 to 7 are unassigned.
// A set bit this build does not know is refused, never ignored, so a body of a newer build is
// not applied as one operation.
const (
	opFlagStamp byte = 0x01
	// opFlagSystem marks a write the owner issued to itself: no client, no dedupe entry (expiry spec section 6).
	opFlagSystem byte = 0x04
	// opFlagExpire says an expire list follows the stamp.
	opFlagExpire byte = 0x08
	// opFlagAggregates says an aggregate stamp follows the expire list (aggregate spec 3.4).
	// Sections follow the flags in ascending bit order: the expiry build's 0x04 and 0x08
	// come before this one.
	opFlagAggregates byte = 0x10
	opFlagsKnown     byte = opFlagStamp | opFlagSystem | opFlagExpire | opFlagAggregates
)

// KnownOpFlags is the set of op-record flag bits this build defines; the on-disk golden of the
// state level records it, so a new flag cannot land without a StateLevel bump.
func KnownOpFlags() byte { return opFlagsKnown }

// Op is one write operation, the payload of a WRITE frame and the body of a
// retained-log record. The body is the class byte then
// the payload; the payload is a 40-byte header then the class form. Fields are
// named by id, never by name, so the apply reads no declaration.
//
// Each class uses exactly the members listed for it. EncodeOp refuses an Op
// that sets a member its class does not use, so one Op has one encoding, and
// DecodeOp returns zero values for the members a class does not use.
//
//	Insert, Upsert  EncID, IDValue, Fields{FieldID, Value}
//	Update          EncID, Fields{FieldID, OpCode, Value}
//	Delete          EncID
//	StoreVerb       EncID, one Field{FieldID, Verb, Value (the argument)}
//	Claim           EncID, one Field{FieldID}, Count, Now, Nonce
//	Ack, Release    EncID, one Field{FieldID}, LeaseID, Now
//	Extend          EncID, one Field{FieldID}, LeaseID, Now, Until
//
// The header members (BucketID, DeclVersion, SchemaVersionSeen, PrevEnd) ride
// with every class.
type Op struct {
	// Class is one of the nine Op constants.
	Class byte
	// BucketID is the bucket the owner resolved the statement against.
	BucketID uint64
	// DeclVersion is the declaration version the owner admitted the write at.
	DeclVersion int64
	// SchemaVersionSeen is the schema version the forwarder admitted it at.
	SchemaVersionSeen int64
	// PrevEnd is the last position of the previous epoch, 0:0 when this is not
	// an epoch opener.
	PrevEnd Position
	// EncID is the document id's EncodeComponent encoding.
	EncID []byte
	// IDValue is the id as a type-tagged value (Insert and Upsert only).
	IDValue []byte
	// Fields are the field entries, at most one per field id.
	Fields []FieldOp
	// Count is how many messages a Claim leases.
	Count uint16
	// Now is the owner's clock, stamped before the append (Claim, Ack, Release, Extend).
	Now int64
	// Until is the lease's new deadline (Extend only).
	Until int64
	// Nonce makes a Claim's lease id unique, stamped by the owner (Claim only).
	Nonce [16]byte
	// LeaseID names the lease (Ack, Release, Extend).
	LeaseID [16]byte
	// Stamp is the index stamp the ordering owner writes after the header of the record
	// body; empty on every client body and on a write that
	// touches no indexed field, and then absent from the bytes (flags bit 0 clear).
	Stamp IndexStamp
	// Expire tells the applier which fields of the write are expiry fields. Empty on every client body and on a write touching no expiry field; then absent from the bytes (flags bit 3 clear).
	Expire []ExpireItem
	// ExpireNow is the owner's wall clock in milliseconds, stamped with the expire items and
	// written after them, so every copy and every replay decides "expired" against the same
	// instant. Zero when Expire is empty.
	ExpireNow int64
	// AdmittedExpire is the expiry declaration the owner admitted this write under, for the owner's
	// precondition only: it is never encoded, so a record and a client body do not carry it. Nil
	// for a caller that has no admission (a test); the precondition then asks the declaration cache.
	AdmittedExpire *AdmittedExpire
	// System marks a write the owner issued to itself (the expiry sweep): flags bit 2. Never on a client body.
	System bool
	// Aggregates are the maintained aggregates the ordering owner stamps on a write to a
	// store field that declares them, in aggregate-id order, so every copy computes the same
	// keys from the record and never reads a declaration. At most 8; StoreVerb, Claim, Ack,
	// Release and Extend only; empty on every client body and then absent from the bytes
	// (flags bit 0x10 clear).
	Aggregates []AggregateStamp
	// Guard is the equalities the stored document must satisfy before the write applies
	// as field id and typed value, ascending ids, no repeats.
	// Update and Delete only. It rides the client body and is never in a record body.
	Guard []GuardTerm
	// Return is ReturnNone, ReturnAfter or ReturnBefore. Update and Delete only. It rides
	// both bodies, because a copy builds the same dedupe result the owner did.
	Return byte
}

// AdmittedExpire is a bucket's expiry declaration as the Writer admitted a write under it: the id
// of the bucket-form datetime field (0 for none) and each expiring queue field with its path.
type AdmittedExpire struct {
	BucketField uint16
	Stores      []ExpireItem
}

// ExpireItem tells the applier which field of the write is an expiry and where its datetime
// lies. Path is empty for the bucket form, where the datetime is the field's
// own scalar, and is the BSON key path inside the element for a queue element store.
type ExpireItem struct {
	FieldID uint16
	Path    []byte
}

// WrittenFields returns every field the operation writes. Today that is Fields; when field
// defaults land it is Fields merged with the defaults the owner filled. Anything that must see
// what a document ends up holding (the expiry items) reads it here, never op.Fields, so a
// defaulted datetime is not missed.
func (op Op) WrittenFields() []FieldOp { return op.Fields }

// IndexStamp is what the ordering owner stamps a record with when the write touches indexed
// fields: the ids of the fields whose index entries the applier writes, and
// the unique claims it holds. Index unit X3 fills the lists; this build carries the place.
type IndexStamp struct {
	// Fields are the indexed field ids the write touched, in the owner's order.
	Fields []uint16
	// Claims are the unique values the owner claimed for the write.
	Claims []UniqueClaim
}

// AggregateStamp is one aggregate of an aggregate stamp: the id that names its keys, its kind
// (1 sum, 2 sum by, 3 count by) and the element paths. Field is empty for count by and By for sum.
type AggregateStamp struct {
	AggID, Kind uint8
	Field, By   string
}

// opcodecMaxStampAggregates is the most aggregates a store field declares and a stamp carries.
const opcodecMaxStampAggregates = 8

// UniqueClaim is one unique-index claim of a stamp: the field and when it was claimed.
type UniqueClaim struct {
	FieldID   uint16
	ClaimedAt int64
}

// Empty reports whether the stamp carries nothing, the only form of "no indexes".
func (s IndexStamp) Empty() bool { return len(s.Fields) == 0 && len(s.Claims) == 0 }

// FlagsError is DecodeOp's refusal of a record body whose flags byte carries a bit this
// build does not know (bit 1 is reserved for a packed multi-operation body; bits 4 to 7 are
// unassigned).
// The replayer wraps it with the record's sequence and position (R2).
type FlagsError struct {
	// Flags is the byte as read.
	Flags byte
}

// Error implements error.
func (e *FlagsError) Error() string {
	return fmt.Sprintf("flags %#02x carry a bit this build does not know", e.Flags)
}

// FieldOp is one field entry of an Op. Which members a class reads is listed
// on Op.
type FieldOp struct {
	// FieldID is the field's id, from 1; id 0 is never allocated.
	FieldID uint16
	// OpCode is an Update* constant (Update only).
	OpCode byte
	// Verb is a Verb* constant (StoreVerb only).
	Verb byte
	// Value is a field value (Insert, Upsert, Update) or a store verb's argument.
	Value []byte
}

func opcodecError(format string, args ...any) error {
	return &Error{Code: CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

// opcodecValidate is the one definition of a well-formed Op, shared by the
// encoder and the decoder so that what one accepts the other writes back.
func opcodecValidate(op Op) error {
	if op.Class < OpInsert || op.Class > OpExtend {
		return opcodecError("operation class 0x%02x is not one of the nine write classes, 0x01 (insert) to 0x09 (extend). Nothing was written; send one of those classes", op.Class)
	}
	if op.DeclVersion < 0 || op.SchemaVersionSeen < 0 {
		return opcodecError("operation class 0x%02x carries a negative version (declaration %d, schema seen %d); versions are BSON int64 counters from 0", op.Class, op.DeclVersion, op.SchemaVersionSeen)
	}
	if err := opcodecCheckEncID(op.EncID); err != nil {
		return err
	}
	if len(op.Stamp.Fields) > opcodecMaxStampList || len(op.Stamp.Claims) > opcodecMaxStampList {
		return opcodecError("operation class 0x%02x carries a stamp of %d fields and %d claims; each list holds at most %d", op.Class, len(op.Stamp.Fields), len(op.Stamp.Claims), opcodecMaxStampList)
	}
	if len(op.Expire) > opcodecMaxExpireList {
		return opcodecError("operation class 0x%02x carries %d expire items; the list holds at most %d", op.Class, len(op.Expire), opcodecMaxExpireList)
	}
	if op.ExpireNow != 0 && len(op.Expire) == 0 {
		return opcodecError("operation class 0x%02x carries an expire clock but no expire items; the clock rides the list", op.Class)
	}
	if len(op.Expire) > 0 && op.ExpireNow == 0 {
		return opcodecError("operation class 0x%02x carries expire items but no clock; the owner stamps both", op.Class)
	}
	for i, stamp := range op.Expire {
		if i > 0 && stamp.FieldID <= op.Expire[i-1].FieldID {
			return opcodecError("operation class 0x%02x carries expire items out of order or twice (field %d after %d); list each field once, ascending", op.Class, stamp.FieldID, op.Expire[i-1].FieldID)
		}
		if len(stamp.Path) > opcodecMaxExpireList {
			return opcodecError("expire stamp for field %d carries a path of %d bytes; a path holds at most %d", stamp.FieldID, len(stamp.Path), opcodecMaxExpireList)
		}
	}
	if err := opcodecValidateAggregates(op); err != nil {
		return fmt.Errorf("checking the aggregate stamp of operation class 0x%02x: %w", op.Class, err)
	}
	usesID := op.Class == OpInsert || op.Class == OpUpsert
	if usesID {
		if len(op.IDValue) == 0 || uint64(len(op.IDValue)) > math.MaxUint32 {
			return opcodecError("operation class 0x%02x needs a non-empty id value", op.Class)
		}
	} else if len(op.IDValue) != 0 {
		return opcodecError("operation class 0x%02x does not carry an id value; drop IDValue", op.Class)
	}
	usesClaim := op.Class == OpClaim
	usesNow := op.Class == OpClaim || op.Class == OpExtend || op.Class == OpAck || op.Class == OpRelease
	usesLease := op.Class == OpAck || op.Class == OpRelease || op.Class == OpExtend
	switch {
	case !usesClaim && (op.Count != 0 || op.Nonce != [16]byte{}):
		return opcodecError("operation class 0x%02x does not use Count or Nonce; those belong to CLAIM only", op.Class)
	case !usesNow && op.Now != 0:
		return opcodecError("operation class 0x%02x does not use Now; only CLAIM, ACK, RELEASE and EXTEND are stamped", op.Class)
	case op.Class != OpExtend && op.Until != 0:
		return opcodecError("operation class 0x%02x does not use Until; only EXTEND has a deadline", op.Class)
	case !usesLease && op.LeaseID != [16]byte{}:
		return opcodecError("operation class 0x%02x does not use LeaseID; only ACK, RELEASE and EXTEND name a lease", op.Class)
	}
	if err := opcodecValidateGuard(op); err != nil {
		return err
	}
	return opcodecValidateFields(op)
}

func opcodecCheckEncID(encID []byte) error {
	if len(encID) == 0 || len(encID) > opcodecMaxEncID {
		return opcodecError("document id encoding is %d bytes, want 1 to %d", len(encID), opcodecMaxEncID)
	}
	width, err := EncodedComponentLen(encID)
	if err != nil {
		return opcodecError("document id encoding is not an EncodeComponent value: %v", err)
	}
	if width != len(encID) {
		return opcodecError("document id encoding is %d bytes but its component is %d", len(encID), width)
	}
	return nil
}

func opcodecValidateFields(op Op) error {
	switch op.Class {
	case OpDelete:
		if len(op.Fields) != 0 {
			return opcodecError("DELETE carries no fields, got %d", len(op.Fields))
		}
		return nil
	case OpStoreVerb, OpClaim, OpAck, OpRelease, OpExtend:
		if len(op.Fields) != 1 {
			return opcodecError("operation class 0x%02x names exactly one store field, got %d", op.Class, len(op.Fields))
		}
	}
	if len(op.Fields) > math.MaxUint16 {
		return opcodecError("operation class 0x%02x carries %d fields, more than the 65535 a field count can hold", op.Class, len(op.Fields))
	}
	seen := make(map[uint16]struct{}, len(op.Fields))
	for _, field := range op.Fields {
		if field.FieldID == 0 {
			return opcodecError("field id 0 is never allocated; fields are named by id from 1")
		}
		if _, dup := seen[field.FieldID]; dup {
			return opcodecError("field id %d appears twice; an operation carries one entry per field id, so combine them into one", field.FieldID)
		}
		seen[field.FieldID] = struct{}{}
		if err := opcodecValidateField(op.Class, field); err != nil {
			return err
		}
	}
	return nil
}

func opcodecValidateField(class byte, field FieldOp) error {
	if uint64(len(field.Value)) > math.MaxUint32 {
		return opcodecError("field %d value is too long to frame", field.FieldID)
	}
	usesOpCode := class == OpUpdate
	usesVerb := class == OpStoreVerb
	usesValue := class == OpInsert || class == OpUpsert || class == OpUpdate || class == OpStoreVerb
	switch {
	case !usesOpCode && field.OpCode != 0:
		return opcodecError("operation class 0x%02x does not use an update code on field %d", class, field.FieldID)
	case !usesVerb && field.Verb != 0:
		return opcodecError("operation class 0x%02x does not use a store verb on field %d", class, field.FieldID)
	case !usesValue && len(field.Value) != 0:
		return opcodecError("operation class 0x%02x does not carry a value on field %d", class, field.FieldID)
	}
	switch class {
	case OpInsert, OpUpsert:
		if len(field.Value) == 0 {
			return opcodecError("field %d has no value; a value is its type byte then the BSON bytes", field.FieldID)
		}
	case OpUpdate:
		if field.OpCode < UpdateSet || field.OpCode > UpdateRenameTo {
			return opcodecError("field %d has update code 0x%02x, want 0x01 to 0x09", field.FieldID, field.OpCode)
		}
		if field.OpCode == UpdateRenameTo && len(field.Value) != 2 {
			return opcodecError("field %d renames to a 2-byte field id, got %d bytes", field.FieldID, len(field.Value))
		}
	case OpStoreVerb:
		if field.Verb < VerbPush || field.Verb > VerbInc {
			return opcodecError("field %d has store verb %d, want 1 push, 2 pop, 3 take, 4 put or 5 inc", field.FieldID, field.Verb)
		}
	}
	return nil
}

// EncodeOp returns the record body for op: [class][flags][header:40][stamp?][expire?][form],
// the bytes the ordering owner appends to the retained log and every copy appends as received
// unchanged. The flags byte is 0x00 and the stamp and expire list absent
// when the write has neither, so a write that touches no indexed or expiry field costs one
// byte over the client body. It refuses an Op that DecodeOp would not accept, with
// INVALID_ARGUMENT.
func EncodeOp(op Op) ([]byte, error) {
	return opcodecEncode(op, true)
}

// EncodeClientOp returns the client body for op: [class][header:40][form], the bytes
// RING_KEYED, RING_BATCH and FORWARD_WRITE carry, byte for byte the body of every earlier
// build. A client body carries no flags byte and no stamp: the ordering owner alone
// stamps, when it builds the record body under the pair lock.
func EncodeClientOp(op Op) ([]byte, error) {
	if !op.Stamp.Empty() || len(op.Aggregates) != 0 {
		return nil, opcodecError("operation class 0x%02x carries a stamp in a client body; only the ordering owner stamps a record", op.Class)
	}
	if len(op.Expire) != 0 {
		return nil, opcodecError("operation class 0x%02x carries expire items in a client body; only the ordering owner stamps expiry", op.Class)
	}
	if op.ExpireNow != 0 {
		return nil, opcodecError("operation class 0x%02x carries an expire clock in a client body; only the ordering owner stamps expiry", op.Class)
	}
	if op.System {
		return nil, opcodecError("operation class 0x%02x carries the system flag in a client body; only the owner issues a write to itself", op.Class)
	}
	return opcodecEncode(op, false)
}

// opcodecEncode is the one encoder: the record form carries the flags byte and the stamp,
// the client form neither.
func opcodecEncode(op Op, record bool) ([]byte, error) {
	if err := opcodecValidate(op); err != nil {
		return nil, err
	}
	if record && len(op.Guard) != 0 {
		return nil, opcodecError("operation class 0x%02x carries a guard in a record body; the guard is checked before the append and never logged", op.Class)
	}
	body := make([]byte, 0, 2+opcodecHeaderLen+len(op.EncID)+len(op.IDValue)+64)
	body = append(body, op.Class)
	if record {
		var flags byte
		if !op.Stamp.Empty() {
			flags |= opFlagStamp
		}
		if op.System {
			flags |= opFlagSystem
		}
		if len(op.Expire) > 0 {
			flags |= opFlagExpire
		}
		if len(op.Aggregates) != 0 {
			flags |= opFlagAggregates
		}
		body = append(body, flags)
	}
	body = binary.BigEndian.AppendUint64(body, op.BucketID)
	body = binary.BigEndian.AppendUint64(body, uint64(op.DeclVersion))
	body = binary.BigEndian.AppendUint64(body, uint64(op.SchemaVersionSeen))
	body = AppendPosition(body, op.PrevEnd)
	if record && !op.Stamp.Empty() {
		body = opcodecAppendStamp(body, op.Stamp)
	}
	if record && len(op.Expire) > 0 {
		body = opcodecAppendExpire(body, op.Expire, op.ExpireNow)
	}
	if record && len(op.Aggregates) != 0 {
		body = opcodecAppendAggregates(body, op.Aggregates)
	}
	body = append(body, op.EncID...)
	switch op.Class {
	case OpInsert, OpUpsert:
		body = binary.BigEndian.AppendUint32(body, uint32(len(op.IDValue)))
		body = append(body, op.IDValue...)
		body = binary.BigEndian.AppendUint16(body, uint16(len(op.Fields)))
		for _, field := range op.Fields {
			body = binary.BigEndian.AppendUint16(body, field.FieldID)
			body = binary.BigEndian.AppendUint32(body, uint32(len(field.Value)))
			body = append(body, field.Value...)
		}
	case OpUpdate:
		body = binary.BigEndian.AppendUint16(body, uint16(len(op.Fields)))
		for _, field := range op.Fields {
			body = append(body, field.OpCode)
			body = binary.BigEndian.AppendUint16(body, field.FieldID)
			body = binary.BigEndian.AppendUint32(body, uint32(len(field.Value)))
			body = append(body, field.Value...)
		}
		body = opcodecAppendTrailer(body, op, record)
	case OpDelete:
		body = opcodecAppendTrailer(body, op, record)
	case OpStoreVerb:
		field := op.Fields[0]
		body = binary.BigEndian.AppendUint16(body, field.FieldID)
		body = append(body, field.Verb)
		body = binary.BigEndian.AppendUint32(body, uint32(len(field.Value)))
		body = append(body, field.Value...)
	case OpClaim:
		body = binary.BigEndian.AppendUint16(body, op.Fields[0].FieldID)
		body = binary.BigEndian.AppendUint16(body, op.Count)
		body = binary.BigEndian.AppendUint64(body, uint64(op.Now))
		body = append(body, op.Nonce[:]...)
	case OpAck, OpRelease:
		body = binary.BigEndian.AppendUint16(body, op.Fields[0].FieldID)
		body = append(body, op.LeaseID[:]...)
		body = binary.BigEndian.AppendUint64(body, uint64(op.Now))
	case OpExtend:
		body = binary.BigEndian.AppendUint16(body, op.Fields[0].FieldID)
		body = append(body, op.LeaseID[:]...)
		body = binary.BigEndian.AppendUint64(body, uint64(op.Now))
		body = binary.BigEndian.AppendUint64(body, uint64(op.Until))
	}
	return body, nil
}

// opcodecAppendStamp writes [n:1][fieldID:2 × n][u:1][(fieldID:2 ‖ claimedAt:8) × u]:
// 2 + 2n + 10u bytes.
func opcodecAppendStamp(body []byte, stamp IndexStamp) []byte {
	body = append(body, byte(len(stamp.Fields)))
	for _, fieldID := range stamp.Fields {
		body = binary.BigEndian.AppendUint16(body, fieldID)
	}
	body = append(body, byte(len(stamp.Claims)))
	for _, claim := range stamp.Claims {
		body = binary.BigEndian.AppendUint16(body, claim.FieldID)
		body = binary.BigEndian.AppendUint64(body, uint64(claim.ClaimedAt))
	}
	return body
}

// opcodecAppendExpire writes [n:1] then n times ([fieldID:2 BE][pLen:1][path:pLen]), then the
// owner's clock [now:8 BE]: 9 + Σ(3+pLen) bytes.
func opcodecAppendExpire(body []byte, items []ExpireItem, now int64) []byte {
	body = append(body, byte(len(items)))
	for _, stamp := range items {
		body = binary.BigEndian.AppendUint16(body, stamp.FieldID)
		body = append(body, byte(len(stamp.Path)))
		body = append(body, stamp.Path...)
	}
	return binary.BigEndian.AppendUint64(body, uint64(now))
}

func opcodecAppendAggregates(body []byte, aggregates []AggregateStamp) []byte {
	body = append(body, byte(len(aggregates)))
	for _, aggregate := range aggregates {
		body = append(body, aggregate.AggID, aggregate.Kind, byte(len(aggregate.Field)))
		body = append(body, aggregate.Field...)
		body = append(body, byte(len(aggregate.By)))
		body = append(body, aggregate.By...)
	}
	return body
}

// opcodecValidateAggregates is the one definition of a well-formed aggregate stamp, shared by
// the encoder and the decoder: only the classes that touch a store field's elements carry one.
func opcodecValidateAggregates(op Op) error {
	if len(op.Aggregates) == 0 {
		return nil
	}
	switch op.Class {
	case OpStoreVerb, OpClaim, OpAck, OpRelease, OpExtend:
	default:
		return opcodecError("operation class 0x%02x carries an aggregate stamp; only STORE_VERB, CLAIM, ACK, RELEASE and EXTEND touch store elements", op.Class)
	}
	if len(op.Aggregates) > opcodecMaxStampAggregates {
		return opcodecError("operation class 0x%02x carries %d aggregates; a store field declares at most %d", op.Class, len(op.Aggregates), opcodecMaxStampAggregates)
	}
	for _, aggregate := range op.Aggregates {
		switch {
		case aggregate.AggID == 0:
			return opcodecError("aggregate id 0 is never minted; ids start at 1")
		case aggregate.Kind < 1 || aggregate.Kind > 3:
			return opcodecError("aggregate %d has kind %d, want 1 (sum), 2 (sum by) or 3 (count by)", aggregate.AggID, aggregate.Kind)
		case len(aggregate.Field) > 255 || len(aggregate.By) > 255:
			return opcodecError("aggregate %d names a path over 255 bytes", aggregate.AggID)
		case (aggregate.Kind == 3) != (aggregate.Field == ""):
			return opcodecError("aggregate %d: a count by names no field and a sum names one", aggregate.AggID)
		case (aggregate.Kind == 1) != (aggregate.By == ""):
			return opcodecError("aggregate %d: a sum names no by and a by form names one", aggregate.AggID)
		}
	}
	return nil
}

// opcodecDecodeAggregates reads the aggregate stamp that flags bit 0x10 announces. An empty
// list is refused: the encoder never writes one, so accepting it would let two byte strings
// name one Op.
func opcodecDecodeAggregates(reader *opcodecReader, op *Op) error {
	n, err := reader.byte("aggregate count")
	if err != nil {
		return fmt.Errorf("decoding the aggregate stamp: %w", err)
	}
	if n == 0 {
		return opcodecError("operation body sets the aggregate flag over an empty list; a write with no aggregates carries none")
	}
	if int(n) > opcodecMaxStampAggregates || uint64(n)*4 > uint64(len(reader.rest)) {
		return opcodecError("aggregate count %d is over the %d a store field declares or cannot fit in the %d bytes that remain", n, opcodecMaxStampAggregates, len(reader.rest))
	}
	for range n {
		head, err := reader.take(3, "aggregate header")
		if err != nil {
			return fmt.Errorf("decoding aggregate %d of the stamp: %w", len(op.Aggregates)+1, err)
		}
		field, err := reader.take(uint64(head[2]), "aggregate field path")
		if err != nil {
			return fmt.Errorf("decoding the field path of aggregate %d of the stamp: %w", head[0], err)
		}
		byLen, err := reader.byte("aggregate by length")
		if err != nil {
			return fmt.Errorf("decoding the by length of aggregate %d of the stamp: %w", head[0], err)
		}
		by, err := reader.take(uint64(byLen), "aggregate by path")
		if err != nil {
			return fmt.Errorf("decoding the by path of aggregate %d of the stamp: %w", head[0], err)
		}
		op.Aggregates = append(op.Aggregates, AggregateStamp{AggID: head[0], Kind: head[1], Field: string(field), By: string(by)})
	}
	return opcodecValidateAggregates(*op)
}

// opcodecReader walks a body, checking every length against what is left
// before it slices or allocates.
type opcodecReader struct {
	rest []byte
}

func (r *opcodecReader) take(n uint64, what string) ([]byte, error) {
	if n > uint64(len(r.rest)) {
		return nil, opcodecError("operation body is truncated: %s needs %d bytes, %d remain", what, n, len(r.rest))
	}
	taken := r.rest[:n]
	r.rest = r.rest[n:]
	return taken, nil
}

func (r *opcodecReader) uint16(what string) (uint16, error) {
	raw, err := r.take(2, what)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(raw), nil
}

func (r *opcodecReader) uint32(what string) (uint32, error) {
	raw, err := r.take(4, what)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(raw), nil
}

func (r *opcodecReader) uint64(what string) (uint64, error) {
	raw, err := r.take(8, what)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(raw), nil
}

func (r *opcodecReader) byte(what string) (byte, error) {
	raw, err := r.take(1, what)
	if err != nil {
		return 0, err
	}
	return raw[0], nil
}

func (r *opcodecReader) bytes(what string) ([]byte, error) {
	length, err := r.uint32(what + " length")
	if err != nil {
		return nil, err
	}
	raw, err := r.take(uint64(length), what)
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	return append([]byte(nil), raw...), nil
}

// DecodeOp reads a record body written by EncodeOp. It never panics: every length is
// checked against the bytes left before anything is sliced or allocated, trailing bytes
// are refused, an unknown class is refused with INVALID_ARGUMENT, a flags byte with a bit
// this build does not know is refused with a *FlagsError, a set stamp bit over an empty stamp
// is refused (absent is the one canonical form of "no indexes"), and so is a set expire bit
// over an empty list. Whether the
// class byte equals the retained-log record's own class byte is the record decoder's
// check; this codec never sees the record header.
func DecodeOp(body []byte) (Op, error) {
	return opcodecDecodeWhole(body, true)
}

// DecodeClientOp reads a client body written by EncodeClientOp: [class][header:40][form],
// no flags byte. It is a decoder of its own because one decoder for both bodies would read
// a client body's second byte (the bucket id's top byte, 0x00 below 2^56) as the flags and
// shift every header field by one: a plausible wrong header, not a refusal.
func DecodeClientOp(body []byte) (Op, error) {
	return opcodecDecodeWhole(body, false)
}

func opcodecDecodeWhole(body []byte, record bool) (Op, error) {
	reader := &opcodecReader{rest: body}
	op, err := opcodecDecodeOne(reader, record)
	if err != nil {
		return Op{}, err
	}
	if len(reader.rest) != 0 {
		return Op{}, opcodecError("operation body has %d bytes after its last field", len(reader.rest))
	}
	if err := opcodecValidate(op); err != nil {
		return Op{}, err
	}
	return op, nil
}

// DecodeClientOpPrefix reads the one client-form operation that starts body and returns it
// with the number of bytes it took. It is DecodeClientOp for a caller that walks a run of
// operations laid end to end (RING_BATCH): bytes after the operation are the next one's,
// not an error, and the operation is validated exactly as DecodeClientOp validates it.
func DecodeClientOpPrefix(body []byte) (Op, int, error) {
	reader := &opcodecReader{rest: body}
	op, err := opcodecDecodeOne(reader, false)
	if err != nil {
		return Op{}, 0, err
	}
	if err := opcodecValidate(op); err != nil {
		return Op{}, 0, err
	}
	return op, len(body) - len(reader.rest), nil
}

// opcodecDecodeOne reads the class, the flags and the stamp of a record body, the header
// and the form of one operation. It checks no trailing bytes and does not validate: its
// callers differ in both.
func opcodecDecodeOne(reader *opcodecReader, record bool) (Op, error) {
	class, err := reader.byte("class")
	if err != nil {
		return Op{}, err
	}
	var op Op
	op.Class = class
	if class < OpInsert || class > OpExtend {
		return Op{}, opcodecValidate(op)
	}
	var flags byte
	if record {
		if flags, err = reader.byte("flags"); err != nil {
			return Op{}, err
		}
		if flags&^opFlagsKnown != 0 {
			return Op{}, &FlagsError{Flags: flags}
		}
		// Only the expiry sweep issues a system write, and it issues a delete or a store verb.
		if flags&opFlagSystem != 0 && class != OpDelete && class != OpStoreVerb {
			return Op{}, &FlagsError{Flags: flags}
		}
	}
	if err := opcodecDecodeHeader(reader, &op); err != nil {
		return Op{}, err
	}
	if flags&opFlagStamp != 0 {
		if err := opcodecDecodeStamp(reader, &op); err != nil {
			return Op{}, err
		}
	}
	op.System = flags&opFlagSystem != 0
	if flags&opFlagExpire != 0 {
		if err := opcodecDecodeExpire(reader, &op); err != nil {
			return Op{}, err
		}
	}
	if flags&opFlagAggregates != 0 {
		if err := opcodecDecodeAggregates(reader, &op); err != nil {
			return Op{}, err
		}
	}
	if err := opcodecDecodeEncID(reader, &op); err != nil {
		return Op{}, err
	}
	if err := opcodecDecodeForm(reader, &op, record); err != nil {
		return Op{}, err
	}
	return op, nil
}

func opcodecDecodeHeader(reader *opcodecReader, op *Op) error {
	header, err := reader.take(opcodecHeaderLen, "payload header")
	if err != nil {
		return err
	}
	op.BucketID = binary.BigEndian.Uint64(header[0:8])
	declVersion := binary.BigEndian.Uint64(header[8:16])
	schemaSeen := binary.BigEndian.Uint64(header[16:24])
	if declVersion > math.MaxInt64 || schemaSeen > math.MaxInt64 {
		return opcodecError("operation header carries a version above the int64 range")
	}
	op.DeclVersion, op.SchemaVersionSeen = int64(declVersion), int64(schemaSeen)
	op.PrevEnd, err = DecodePosition(header[24:40])
	if err != nil {
		return fmt.Errorf("operation header prevEnd: %w", err)
	}
	return nil
}

// opcodecDecodeStamp reads the stamp that flags bit 0 announces. An empty one is refused:
// the encoder never writes it, so accepting it would let two byte strings name one Op.
func opcodecDecodeStamp(reader *opcodecReader, op *Op) error {
	n, err := reader.byte("stamp field count")
	if err != nil {
		return err
	}
	if uint64(n)*2 > uint64(len(reader.rest)) {
		return opcodecError("stamp field count %d cannot fit in the %d bytes that remain", n, len(reader.rest))
	}
	for range n {
		fieldID, err := reader.uint16("stamp field id")
		if err != nil {
			return err
		}
		op.Stamp.Fields = append(op.Stamp.Fields, fieldID)
	}
	u, err := reader.byte("stamp claim count")
	if err != nil {
		return err
	}
	if uint64(u)*10 > uint64(len(reader.rest)) {
		return opcodecError("stamp claim count %d cannot fit in the %d bytes that remain", u, len(reader.rest))
	}
	for range u {
		fieldID, err := reader.uint16("stamp claim field id")
		if err != nil {
			return err
		}
		claimedAt, err := reader.uint64("stamp claim time")
		if err != nil {
			return err
		}
		op.Stamp.Claims = append(op.Stamp.Claims, UniqueClaim{FieldID: fieldID, ClaimedAt: int64(claimedAt)})
	}
	if op.Stamp.Empty() {
		return opcodecError("operation body sets the stamp flag over an empty stamp; a write with no indexes carries no stamp")
	}
	return nil
}

// opcodecDecodeExpire reads the expire list that flags bit 3 announces. An empty one is
// refused: the encoder never writes it, so accepting it would let two byte strings name one
// Op. Each item needs at least its field id and path length, three bytes, and the list is
// followed by the owner's clock, eight.
func opcodecDecodeExpire(reader *opcodecReader, op *Op) error {
	n, err := reader.byte("expire count")
	if err != nil {
		return err
	}
	if uint64(n)*3+8 > uint64(len(reader.rest)) {
		return opcodecError("expire count %d cannot fit in the %d bytes that remain", n, len(reader.rest))
	}
	for range n {
		fieldID, err := reader.uint16("expire field id")
		if err != nil {
			return err
		}
		pathLen, err := reader.byte("expire path length")
		if err != nil {
			return err
		}
		var path []byte
		if pathLen != 0 {
			raw, err := reader.take(uint64(pathLen), "expire path")
			if err != nil {
				return err
			}
			path = append([]byte(nil), raw...)
		}
		op.Expire = append(op.Expire, ExpireItem{FieldID: fieldID, Path: path})
	}
	if len(op.Expire) == 0 {
		return opcodecError("operation body sets the expire flag over an empty expire list; a write that expires nothing carries no list")
	}
	now, err := reader.uint64("expire clock")
	if err != nil {
		return err
	}
	op.ExpireNow = int64(now)
	if op.ExpireNow == 0 {
		return opcodecError("operation body carries expire items with a zero clock; the owner stamps both")
	}
	for i := 1; i < len(op.Expire); i++ {
		if op.Expire[i].FieldID <= op.Expire[i-1].FieldID {
			return opcodecError("operation body carries expire items out of order or twice (field %d after %d)", op.Expire[i].FieldID, op.Expire[i-1].FieldID)
		}
	}
	return nil
}

func opcodecDecodeEncID(reader *opcodecReader, op *Op) error {
	width, err := EncodedComponentLen(reader.rest)
	if err != nil {
		return opcodecError("document id encoding does not parse: %v", err)
	}
	encID, err := reader.take(uint64(width), "document id encoding")
	if err != nil {
		return err
	}
	op.EncID = append([]byte(nil), encID...)
	return nil
}

func opcodecDecodeForm(reader *opcodecReader, op *Op, record bool) error {
	switch op.Class {
	case OpInsert, OpUpsert:
		return opcodecDecodeInsert(reader, op)
	case OpUpdate:
		return opcodecDecodeUpdate(reader, op, record)
	case OpDelete:
		return opcodecDecodeTrailer(reader, op, record)
	case OpStoreVerb:
		fieldID, err := reader.uint16("field id")
		if err != nil {
			return err
		}
		verb, err := reader.byte("verb")
		if err != nil {
			return err
		}
		arg, err := reader.bytes("verb argument")
		if err != nil {
			return err
		}
		op.Fields = []FieldOp{{FieldID: fieldID, Verb: verb, Value: arg}}
		return nil
	}
	fieldID, err := reader.uint16("field id")
	if err != nil {
		return err
	}
	op.Fields = []FieldOp{{FieldID: fieldID}}
	return opcodecDecodeLease(reader, op)
}

// opcodecDecodeLease reads the tail of CLAIM, ACK, RELEASE and EXTEND.
func opcodecDecodeLease(reader *opcodecReader, op *Op) error {
	if op.Class == OpClaim {
		count, err := reader.uint16("count")
		if err != nil {
			return err
		}
		now, err := reader.uint64("now")
		if err != nil {
			return err
		}
		nonce, err := reader.take(opcodecLeaseLen, "nonce")
		if err != nil {
			return err
		}
		op.Count, op.Now = count, int64(now)
		copy(op.Nonce[:], nonce)
		return nil
	}
	lease, err := reader.take(opcodecLeaseLen, "lease id")
	if err != nil {
		return err
	}
	copy(op.LeaseID[:], lease)
	now, err := reader.uint64("now")
	if err != nil {
		return err
	}
	if op.Class != OpExtend {
		op.Now = int64(now)
		return nil
	}
	until, err := reader.uint64("until")
	if err != nil {
		return err
	}
	op.Now, op.Until = int64(now), int64(until)
	return nil
}

func opcodecDecodeInsert(reader *opcodecReader, op *Op) error {
	idValue, err := reader.bytes("id value")
	if err != nil {
		return err
	}
	op.IDValue = idValue
	count, err := reader.uint16("field count")
	if err != nil {
		return err
	}
	// Each entry is at least 6 bytes, so a count the body cannot hold is
	// refused before the slice is allocated.
	if uint64(count)*6 > uint64(len(reader.rest)) {
		return opcodecError("field count %d cannot fit in the %d bytes that remain", count, len(reader.rest))
	}
	for range count {
		fieldID, err := reader.uint16("field id")
		if err != nil {
			return err
		}
		fieldValue, err := reader.bytes("field value")
		if err != nil {
			return err
		}
		op.Fields = append(op.Fields, FieldOp{FieldID: fieldID, Value: fieldValue})
	}
	return nil
}

func opcodecDecodeUpdate(reader *opcodecReader, op *Op, record bool) error {
	count, err := reader.uint16("operation count")
	if err != nil {
		return err
	}
	if uint64(count)*7 > uint64(len(reader.rest)) {
		return opcodecError("operation count %d cannot fit in the %d bytes that remain", count, len(reader.rest))
	}
	for range count {
		opCode, err := reader.byte("update code")
		if err != nil {
			return err
		}
		fieldID, err := reader.uint16("field id")
		if err != nil {
			return err
		}
		fieldValue, err := reader.bytes("field value")
		if err != nil {
			return err
		}
		op.Fields = append(op.Fields, FieldOp{FieldID: fieldID, OpCode: opCode, Value: fieldValue})
	}
	return opcodecDecodeTrailer(reader, op, record)
}
