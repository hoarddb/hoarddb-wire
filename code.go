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
	"fmt"
	"sort"
)

// Code is one refusal code of the entry-ring. A code
// is one fact; a code that serves two conditions has two Condition texts.
type Code uint16

// The refusal codes. 0x01-0x12 are the product's, 0x13-0x2A this design's.
// STALE_INCARNATION has no code: it is refused at the handshake and never
// surfaced.
const (
	CodeNotFound             Code = 0x01
	CodePermissionDenied     Code = 0x03
	CodeInvalidArgument      Code = 0x04
	CodeUnavailable          Code = 0x06
	CodeResultTooLarge       Code = 0x0E
	CodeFieldType            Code = 0x10
	CodeDocumentExists       Code = 0x12
	CodeNotOwner             Code = 0x13
	CodeStaleEpoch           Code = 0x14
	CodeOwnerDown            Code = 0x15
	CodeNoQuorum             Code = 0x16
	CodeRFUnsupported        Code = 0x17
	CodeLeaseLapsed          Code = 0x18
	CodeEpochFenced          Code = 0x19
	CodeSeqGap               Code = 0x1A
	CodeLogBelowHorizon      Code = 0x1B
	CodeStateMoved           Code = 0x1C
	CodeConflict             Code = 0x1D
	CodeClusterMismatch      Code = 0x1E
	CodeNotADataNode         Code = 0x1F
	CodeSchemaNotCurrent     Code = 0x20
	CodeBucketDropping       Code = 0x21
	CodeDeclUnavailable      Code = 0x22
	CodeBucketExists         Code = 0x23
	CodeReservedName         Code = 0x24
	CodeBucketIDCollision    Code = 0x25
	CodeSeedNotHeld          Code = 0x26
	CodeReplaceTargetHasData Code = 0x27
	CodeMintFailed           Code = 0x28
	CodeCorruptKey           Code = 0x29
	CodeSeedHeld             Code = 0x2A
)

// codeInfo is the name and exit status of a code. Exit 2 is a mistake the
// caller can correct, 8 is a condition that clears by retrying or waiting,
// and 0 is a code that never reaches a user (node to node only).
type codeInfo struct {
	name string
	exit int
}

var codeTable = map[Code]codeInfo{
	CodeNotFound:             {"NOT_FOUND", 2},
	CodePermissionDenied:     {"PERMISSION_DENIED", 2},
	CodeInvalidArgument:      {"INVALID_ARGUMENT", 2},
	CodeUnavailable:          {"UNAVAILABLE", 8},
	CodeResultTooLarge:       {"RESULT_TOO_LARGE", 2},
	CodeFieldType:            {"FIELD_TYPE", 2},
	CodeDocumentExists:       {"DOCUMENT_EXISTS", 2},
	CodeNotOwner:             {"NOT_OWNER", 8},
	CodeStaleEpoch:           {"STALE_EPOCH", 8},
	CodeOwnerDown:            {"OWNER_DOWN", 8},
	CodeNoQuorum:             {"NO_QUORUM", 8},
	CodeRFUnsupported:        {"RF_UNSUPPORTED", 2},
	CodeLeaseLapsed:          {"LEASE_LAPSED", 8},
	CodeEpochFenced:          {"EPOCH_FENCED", 0},
	CodeSeqGap:               {"SEQ_GAP", 0},
	CodeLogBelowHorizon:      {"LOG_BELOW_HORIZON", 8},
	CodeStateMoved:           {"STATE_MOVED", 8},
	CodeConflict:             {"CONFLICT", 8},
	CodeClusterMismatch:      {"CLUSTER_MISMATCH", 2},
	CodeNotADataNode:         {"NOT_A_DATA_NODE", 2},
	CodeSchemaNotCurrent:     {"SCHEMA_NOT_CURRENT", 8},
	CodeBucketDropping:       {"BUCKET_DROPPING", 8},
	CodeDeclUnavailable:      {"DECL_UNAVAILABLE", 8},
	CodeBucketExists:         {"BUCKET_EXISTS", 2},
	CodeReservedName:         {"RESERVED_NAME", 2},
	CodeBucketIDCollision:    {"BUCKET_ID_COLLISION", 8},
	CodeSeedNotHeld:          {"SEED_NOT_HELD", 2},
	CodeReplaceTargetHasData: {"REPLACE_TARGET_HAS_DATA", 8},
	CodeMintFailed:           {"MINT_FAILED", 8},
	CodeCorruptKey:           {"CORRUPT_KEY", 2},
	CodeSeedHeld:             {"SEED_HELD", 8},
}

// Codes returns every registered code in ascending order.
func Codes() []Code {
	all := make([]Code, 0, len(codeTable))
	for code := range codeTable {
		all = append(all, code)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return all
}

// String returns the code's registered name, or its number when unregistered.
func (c Code) String() string {
	if info, ok := codeTable[c]; ok {
		return info.name
	}
	return fmt.Sprintf("UNKNOWN(0x%02X)", uint16(c))
}

// Exit returns the process exit status the CLI uses for the code. An
// unregistered code exits 1, the generic failure.
func (c Code) Exit() int {
	if info, ok := codeTable[c]; ok {
		return info.exit
	}
	return 1
}

// Error is a refusal carrying its code, for callers that branch on it with
// errors.As.
type Error struct {
	Code    Code
	Message string
}

// Error returns the message, which already names the fix.
func (e *Error) Error() string { return e.Message }

// Detail is the structured part of an error payload that a client acts on,
// so no driver parses a message. Each code fills only its own shape:
//
//	NOT_OWNER      Owner, OwnerAddr, Epoch, Slot
//	STALE_EPOCH    Epoch, Owner, OwnerAddr
//	OWNER_DOWN     Owner, MissingSlots
//	NO_QUORUM      Voters, Answered
//	LEASE_LAPSED   Node, LapsedMs
//	RF_UNSUPPORTED Rf, Voters, DataMembers
//	NOT_FOUND      Expired (a keyed miss on a document past its expire time; no detail otherwise)
type Detail struct {
	Owner        string
	OwnerAddr    string
	Node         string
	Epoch, Slot  uint64
	Voters       int
	Answered     int
	Rf           int
	DataMembers  int
	MissingSlots int
	LapsedMs     int
	// Expired is set on the NOT_FOUND of a document that exists but is past its expire time.
	Expired bool
}
