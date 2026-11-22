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
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// MaxName bounds a database or bucket name on the wire: a bucket name is at most 1021
// bytes, and a database name is held to the same.
const MaxName = 1021

// ringListDepth is the nesting a filter may use: equality matches on scalar fields.
const ringListDepth = 4

// RingListRequest is the RING_LIST body: one BSON document that names a bucket, a filter and a page.
type RingListRequest struct {
	// Database and Bucket name the bucket.
	Database, Bucket string
	// Filter is one BSON document of equality matches, or nil.
	Filter []byte
	// Fields names the scalar fields to return besides _id_, or nil for every field.
	Fields []string
	// Limit is the page size; nil leaves it out and asks for the default. A value below 0
	// is carried to the node, which refuses it with the registered text of that refusal.
	Limit *int32
	// Cursor is the opaque resume point of the previous page, or nil for the first.
	Cursor []byte
	// Epoch is the epoch of the client's table; 0 asks the node to route.
	Epoch int64
	// Options is what the list carries besides its filter and page: local and expand.
	Options RingReadOptions
}

// RingCountRequest is the RING_COUNT body: one BSON document that names a bucket and a filter.
type RingCountRequest struct {
	// Database and Bucket name the bucket.
	Database, Bucket string
	// Filter is one BSON document of equality matches, or nil.
	Filter []byte
}

// The wire documents. Member order is queries Formats' order and is the canonical layout.
type ringListDoc struct {
	Database string          `bson:"database"`
	Bucket   string          `bson:"bucket"`
	Filter   bson.Raw        `bson:"filter,omitempty"`
	Fields   []string        `bson:"fields,omitempty"`
	Limit    *int32          `bson:"limit,omitempty"`
	Cursor   []byte          `bson:"cursor,omitempty"`
	Epoch    int64           `bson:"epoch,omitempty"`
	Local    bool            `bson:"local,omitempty"`
	Expand   []ringExpandDoc `bson:"expand,omitempty"`
}

type ringCountDoc struct {
	Database string   `bson:"database"`
	Bucket   string   `bson:"bucket"`
	Filter   bson.Raw `bson:"filter,omitempty"`
}

// checkRingBucketName checks the database name before the bucket name, in that fixed
// order, so a request with both invalid is always refused naming the database: a
// refusal is read, quoted and pinned by tests, so it cannot depend on map order.
func checkRingBucketName(database, bucket string) error {
	if err := checkRingName("database", database); err != nil {
		return err
	}
	return checkRingName("bucket", bucket)
}

// checkRingName applies the one name-length rule to what, so checkRingBucketName can
// order the two names instead of ranging over them.
func checkRingName(what, name string) error {
	if len(name) == 0 || len(name) > MaxName {
		return Refusal("the %s name is %d bytes and must be 1 to %d. Send the name 'show %ss' lists", what, len(name), MaxName, what)
	}
	return nil
}

// checkRingFilter validates a filter document with the library's bounded validator.
func checkRingFilter(filter []byte) error {
	if len(filter) == 0 {
		return nil
	}
	if _, err := bson.Validate(filter, bson.Limits{Depth: ringListDepth, Size: MaxVerbArgBytes}); err != nil {
		return Refusal("the filter is not a valid BSON document: %v. Send one document of equality matches such as {City: \"Oslo\"}", err)
	}
	return nil
}

func checkRingList(req RingListRequest) error {
	if err := checkRingBucketName(req.Database, req.Bucket); err != nil {
		return err
	}
	if err := checkRingFilter(req.Filter); err != nil {
		return err
	}
	if req.Fields != nil {
		if err := CheckFieldNames("fields", req.Fields); err != nil {
			return err
		}
	}
	switch {
	case len(req.Cursor) > MaxAfter:
		return Refusal("the list cursor is %d bytes and at most %d are taken", len(req.Cursor), MaxAfter)
	case req.Epoch < 0:
		return Refusal("epoch is %d and an epoch counts from 0", req.Epoch)
	}
	return checkRingReadOptions(req.Options)
}

func ringListDocOf(req RingListRequest) ringListDoc {
	return ringListDoc{Database: req.Database, Bucket: req.Bucket, Filter: req.Filter, Fields: req.Fields, Limit: req.Limit, Cursor: req.Cursor, Epoch: req.Epoch, Local: req.Options.Local, Expand: ringExpandDocsOf(req.Options.Expand)}
}

// EncodeRingList returns the RING_LIST body for req. It refuses a request DecodeRingList
// would not accept, with INVALID_ARGUMENT.
func EncodeRingList(req RingListRequest) ([]byte, error) {
	if err := checkRingList(req); err != nil {
		return nil, err
	}
	raw, err := bson.Marshal(ringListDocOf(req))
	if err != nil {
		return nil, fmt.Errorf("encoding a list request: %w", err)
	}
	return raw, nil
}

// DecodeRingList reads a RING_LIST body written by EncodeRingList. It never panics: the
// document is validated (size, depth) before it is unpacked, and it is accepted only if it
// re-encodes to the same bytes, so a reordered, duplicated or extra member, a widened
// number and a written default are all refused.
func DecodeRingList(raw []byte) (RingListRequest, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: ringListDepth + 1, Size: MaxVerbArgBytes + 2*MaxName + MaxAfter + ringMaxFieldsBytes + MaxOptionsTrailer}); err != nil {
		return RingListRequest{}, Refusal("a list request is not a valid BSON document: %v", err)
	}
	// The fields member is judged from the raw document first, so a wrong type or an empty
	// array is refused naming the member, not as a generic unpacking error.
	fieldNames, hasFields, err := rawFieldNames(raw)
	if err != nil {
		return RingListRequest{}, err
	}
	if hasFields {
		if err := CheckFieldNames("fields", fieldNames); err != nil {
			return RingListRequest{}, err
		}
	}
	var doc ringListDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingListRequest{}, Refusal("a list request is not the document of a list (database, bucket, filter, fields as an array of strings, limit as int32, cursor as binary, epoch as int64): %v", err)
	}
	req := RingListRequest{Database: doc.Database, Bucket: doc.Bucket, Filter: doc.Filter, Fields: doc.Fields, Limit: doc.Limit, Cursor: doc.Cursor, Epoch: doc.Epoch,
		Options: RingReadOptions{Local: doc.Local, Expand: ringExpandSpecsOf(doc.Expand)}}
	if err := checkRingList(req); err != nil {
		return RingListRequest{}, err
	}
	again, err := bson.Marshal(ringListDocOf(req))
	if err != nil {
		return RingListRequest{}, Refusal("a list request holds a value that cannot be read back: %v", err)
	}
	if !bytes.Equal(again, raw) {
		return RingListRequest{}, Refusal("a list request does not have the canonical layout (members in order, none duplicated or extra, the stated types, absent rather than empty or 0 for the epoch and the cursor)")
	}
	return req, nil
}

func ringCountDocOf(req RingCountRequest) ringCountDoc {
	return ringCountDoc{Database: req.Database, Bucket: req.Bucket, Filter: req.Filter}
}

func checkRingCount(req RingCountRequest) error {
	if err := checkRingBucketName(req.Database, req.Bucket); err != nil {
		return err
	}
	return checkRingFilter(req.Filter)
}

// EncodeRingCount returns the RING_COUNT body for req. It refuses a request
// DecodeRingCount would not accept, with INVALID_ARGUMENT.
func EncodeRingCount(req RingCountRequest) ([]byte, error) {
	if err := checkRingCount(req); err != nil {
		return nil, err
	}
	raw, err := bson.Marshal(ringCountDocOf(req))
	if err != nil {
		return nil, fmt.Errorf("encoding a count request: %w", err)
	}
	return raw, nil
}

// DecodeRingCount reads a RING_COUNT body written by EncodeRingCount, with the rules of
// DecodeRingList. A count takes no limit and no cursor: a limited count is a wrong number.
func DecodeRingCount(raw []byte) (RingCountRequest, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: ringListDepth + 1, Size: MaxVerbArgBytes + 2*MaxName}); err != nil {
		return RingCountRequest{}, Refusal("a count request is not a valid BSON document: %v", err)
	}
	var doc ringCountDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingCountRequest{}, Refusal("a count request is not the document of a count (database, bucket, filter): %v", err)
	}
	req := RingCountRequest{Database: doc.Database, Bucket: doc.Bucket, Filter: doc.Filter}
	if err := checkRingCount(req); err != nil {
		return RingCountRequest{}, err
	}
	again, err := bson.Marshal(ringCountDocOf(req))
	if err != nil {
		return RingCountRequest{}, Refusal("a count request holds a value that cannot be read back: %v", err)
	}
	if !bytes.Equal(again, raw) {
		return RingCountRequest{}, Refusal("a count request does not have the canonical layout (database, bucket and an optional filter in that order, and nothing else: a count takes no limit)")
	}
	return req, nil
}
