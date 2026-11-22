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
	"strconv"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// MaxExpandFields bounds the expansions one call names: a call takes at most 8, so
// one request reads at most 8 far buckets.
const MaxExpandFields = 8

// MaxOptionsTrailer bounds a read options trailer, its 2-byte length included.
const MaxOptionsTrailer = 4096

// RingExpandSpec names one expansion of a read. Field is the scalar field whose value is
// the far id; on a store list it is empty, because the element keys are the ids. Database
// and Bucket name the far bucket and are both empty when the declaration names the
// reference itself. Fields projects the far document (nil: every scalar). Where is a BSON
// document of equalities on the far document's declared scalars, or nil.
type RingExpandSpec struct {
	Field, Database, Bucket string
	Fields                  []string
	Where                   []byte
}

// RingReadOptions is what a read carries besides its class form: Local asks the owner to
// answer from its own copy, and Expand names the fields to read from a far bucket.
type RingReadOptions struct {
	Local  bool
	Expand []RingExpandSpec
}

// Empty reports whether o names nothing, which is the request with no trailer at all.
func (o RingReadOptions) Empty() bool {
	return !o.Local && len(o.Expand) == 0
}

// ringExpandDoc is the wire form of one RingExpandSpec. It is also the expand member of a
// list request, so both carry one document.
type ringExpandDoc struct {
	Field    string   `bson:"field,omitempty"`
	Database string   `bson:"database,omitempty"`
	Bucket   string   `bson:"bucket,omitempty"`
	Fields   []string `bson:"fields,omitempty"`
	Where    bson.Raw `bson:"where,omitempty"`
}

// ringOptionsDoc is the trailer document. Its members are the list request's own last two,
// in the same order, so one decoder reads both.
type ringOptionsDoc struct {
	Local  bool            `bson:"local,omitempty"`
	Expand []ringExpandDoc `bson:"expand,omitempty"`
}

func ringExpandDocsOf(specs []RingExpandSpec) []ringExpandDoc {
	if len(specs) == 0 {
		return nil
	}
	docs := make([]ringExpandDoc, len(specs))
	for i, spec := range specs {
		docs[i] = ringExpandDoc{Field: spec.Field, Database: spec.Database, Bucket: spec.Bucket, Fields: spec.Fields, Where: spec.Where}
	}
	return docs
}

func ringExpandSpecsOf(docs []ringExpandDoc) []RingExpandSpec {
	if len(docs) == 0 {
		return nil
	}
	specs := make([]RingExpandSpec, len(docs))
	for i, doc := range docs {
		specs[i] = RingExpandSpec{Field: doc.Field, Database: doc.Database, Bucket: doc.Bucket, Fields: doc.Fields, Where: doc.Where}
	}
	return specs
}

func ringOptionsDocOf(o RingReadOptions) ringOptionsDoc {
	return ringOptionsDoc{Local: o.Local, Expand: ringExpandDocsOf(o.Expand)}
}

func ringReadOptionsOf(doc ringOptionsDoc) RingReadOptions {
	return RingReadOptions{Local: doc.Local, Expand: ringExpandSpecsOf(doc.Expand)}
}

func checkRingExpandSpec(index int, spec RingExpandSpec) error {
	switch {
	case spec.Database != "" && spec.Bucket == "":
		return Refusal("expand[%d] names database %q and no bucket. Name the bucket with it, or leave both empty and expand the field the declaration names as a reference", index, spec.Database)
	case spec.Bucket != "" && spec.Database != "":
		if err := checkRingBucketName(spec.Database, spec.Bucket); err != nil {
			return err
		}
	case len(spec.Bucket) > MaxName:
		return Refusal("expand[%d] names a bucket of %d bytes and a bucket name is at most %d", index, len(spec.Bucket), MaxName)
	}
	if spec.Field != "" {
		if err := CheckFieldNames(fmt.Sprintf("expand[%d].field", index), []string{spec.Field}); err != nil {
			return err
		}
	}
	if spec.Fields != nil && len(spec.Fields) == 0 {
		return Refusal("expand[%d].fields is empty, and an empty list has no encoding: it would arrive as every scalar field. Leave fields out for every field, or name the ones you want", index)
	}
	if spec.Fields != nil {
		if err := CheckFieldNames(fmt.Sprintf("expand[%d].fields", index), spec.Fields); err != nil {
			return err
		}
	}
	return checkRingFilter(spec.Where)
}

// checkRingReadOptions holds the options to what a trailer can carry: at most MaxExpandFields
// specs, a database only with its bucket, and a where that is one filter document.
func checkRingReadOptions(o RingReadOptions) error {
	if len(o.Expand) > MaxExpandFields {
		return &Error{Code: CodeInvalidArgument, Message: CodeInvalidArgument.Text(CondExpandTooMany, map[string]string{"n": strconv.Itoa(len(o.Expand))})}
	}
	for index, spec := range o.Expand {
		if err := checkRingExpandSpec(index, spec); err != nil {
			return err
		}
	}
	return nil
}

func ringClassCarriesOptions(class byte) bool {
	return class == RingClassGet || class == RingClassStoreList || class == RingClassGetFields
}

// EncodeRingReadOptions returns the trailer [len:2 BE][BSON document], or nil when o is
// empty. It refuses more than MaxExpandFields specs with CondExpandTooMany, a spec with a
// Database and no Bucket, and a trailer over 4096 bytes.
func EncodeRingReadOptions(o RingReadOptions) ([]byte, error) {
	if o.Empty() {
		return nil, nil
	}
	if err := checkRingReadOptions(o); err != nil {
		return nil, err
	}
	raw, err := bson.Marshal(ringOptionsDocOf(o))
	if err != nil {
		return nil, fmt.Errorf("encoding the read options trailer: %w", err)
	}
	if 2+len(raw) > MaxOptionsTrailer {
		return nil, Refusal("the read options trailer is %d bytes and at most %d are taken. Drop local and expand, or ask for fewer fields", 2+len(raw), MaxOptionsTrailer)
	}
	out := binary.BigEndian.AppendUint16(nil, uint16(len(raw)))
	return append(out, raw...), nil
}

// DecodeRingReadOptions parses a trailer and reports the bytes it used. It refuses a length
// that lies, a document that does not re-encode to itself, and any member it does not know.
func DecodeRingReadOptions(tail []byte) (RingReadOptions, int, error) {
	if len(tail) == 0 {
		return RingReadOptions{}, 0, nil
	}
	if len(tail) < 2 {
		return RingReadOptions{}, 0, Refusal("a read options trailer after the last field is a 2-byte length and a BSON document, and %d bytes remain. Drop local and expand (the fields names of a get_fields end before it)", len(tail))
	}
	size := int(binary.BigEndian.Uint16(tail))
	if 2+size > MaxOptionsTrailer {
		return RingReadOptions{}, 0, Refusal("the read options trailer claims %d bytes and at most %d are taken. Drop local and expand, or ask for fewer fields", size, MaxOptionsTrailer-2)
	}
	if size > len(tail)-2 {
		return RingReadOptions{}, 0, Refusal("the read options trailer claims %d bytes and %d follow its length. Drop local and expand", size, len(tail)-2)
	}
	raw := tail[2 : 2+size]
	if _, err := bson.Validate(raw, bson.Limits{Depth: ringListDepth + 1, Size: MaxOptionsTrailer}); err != nil {
		return RingReadOptions{}, 0, Refusal("the read options trailer is not a valid BSON document: %v. Send local and expand only", err)
	}
	var doc ringOptionsDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingReadOptions{}, 0, Refusal("the read options trailer is not the document of local and expand (local as a bool, expand as an array of field, database, bucket, fields and where): %v. Drop local and expand", err)
	}
	options := ringReadOptionsOf(doc)
	if options.Empty() {
		return RingReadOptions{}, 0, Refusal("the read options trailer is present and names neither local nor expand; leave the trailer off, as an empty one has no encoding. Drop local and expand")
	}
	if err := checkRingReadOptions(options); err != nil {
		return RingReadOptions{}, 0, err
	}
	again, err := bson.Marshal(ringOptionsDocOf(options))
	if err != nil {
		return RingReadOptions{}, 0, Refusal("the read options trailer holds a value that cannot be read back: %v. Drop local and expand", err)
	}
	if !bytes.Equal(again, raw) {
		return RingReadOptions{}, 0, Refusal("the read options trailer does not have the layout of local and expand (members in order, none duplicated or extra, absent rather than false or empty). Drop local and expand")
	}
	return options, 2 + size, nil
}
