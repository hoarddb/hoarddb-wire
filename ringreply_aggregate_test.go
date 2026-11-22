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
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The aggregate member is a whole BSON document like expanded and document, so bytes that are not
// one are refused by the encoder and by the decoder.
func TestAMalformedAggregateMemberIsRefused(t *testing.T) {
	if _, err := EncodeRingReply(RingReply{Aggregate: []byte{1, 2, 3}}); err == nil {
		t.Error("EncodeRingReply accepted an aggregate that is not a BSON document")
	}
	raw, err := bson.Marshal(RingReplyDoc{Aggregate: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRingReply(raw); err == nil {
		t.Error("DecodeRingReply accepted an aggregate that is not a BSON document")
	}
	good, err := bson.Marshal(bson.D{{Key: "sum", Value: int64(7)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeRingReply(RingReply{Aggregate: good}); err != nil {
		t.Errorf("EncodeRingReply refused a valid aggregate document: %v", err)
	}
}
