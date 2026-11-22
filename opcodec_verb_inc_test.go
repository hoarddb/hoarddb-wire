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

import "testing"

// TestOpcodecAcceptsVerbFive pins the store verb table at 5 (inc): an owner that refuses
// verb 5 cannot decode the record a client's inc writes, so the record is lost at the owner.
func TestOpcodecAcceptsVerbFive(t *testing.T) {
	op := opcodecHeader(Op{Class: OpStoreVerb, Fields: []FieldOp{{FieldID: 4, Verb: VerbInc, Value: []byte("arg")}}})
	body, err := EncodeOp(op)
	if err != nil {
		t.Fatalf("encode of verb 5: %v", err)
	}
	got, err := DecodeOp(body)
	if err != nil {
		t.Fatalf("decode of verb 5: %v", err)
	}
	if got.Fields[0].Verb != VerbInc {
		t.Fatalf("verb after round trip = %d, want %d", got.Fields[0].Verb, VerbInc)
	}
	past := opcodecHeader(Op{Class: OpStoreVerb, Fields: []FieldOp{{FieldID: 4, Verb: VerbInc + 1, Value: []byte("arg")}}})
	if _, err := EncodeOp(past); err == nil {
		t.Fatal("verb 6 is past the table and was encoded")
	}
}
