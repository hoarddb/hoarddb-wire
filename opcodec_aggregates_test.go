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
	"reflect"
	"strings"
	"testing"
)

func TestAggregateStampRoundTripsAndCostsFourPlusLens(t *testing.T) {
	base := opcodecEveryClass()[OpStoreVerb-1]
	plain, err := EncodeOp(base)
	if err != nil {
		t.Fatal(err)
	}
	stamped := base
	stamped.Aggregates = []AggregateStamp{{AggID: 1, Kind: 1, Field: "Amount"}, {AggID: 2, Kind: 2, Field: "Amount", By: "Currency"}}
	record, err := EncodeOp(stamped)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(record)-len(plain), 1+(4+6+0)+(4+6+8); got != want {
		t.Fatalf("two aggregates cost %d bytes over an unstamped record, want n 1 + 4+fLen+bLen each = %d", got, want)
	}
	if record[1] != opFlagAggregates {
		t.Fatalf("flags %#02x, want %#02x", record[1], opFlagAggregates)
	}
	if decoded, err := DecodeOp(record); err != nil || !reflect.DeepEqual(decoded, stamped) {
		t.Fatalf("DecodeOp = %+v, %v; want the stamp back", decoded, err)
	}
}

// TestAggregateStampComesAfterTheIndexStamp: both stamps on one record keep their own flag
// bits and their own order, so neither decoder reads the other's bytes.
func TestAggregateStampComesAfterTheIndexStamp(t *testing.T) {
	op := opcodecEveryClass()[OpStoreVerb-1]
	op.Stamp = IndexStamp{Fields: []uint16{3}}
	op.Aggregates = []AggregateStamp{{AggID: 1, Kind: 3, By: "Status"}}
	record, err := EncodeOp(op)
	if err != nil {
		t.Fatal(err)
	}
	if record[1] != opFlagStamp|opFlagAggregates {
		t.Fatalf("flags %#02x, want both bits", record[1])
	}
	if decoded, err := DecodeOp(record); err != nil || !reflect.DeepEqual(decoded, op) {
		t.Fatalf("DecodeOp = %+v, %v", decoded, err)
	}
}

func TestAggregateStampIsRefusedOnAClientBodyAndOnInsert(t *testing.T) {
	stamp := []AggregateStamp{{AggID: 1, Kind: 1, Field: "Amount"}}
	client := opcodecEveryClass()[OpStoreVerb-1]
	client.Aggregates = stamp
	if _, err := EncodeClientOp(client); err == nil || !strings.Contains(err.Error(), "client body") {
		t.Fatalf("EncodeClientOp with aggregates = %v, want a refusal naming the client body", err)
	}
	insert := opcodecEveryClass()[OpInsert-1]
	insert.Aggregates = stamp
	if _, err := EncodeOp(insert); err == nil {
		t.Fatal("EncodeOp accepted an aggregate stamp on an insert")
	}
	bad := map[string][]AggregateStamp{
		"nine":         make([]AggregateStamp, 9),
		"kind":         {{AggID: 1, Kind: 4, Field: "Amount"}},
		"id zero":      {{AggID: 0, Kind: 1, Field: "Amount"}},
		"long path":    {{AggID: 1, Kind: 1, Field: strings.Repeat("a", 256)}},
		"sum with by":  {{AggID: 1, Kind: 1, Field: "Amount", By: "x"}},
		"count field":  {{AggID: 1, Kind: 3, Field: "Amount", By: "x"}},
		"by form none": {{AggID: 1, Kind: 2, Field: "Amount"}},
	}
	for name, list := range bad {
		op := opcodecEveryClass()[OpStoreVerb-1]
		op.Aggregates = list
		if _, err := EncodeOp(op); err == nil {
			t.Errorf("%s: EncodeOp accepted %+v", name, list)
		}
	}
}

func TestDecodeOpRefusesTheAggregateFlagOverAnEmptyList(t *testing.T) {
	record, err := EncodeOp(opcodecEveryClass()[OpStoreVerb-1])
	if err != nil {
		t.Fatal(err)
	}
	mutant := append([]byte(nil), record[:2+opcodecHeaderLen]...)
	mutant[1] = opFlagAggregates
	mutant = append(mutant, 0) // n = 0
	mutant = append(mutant, record[2+opcodecHeaderLen:]...)
	if _, err := DecodeOp(mutant); err == nil {
		t.Fatal("DecodeOp accepted the aggregate flag over an empty list")
	}
}
