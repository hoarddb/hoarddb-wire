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

// The kinds of an aggregate read (RingAggregateRead.Kind).
const (
	// AggregateReadDescribe lists every aggregate of the store field and its value.
	AggregateReadDescribe uint8 = 0
	// AggregateReadSum reads a plain sum; AggregateReadSumBy a sum per by value;
	// AggregateReadCountBy a count per by value.
	AggregateReadSum     uint8 = 1
	AggregateReadSumBy   uint8 = 2
	AggregateReadCountBy uint8 = 3
)

// AggregateMaxLimit is the most by values one aggregate read returns.
const AggregateMaxLimit = 10000

// RingAggregateRead is the form of a RingClassAggregate: the kind and the element paths that
// name one declared aggregate. Field is the summed path (sum and sum by), By the grouping path
// (sum by and count by); both are empty for describe.
type RingAggregateRead struct {
	Kind      uint8
	Field, By string
}
