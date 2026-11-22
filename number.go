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
	"math"

	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
)

// A ScalarNumber is the decoded form of an int, a long or a double: the type byte then the BSON
// value bytes.
type ScalarNumber struct {
	Kind  bsontype.Type
	Whole int64
	Real  float64
}

// ReadNumber reads an int32, int64 or double of any value, finite or not.
func ReadNumber(kind bsontype.Type, body []byte) (ScalarNumber, bool) {
	switch {
	case kind == bsontype.Int32 && len(body) == 4:
		return ScalarNumber{Kind: kind, Whole: int64(int32(binary.LittleEndian.Uint32(body)))}, true
	case kind == bsontype.Int64 && len(body) == 8:
		return ScalarNumber{Kind: kind, Whole: int64(binary.LittleEndian.Uint64(body))}, true
	case kind == bsontype.Double && len(body) == 8:
		return ScalarNumber{Kind: kind, Real: math.Float64frombits(binary.LittleEndian.Uint64(body))}, true
	}
	return ScalarNumber{}, false
}

// Typed is the stored form of n again.
func (n ScalarNumber) Typed() []byte {
	switch n.Kind {
	case bsontype.Int32:
		return binary.LittleEndian.AppendUint32([]byte{byte(n.Kind)}, uint32(int32(n.Whole)))
	case bsontype.Int64:
		return binary.LittleEndian.AppendUint64([]byte{byte(n.Kind)}, uint64(n.Whole))
	}
	return binary.LittleEndian.AppendUint64([]byte{byte(n.Kind)}, math.Float64bits(n.Real))
}

// Float is n as a float64, exact for a double and rounded for a whole number past 2^53.
func (n ScalarNumber) Float() float64 {
	if n.Kind == bsontype.Double {
		return n.Real
	}
	return float64(n.Whole)
}

// IsFinite reports whether real is neither a NaN nor an infinity.
func IsFinite(real float64) bool { return !math.IsNaN(real) && !math.IsInf(real, 0) }
