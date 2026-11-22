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

import "encoding/binary"

// Slots is the slot count of the ring. It is written as a number because this module
// imports no server package; the server test TestWireLimitsMatchTheirSources holds it equal
// to the server's own slot count.
const Slots = 8192

// ReadCanonicalUvarint reads a uvarint and refuses one that is truncated,
// overflows or is not the shortest encoding, so one table is one byte string.
func ReadCanonicalUvarint(raw []byte) (uint64, int, bool) {
	v, n := binary.Uvarint(raw)
	if n <= 0 || n != len(binary.AppendUvarint(nil, v)) {
		return 0, 0, false
	}
	return v, n, true
}
