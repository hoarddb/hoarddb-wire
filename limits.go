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

import "fmt"

// MaxEncodedID is the largest encoded document id (encID), in bytes.
const MaxEncodedID = 1024

// The page bounds of a list: a page is 100 documents unless the caller
// says otherwise, and at most 10000.
const (
	DefaultPageLimit = 100
	MaxPageLimit     = 10000
)

// MaxPeekCount is the most elements one peek returns (the pop limit cap of 100).
const MaxPeekCount = 100

// MaxSubScanBody is the frame ceiling, 32 MiB; no body is
// read past it, so a lying length header cannot make a decoder allocate more.
const MaxSubScanBody = 32 << 20

// MaxBatchOps is the most operations one batch carries: an operation's index is one byte. The server test
// TestWireLimitsMatchTheirSources holds it equal to the cap the server's dedupe window takes.
const MaxBatchOps = 255

// CheckBatchSize is checkBatch without a Writer, for the client side of the same cap.
func CheckBatchSize(n int) error {
	if n >= 0 && n <= MaxBatchOps {
		return nil
	}
	return &Error{
		Code: CodeInvalidArgument,
		Message: fmt.Sprintf("a batch of %d operations is too large: a batch carries at most %d operations, because each is numbered in one byte and a larger index would wrap onto another operation's dedupe entry. Nothing was written. Split it into batches of %d or fewer",
			n, MaxBatchOps, MaxBatchOps),
	}
}

// MaxUserFields is the entry cap of a field table less the implicit _id_: the
// server's store.MaxFieldTableEntries - 1, written as a number because this module imports
// no server package. The server test TestWireLimitsMatchTheirSources holds the two equal.
const MaxUserFields = 999
