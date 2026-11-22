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
	"strconv"
	"strings"
	"testing"
)

// refusalCalls is how many times one case is checked: the bug is a random map order,
// so a single call can pass by luck. 200 calls leave a chance of 2^-200 (below 1e-59)
// that a buggy build names the database every time.
const refusalCalls = 200

// TestRingBucketNameRefusalIsDeterministic is BUG-0825: checkRingBucketName ranged over
// a map of the two names, so with both invalid the refusal named whichever the map
// yielded first. The database must be checked first, always.
func TestRingBucketNameRefusalIsDeterministic(t *testing.T) {
	oversize := strings.Repeat("x", MaxName+1)
	cases := []struct {
		name             string
		database, bucket string
		want             string
	}{
		{"both names empty", "", "", databaseNameRefusal(0)},
		{"both names oversize", oversize, oversize, databaseNameRefusal(len(oversize))},
		{"only the bucket empty", "app", "", bucketNameRefusal(0)},
		{"only the bucket oversize", "app", oversize, bucketNameRefusal(len(oversize))},
	}
	for _, test := range cases {
		for call := 0; call < refusalCalls; call++ {
			err := checkRingBucketName(test.database, test.bucket)
			if err == nil {
				t.Fatalf("%s: call %d was accepted and both names are invalid", test.name, call)
			}
			if got := err.Error(); got != test.want {
				t.Fatalf("%s: call %d was refused with\n%q\nwant\n%q", test.name, call, got, test.want)
			}
		}
	}
}

// databaseNameRefusal is the text of a refusal naming the database of length size.
func databaseNameRefusal(size int) string {
	return "the database name is " + strconv.Itoa(size) + " bytes and must be 1 to " + strconv.Itoa(MaxName) + ". Send the name 'show databases' lists"
}

// bucketNameRefusal is the text of a refusal naming the bucket of length size.
func bucketNameRefusal(size int) string {
	return "the bucket name is " + strconv.Itoa(size) + " bytes and must be 1 to " + strconv.Itoa(MaxName) + ". Send the name 'show buckets' lists"
}
