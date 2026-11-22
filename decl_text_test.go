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
	"strings"
	"testing"
)

// TestDeclUnavailableNamesAnUnansweredReadAndTheCheck is BUG-0770: the refusal said the
// slot owner "did not answer" and sent the developer to replace-node, for a read that had
// only waited its turn; and a refusal built from ids left "<slot>" and "<node>" in the text.
// It must say the read did not complete, name the check, and send nobody to replace a node.
func TestDeclUnavailableNamesAnUnansweredReadAndTheCheck(t *testing.T) {
	text := CodeDeclUnavailable.Text(CondDeclUnavailable, map[string]string{"db": "1", "bucket": "3182714196521298707"})
	for _, want := range []string{"'nodes'", "'status'", "Serving: ok", "busy", "send the write again"} {
		if !strings.Contains(text, want) {
			t.Errorf("DECL_UNAVAILABLE text lacks %q:\n%s", want, text)
		}
	}
	for _, banned := range []string{"replace-node", "<slot>", "<node>", "is owned by", "state=active serving=ok", "it did not answer, and"} {
		if strings.Contains(text, banned) {
			t.Errorf("DECL_UNAVAILABLE text still has %q:\n%s", banned, text)
		}
	}
}
