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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// pinnedLimits is every exported number of this module that a server and a driver must agree
// on: a limit, a fixed length, a default and the opcode of each ring frame. The wanted value is
// written here by hand. A driver in another language reads the same number from its own
// source, so a value that changes here without a wire change splits the two sides: one builds
// a request the other refuses, or takes one it cannot hold.
var pinnedLimits = []struct {
	name      string
	got, want int64
}{
	{"Slots", Slots, 8192},
	{"RequestIDLen", RequestIDLen, 16},
	{"RingPrologueLen", RingPrologueLen, 20},
	{"HeaderLen", HeaderLen, 40},
	{"MaxSubScanBody", MaxSubScanBody, 33554432},
	{"MaxEncodedID", MaxEncodedID, 1024},
	{"DefaultPageLimit", DefaultPageLimit, 100},
	{"MaxPageLimit", MaxPageLimit, 10000},
	{"MaxPeekCount", MaxPeekCount, 100},
	{"MaxPeek", MaxPeek, 100},
	{"MaxBatchOps", MaxBatchOps, 255},
	{"MaxGetManyKeys", MaxGetManyKeys, 255},
	{"MaxUserFields", MaxUserFields, 999},
	{"MaxFields", MaxFields, 64},
	{"MaxName", MaxName, 1021},
	{"MaxAfter", MaxAfter, 2059},
	{"MaxVerbArgBytes", MaxVerbArgBytes, 1048576},
	{"MaxVerbKeyBytes", MaxVerbKeyBytes, 1024},
	{"VerbArgDepth", VerbArgDepth, 8},
	{"MaxGuardTerms", MaxGuardTerms, 16},
	{"MaxExpandFields", MaxExpandFields, 8},
	{"MaxOptionsTrailer", MaxOptionsTrailer, 4096},
	{"MaxNodeText", MaxNodeText, 256},
	{"MaxRingErrorPayload", MaxRingErrorPayload, 69632},
	{"MaxNearestVector", MaxNearestVector, 16384},
	{"MaxNearestName", MaxNearestName, 255},
	{"AggregateMaxLimit", AggregateMaxLimit, 10000},
	{"MaxAdminRefusedPeers", MaxAdminRefusedPeers, 64},
	{"MaxMemberResult", MaxMemberResult, 512},
	{"SeedingMaxPage", SeedingMaxPage, 1000},
	{"SeedingMaxRefusal", SeedingMaxRefusal, 512},
	{"SeedingCursorMax", SeedingCursorMax, 1023},
	{"OpRingList", OpRingList, 0xDA},
	{"OpRingCount", OpRingCount, 0xDB},
	{"OpRingKeyed", OpRingKeyed, 0xDC},
	{"OpRingAdmin", OpRingAdmin, 0xDD},
	{"OpRingTopology", OpRingTopology, 0xDE},
	{"OpRingReply", OpRingReply, 0xDF},
	{"OpRingBatch", OpRingBatch, 0xE0},
	{"OpRingGetMany", OpRingGetMany, 0xE1},
}

// isPinnedName reports whether an exported constant of this module is a number the two sides
// must agree on, by its name: a limit, a fixed length, a default or a frame opcode.
func isPinnedName(name string) bool {
	// A refusal condition and an update code can carry "Max" in their names and are not limits.
	if strings.HasPrefix(name, "Cond") || strings.HasPrefix(name, "Update") {
		return false
	}
	return name == "Slots" || name == "VerbArgDepth" || strings.Contains(name, "Max") ||
		strings.HasSuffix(name, "Len") || strings.HasPrefix(name, "Default") || strings.HasPrefix(name, "OpRing")
}

// declaredLimitNames reads the module's own source for every exported constant isPinnedName
// names, so a limit added later is found without anyone remembering this test.
func declaredLimitNames(t *testing.T) []string {
	t.Helper()
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("listing the module directory: %v", err)
	}
	var names []string
	parsed := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".go") || strings.HasSuffix(file.Name(), "_test.go") {
			continue
		}
		tree, err := parser.ParseFile(token.NewFileSet(), file.Name(), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", file.Name(), err)
		}
		parsed++
		for _, decl := range tree.Decls {
			group, isGroup := decl.(*ast.GenDecl)
			if !isGroup || group.Tok != token.CONST {
				continue
			}
			for _, spec := range group.Specs {
				for _, name := range spec.(*ast.ValueSpec).Names {
					if name.IsExported() && isPinnedName(name.Name) {
						names = append(names, name.Name)
					}
				}
			}
		}
	}
	if parsed == 0 {
		t.Fatalf("no Go source was found beside this test, so no limit was read")
	}
	sort.Strings(names)
	return names
}

// TestWireLimitsArePinned holds every limit, fixed length and opcode to the number written in
// pinnedLimits, and holds pinnedLimits to the source: a constant the table lacks fails, and so
// does a row for a constant that is gone.
func TestWireLimitsArePinned(t *testing.T) {
	pinned := map[string]bool{}
	for _, limit := range pinnedLimits {
		if pinned[limit.name] {
			t.Fatalf("pinnedLimits names %s twice", limit.name)
		}
		pinned[limit.name] = true
		if limit.got != limit.want {
			t.Errorf("%s is %d and the wire contract says %d. Every driver and every server carries %d; a change is a new protocol version, not an edit", limit.name, limit.got, limit.want, limit.want)
		}
	}
	declared := map[string]bool{}
	for _, name := range declaredLimitNames(t) {
		declared[name] = true
		if !pinned[name] {
			t.Errorf("%s is an exported limit, length, default or opcode with no row in pinnedLimits. Add {%q, %s, <its value>} to the table in limits_pin_test.go, and a row to bdRows in boundary_test.go for the function that enforces it", name, name, name)
		}
	}
	for name := range pinned {
		if !declared[name] {
			t.Errorf("pinnedLimits has a row for %s and the module declares no such constant. Remove the row, or restore the constant", name)
		}
	}
}
