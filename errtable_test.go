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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var errtableUpdate = flag.Bool("errtable-update", false, "rewrite testdata/errors-golden.txt from the parsed conventions Errors table (needs the hoardDB repository checkout)")

const errtableGoldenPath = "testdata/errors-golden.txt"

// errtableRow is one row of the conventions Errors table, as written there.
type errtableRow struct {
	key     string // the name cell without backticks, e.g. "NOT_FOUND (bucket)"
	name    string // the code name, e.g. "NOT_FOUND"
	code    string // "0x01" or "—"
	exit    string // "2", "8" or "—"
	text    string // the first backtick span of the text cell; "" when the row has none
	fix     string // the fix cell
	hasText bool
}

// errtableCondition maps a table row to the registry's code and condition.
// INVALID_ARGUMENT has no exact text and STALE_INCARNATION has no code, so
// neither is here.
var errtableCondition = map[string]struct {
	code Code
	cond Condition
}{
	"NOT_FOUND (bucket)":                          {CodeNotFound, CondBucketNotFound},
	"NOT_FOUND (document)":                        {CodeNotFound, CondDocumentNotFound},
	"UNAVAILABLE (poisoned log, failed append)":   {CodeUnavailable, CondPoisonedLog},
	"UNAVAILABLE (declaration write not durable)": {CodeUnavailable, CondDeclarationWriteFailed},
	"RESULT_TOO_LARGE":                            {CodeResultTooLarge, CondResultTooLarge},
	"FIELD_TYPE":                                  {CodeFieldType, CondFieldNotDeclared},
	"DOCUMENT_EXISTS":                             {CodeDocumentExists, CondDocumentExists},
	"NOT_OWNER":                                   {CodeNotOwner, CondNotOwner},
	"STALE_EPOCH":                                 {CodeStaleEpoch, CondStaleEpoch},
	"OWNER_DOWN":                                  {CodeOwnerDown, CondOwnerDown},
	"NO_QUORUM":                                   {CodeNoQuorum, CondNoQuorum},
	"RF_UNSUPPORTED":                              {CodeRFUnsupported, CondRFUnsupported},
	"RF_UNSUPPORTED (create, voters)":             {CodeRFUnsupported, CondRFUnsupportedCreateVoters},
	"RF_UNSUPPORTED (create, data members)":       {CodeRFUnsupported, CondRFUnsupportedCreateData},
	"LEASE_LAPSED":                                {CodeLeaseLapsed, CondLeaseLapsed},
	"EPOCH_FENCED":                                {CodeEpochFenced, CondEpochFenced},
	"SEQ_GAP":                                     {CodeSeqGap, CondSeqGap},
	"LOG_BELOW_HORIZON":                           {CodeLogBelowHorizon, CondLogBelowHorizon},
	"STATE_MOVED":                                 {CodeStateMoved, CondStateMoved},
	"CONFLICT":                                    {CodeConflict, CondConflict},
	"CLUSTER_MISMATCH":                            {CodeClusterMismatch, CondClusterMismatch},
	"NOT_A_DATA_NODE":                             {CodeNotADataNode, CondNotADataNode},
	"SCHEMA_NOT_CURRENT":                          {CodeSchemaNotCurrent, CondSchemaNotCurrent},
	"SCHEMA_NOT_CURRENT (stale client header)":    {CodeSchemaNotCurrent, CondClientDeclarationStale},
	"SCHEMA_NOT_CURRENT (bucket unnamed)":         {CodeSchemaNotCurrent, CondSchemaNotCurrentUnnamed},
	"BUCKET_DROPPING":                             {CodeBucketDropping, CondBucketDropping},
	"DECL_UNAVAILABLE":                            {CodeDeclUnavailable, CondDeclUnavailable},
	"BUCKET_EXISTS":                               {CodeBucketExists, CondBucketExists},
	"RESERVED_NAME":                               {CodeReservedName, CondReservedName},
	"BUCKET_ID_COLLISION":                         {CodeBucketIDCollision, CondBucketIDCollision},
	"SEED_NOT_HELD":                               {CodeSeedNotHeld, CondSeedNotHeld},
	"SEED_NOT_HELD (forget)":                      {CodeSeedNotHeld, CondSeedNotHeldForget},
	"REPLACE_TARGET_HAS_DATA":                     {CodeReplaceTargetHasData, CondReplaceTargetHasData},
	"MINT_FAILED":                                 {CodeMintFailed, CondMintFailed},
	"CORRUPT_KEY":                                 {CodeCorruptKey, CondCorruptKey},
	"SEED_HELD":                                   {CodeSeedHeld, CondSeedHeld},
	"UNAVAILABLE (forwarded write, owner unreachable)": {CodeUnavailable, CondForwardWriteUnreachable},
	"UNAVAILABLE (forwarded read, owner unreachable)":  {CodeUnavailable, CondForwardReadUnreachable},
}

// errtableRename is one placeholder the code spells differently from the
// table, because the table's is not an identifier (<e′>, <a, b, c>) or one
// <n> stands for several facts. Renames apply in order, each to the first
// occurrence still unrenamed, so a row that uses <n> twice lists it twice.
type errtableRename struct{ from, to string }

var errtableRenames = map[string][]errtableRename{
	"STALE_EPOCH":       {{"<e′>", "<e_seen>"}},
	"EPOCH_FENCED":      {{"<e′>", "<e_seen>"}},
	"SEQ_GAP":           {{"<m+1>", "<next>"}, {"<n-1>", "<last>"}},
	"FIELD_TYPE":        {{"<a, b, c>", "<declared>"}},
	"LEASE_LAPSED":      {{"<n>", "<lapsed_ms>"}, {"<n>", "<step_down_ms>"}},
	"LOG_BELOW_HORIZON": {{"<e>:<n>", "<need_pos>"}, {"<e>:<m>", "<oldest_pos>"}},
	"CLUSTER_MISMATCH":  {{"<id8>…", "<node_cluster>…"}, {"<id8>…", "<this_cluster>…"}, {"<addr of a node in this cluster>", "<seed_addr>"}},
	"CORRUPT_KEY":       {{"<n>", "<bucket_id>"}, {"<n>", "<tag>"}, {"<n>", "<offset>"}},
}

// errtableConventions is where the Errors table lives, relative to the root of the hoardDB
// repository. The module is also built and tested on its own, where that file does not exist.
var errtableConventions = filepath.Join("docs", "specs", "entry-ring-conventions.md")

// errtableConventionsPath finds the conventions file above the working directory. It skips the
// calling test when there is none: the table is the server repository's, so a test that reads
// it can only run inside that checkout. TestErrtableGoldenMatchesTheCode needs no table and
// holds the same texts to testdata/errors-golden.txt wherever the module is tested.
func errtableConventionsPath(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	for {
		path := filepath.Join(dir, errtableConventions)
		if _, statErr := os.Stat(path); statErr == nil {
			return path
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skipf("skipped: no %s above the working directory, so this module is being tested outside the hoardDB repository and the Errors table it would be compared with is not here. %s is still checked against the code by TestErrtableGoldenMatchesTheCode", errtableConventions, errtableGoldenPath)
		}
		dir = parent
	}
}

var errtableBacktick = regexp.MustCompile("`([^`]*)`")

// errtableParse reads the Errors table out of the conventions file.
func errtableParse(t *testing.T) []errtableRow {
	t.Helper()
	path := errtableConventionsPath(t)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the conventions: %v", err)
	}
	lines := strings.Split(string(raw), "\n")
	var rows []errtableRow
	inErrors := false
	for _, line := range lines {
		if strings.HasPrefix(line, "## ") {
			inErrors = line == "## Errors"
			continue
		}
		if !inErrors || !strings.HasPrefix(line, "| `") {
			continue
		}
		cells := strings.Split(strings.TrimSuffix(strings.TrimPrefix(line, "| "), " |"), " | ")
		if len(cells) != 5 {
			t.Fatalf("Errors row has %d cells, want 5: %.80s", len(cells), line)
		}
		row := errtableRow{
			key:  strings.ReplaceAll(cells[0], "`", ""),
			code: cells[1],
			exit: cells[2],
			fix:  cells[4],
		}
		row.name = strings.Fields(row.key)[0]
		if m := errtableBacktick.FindStringSubmatch(cells[3]); m != nil {
			row.text, row.hasText = m[1], true
		}
		rows = append(rows, row)
	}
	if len(rows) != 40 {
		t.Fatalf("parsed %d Errors rows from %s, want 40; the table moved or changed shape", len(rows), path)
	}
	return rows
}

// errtableRenamed applies a row's documented renames to the table's text.
func errtableRenamed(row errtableRow) string {
	text := row.text
	for _, r := range errtableRenames[row.key] {
		text = strings.Replace(text, r.from, r.to, 1)
	}
	return text
}

// TestEveryRefusalTextMatchesTheTable compares the registry with the spec's
// Errors table: code value, exit status and the exact template, after the
// renames listed in errtableRenames. Every drifting row is reported.
func TestEveryRefusalTextMatchesTheTable(t *testing.T) {
	for _, row := range errtableParse(t) {
		target, mapped := errtableCondition[row.key]
		if !mapped {
			continue // INVALID_ARGUMENT and PERMISSION_DENIED (no exact text) and STALE_INCARNATION (no code)
		}
		t.Run(row.key, func(t *testing.T) {
			wantValue, err := strconv.ParseUint(strings.TrimPrefix(row.code, "0x"), 16, 16)
			if err != nil {
				t.Fatalf("table code %q: %v", row.code, err)
			}
			if uint16(target.code) != uint16(wantValue) {
				t.Errorf("code is 0x%02X, table says %s", uint16(target.code), row.code)
			}
			wantExit := 0
			if row.exit != "—" {
				if wantExit, err = strconv.Atoi(row.exit); err != nil {
					t.Fatalf("table exit %q: %v", row.exit, err)
				}
			}
			if target.code.Exit() != wantExit {
				t.Errorf("exit is %d, table says %s", target.code.Exit(), row.exit)
			}
			got, ok := Template(target.code, target.cond)
			if !ok {
				t.Fatalf("registry has no text for %s", row.key)
			}
			if want := errtableRenamed(row); got != want {
				t.Errorf("text drifts from the table\n table: %s\n  code: %s", want, got)
			}
			kind, _ := KindOf(target.code, target.cond)
			if (kind == KindInternal) != (row.exit == "—") {
				t.Errorf("kind %v disagrees with exit %s: internal rows are exactly the ones with no exit", kind, row.exit)
			}
			fix := strings.Trim(row.fix, "`")
			if kind == KindCLI && strings.HasPrefix(row.fix, "`") && !strings.ContainsAny(fix, "…/") && !strings.Contains(errtableRenamed(row), fix) {
				t.Errorf("cli text does not quote its fix %q", fix)
			}
		})
	}
}

// TestErrtableRegistryAndTableHaveTheSameCodes: every code in the registry
// has a row and every row with a code is in the registry. The exceptions are
// named: STALE_INCARNATION has no code; INVALID_ARGUMENT has a code and no
// single text.
func TestErrtableRegistryAndTableHaveTheSameCodes(t *testing.T) {
	inTable := map[uint16]string{}
	for _, row := range errtableParse(t) {
		if row.code == "—" && row.name != "STALE_INCARNATION" {
			t.Errorf("row %s has no code and is not the documented exception STALE_INCARNATION", row.key)
		}
		if codeValue, err := strconv.ParseUint(strings.TrimPrefix(row.code, "0x"), 16, 16); err == nil {
			inTable[uint16(codeValue)] = row.name
		}
		if row.name != "INVALID_ARGUMENT" && row.name != "PERMISSION_DENIED" && row.name != "STALE_INCARNATION" {
			if _, mapped := errtableCondition[row.key]; !mapped {
				t.Errorf("table row %s has no entry in errtableCondition", row.key)
			}
		}
	}
	for _, code := range Codes() {
		name, ok := inTable[uint16(code)]
		if !ok {
			t.Errorf("registry code 0x%02X (%s) has no row in the table", uint16(code), code)
		} else if name != code.String() {
			t.Errorf("registry code 0x%02X is %s, the table calls it %s", uint16(code), code, name)
		}
	}
	for codeValue, name := range inTable {
		if _, ok := codeTable[Code(codeValue)]; !ok {
			t.Errorf("table row %s (0x%02X) is not in the registry", name, codeValue)
		}
	}
}

// TestCondDeclarationWriteFailedNamesTheFailure pins the declaration-write
// text: it names the copies that confirmed the write, requires every ready copy
// (the text does not say majority), and names the fix for a replication factor
// the copies cannot meet.
func TestCondDeclarationWriteFailedNamesTheFailure(t *testing.T) {
	text := CodeUnavailable.Text(CondDeclarationWriteFailed, map[string]string{
		"op": "create bucket", "db": "app", "bucket": "users", "c": "1", "rf": "3", "slot": "7", "n": "2",
	})
	for _, want := range []string{
		"must be durable on every copy that is ready to serve it before a schema change is published",
		"alter database app { replication_factor: 2 }",
		"confirmed by 1 of 3 copies",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("declaration-write text lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "could be lost with its owner") {
		t.Errorf("declaration-write text keeps the withdrawn 'all copies' wording:\n%s", text)
	}
}

var errtableSamples = map[string]string{
	"bucket": "users", "db": "app", "id": "u-1", "node": "n2", "owner": "n1", "slot": "17",
	"e": "3", "e_seen": "2", "op": "create bucket", "n": "5", "m": "4", "c": "1", "rf": "3",
	"v": "3", "d": "2", "addr": "192.0.2.1:7000", "state": "active",
}

// errtableArgs gives every placeholder a fixed sample value: a known name its
// sample, any other name V_<name>, so a renamed placeholder still shows.
func errtableArgs(template string) map[string]string {
	args := map[string]string{}
	for _, token := range placeholder.FindAllString(template, -1) {
		name := token[1 : len(token)-1]
		if sample, ok := errtableSamples[name]; ok {
			args[name] = sample
		} else {
			args[name] = "V_" + name
		}
	}
	return args
}

func errtableSubstitute(template string) string {
	args := errtableArgs(template)
	return placeholder.ReplaceAllStringFunc(template, func(token string) string { return args[token[1:len(token)-1]] })
}

// errtableGoldenFromTable renders every text row of the parsed table.
func errtableGoldenFromTable(rows []errtableRow) string {
	var out strings.Builder
	for _, row := range rows {
		if _, mapped := errtableCondition[row.key]; !mapped {
			continue
		}
		fmt.Fprintf(&out, "== %s %s exit=%s\n%s\n\n", row.key, row.code, row.exit, errtableSubstitute(errtableRenamed(row)))
	}
	return out.String()
}

// errtableGoldenFromCode renders the same rows from the registry.
func errtableGoldenFromCode(rows []errtableRow) string {
	var out strings.Builder
	for _, row := range rows {
		target, mapped := errtableCondition[row.key]
		if !mapped {
			continue
		}
		template, _ := Template(target.code, target.cond)
		exit := strconv.Itoa(target.code.Exit())
		if target.code.Exit() == 0 {
			exit = "—"
		}
		fmt.Fprintf(&out, "== %s 0x%02X exit=%s\n%s\n\n", row.key, uint16(target.code), exit, target.code.Text(target.cond, errtableArgs(template)))
	}
	return out.String()
}

// TestErrtableGoldenIsFromTheTable writes (with -errtable-update) or checks
// the golden file against the parsed table, so the file cannot go stale
// against the spec.
func TestErrtableGoldenIsFromTheTable(t *testing.T) {
	want := errtableGoldenFromTable(errtableParse(t))
	if *errtableUpdate {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("creating testdata: %v", err)
		}
		if err := os.WriteFile(errtableGoldenPath, []byte(want), 0o644); err != nil {
			t.Fatalf("writing the golden: %v", err)
		}
		return
	}
	got, err := os.ReadFile(errtableGoldenPath)
	if err != nil {
		t.Fatalf("reading the golden (generate it with -errtable-update): %v", err)
	}
	if string(got) != want {
		t.Errorf("%s is stale against the conventions table; regenerate with 'go test -run TestErrtableGoldenIsFromTheTable -errtable-update' in clients/wire of the hoardDB repository and review the diff", errtableGoldenPath)
	}
}

// errtableGoldenRows reads the row keys back out of the golden's "== <key> <code> exit=<n>"
// lines, in file order, so the code can be rendered for the same rows without the table.
func errtableGoldenRows(t *testing.T, golden string) []errtableRow {
	t.Helper()
	var rows []errtableRow
	for _, line := range strings.Split(golden, "\n") {
		head, isHead := strings.CutPrefix(line, "== ")
		if !isHead {
			continue
		}
		words := strings.Fields(head)
		if len(words) < 3 {
			t.Fatalf("%s: the line %q is not '== <key> <code> exit=<n>'", errtableGoldenPath, line)
		}
		rows = append(rows, errtableRow{key: strings.Join(words[:len(words)-2], " ")})
	}
	if len(rows) == 0 {
		t.Fatalf("%s holds no '== <key> <code> exit=<n>' line", errtableGoldenPath)
	}
	return rows
}

// TestErrtableGoldenMatchesTheCode compares the golden with what the code
// renders for the same sample arguments. It reads only testdata, so it runs
// wherever the module is tested; TestErrtableGoldenIsFromTheTable is what
// ties the golden to the table, inside the hoardDB repository.
func TestErrtableGoldenMatchesTheCode(t *testing.T) {
	golden, err := os.ReadFile(errtableGoldenPath)
	if err != nil {
		t.Fatalf("reading the golden: %v", err)
	}
	want := strings.Split(string(golden), "\n\n")
	got := strings.Split(errtableGoldenFromCode(errtableGoldenRows(t, string(golden))), "\n\n")
	if len(want) != len(got) {
		t.Fatalf("golden has %d entries, the code renders %d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Errorf("rendering drifts\n golden: %s\n   code: %s", want[i], got[i])
		}
	}
}
