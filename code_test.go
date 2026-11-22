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
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// specCode is one row of conventions "Errors", copied here by hand so the
// tests do not read the table they check.
type specCode struct {
	code  Code
	name  string
	value uint16
	exit  int
}

var specCodes = []specCode{
	{CodeNotFound, "NOT_FOUND", 0x01, 2},
	{CodePermissionDenied, "PERMISSION_DENIED", 0x03, 2},
	{CodeInvalidArgument, "INVALID_ARGUMENT", 0x04, 2},
	{CodeUnavailable, "UNAVAILABLE", 0x06, 8},
	{CodeResultTooLarge, "RESULT_TOO_LARGE", 0x0E, 2},
	{CodeFieldType, "FIELD_TYPE", 0x10, 2},
	{CodeDocumentExists, "DOCUMENT_EXISTS", 0x12, 2},
	{CodeNotOwner, "NOT_OWNER", 0x13, 8},
	{CodeStaleEpoch, "STALE_EPOCH", 0x14, 8},
	{CodeOwnerDown, "OWNER_DOWN", 0x15, 8},
	{CodeNoQuorum, "NO_QUORUM", 0x16, 8},
	{CodeRFUnsupported, "RF_UNSUPPORTED", 0x17, 2},
	{CodeLeaseLapsed, "LEASE_LAPSED", 0x18, 8},
	{CodeEpochFenced, "EPOCH_FENCED", 0x19, 0},
	{CodeSeqGap, "SEQ_GAP", 0x1A, 0},
	{CodeLogBelowHorizon, "LOG_BELOW_HORIZON", 0x1B, 8},
	{CodeStateMoved, "STATE_MOVED", 0x1C, 8},
	{CodeConflict, "CONFLICT", 0x1D, 8},
	{CodeClusterMismatch, "CLUSTER_MISMATCH", 0x1E, 2},
	{CodeNotADataNode, "NOT_A_DATA_NODE", 0x1F, 2},
	{CodeSchemaNotCurrent, "SCHEMA_NOT_CURRENT", 0x20, 8},
	{CodeBucketDropping, "BUCKET_DROPPING", 0x21, 8},
	{CodeDeclUnavailable, "DECL_UNAVAILABLE", 0x22, 8},
	{CodeBucketExists, "BUCKET_EXISTS", 0x23, 2},
	{CodeReservedName, "RESERVED_NAME", 0x24, 2},
	{CodeBucketIDCollision, "BUCKET_ID_COLLISION", 0x25, 8},
	{CodeSeedNotHeld, "SEED_NOT_HELD", 0x26, 2},
	{CodeReplaceTargetHasData, "REPLACE_TARGET_HAS_DATA", 0x27, 8},
	{CodeMintFailed, "MINT_FAILED", 0x28, 8},
	{CodeCorruptKey, "CORRUPT_KEY", 0x29, 2},
	{CodeSeedHeld, "SEED_HELD", 0x2A, 8},
}

// specCondition is one text of conventions "Errors" (and the six queries
// texts), with the kind of fix it names.
type specCondition struct {
	code Code
	cond Condition
	kind Kind
}

var specConditions = []specCondition{
	{CodeNotFound, CondBucketNotFound, KindCLI},
	{CodeNotFound, CondDocumentNotFound, KindAPI},
	{CodeInvalidArgument, CondBadCursor, KindAPI},
	{CodeInvalidArgument, CondLimitAboveMax, KindAPI},
	{CodeInvalidArgument, CondLimitBelowOne, KindAPI},
	{CodeInvalidArgument, CondSortNotSupported, KindAPI},
	{CodeInvalidArgument, CondReadPreference, KindConfig},
	{CodeInvalidArgument, CondExpandTooMany, KindAPI},
	{CodeInvalidArgument, CondExpandDepth, KindAPI},
	{CodeInvalidArgument, CondExpandStoreType, KindAPI},
	{CodeInvalidArgument, CondExpandNoBucket, KindAPI},
	{CodeInvalidArgument, CondOfTypeWord, KindCLI},
	{CodeInvalidArgument, CondRefIDType, KindAPI},
	{CodeInvalidArgument, CondExpandFieldProjected, KindAPI},
	{CodeInvalidArgument, CondExpandWhereField, KindAPI},
	{CodeInvalidArgument, CondExpandTooManyReads, KindAPI},
	{CodeInvalidArgument, CondExpandFieldType, KindAPI},
	{CodeNotFound, CondOfBucketNotFound, KindCLI},
	{CodeUnavailable, CondPoisonedLog, KindCLI},
	{CodeUnavailable, CondDeclarationWriteFailed, KindCLI},
	{CodeResultTooLarge, CondResultTooLarge, KindAPI},
	{CodeFieldType, CondFieldNotDeclared, KindCLI},
	{CodeDocumentExists, CondDocumentExists, KindAPI},
	{CodeNotOwner, CondNotOwner, KindNone},
	{CodeStaleEpoch, CondStaleEpoch, KindCLI},
	{CodeOwnerDown, CondOwnerDown, KindCLI},
	{CodeNoQuorum, CondNoQuorum, KindCLI},
	{CodeRFUnsupported, CondRFUnsupported, KindCLI},
	{CodeLeaseLapsed, CondLeaseLapsed, KindCLI},
	{CodeEpochFenced, CondEpochFenced, KindInternal},
	{CodeSeqGap, CondSeqGap, KindInternal},
	{CodeLogBelowHorizon, CondLogBelowHorizon, KindNone},
	{CodeStateMoved, CondStateMoved, KindCLI},
	{CodeConflict, CondConflict, KindCLI},
	{CodeClusterMismatch, CondClusterMismatch, KindCLI},
	{CodeNotADataNode, CondNotADataNode, KindCLI},
	{CodeSchemaNotCurrent, CondSchemaNotCurrent, KindCLI},
	{CodeSchemaNotCurrent, CondClientDeclarationStale, KindCLI},
	{CodeSchemaNotCurrent, CondSchemaNotCurrentUnnamed, KindNone},
	{CodeBucketDropping, CondBucketDropping, KindCLI},
	{CodeDeclUnavailable, CondDeclUnavailable, KindCLI},
	{CodeBucketExists, CondBucketExists, KindCLI},
	{CodeReservedName, CondReservedName, KindCLI},
	{CodeBucketIDCollision, CondBucketIDCollision, KindCLI},
	{CodeSeedNotHeld, CondSeedNotHeld, KindCLI},
	{CodeSeedNotHeld, CondSeedNotHeldForget, KindCLI},
	{CodeReplaceTargetHasData, CondReplaceTargetHasData, KindCLI},
	{CodeMintFailed, CondMintFailed, KindAPI},
	{CodeCorruptKey, CondCorruptKey, KindNone},
	{CodeSeedHeld, CondSeedHeld, KindCLI},
	{CodeSeedHeld, CondSeedHeldOnlyCopy, KindCLI},
	{CodeUnavailable, CondForwardWriteUnreachable, KindCLI},
	{CodeUnavailable, CondForwardReadUnreachable, KindCLI},
	{CodeRFUnsupported, CondRFUnsupportedCreateVoters, KindCLI},
	{CodeRFUnsupported, CondRFUnsupportedCreateData, KindCLI},
}

// TestErrorCodesAreUnique is the guard for "no code appears twice": the
// registry holds each spec code exactly once, with the spec's value and name.
func TestErrorCodesAreUnique(t *testing.T) {
	seenValue := map[uint16]string{}
	seenName := map[string]bool{}
	for _, row := range specCodes {
		if got := uint16(row.code); got != row.value {
			t.Errorf("%s = 0x%02X, want 0x%02X", row.name, got, row.value)
		}
		if row.code.String() != row.name {
			t.Errorf("0x%02X names itself %q, want %q", row.value, row.code.String(), row.name)
		}
		if other, dup := seenValue[uint16(row.code)]; dup {
			t.Errorf("code 0x%02X is both %s and %s", uint16(row.code), other, row.name)
		}
		seenValue[uint16(row.code)] = row.name
		seenName[row.name] = true
	}
	registered := Codes()
	if len(registered) != len(specCodes) {
		t.Fatalf("Codes() holds %d codes, the spec has %d", len(registered), len(specCodes))
	}
	seenRegistered := map[Code]bool{}
	for _, code := range registered {
		if seenRegistered[code] {
			t.Errorf("Codes() lists %s twice", code)
		}
		seenRegistered[code] = true
		if !seenName[code.String()] {
			t.Errorf("Codes() lists %s, which is not a spec code", code)
		}
	}
}

// TestEveryCodeHasOneExitStatus pins each code's exit status to the spec's
// column. The guard breaks a real row: CodeNotOwner is 8 and a build that
// sets it to 2 must fail here.
func TestEveryCodeHasOneExitStatus(t *testing.T) {
	for _, row := range specCodes {
		if got := row.code.Exit(); got != row.exit {
			t.Errorf("%s exits %d, want %d", row.name, got, row.exit)
		}
		switch row.code.Exit() {
		case 0, 2, 8:
		default:
			t.Errorf("%s exits %d, which is not one of 0, 2, 8", row.name, row.code.Exit())
		}
	}
	if got := Code(0xFFFF).Exit(); got != 1 {
		t.Errorf("an unregistered code exits %d, want 1", got)
	}
}

// TestInternalCodesHaveTheirExactText pins the two node-to-node codes: they
// exit 0 and render the exact text of conventions Errors (G14, amended).
func TestInternalCodesHaveTheirExactText(t *testing.T) {
	fenced := CodeEpochFenced.Text(CondEpochFenced, map[string]string{
		"kind": "WRITE", "node": "node-b", "e": "42", "e_seen": "43",
	})
	if want := "refused WRITE from node-b at epoch 42: this node is at epoch 43"; fenced != want {
		t.Errorf("EPOCH_FENCED text = %q, want %q", fenced, want)
	}
	gap := CodeSeqGap.Text(CondSeqGap, map[string]string{
		"e": "42", "n": "9", "slot": "4312", "db": "app", "node": "node-b", "m": "5", "next": "6", "last": "8",
	})
	want := "refused position 42:9 for slot 4312 of app from node-b: this copy is at 42:5, and 42:6…42:8 are missing. Catching up; no client action"
	if gap != want {
		t.Errorf("SEQ_GAP text = %q, want %q", gap, want)
	}
	for _, code := range []Code{CodeEpochFenced, CodeSeqGap} {
		if code.Exit() != 0 {
			t.Errorf("%s exits %d, want 0", code, code.Exit())
		}
	}
}

var placeholderPattern = regexp.MustCompile(`<[A-Za-z][A-Za-z0-9_]*>`)

// TestTextReplacesEveryPlaceholder gives every placeholder an argument and
// checks that none survives and each argument appears.
func TestTextReplacesEveryPlaceholder(t *testing.T) {
	for _, row := range specConditions {
		template, ok := Template(row.code, row.cond)
		if !ok {
			t.Errorf("%s has no template for condition %d", row.code, row.cond)
			continue
		}
		args := map[string]string{}
		for _, ph := range placeholderPattern.FindAllString(template, -1) {
			name := strings.Trim(ph, "<>")
			args[name] = "ARG_" + name
		}
		got := row.code.Text(row.cond, args)
		if left := placeholderPattern.FindAllString(got, -1); len(left) != 0 {
			t.Errorf("%s: placeholders %v survive in %q", row.code, left, got)
		}
		for name, value := range args {
			if !strings.Contains(got, value) {
				t.Errorf("%s: argument %s never appears in %q", row.code, name, got)
			}
		}
	}
}

// TestTextMissingArgIsVisible: a placeholder without an argument stays as
// written, so the gap is visible in the output, and nothing panics.
func TestTextMissingArgIsVisible(t *testing.T) {
	got := CodeNotFound.Text(CondBucketNotFound, map[string]string{"bucket": "users"})
	if !strings.Contains(got, `bucket "users" not found`) {
		t.Errorf("bucket was not substituted: %q", got)
	}
	if !strings.Contains(got, "<db>") {
		t.Errorf("a missing db argument must stay visible as <db>: %q", got)
	}
	if nilArgs := CodeNotFound.Text(CondBucketNotFound, nil); !strings.Contains(nilArgs, "<bucket>") {
		t.Errorf("nil args must leave every placeholder: %q", nilArgs)
	}
}

// TestTextRejectsOtherCodesCondition: a condition belongs to one code, and
// an unknown pairing returns one fixed text that names the code.
func TestTextRejectsOtherCodesCondition(t *testing.T) {
	for _, row := range specConditions {
		for _, other := range specCodes {
			if other.code == row.code {
				continue
			}
			got := other.code.Text(row.cond, map[string]string{"bucket": "users", "db": "app"})
			if !strings.Contains(got, other.name) {
				t.Errorf("%s with %s's condition returned %q, which does not name the code", other.name, row.code, got)
			}
			if _, ok := Template(other.code, row.cond); ok {
				t.Errorf("%s accepts a condition of %s", other.name, row.code)
			}
		}
	}
	if a, b := CodeNotFound.Text(Condition(250), nil), CodeNotFound.Text(Condition(251), map[string]string{"x": "y"}); a != b {
		t.Errorf("the unknown-condition text must be fixed: %q vs %q", a, b)
	}
}

// TestEveryTextHasAKind pins G18: each text declares what its fix is, and the
// registry holds exactly the spec's conditions.
func TestEveryTextHasAKind(t *testing.T) {
	total := 0
	for _, code := range Codes() {
		total += len(Conditions(code))
	}
	if total != len(specConditions) {
		t.Errorf("registry holds %d texts, the spec table has %d", total, len(specConditions))
	}
	for _, row := range specConditions {
		kind, ok := KindOf(row.code, row.cond)
		if !ok || kind == 0 {
			t.Errorf("%s condition %d has no kind", row.code, row.cond)
			continue
		}
		if kind != row.kind {
			t.Errorf("%s condition %d has kind %v, want %v", row.code, row.cond, kind, row.kind)
		}
	}
}

// grammarForms are the statement forms of conventions section 17, with the
// optional parts expanded. A "<...>" is one word, except <body> and <args>,
// which match anything (a brace body, a call's arguments).
var grammarForms = []string{
	"create database <name>",
	"create database <name> <body>",
	"alter database <name> <body>",
	"drop database <name>",
	"create bucket <name> <body>",
	"create bucket <name> in <db> <body>",
	"alter bucket <name> <body>",
	"alter bucket <name> in <db> <body>",
	"drop bucket <name>",
	"drop bucket <name> in <db>",
	"join <node-id>@<host:port>",
	"promote <node-id>",
	"leave <node-id>",
	"remove-node <node-id>",
	"replace-node <node-id> --with <node-id>@<host:port>",
	"replace-node <node-id> --with <node-id>@<host:port> --clear",
	"nodes",
	"status",
	"status seeding",
	"status seeding <db>",
	"status seeding <db> <slot>",
	"seeding retry <db> <slot>",
	"seeding forget <db> <slot>",
	"seeding forget <db> <slot> anyway",
	"show databases",
	"show buckets in <db>",
	"show buckets in <db> limit <n>",
	"show buckets in <db> cursor <hex>",
	"show buckets in <db> limit <n> cursor <hex>",
	"describe database <name>",
	"describe bucket <name>",
	"describe bucket <name> in <db>",
	"show placement for <id> in <db>.<bucket>",
	"show replication for <db>.<bucket> slot <n>",
	"use <database>",
	"db.<bucket>.insert(<args>)",
	"db.<bucket>.upsert(<args>)",
	"db.<bucket>.update(<args>)",
	"db.<bucket>.delete(<args>)",
	"db.<bucket>.list(<args>)",
	"db.<bucket>.count(<args>)",
	`db.<bucket>["<id>"].get()`,
	`db.<bucket>["<id>"].<storeField>.<verb>(<args>)`,
}

var formPlaceholder = regexp.MustCompile(`<[^<>]+>`)

func compileForm(form string) *regexp.Regexp {
	var out strings.Builder
	out.WriteString("^")
	last := 0
	for _, loc := range formPlaceholder.FindAllStringIndex(form, -1) {
		out.WriteString(regexp.QuoteMeta(form[last:loc[0]]))
		switch form[loc[0]:loc[1]] {
		case "<body>", "<args>":
			out.WriteString(`.*`)
		default:
			out.WriteString(`\S+`)
		}
		last = loc[1]
	}
	out.WriteString(regexp.QuoteMeta(form[last:]))
	out.WriteString("$")
	return regexp.MustCompile(out.String())
}

func matchesGrammar(command string) bool {
	for _, form := range grammarForms {
		if compileForm(form).MatchString(command) {
			return true
		}
	}
	return false
}

func isLetterOrDigit(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

// quotedCommands returns the text between each pair of single quotes. An
// opening quote follows a space or "(" and a closing quote is not followed
// by a letter, so the apostrophe in "cluster's" opens nothing.
func quotedCommands(text string) []string {
	var found []string
	for i := 0; i < len(text); i++ {
		if text[i] != '\'' || (i > 0 && text[i-1] != ' ' && text[i-1] != '(') {
			continue
		}
		for j := i + 1; j < len(text); j++ {
			if text[j] == '\'' && (j+1 == len(text) || !isLetterOrDigit(text[j+1])) {
				found = append(found, text[i+1:j])
				i = j
				break
			}
		}
	}
	return found
}

// TestEveryTextQuotesAGrammarCommand checks each CLI text: it quotes at
// least one command and every quoted command is a statement of section 17.
// Config, api and internal texts are exempt from the grammar; a text marked
// none must quote nothing.
func TestEveryTextQuotesAGrammarCommand(t *testing.T) {
	for _, bogus := range []string{"frobnicate <db>", "show buckets", "nodes now", "db.x"} {
		if matchesGrammar(bogus) {
			t.Errorf("the grammar table accepts %q", bogus)
		}
	}
	for _, row := range specConditions {
		template, _ := Template(row.code, row.cond)
		quoted := quotedCommands(template)
		switch row.kind {
		case KindCLI:
			if len(quoted) == 0 {
				t.Errorf("%s: a cli text quotes no command: %q", row.code, template)
			}
			for _, command := range quoted {
				if !matchesGrammar(command) {
					t.Errorf("%s quotes %q, which is not a statement of conventions section 17", row.code, command)
				}
			}
		case KindNone:
			if len(quoted) != 0 {
				t.Errorf("%s is marked none but quotes %v", row.code, quoted)
			}
		}
	}
}

// TestDetailShapes: each of the six shapes of queries Formats (error detail)
// can be populated and read back through the exact field names below.
//
//	NOT_OWNER      {owner, owner_addr, epoch, slot}      Owner, OwnerAddr, Epoch, Slot
//	STALE_EPOCH    {epoch, owner, owner_addr}            Epoch, Owner, OwnerAddr
//	OWNER_DOWN     {owner, missing_slots}                Owner, MissingSlots
//	NO_QUORUM      {voters, answered}                    Voters, Answered
//	LEASE_LAPSED   {node, lapsed_ms}                     Node, LapsedMs
//	RF_UNSUPPORTED {rf, voters, data_members}            Rf, Voters, DataMembers
func TestDetailShapes(t *testing.T) {
	shapes := []struct {
		code   Code
		fields []string
	}{
		{CodeNotOwner, []string{"Owner", "OwnerAddr", "Epoch", "Slot"}},
		{CodeStaleEpoch, []string{"Epoch", "Owner", "OwnerAddr"}},
		{CodeOwnerDown, []string{"Owner", "MissingSlots"}},
		{CodeNoQuorum, []string{"Voters", "Answered"}},
		{CodeLeaseLapsed, []string{"Node", "LapsedMs"}},
		{CodeRFUnsupported, []string{"Rf", "Voters", "DataMembers"}},
	}
	for _, shape := range shapes {
		var detail Detail
		value := reflect.ValueOf(&detail).Elem()
		for i, name := range shape.fields {
			field := value.FieldByName(name)
			if !field.IsValid() {
				t.Fatalf("%s: Detail has no field %s", shape.code, name)
			}
			switch field.Kind() {
			case reflect.String:
				field.SetString("v" + name)
			case reflect.Uint64:
				field.SetUint(uint64(100 + i))
			case reflect.Int:
				field.SetInt(int64(100 + i))
			default:
				t.Fatalf("%s: field %s has unexpected kind %v", shape.code, name, field.Kind())
			}
		}
		for i, name := range shape.fields {
			field := value.FieldByName(name)
			switch field.Kind() {
			case reflect.String:
				if field.String() != "v"+name {
					t.Errorf("%s: %s read back %q", shape.code, name, field.String())
				}
			case reflect.Uint64:
				if field.Uint() != uint64(100+i) {
					t.Errorf("%s: %s read back %d", shape.code, name, field.Uint())
				}
			case reflect.Int:
				if field.Int() != int64(100+i) {
					t.Errorf("%s: %s read back %d", shape.code, name, field.Int())
				}
			}
		}
	}
}

// TestErrorCarriesItsCode: the typed error exposes its code to errors.As.
func TestErrorCarriesItsCode(t *testing.T) {
	var err error = &Error{Code: CodeInvalidArgument, Message: "m"}
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != CodeInvalidArgument || err.Error() != "m" {
		t.Errorf("Error does not round-trip: %v", err)
	}
}
