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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The byte contract, held by this module's own tests. testdata/ringwire-structural.golden is
// one row per request, reply, record and number of the wire: name, kind, bytes in hex. The
// server holds its encoders to the same rows; these tests hold this module's, so the module
// checks the contract wherever it is built, with no server beside it.
//
// testdata/ringwire-decoded.golden is what each row means: the value this module's decoder
// reads from the row's bytes. A round trip alone cannot see an encoder and a decoder that
// both swap two members; the written-out value can.

const (
	sgStructural = "testdata/ringwire-structural.golden"
	sgDecoded    = "testdata/ringwire-decoded.golden"
	// sgRowsSHA256 is the sha256 of the structural rows (every line that is not a comment,
	// each with its newline). The server pins its own copy of the rows to the same sum, so
	// the two files cannot drift apart.
	sgRowsSHA256 = "8bd5671c4d817ec0c5c935b90cef62da1dbdaeaed83d8196955444d95a8d45ca"
	// sgLongValue is the length past which a decoded value is recorded as its sha256.
	sgLongValue = 1024
)

var sgUpdateDecoded = flag.Bool("update-decoded-golden", false, "rewrite testdata/ringwire-decoded.golden from what the decoders read today; only for a row that was added, never to make a failing row pass")

type sgRow struct {
	name, kind string
	raw        []byte
}

// sgCodec decodes one row and encodes the value again.
type sgCodec func(raw []byte) (value any, again []byte, err error)

func sgCodecOf[T any](decode func([]byte) (T, error), encode func(T) ([]byte, error)) sgCodec {
	return func(raw []byte) (any, []byte, error) {
		value, err := decode(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("decoding: %w", err)
		}
		again, err := encode(value)
		if err != nil {
			return value, nil, fmt.Errorf("encoding the decoded value: %w", err)
		}
		return value, again, nil
	}
}

// sgEncodeEntries writes an entries blob from its layout: per record a 2-byte slot, then the
// id and the body, each after its length as a shortest uvarint.
func sgEncodeEntries(entries []ListEntry) ([]byte, error) {
	var blob []byte
	for _, entry := range entries {
		blob = binary.BigEndian.AppendUint16(blob, entry.Slot)
		blob = append(binary.AppendUvarint(blob, uint64(len(entry.EncID))), entry.EncID...)
		blob = append(binary.AppendUvarint(blob, uint64(len(entry.Body))), entry.Body...)
	}
	return blob, nil
}

// sgComponent measures a key component as written and inverted; its value is its width.
func sgComponent(raw []byte) (any, []byte, error) {
	width, err := EncodedComponentLen(raw)
	if err != nil {
		return nil, nil, err
	}
	inverted := make([]byte, len(raw))
	for i, b := range raw {
		inverted[i] = ^b
	}
	descending, err := EncodedComponentLenDesc(inverted)
	if err != nil {
		return nil, nil, fmt.Errorf("the inverted component: %w", err)
	}
	if width != len(raw) || descending != len(raw) {
		return nil, nil, fmt.Errorf("the component measures %d bytes as written and %d inverted, and the row is %d", width, descending, len(raw))
	}
	return width, bytes.Clone(raw), nil
}

var sgCodecs = map[string]sgCodec{
	"keyed":      sgCodecOf(DecodeRingKeyed, EncodeRingKeyed),
	"batch":      sgCodecOf(DecodeRingBatch, EncodeRingBatch),
	"getmany":    sgCodecOf(DecodeRingGetMany, EncodeRingGetMany),
	"list":       sgCodecOf(DecodeRingList, EncodeRingList),
	"count":      sgCodecOf(DecodeRingCount, EncodeRingCount),
	"admin":      sgCodecOf(DecodeRingAdmin, EncodeRingAdmin),
	"adminreply": sgCodecOf(DecodeRingAdminReply, EncodeRingAdminReply),
	"reply":      sgCodecOf(DecodeRingReply, EncodeRingReply),
	"error":      sgCodecOf(DecodeRingError, EncodeRingError),
	"oprecord":   sgCodecOf(DecodeOp, EncodeOp),
	"entries":    sgCodecOf(DecodeEntries, sgEncodeEntries),
	"component":  sgComponent,
}

// sgTable is the kind of a row that is one number of the wire; sgTableRows says which.
const sgTable = "table"

// sgServerKinds are the kinds whose bytes the server alone writes and reads: the envelope of a
// log record, which wraps an "oprecord" body this module does read.
var sgServerKinds = map[string]bool{"walrecord": true}

// sgServerTableRows are the table rows for numbers this module does not declare.
var sgServerTableRows = map[string]bool{"table/length/WALRecordHeader": true}

// sgTableRows is every number of the wire this module declares, under the name its golden row
// has. The row holds the bytes; this holds the constant to them.
func sgTableRows() map[string][]byte {
	rows := map[string][]byte{}
	for name, value := range map[string]byte{
		"tag/Null": TagNull, "tag/Bool": TagBool, "tag/Number": TagNumber, "tag/DateTime": TagDateTime,
		"tag/String": TagString, "tag/Binary": TagBinary, "tag/ObjectID": TagObjectID,
		"class/Insert": OpInsert, "class/Update": OpUpdate, "class/Upsert": OpUpsert, "class/Delete": OpDelete,
		"class/StoreVerb": OpStoreVerb, "class/Claim": OpClaim, "class/Ack": OpAck, "class/Release": OpRelease,
		"class/Extend": OpExtend,
		"class/Get":    RingClassGet, "class/Peek": RingClassPeek, "class/StoreList": RingClassStoreList,
		"class/StoreCount": RingClassStoreCount, "class/LeaseGet": RingClassLeaseGet, "class/StoreGet": RingClassStoreGet,
		"class/GetFields": RingClassGetFields, "class/Nearest": RingClassNearest, "class/Aggregate": RingClassAggregate,
		"verb/Push": VerbPush, "verb/Pop": VerbPop, "verb/Take": VerbTake, "verb/Put": VerbPut, "verb/Inc": VerbInc,
		"update/Set": UpdateSet, "update/Unset": UpdateUnset, "update/Inc": UpdateInc, "update/Mul": UpdateMul,
		"update/Push": UpdatePush, "update/AddToSet": UpdateAddToSet, "update/Min": UpdateMin, "update/Max": UpdateMax,
		"update/RenameTo": UpdateRenameTo,
		"return/None":     ReturnNone, "return/After": ReturnAfter, "return/Before": ReturnBefore,
		"metric/Cosine": RingMetricCosine, "metric/Dot": RingMetricDot, "metric/L2": RingMetricL2,
		"aggregate-read/Describe": AggregateReadDescribe, "aggregate-read/Sum": AggregateReadSum,
		"aggregate-read/SumBy": AggregateReadSumBy, "aggregate-read/CountBy": AggregateReadCountBy,
		"op-flags/known":  KnownOpFlags(),
		"opcode/RingList": OpRingList, "opcode/RingCount": OpRingCount, "opcode/RingKeyed": OpRingKeyed,
		"opcode/RingAdmin": OpRingAdmin, "opcode/RingTopology": OpRingTopology, "opcode/RingReply": OpRingReply,
		"opcode/RingBatch": OpRingBatch, "opcode/RingGetMany": OpRingGetMany,
		"length/RequestID": RequestIDLen, "length/RingPrologue": RingPrologueLen,
		"limit/MaxGuardTerms": MaxGuardTerms, "limit/MaxExpandFields": MaxExpandFields,
	} {
		rows["table/"+name] = []byte{value}
	}
	for _, code := range Codes() {
		rows["table/code/"+code.String()] = []byte{byte(code >> 8), byte(code)}
	}
	return rows
}

func sgReadRows(t *testing.T) []sgRow {
	t.Helper()
	text, err := os.ReadFile(sgStructural)
	if err != nil {
		t.Fatalf("reading the structural golden: %v. Restore it with 'git checkout HEAD -- %s'; it is never regenerated", err, sgStructural)
	}
	var rows []sgRow
	seen := map[string]bool{}
	for number, line := range strings.Split(strings.TrimRight(string(text), "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 3 {
			t.Fatalf("%s line %d has %d tab-separated parts, want name, kind and hex", sgStructural, number+1, len(parts))
		}
		raw, err := hex.DecodeString(parts[2])
		if err != nil || len(raw) == 0 {
			t.Fatalf("%s line %d (%s): the bytes are not hex, or there are none: %v", sgStructural, number+1, parts[0], err)
		}
		if seen[parts[0]] {
			t.Fatalf("%s names row %s twice", sgStructural, parts[0])
		}
		seen[parts[0]] = true
		rows = append(rows, sgRow{name: parts[0], kind: parts[1], raw: raw})
	}
	if len(rows) == 0 {
		t.Fatalf("%s holds no row. Restore it with 'git checkout HEAD -- %s'", sgStructural, sgStructural)
	}
	return rows
}

// TestStructuralGoldenRowsAreUnchanged pins the rows themselves, so a row cannot be edited to
// follow a codec that moved.
func TestStructuralGoldenRowsAreUnchanged(t *testing.T) {
	text, err := os.ReadFile(sgStructural)
	if err != nil {
		t.Fatalf("reading the structural golden: %v", err)
	}
	var rows strings.Builder
	for _, line := range strings.SplitAfter(string(text), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			rows.WriteString(line)
		}
	}
	sum := sha256.Sum256([]byte(rows.String()))
	if got := hex.EncodeToString(sum[:]); got != sgRowsSHA256 {
		t.Fatalf("the rows of %s hash to %s and the pinned sum is %s. A row was changed, added or removed. The rows are the wire and the log format: restore the file with 'git checkout HEAD -- %s'. A new row is added to the server's golden and this one together, with both pinned sums", sgStructural, got, sgRowsSHA256, sgStructural)
	}
}

// TestStructuralGoldenDecodesAndEncodesBack holds each decoder to the bytes on the wire and
// each encoder to the same bytes: every row decodes, and the decoded value encodes to the row.
func TestStructuralGoldenDecodesAndEncodesBack(t *testing.T) {
	read := map[string]int{}
	for _, row := range sgReadRows(t) {
		codec, decodes := sgCodecs[row.kind]
		switch {
		case decodes:
		case row.kind == sgTable || sgServerKinds[row.kind]:
			continue
		default:
			t.Errorf("%s: kind %q has no codec in sgCodecs. Add its decoder and encoder, or name it in sgServerKinds if this module does not read it", row.name, row.kind)
			continue
		}
		read[row.kind]++
		_, again, err := codec(row.raw)
		switch {
		case err != nil:
			t.Errorf("%s: %v. These bytes are on the wire or in a log today; a build that does not read them cannot talk to the others", row.name, err)
		case !bytes.Equal(again, row.raw):
			t.Errorf("%s: the decoded value encodes to\n  %x\nand the row is\n  %x\nThe encoder moved: what this build writes, the others do not read", row.name, again, row.raw)
		}
	}
	for kind := range sgCodecs {
		if read[kind] == 0 {
			t.Errorf("no row of kind %q was read, so its codec is not held to any bytes. Restore the rows with 'git checkout HEAD -- %s'", kind, sgStructural)
		}
	}
}

// TestStructuralGoldenNumbersAreTheConstants holds every tag, class, verb, code, opcode and
// length this module declares to its row, in both directions.
func TestStructuralGoldenNumbersAreTheConstants(t *testing.T) {
	declared := sgTableRows()
	inGolden := map[string]bool{}
	for _, row := range sgReadRows(t) {
		if row.kind != sgTable {
			continue
		}
		inGolden[row.name] = true
		want, known := declared[row.name]
		switch {
		case sgServerTableRows[row.name]:
		case !known:
			t.Errorf("%s: the golden holds this number and sgTableRows names no constant for it. Add the constant to sgTableRows, or the row to sgServerTableRows if the server alone declares it", row.name)
		case !bytes.Equal(want, row.raw):
			t.Errorf("%s: the constant is %x and the wire carries %x. The number changed; every other build still sends and expects %x", row.name, want, row.raw, row.raw)
		}
	}
	for name := range declared {
		if !inGolden[name] {
			t.Errorf("%s: sgTableRows names this number and the golden has no row for it. A new number of the wire gets a row in the server's golden and in %s", name, sgStructural)
		}
	}
}

// sgDump writes a value out in full, in a form that is the same on every run: no addresses,
// members by name, zero members left out, bytes in hex, floats by their bits.
func sgDump(out *strings.Builder, value reflect.Value) {
	switch value.Kind() {
	case reflect.Pointer:
		out.WriteByte('&')
		sgDump(out, value.Elem())
	case reflect.Struct:
		out.WriteByte('{')
		first := true
		for i := range value.NumField() {
			if value.Field(i).IsZero() {
				continue
			}
			if !first {
				out.WriteByte(' ')
			}
			first = false
			out.WriteString(value.Type().Field(i).Name)
			out.WriteByte(':')
			sgDump(out, value.Field(i))
		}
		out.WriteByte('}')
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			raw := make([]byte, value.Len())
			for i := range raw {
				raw[i] = byte(value.Index(i).Uint())
			}
			out.WriteString("x'" + hex.EncodeToString(raw) + "'")
			return
		}
		out.WriteByte('[')
		for i := range value.Len() {
			if i > 0 {
				out.WriteByte(' ')
			}
			sgDump(out, value.Index(i))
		}
		out.WriteByte(']')
	case reflect.Map:
		keys := value.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		out.WriteString("map[")
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(' ')
			}
			sgDump(out, key)
			out.WriteByte(':')
			sgDump(out, value.MapIndex(key))
		}
		out.WriteByte(']')
	case reflect.String:
		fmt.Fprintf(out, "%q", value.String())
	case reflect.Bool:
		fmt.Fprintf(out, "%t", value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprintf(out, "%d", value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fmt.Fprintf(out, "%d", value.Uint())
	case reflect.Float32, reflect.Float64:
		fmt.Fprintf(out, "f'%016x'", math.Float64bits(value.Float()))
	default:
		fmt.Fprintf(out, "<kind %s>", value.Kind())
	}
}

// sgDecodedLines is the decoded golden as the decoders read it today: one line per row that
// has a decoder, name and value separated by a tab.
func sgDecodedLines(t *testing.T) []string {
	t.Helper()
	var lines []string
	for _, row := range sgReadRows(t) {
		codec, decodes := sgCodecs[row.kind]
		if !decodes {
			continue
		}
		value, _, err := codec(row.raw)
		if err != nil {
			t.Errorf("%s: %v", row.name, err)
			continue
		}
		var out strings.Builder
		sgDump(&out, reflect.ValueOf(value))
		text := out.String()
		if strings.Contains(text, "<kind ") {
			t.Fatalf("%s: the decoded value holds a member sgDump cannot write out: %s. Teach sgDump the kind", row.name, text)
		}
		if len(text) > sgLongValue {
			text = fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(text)))
		}
		lines = append(lines, row.name+"\t"+text)
	}
	return lines
}

const sgDecodedHeader = "# What each row of ringwire-structural.golden means: name, then the value the decoder reads from the\n" +
	"# row's bytes (members by name, zero members left out, bytes as x'hex', floats as f'bits'; a value over\n" +
	"# 1024 characters as its sha256). A row that differs is a decoder that reads the same bytes as another value.\n"

// TestStructuralGoldenDecodedValuesAreUnchanged holds what each row means: the value read from
// its bytes is the one recorded when the row was written.
func TestStructuralGoldenDecodedValuesAreUnchanged(t *testing.T) {
	lines := sgDecodedLines(t)
	if t.Failed() {
		return
	}
	if *sgUpdateDecoded {
		if err := os.WriteFile(sgDecoded, []byte(sgDecodedHeader+strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("writing %s: %v", sgDecoded, err)
		}
		t.Logf("wrote %d rows to %s", len(lines), sgDecoded)
		return
	}
	text, err := os.ReadFile(sgDecoded)
	if err != nil {
		t.Fatalf("reading the decoded golden: %v. Restore it with 'git checkout HEAD -- %s'", err, sgDecoded)
	}
	want := map[string]string{}
	for _, line := range strings.Split(strings.TrimRight(string(text), "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(line, "\t")
		if !found {
			t.Fatalf("%s has a line that is not name, tab, value: %q", sgDecoded, line)
		}
		want[name] = value
	}
	for _, line := range lines {
		name, value, _ := strings.Cut(line, "\t")
		recorded, known := want[name]
		switch {
		case !known:
			t.Errorf("%s: %s has no value for this row. For a row that was added, run 'go test -run TestStructuralGoldenDecodedValuesAreUnchanged -update-decoded-golden .' and read the new line before committing it", name, sgDecoded)
		case recorded != value:
			t.Errorf("%s: the bytes of this row now decode as\n  %s\nand they were recorded as\n  %s\nThe decoder reads the same bytes as a different value. Fix the decoder; the recorded value is what every other build reads", name, value, recorded)
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("%s: %s records a value and the structural golden has no such row. Remove the line", name, sgDecoded)
	}
}
