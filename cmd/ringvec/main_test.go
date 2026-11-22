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

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	ringwire "github.com/hoarddb/hoarddb-wire"
)

// inputRow is one X6 input row: the request as data, and the committed oracle
// row holding the payload it must encode to. The expected bytes are read from
// that file at run time and never transcribed here, so a changed golden or
// vector fails this test instead of leaving it green. Until ring-inputs.json
// exists these rows stand in for it; this input format is the contract X6 must
// emit (docs/specs/ring-py-node-drivers-design.md section 2b).
type inputRow struct {
	name   string
	input  string
	oracle oracleRow
}

// oracleRow names one committed row holding an expected payload: a row of
// clients/wire/testdata/ringwire-structural.golden ("golden"), or the request
// of a row of clients/testdata/driver-vectors/v1.json ("vectors").
type oracleRow struct {
	file, row string
}

const (
	structuralGolden = "../../testdata/ringwire-structural.golden"
	driverVectors    = "../../testdata/driver-vectors/v1.json"
)

// inputRows is one row per request form ringvec encodes, so a form that stops
// encoding, or encodes differently, fails here.
var inputRows = []inputRow{
	{
		name: "keyed/get",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"read":{"class":129,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000"}}`,
		oracle: oracleRow{file: "golden", row: "keyed/get"},
	},
	{
		name: "keyed/insert",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"write":{"class":1,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","id_value_hex":"0203000000753100",
				"fields":[{"id":2,"value_hex":"020400000041646100"},{"id":3,"value_hex":"1024000000"}]}}`,
		oracle: oracleRow{file: "golden", row: "keyed/insert"},
	},
	{
		name: "keyed/verb-push",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"write":{"class":5,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","fields":[{"id":4,"verb":1,
					"value_hex":"2a0000000576001d000000001d00000002536b750004000000412d3100105072696365000c0000000000"}]}}`,
		oracle: oracleRow{file: "golden", row: "keyed/verb-push"},
	},
	{
		name: "keyed/claim",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"write":{"class":6,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","fields":[{"id":4}],"count":5,"now":1700000000000,
				"nonce_hex":"202122232425262728292a2b2c2d2e2f"}}`,
		oracle: oracleRow{file: "golden", row: "keyed/claim"},
	},
	{
		name: "keyed/update-guard-return-after",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"write":{"class":2,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","fields":[{"id":3,"op":3,"value_hex":"1001000000"}],
				"guard":[{"id":2,"value_hex":"020400000041646100"},{"id":5,"value_hex":"0a"}],"return":1}}`,
		oracle: oracleRow{file: "golden", row: "keyed/update-guard-return-after"},
	},
	{
		name: "keyed/store-list-after-expand",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"read":{"class":131,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","field_id":4,"limit":1000,"after_hex":"506b310000",
				"options":{"local":true,"expand":[
					{"field":"Author","database":"app","bucket":"User","fields":["Name"],"where":{"City":"Oslo"}},
					{"field":"Editor"}]}}}`,
		oracle: oracleRow{file: "golden", row: "keyed/store-list-after-expand"},
	},
	{
		name: "keyed/nearest-cosine",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"read":{"class":138,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","field_id":4,
				"nearest":{"metric":1,"k":10,"field":"Vector","vector_hex":"0000003f0000a0bf00004040"}}}`,
		oracle: oracleRow{file: "golden", row: "keyed/nearest-cosine"},
	},
	{
		name: "keyed/aggregate-sum-by",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"read":{"class":139,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","field_id":4,"limit":100,"after_hex":"50412d310000",
				"aggregate":{"kind":2,"field":"Price","by":"Sku"}}}`,
		oracle: oracleRow{file: "golden", row: "keyed/aggregate-sum-by"},
	},
	{
		name: "keyed/get-fields-local",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"read":{"class":137,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","fields":["Name","Age"],"options":{"local":true}}}`,
		oracle: oracleRow{file: "golden", row: "keyed/get-fields-local"},
	},
	{
		name: "keyed/lease-get",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"read":{"class":135,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","field_id":4,
				"lease_hex":"404142434445464748494a4b4c4d4e4f"}}`,
		oracle: oracleRow{file: "golden", row: "keyed/lease-get"},
	},
	{
		name: "keyed/store-get",
		input: `{"opcode":220,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"read":{"class":136,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
				"id_hex":"5075310000","field_id":4,"key":"k1"}}`,
		oracle: oracleRow{file: "golden", row: "keyed/store-get"},
	},
	{
		name: "list/every-member",
		input: `{"opcode":218,"list":{"database":"app","bucket":"User","filter":{"City":"Oslo"},
			"fields":["Name","Age"],"limit":100,"cursor_hex":"010203","epoch":7,"local":true,
			"expand":[{"field":"Author","fields":["Name"],"where":{"City":"Oslo"}}]}}`,
		oracle: oracleRow{file: "golden", row: "list/every-member"},
	},
	{
		name:   "count/filter",
		input:  `{"opcode":219,"count":{"database":"app","bucket":"User","filter":{"City":"Oslo"}}}`,
		oracle: oracleRow{file: "golden", row: "count/filter"},
	},
	{
		name: "getmany/get-and-get-fields",
		input: `{"opcode":225,"prologue":{"epoch":7,"state_seq":9,"deadline_ms":1500},
			"reads":[{"class":129,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
					"id_hex":"5075300000"},
				{"class":129,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
					"id_hex":"5075310000","options":{"local":true}},
				{"class":137,"bucket_id":2254663609795936277,"decl_version":3,"schema_seen":2,
					"id_hex":"5075320000","fields":["Name"]}]}`,
		oracle: oracleRow{file: "golden", row: "getmany/get-and-get-fields"},
	},
	{
		name: "batch/one-op",
		input: `{"opcode":224,"prologue":{"epoch":4,"state_seq":9,"deadline_ms":5000},
			"ops":[{"class":3,"bucket_id":2254663609795936277,"decl_version":1,"schema_seen":1,
				"id_hex":"506164610000","id_value_hex":"020400000061646100",
				"fields":[{"id":2,"value_hex":"020400000041646100"}]}]}`,
		oracle: oracleRow{file: "vectors", row: "ring batch: every operation applied"},
	},
	{
		name:   "admin/create-database",
		input:  `{"opcode":221,"admin":{"cmd":"create_database","name":"app"}}`,
		oracle: oracleRow{file: "golden", row: "admin/create-database"},
	},
	{
		name: "admin/create-database-rf-memory",
		input: `{"opcode":221,"admin":{"cmd":"create_database","name":"app","rf":2,
			"storage":"memory"}}`,
		oracle: oracleRow{file: "golden", row: "admin/create-database-rf-memory"},
	},
	{
		name:   "admin/create-bucket-no-fields",
		input:  `{"opcode":221,"admin":{"cmd":"create_bucket","database":"app","name":"User","fields":[]}}`,
		oracle: oracleRow{file: "golden", row: "admin/create-bucket-no-fields"},
	},
	{
		name: "admin/create-bucket-every-member",
		input: `{"opcode":221,"admin":{"cmd":"create_bucket","database":"app","name":"User",
			"fields":[{"name":"Name","type":"string"},{"name":"Born","type":"datetime"},
				{"name":"Author","type":"string","of":"app.User"},{"name":"Purchases","type":"fifoStore"},
				{"name":"Photos","type":"lifoStore"},{"name":"Friends","type":"hashStore"},
				{"name":"Recipes","type":"btreeStore"},{"name":"Books","type":"heapStore"},
				{"name":"Data","type":"blobStore"}],
			"aggregates":[{"store":"Purchases","kind":"sum","field":"Price"},
				{"store":"Purchases","kind":"sum by","field":"Price","by":"Sku"},
				{"store":"Purchases","kind":"count by","by":"Sku"}],
			"expire_field":"Born","expire_paths":{"Photos":"Taken","Purchases":"At"}}}`,
		oracle: oracleRow{file: "golden", row: "admin/create-bucket-every-member"},
	},
	{
		name: "admin/alter-bucket-add-drop",
		input: `{"opcode":221,"admin":{"cmd":"alter_bucket","database":"app","bucket":"User",
			"add":[{"name":"Age","type":"int"},{"name":"Editor","type":"string","of":"User"}],
			"drop":["Born"]}}`,
		oracle: oracleRow{file: "golden", row: "admin/alter-bucket-add-drop"},
	},
	{
		name: "admin/alter-bucket-set-expire",
		input: `{"opcode":221,"admin":{"cmd":"alter_bucket","database":"app","bucket":"User",
			"add":[],"drop":[],"expire_field":"Born"}}`,
		oracle: oracleRow{file: "golden", row: "admin/alter-bucket-set-expire"},
	},
	{
		name: "admin/alter-bucket-drop-expire",
		input: `{"opcode":221,"admin":{"cmd":"alter_bucket","database":"app","bucket":"User",
			"add":[],"drop":[],"drop_expire":true}}`,
		oracle: oracleRow{file: "golden", row: "admin/alter-bucket-drop-expire"},
	},
	{
		name:   "admin/drop-bucket",
		input:  `{"opcode":221,"admin":{"cmd":"drop_bucket","database":"app","bucket":"User"}}`,
		oracle: oracleRow{file: "golden", row: "admin/drop-bucket"},
	},
	{
		name: "admin/drop-bucket-expect-id",
		input: `{"opcode":221,"admin":{"cmd":"drop_bucket","database":"app","bucket":"User",
			"expect_bucket_id":2254663609795936277}}`,
		oracle: oracleRow{file: "golden", row: "admin/drop-bucket-expect-id"},
	},
	{
		name:   "admin/describe-bucket",
		input:  `{"opcode":221,"admin":{"cmd":"describe_bucket","database":"app","bucket":"User"}}`,
		oracle: oracleRow{file: "golden", row: "admin/describe-bucket"},
	},
	{
		name:   "admin/show-buckets",
		input:  `{"opcode":221,"admin":{"cmd":"show_buckets","database":"app"}}`,
		oracle: oracleRow{file: "golden", row: "admin/show-buckets"},
	},
	{
		name: "admin/show-buckets-page",
		input: `{"opcode":221,"admin":{"cmd":"show_buckets","database":"app","limit":50,
			"cursor_hex":"010203"}}`,
		oracle: oracleRow{file: "golden", row: "admin/show-buckets-page"},
	},
	{
		name:   "admin/show-databases",
		input:  `{"opcode":221,"admin":{"cmd":"show_databases"}}`,
		oracle: oracleRow{file: "golden", row: "admin/show-databases"},
	},
	{
		name: "admin/show-databases-page",
		input: `{"opcode":221,"admin":{"cmd":"show_databases","limit":50,
			"cursor_hex":"010203"}}`,
		oracle: oracleRow{file: "golden", row: "admin/show-databases-page"},
	},
	{
		name:   "admin/describe-database",
		input:  `{"opcode":221,"admin":{"cmd":"describe_database","database":"app"}}`,
		oracle: oracleRow{file: "golden", row: "admin/describe-database"},
	},
	{
		name: "admin/describe-database-page",
		input: `{"opcode":221,"admin":{"cmd":"describe_database","database":"app","limit":50,
			"cursor_hex":"010203"}}`,
		oracle: oracleRow{file: "golden", row: "admin/describe-database-page"},
	},
	{
		name:   "admin/describe-node",
		input:  `{"opcode":221,"admin":{"cmd":"describe_node"}}`,
		oracle: oracleRow{file: "golden", row: "admin/describe-node"},
	},
	{
		name: "admin/join",
		input: `{"opcode":221,"admin":{"cmd":"join","node_id":"node-b",
			"data_addr":"node-b.example:4433"}}`,
		oracle: oracleRow{file: "golden", row: "admin/join"},
	},
	{
		name: "admin/join-witness",
		input: `{"opcode":221,"admin":{"cmd":"join","node_id":"node-w",
			"data_addr":"node-w.example:4433","witness":true}}`,
		oracle: oracleRow{file: "golden", row: "admin/join-witness"},
	},
	{
		name:   "admin/promote",
		input:  `{"opcode":221,"admin":{"cmd":"promote","node_id":"node-b"}}`,
		oracle: oracleRow{file: "golden", row: "admin/promote"},
	},
	{
		name:   "admin/leave",
		input:  `{"opcode":221,"admin":{"cmd":"leave","node_id":"node-b"}}`,
		oracle: oracleRow{file: "golden", row: "admin/leave"},
	},
	{
		name:   "admin/remove-node",
		input:  `{"opcode":221,"admin":{"cmd":"remove_node","node_id":"node-b"}}`,
		oracle: oracleRow{file: "golden", row: "admin/remove-node"},
	},
	{
		name: "admin/replace-node",
		input: `{"opcode":221,"admin":{"cmd":"replace_node","node_id":"node-b",
			"data_addr":"node-c.example:4433"}}`,
		oracle: oracleRow{file: "golden", row: "admin/replace-node"},
	},
	{
		name:   "admin/seeding-status",
		input:  `{"opcode":221,"admin":{"cmd":"seeding_status"}}`,
		oracle: oracleRow{file: "golden", row: "admin/seeding-status"},
	},
	{
		name: "admin/seeding-status-every-member",
		input: `{"opcode":221,"admin":{"cmd":"seeding_status","database":"app","slot":5,
			"limit":50,"cursor_hex":"010203"}}`,
		oracle: oracleRow{file: "golden", row: "admin/seeding-status-every-member"},
	},
	{
		name:   "admin/seeding-retry",
		input:  `{"opcode":221,"admin":{"cmd":"seeding_retry","database":"app","slot":5}}`,
		oracle: oracleRow{file: "golden", row: "admin/seeding-retry"},
	},
	{
		name:   "admin/seeding-forget",
		input:  `{"opcode":221,"admin":{"cmd":"seeding_forget","database":"app","slot":5}}`,
		oracle: oracleRow{file: "golden", row: "admin/seeding-forget"},
	},
	{
		name: "admin/seeding-forget-anyway",
		input: `{"opcode":221,"admin":{"cmd":"seeding_forget","database":"app","slot":8191,
			"anyway":true}}`,
		oracle: oracleRow{file: "golden", row: "admin/seeding-forget-anyway"},
	},
}

func TestRingvecEncodesEveryInputRow(t *testing.T) {
	golden := committedGolden(t)
	vectors := committedVectors(t)
	for _, row := range inputRows {
		var wantHex string
		switch row.oracle.file {
		case "golden":
			committed, ok := golden[row.oracle.row]
			if !ok {
				t.Fatalf("%s names golden row %q, which %s does not hold", row.name, row.oracle.row, structuralGolden)
			}
			wantHex = committed.bytes
		case "vectors":
			vector, ok := vectors[row.oracle.row]
			if !ok || vector.Request == nil {
				t.Fatalf("%s names vector row %q, which %s does not hold with a request", row.name, row.oracle.row, driverVectors)
			}
			wantHex = vector.Request.PayloadHex
		default:
			t.Fatalf("%s names oracle file %q: name golden or vectors", row.name, row.oracle.file)
		}
		var stdout bytes.Buffer
		if err := run([]string{"encode"}, strings.NewReader(row.input), &stdout); err != nil {
			t.Errorf("%s: %v", row.name, err)
			continue
		}
		if got := strings.TrimSpace(stdout.String()); got != wantHex {
			t.Errorf("%s encoded\n got %s\nwant %s (%s row %q)", row.name, got, wantHex, row.oracle.file, row.oracle.row)
		}
	}
}

// committedGolden is clients/wire's structural golden by row name: the bytes the
// server and every driver agree on, and the kind column that names what a row is.
func committedGolden(t *testing.T) map[string]goldenRow {
	t.Helper()
	text, err := os.ReadFile(structuralGolden)
	if err != nil {
		t.Fatalf("reading the golden the expected bytes come from: %v", err)
	}
	rows := map[string]goldenRow{}
	for _, line := range strings.Split(string(text), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			t.Fatalf("golden line %q is not name, kind, hex", line)
		}
		rows[fields[0]] = goldenRow{kind: fields[1], bytes: fields[2]}
	}
	return rows
}

// goldenRow is one structural golden row: its kind column and its bytes in hex.
type goldenRow struct {
	kind  string
	bytes string
}

// committedVectors is the driver vectors by row name, as much of each row as a
// ringvec test reads.
type vectorRow struct {
	Kind    string `json:"kind"`
	Request *struct {
		PayloadHex string `json:"payload_hex"`
	} `json:"request"`
	Response *struct {
		Opcode      int             `json:"opcode"`
		PayloadHex  string          `json:"payload_hex"`
		PayloadJSON json.RawMessage `json:"payload_json"`
	} `json:"response"`
	Error *struct {
		Name string `json:"name"`
	} `json:"error"`
}

func committedVectors(t *testing.T) map[string]vectorRow {
	t.Helper()
	payload, err := os.ReadFile(driverVectors)
	if err != nil {
		t.Fatalf("reading the vectors the expected bytes come from: %v", err)
	}
	var file struct {
		Vectors []struct {
			Name string `json:"name"`
			vectorRow
		} `json:"vectors"`
	}
	if err := json.Unmarshal(payload, &file); err != nil {
		t.Fatalf("%s is not the vectors file: %v", driverVectors, err)
	}
	rows := make(map[string]vectorRow, len(file.Vectors))
	for _, vector := range file.Vectors {
		rows[vector.Name] = vector.vectorRow
	}
	return rows
}

// adminCommandRows is every RING_ADMIN cmd that adminForm.ringAdmin maps, each
// to the row whose golden bytes hold its mapping. The command list is
// ringwire.AdminCommands, the wire module's own: a command added there without a
// row fails here, and a swapped pair of fields in the mapping fails that row's
// bytes, because the mapping is a hand-written table that no compiler checks.
var adminCommandRows = map[string]string{
	"create_database":   "admin/create-database",
	"create_bucket":     "admin/create-bucket-no-fields",
	"alter_bucket":      "admin/alter-bucket-add-drop",
	"drop_bucket":       "admin/drop-bucket-expect-id",
	"describe_bucket":   "admin/describe-bucket",
	"show_buckets":      "admin/show-buckets-page",
	"show_databases":    "admin/show-databases-page",
	"describe_database": "admin/describe-database-page",
	"describe_node":     "admin/describe-node",
	"join":              "admin/join",
	"promote":           "admin/promote",
	"leave":             "admin/leave",
	"remove_node":       "admin/remove-node",
	"replace_node":      "admin/replace-node",
	"seeding_status":    "admin/seeding-status-every-member",
	"seeding_retry":     "admin/seeding-retry",
	"seeding_forget":    "admin/seeding-forget-anyway",
}

func TestRingvecEncodesEveryAdminCommand(t *testing.T) {
	rowsByCmd := map[string][]string{}
	for _, row := range inputRows {
		var req request
		if err := json.Unmarshal([]byte(row.input), &req); err != nil {
			t.Fatalf("%s is not a request: %v", row.name, err)
		}
		if req.Admin != nil {
			rowsByCmd[req.Admin.Cmd] = append(rowsByCmd[req.Admin.Cmd], row.name)
		}
	}
	commands := ringwire.AdminCommands()
	for _, cmd := range commands {
		row, ok := adminCommandRows[cmd]
		if !ok {
			t.Errorf("admin command %q is one of ringwire.AdminCommands and adminCommandRows has no row for it; add one (its rows today are %v)", cmd, rowsByCmd[cmd])
			continue
		}
		if !slices.Contains(rowsByCmd[cmd], row) {
			t.Errorf("admin command %q has no row %q; its rows are %v", cmd, row, rowsByCmd[cmd])
		}
	}
	for cmd := range adminCommandRows {
		if !slices.Contains(commands, cmd) {
			t.Errorf("adminCommandRows names %q, which ringwire.AdminCommands does not list", cmd)
		}
	}
	for cmd := range rowsByCmd {
		if !slices.Contains(commands, cmd) {
			t.Errorf("a row sends admin command %q, which ringwire.AdminCommands does not list", cmd)
		}
	}
}

func TestRingvecDecodesReply(t *testing.T) {
	// Whole-document diff against the vector's payload_json: a decoder that
	// drops copies_confirmed, position, id or epoch fails here, which a check
	// of two members cannot see.
	vector := ringVector(t, "ring keyed: insert one document")
	var stdout bytes.Buffer
	if err := run([]string{"decode-reply"}, strings.NewReader(vector.Response.PayloadHex), &stdout); err != nil {
		t.Fatalf("decode-reply: %v", err)
	}
	got := decodedDocument(t, stdout.Bytes())
	want := decodedDocument(t, vector.Response.PayloadJSON)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decode-reply read\n got %s\nwant %s (payload_json of vector %q)",
			stdout.String(), vector.Response.PayloadJSON, "ring keyed: insert one document")
	}
}

// TestRingvecDecodesEveryReplyRow holds section 2b's output claim over the whole
// reply corpus: for every reply body the tool reads, the document it prints holds the
// members the payload holds, and where a row carries payload_json the printed document
// equals it member for member. A decoder or a document function that drops a member (the
// batch and get-many answers, a nested one) fails here; a test of one keyed reply cannot.
func TestRingvecDecodesEveryReplyRow(t *testing.T) {
	t.Run("the structural golden", func(t *testing.T) {
		checked := 0
		for name, row := range committedGolden(t) {
			if row.kind != "reply" {
				continue
			}
			checked++
			payload, err := hex.DecodeString(row.bytes)
			if err != nil {
				t.Fatalf("golden row %q is not hex: %v", name, err)
			}
			printed := printedReply(t, name, row.bytes)
			document := decodedDocument(t, printed)
			if missing, extra := memberTreeDiff(jsonMemberTree(document), bsonMemberTree(t, payload)); len(missing) != 0 || len(extra) != 0 {
				t.Errorf("golden row %q printed\n %s\nand its payload differs: it holds %s the printed document does not, and the printed document holds %s it does not", name, printed, memberList(missing), memberList(extra))
			}
		}
		if checked == 0 {
			t.Fatalf("%s holds no row of kind reply, so this test read nothing", structuralGolden)
		}
	})
	t.Run("the driver vectors", func(t *testing.T) {
		checked, whole := 0, 0
		for name, vector := range committedVectors(t) {
			// An exchange row's reply body, read by DecodeRingReply: the admin reply and the
			// topology reply are other bodies with their own codecs, and a refuse row's payload
			// is one the tool must refuse, not print.
			if vector.Kind != "exchange" || vector.Response == nil || vector.Response.Opcode != ringwire.OpRingReply || !replyBodyVector(name) {
				continue
			}
			checked++
			payload, err := hex.DecodeString(vector.Response.PayloadHex)
			if err != nil {
				t.Fatalf("vector %q payload is not hex: %v", name, err)
			}
			printed := printedReply(t, name, vector.Response.PayloadHex)
			document := decodedDocument(t, printed)
			if missing, extra := memberTreeDiff(jsonMemberTree(document), bsonMemberTree(t, payload)); len(missing) != 0 || len(extra) != 0 {
				t.Errorf("vector %q printed\n %s\nand its payload differs: it holds %s the printed document does not, and the printed document holds %s it does not", name, printed, memberList(missing), memberList(extra))
				continue
			}
			if len(vector.Response.PayloadJSON) == 0 {
				continue
			}
			whole++
			if want := decodedDocument(t, vector.Response.PayloadJSON); !reflect.DeepEqual(document, want) {
				t.Errorf("vector %q printed\n %s\nwant its payload_json %s", name, printed, vector.Response.PayloadJSON)
			}
		}
		if checked == 0 || whole == 0 {
			t.Fatalf("%s holds no reply body this test read (%d rows, %d with payload_json)", driverVectors, checked, whole)
		}
	})
}

// replyBodyVector reports whether a vector's name is one whose response body
// DecodeRingReply reads: everything but an admin reply (whose document starts with cmd)
// and a topology reply (a table, not this document).
func replyBodyVector(name string) bool {
	return !strings.HasPrefix(name, "ring admin:") && !strings.HasPrefix(name, "ring topology:")
}

// printedReply runs decode-reply on one payload and returns what it printed.
func printedReply(t *testing.T, name, payloadHex string) []byte {
	t.Helper()
	var stdout bytes.Buffer
	if err := run([]string{"decode-reply"}, strings.NewReader(payloadHex), &stdout); err != nil {
		t.Fatalf("decode-reply of %q: %v", name, err)
	}
	return bytes.TrimSpace(stdout.Bytes())
}

// jsonMemberTree is one JSON document as the member names it holds, nested: documents and
// arrays recurse, a canonical Extended JSON wrapper ($binary, $numberLong) is a leaf, as
// the value it stands for is in BSON.
func jsonMemberTree(value any) any {
	switch held := value.(type) {
	case map[string]any:
		if len(held) == 1 {
			for name := range held {
				if strings.HasPrefix(name, "$") {
					return "value"
				}
			}
		}
		tree := make(map[string]any, len(held))
		for name, member := range held {
			tree[name] = jsonMemberTree(member)
		}
		return tree
	case []any:
		tree := make(map[string]any, len(held))
		for index, element := range held {
			tree[strconv.Itoa(index)] = jsonMemberTree(element)
		}
		return tree
	}
	return "value"
}

// memberList is the first paths of a difference and a count of the rest: a batch of 255
// answers names its first members, not all 255.
func memberList(paths []string) string {
	const shown = 8
	if len(paths) <= shown {
		return fmt.Sprint(paths)
	}
	return fmt.Sprintf("%v and %d more", paths[:shown], len(paths)-shown)
}

// memberTreeDiff names the member paths one side holds and the other does not: what the
// payload holds and the printed document does not (missing), and the reverse (extra). A
// batch of 255 answers then fails with the member that differs, not with the whole document.
func memberTreeDiff(got, want any) (missing, extra []string) {
	gotPaths := memberTreePaths(got, "")
	wantPaths := memberTreePaths(want, "")
	gotSet := make(map[string]bool, len(gotPaths))
	wantSet := make(map[string]bool, len(wantPaths))
	for _, path := range gotPaths {
		gotSet[path] = true
	}
	for _, path := range wantPaths {
		wantSet[path] = true
	}
	for _, path := range wantPaths {
		if !gotSet[path] {
			missing = append(missing, path)
		}
	}
	for _, path := range gotPaths {
		if !wantSet[path] {
			extra = append(extra, path)
		}
	}
	return missing, extra
}

// memberTreePaths is a document tree as one sorted path per member, "answers.0.reply" for a
// member of an answer in an array.
func memberTreePaths(tree any, prefix string) []string {
	members, ok := tree.(map[string]any)
	if !ok {
		return []string{prefix}
	}
	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	slices.Sort(names)
	paths := make([]string, 0, len(names))
	for _, name := range names {
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		paths = append(paths, memberTreePaths(members[name], path)...)
	}
	return paths
}

// bsonMemberTree is one committed payload as the member names it holds, nested the same way
// jsonMemberTree nests the printed document: an embedded document and an array recurse, every
// other value is a leaf. It reads the payload itself, not the codec's document, so a document
// function that drops a member cannot hide here.
func bsonMemberTree(t *testing.T, raw []byte) any {
	t.Helper()
	elements, err := bson.Raw(raw).Elements()
	if err != nil {
		t.Fatalf("reading the members of a committed payload: %v", err)
	}
	tree := make(map[string]any, len(elements))
	for _, element := range elements {
		value := element.Value()
		if document, ok := value.DocumentOK(); ok {
			tree[element.Key()] = bsonMemberTree(t, document)
			continue
		}
		if array, ok := value.ArrayOK(); ok {
			tree[element.Key()] = bsonMemberTree(t, array)
			continue
		}
		tree[element.Key()] = "value"
	}
	return tree
}

func TestRingvecDecodesError(t *testing.T) {
	// The error document, member for member, against the vector's payload_json
	// plus the registered name the tool adds (the payload carries the number).
	vector := ringVector(t, "ring keyed: the client's epoch is behind")
	var stdout bytes.Buffer
	if err := run([]string{"decode-error"}, strings.NewReader(vector.Response.PayloadHex), &stdout); err != nil {
		t.Fatalf("decode-error: %v", err)
	}
	got := decodedDocument(t, stdout.Bytes())
	want := decodedDocument(t, vector.Response.PayloadJSON)
	refusal, ok := want.(map[string]any)
	if !ok {
		t.Fatalf("payload_json of %q is not a document: %s", "ring keyed: the client's epoch is behind", vector.Response.PayloadJSON)
	}
	refusal["name"] = vector.Error.Name
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decode-error read\n got %s\nwant payload_json plus name %q",
			stdout.String(), vector.Error.Name)
	}
}

// ringVector is one committed vector that carries a response with both its
// payload bytes and the document it means.
func ringVector(t *testing.T, name string) vectorRow {
	t.Helper()
	vector, ok := committedVectors(t)[name]
	if !ok || vector.Response == nil || len(vector.Response.PayloadJSON) == 0 {
		t.Fatalf("%s holds no vector %q with a response payload and payload_json", driverVectors, name)
	}
	if name == "ring keyed: the client's epoch is behind" && vector.Error == nil {
		t.Fatalf("%s vector %q has no error name", driverVectors, name)
	}
	return vector
}

// decodedDocument is one JSON document as values: Extended JSON number wrappers
// ($numberInt, $numberLong) become the number they hold, so a committed
// payload_json and the tool's plain JSON compare member for member. Binary and
// other wrappers stay as they are on both sides.
func decodedDocument(t *testing.T, text []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(text))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("%q is not one JSON document: %v", text, err)
	}
	return unwrapNumbers(document)
}

func unwrapNumbers(value any) any {
	switch held := value.(type) {
	case map[string]any:
		if len(held) == 1 {
			for _, word := range []string{"$numberInt", "$numberLong"} {
				if number, ok := held[word].(string); ok {
					return json.Number(number)
				}
			}
		}
		for member, memberValue := range held {
			held[member] = unwrapNumbers(memberValue)
		}
		return held
	case []any:
		for i, element := range held {
			held[i] = unwrapNumbers(element)
		}
		return held
	}
	return value
}

func TestRingvecRefusesAMemberNoFormTakes(t *testing.T) {
	// A mistyped member must be named, not dropped: the oracle exists so a driver's
	// encoder is held to these bytes, and a silently ignored member would make a
	// wrong driver look right.
	rows := []struct {
		name   string
		input  string
		member string
	}{
		{
			name:   "list",
			input:  `{"opcode":218,"list":{"database":"app","bucket":"User","limt":10}}`,
			member: "limt",
		},
		{
			name:   "count",
			input:  `{"opcode":219,"count":{"database":"app","bucket":"User","filtre":{}}}`,
			member: "filtre",
		},
		{
			name:   "admin",
			input:  `{"opcode":221,"admin":{"cmd":"create_database","name":"app","rff":3}}`,
			member: "rff",
		},
		{
			name:   "read",
			input:  `{"opcode":220,"read":{"class":129,"id_hex":"5075310000","limt":10}}`,
			member: "limt",
		},
		{
			name:   "write",
			input:  `{"opcode":220,"write":{"class":1,"id_hex":"5075310000","retrun":1}}`,
			member: "retrun",
		},
		{
			name:   "a read's nearest",
			input:  `{"opcode":220,"read":{"class":138,"nearest":{"metric":1,"vectr_hex":"00"}}}`,
			member: "vectr_hex",
		},
		{
			name:   "a read's expand",
			input:  `{"opcode":218,"list":{"database":"app","expand":[{"field":"Author","buckett":"User"}]}}`,
			member: "buckett",
		},
		{
			name:   "a batch operation",
			input:  `{"opcode":224,"ops":[{"class":1,"id_hex":"50","retrun":1}]}`,
			member: "retrun",
		},
		{
			name:   "a get-many read",
			input:  `{"opcode":225,"reads":[{"class":129,"limt":10}]}`,
			member: "limt",
		},
		{
			name:   "the top level",
			input:  `{"opcode":218,"list":{"database":"app"},"opcod":218}`,
			member: "opcod",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := run([]string{"encode"}, strings.NewReader(row.input), &stdout)
			if err == nil {
				t.Fatalf("encode of %s succeeded and wrote %s, want a refusal naming %q", row.input, stdout.String(), row.member)
			}
			if !strings.Contains(err.Error(), "\""+row.member+"\"") {
				t.Errorf("refusal %q does not name the member %q", err, row.member)
			}
		})
	}
}

func TestRingvecRefusesAPrologueAnOpcodeDoesNotCarry(t *testing.T) {
	// RING_LIST, RING_COUNT and RING_ADMIN carry no prologue. Encoding one and
	// dropping it makes the same bytes as the request without it.
	rows := []struct {
		name  string
		input string
	}{
		{
			name:  "RING_LIST",
			input: `{"opcode":218,"prologue":{"epoch":9,"state_seq":1,"deadline_ms":5},"list":{"database":"app","bucket":"User"}}`,
		},
		{
			name:  "RING_COUNT",
			input: `{"opcode":219,"prologue":{"epoch":9,"state_seq":1,"deadline_ms":5},"count":{"database":"app","bucket":"User"}}`,
		},
		{
			name:  "RING_ADMIN",
			input: `{"opcode":221,"prologue":{"epoch":9,"state_seq":1,"deadline_ms":5},"admin":{"cmd":"show_databases"}}`,
		},
		{
			name:  "a list under a keyed request",
			input: `{"opcode":220,"read":{"class":129,"id_hex":"50"},"list":{"database":"app"}}`,
		},
		{
			name:  "a read under a list request",
			input: `{"opcode":218,"list":{"database":"app"},"read":{"class":129}}`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := run([]string{"encode"}, strings.NewReader(row.input), &stdout)
			if err == nil {
				t.Fatalf("encode of %s succeeded and wrote %s, want a refusal", row.input, stdout.String())
			}
		})
	}
}

func TestRingvecRefusalNamesTheFix(t *testing.T) {
	var stdout bytes.Buffer
	err := run([]string{"encode"}, strings.NewReader(`{"opcode":999}`), &stdout)
	if err == nil {
		t.Fatalf("encode of opcode 999 succeeded: %s", stdout.String())
	}
	if !strings.Contains(err.Error(), "RING_KEYED") {
		t.Errorf("refusal %q does not name an opcode it takes", err)
	}
}

func TestRingvecRefusesAReplyOpcodeAsARequest(t *testing.T) {
	// Section 2b says what 222 and 223 are when they arrive as a request, so the
	// refusal names the opcode it was given.
	rows := []struct {
		opcode int
		name   string
	}{
		{opcode: ringwire.OpRingReply, name: "RING_REPLY"},
		{opcode: ringwire.OpRingTopology, name: "RING_TOPOLOGY"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			input := "{\"opcode\":" + strconv.Itoa(row.opcode) + "}"
			var stdout bytes.Buffer
			err := run([]string{"encode"}, strings.NewReader(input), &stdout)
			if err == nil {
				t.Fatalf("encode of %s succeeded and wrote %s, want a refusal naming it", input, stdout.String())
			}
			if !strings.Contains(err.Error(), row.name) {
				t.Errorf("refusal %q does not name %s", err, row.name)
			}
		})
	}
}

func TestRingvecPrintsEveryDetailMemberThePayloadCarries(t *testing.T) {
	// A detail member the payload carries is printed even when it is that member's zero
	// value: the payload says which slot is not owned, or that no voter answered, and a
	// driver reading detail needs that member. The codec writes every member of a detail's
	// shape, so these payloads carry the zeros.
	rows := []struct {
		name    string
		refusal ringwire.RingError
		want    string
	}{
		{
			name: "NOT_OWNER with slot 0",
			refusal: ringwire.RingError{Code: ringwire.CodeNotOwner, Message: "the slot belongs to another node",
				Detail: &ringwire.Detail{Owner: "node-a", OwnerAddr: "node-a.example:4433", Epoch: 4, Slot: 0}},
			want: fmt.Sprintf(`{"code":%d,"name":"NOT_OWNER","message":"the slot belongs to another node","detail":{"owner":"node-a","owner_addr":"node-a.example:4433","epoch":4,"slot":0}}`, ringwire.CodeNotOwner),
		},
		{
			name: "NO_QUORUM with answered 0",
			refusal: ringwire.RingError{Code: ringwire.CodeNoQuorum, Message: "no voter answered",
				Detail: &ringwire.Detail{Voters: 3, Answered: 0}},
			want: fmt.Sprintf(`{"code":%d,"name":"NO_QUORUM","message":"no voter answered","detail":{"voters":3,"answered":0}}`, ringwire.CodeNoQuorum),
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			payload, err := ringwire.EncodeRingError(row.refusal)
			if err != nil {
				t.Fatalf("encoding the payload: %v", err)
			}
			var stdout bytes.Buffer
			if err := run([]string{"decode-error"}, strings.NewReader(hex.EncodeToString(payload)), &stdout); err != nil {
				t.Fatalf("decode-error: %v", err)
			}
			got := decodedDocument(t, stdout.Bytes())
			want := decodedDocument(t, []byte(row.want))
			if !reflect.DeepEqual(got, want) {
				t.Errorf("decode-error read\n %s\nwant %s", stdout.Bytes(), row.want)
			}
		})
	}
}

func TestRingvecNamesAnAdminOrTopologyReplyBody(t *testing.T) {
	// decode-reply reads the reply to one of the five requests. Handed another body, it says
	// which body it is and where that body is decoded, so a driver author is not left with a
	// canonical-layout complaint about a body of the wrong kind.
	rows := []struct {
		vector string
		want   []string
	}{
		{
			vector: "ring admin: create a database",
			want:   []string{"RING_ADMIN reply body", "create_database", "DecodeRingAdminReply"},
		},
		{
			vector: "ring topology: the view held is current",
			want:   []string{"RING_TOPOLOGY reply body", "no topology reply decoder"},
		},
	}
	for _, row := range rows {
		t.Run(row.vector, func(t *testing.T) {
			vector := ringVector(t, row.vector)
			var stdout bytes.Buffer
			err := run([]string{"decode-reply"}, strings.NewReader(vector.Response.PayloadHex), &stdout)
			if err == nil {
				t.Fatalf("decode-reply of %q succeeded and wrote %s, want a refusal naming the body", row.vector, stdout.Bytes())
			}
			for _, want := range row.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not name %q", err, want)
				}
			}
		})
	}
}
