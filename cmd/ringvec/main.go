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

// Command ringvec is the Go codec as a test oracle for the Node.js and Python
// drivers: the same request data in, the payload bytes out, so a driver's
// encoder is checked against the codec that writes the wire, not against a
// second hand-written expectation.
//
//	ringvec encode        one request as JSON on stdin, the payload hex on stdout
//	ringvec decode-reply  one RING_REPLY payload as hex on stdin, Extended JSON on stdout
//	ringvec decode-error  one error payload as hex on stdin, JSON on stdout
//
// The request JSON is the request as data, one object: opcode, the prologue of
// a frame that carries one, then the members of that opcode's wire struct under
// the member named after its form: list, count, read or write, admin, reads,
// ops. Byte strings (docs payloads, keys, ids, field values) are hex, since
// the codec takes them already encoded; a BSON document (a filter, a where)
// is Extended JSON. Nothing is defaulted and nothing is ignored: a member that
// no form takes is refused by name, and a member the opcode's own form does not
// take (a prologue on RING_LIST, a list on RING_KEYED) is refused too. Every
// member of every form, and what each refuses, is the contract in
// docs/specs/ring-py-node-drivers-design.md section 2b: that section is what
// the X6 inputs hold, so ringvec and the drivers encode the same data.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	ringwire "github.com/hoarddb/hoarddb-wire"
)

// request is one X6 input: the opcode, the prologue it may carry, and the
// members of the one form that opcode names.
type request struct {
	Opcode   int          `json:"opcode"`
	Prologue prologueForm `json:"prologue"`
	Read     *readForm    `json:"read"`
	Write    *writeForm   `json:"write"`
	Reads    []readForm   `json:"reads"`
	Ops      []writeForm  `json:"ops"`
	List     *listForm    `json:"list"`
	Count    *countForm   `json:"count"`
	Admin    *adminForm   `json:"admin"`
}

type prologueForm struct {
	Epoch      uint64 `json:"epoch"`
	StateSeq   uint64 `json:"state_seq"`
	DeadlineMs uint32 `json:"deadline_ms"`
}

// readForm is the data of one ringwire.RingRead, its byte members as hex.
type readForm struct {
	Class       byte           `json:"class"`
	BucketID    uint64         `json:"bucket_id"`
	DeclVersion int64          `json:"decl_version"`
	SchemaSeen  int64          `json:"schema_seen"`
	IDHex       string         `json:"id_hex"`
	FieldID     uint16         `json:"field_id"`
	Limit       uint16         `json:"limit"`
	AfterHex    string         `json:"after_hex"`
	LeaseHex    string         `json:"lease_hex"`
	Key         string         `json:"key"`
	Fields      []string       `json:"fields"`
	Nearest     *nearestForm   `json:"nearest"`
	Aggregate   *aggregateForm `json:"aggregate"`
	Options     *optionsForm   `json:"options"`
}

type nearestForm struct {
	Metric    byte   `json:"metric"`
	K         uint8  `json:"k"`
	Field     string `json:"field"`
	VectorHex string `json:"vector_hex"`
}

type aggregateForm struct {
	Kind  uint8  `json:"kind"`
	Field string `json:"field"`
	By    string `json:"by"`
}

type optionsForm struct {
	Local  bool         `json:"local"`
	Expand []expandForm `json:"expand"`
}

type expandForm struct {
	Field    string          `json:"field"`
	Database string          `json:"database"`
	Bucket   string          `json:"bucket"`
	Fields   []string        `json:"fields"`
	Where    json.RawMessage `json:"where"`
}

// writeForm is the data of one ringwire.Op, its byte members as hex.
type writeForm struct {
	Class       byte        `json:"class"`
	BucketID    uint64      `json:"bucket_id"`
	DeclVersion int64       `json:"decl_version"`
	SchemaSeen  int64       `json:"schema_seen"`
	IDHex       string      `json:"id_hex"`
	IDValueHex  string      `json:"id_value_hex"`
	Fields      []fieldForm `json:"fields"`
	Count       uint16      `json:"count"`
	Now         int64       `json:"now"`
	Until       int64       `json:"until"`
	NonceHex    string      `json:"nonce_hex"`
	LeaseHex    string      `json:"lease_hex"`
	Guard       []guardForm `json:"guard"`
	Return      byte        `json:"return"`
}

type fieldForm struct {
	ID       uint16 `json:"id"`
	Op       byte   `json:"op"`
	Verb     byte   `json:"verb"`
	ValueHex string `json:"value_hex"`
}

type guardForm struct {
	ID       uint16 `json:"id"`
	ValueHex string `json:"value_hex"`
}

// listForm is the data of one ringwire.RingListRequest.
type listForm struct {
	Database  string          `json:"database"`
	Bucket    string          `json:"bucket"`
	Filter    json.RawMessage `json:"filter"`
	Fields    []string        `json:"fields"`
	Limit     *int32          `json:"limit"`
	CursorHex string          `json:"cursor_hex"`
	Epoch     int64           `json:"epoch"`
	Local     bool            `json:"local"`
	Expand    []expandForm    `json:"expand"`
}

// countForm is the data of one ringwire.RingCountRequest.
type countForm struct {
	Database string          `json:"database"`
	Bucket   string          `json:"bucket"`
	Filter   json.RawMessage `json:"filter"`
}

// adminForm is the data of one ringwire.RingAdminRequest.
type adminForm struct {
	Cmd            string              `json:"cmd"`
	Database       string              `json:"database"`
	Name           string              `json:"name"`
	Bucket         string              `json:"bucket"`
	RF             *int32              `json:"rf"`
	Storage        string              `json:"storage"`
	Fields         []adminFieldForm    `json:"fields"`
	Aggregates     []aggregateDeclForm `json:"aggregates"`
	Add            []adminFieldForm    `json:"add"`
	Drop           []string            `json:"drop"`
	ExpireField    string              `json:"expire_field"`
	ExpirePaths    map[string]string   `json:"expire_paths"`
	DropExpire     bool                `json:"drop_expire"`
	Slot           *int32              `json:"slot"`
	Anyway         bool                `json:"anyway"`
	ExpectBucketID uint64              `json:"expect_bucket_id"`
	Limit          *int32              `json:"limit"`
	CursorHex      string              `json:"cursor_hex"`
	NodeID         string              `json:"node_id"`
	DataAddr       string              `json:"data_addr"`
	Witness        bool                `json:"witness"`
}

type adminFieldForm struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Of   string `json:"of"`
}

type aggregateDeclForm struct {
	Store string `json:"store"`
	Kind  string `json:"kind"`
	Field string `json:"field"`
	By    string `json:"by"`
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "ringvec:", err)
		os.Exit(1)
	}
}

// run is main without the process: the subcommand, its input and its output.
func run(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("ringvec reads one subcommand: encode, decode-reply or decode-error. For example: echo '{\"opcode\":218,\"list\":{\"database\":\"app\",\"bucket\":\"User\"}}' | ringvec encode")
	}
	input, err := io.ReadAll(stdin)
	if err != nil {
		return fmt.Errorf("reading stdin: %w", err)
	}
	switch args[0] {
	case "encode":
		payload, err := encode(input)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, hex.EncodeToString(payload))
		return err
	case "decode-reply", "decode-error":
		payload, err := hex.DecodeString(strings.TrimSpace(string(input)))
		if err != nil {
			return fmt.Errorf("%s reads one payload as hex on stdin: %w", args[0], err)
		}
		var out []byte
		if args[0] == "decode-reply" {
			out, err = decodeReply(payload)
		} else {
			out, err = decodeError(payload)
		}
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", out)
		return err
	}
	return fmt.Errorf("%q is not a ringvec subcommand: send encode, decode-reply or decode-error", args[0])
}

// formMembers is the top-level members each opcode takes: opcode, the prologue
// only where the frame carries one, and the form that opcode names. A member
// outside this list is refused by name rather than dropped.
var formMembers = map[int][]string{
	ringwire.OpRingKeyed:   {"opcode", "prologue", "read", "write"},
	ringwire.OpRingList:    {"opcode", "list"},
	ringwire.OpRingCount:   {"opcode", "count"},
	ringwire.OpRingAdmin:   {"opcode", "admin"},
	ringwire.OpRingBatch:   {"opcode", "prologue", "ops"},
	ringwire.OpRingGetMany: {"opcode", "prologue", "reads"},
}

// opcodeNames names the opcodes ringvec talks about in a refusal.
var opcodeNames = map[int]string{
	ringwire.OpRingKeyed:    "RING_KEYED",
	ringwire.OpRingList:     "RING_LIST",
	ringwire.OpRingCount:    "RING_COUNT",
	ringwire.OpRingAdmin:    "RING_ADMIN",
	ringwire.OpRingBatch:    "RING_BATCH",
	ringwire.OpRingGetMany:  "RING_GET_MANY",
	ringwire.OpRingReply:    "RING_REPLY",
	ringwire.OpRingTopology: "RING_TOPOLOGY",
}

// encode returns the payload bytes of one X6 input.
func encode(input []byte) ([]byte, error) {
	req, err := readRequest(input)
	if err != nil {
		return nil, err
	}
	switch req.Opcode {
	case ringwire.OpRingKeyed:
		return encodeKeyed(req)
	case ringwire.OpRingList:
		if req.List == nil {
			return nil, fmt.Errorf("opcode %d is RING_LIST and takes the request under \"list\"", req.Opcode)
		}
		list, err := req.List.ringList()
		if err != nil {
			return nil, err
		}
		return ringwire.EncodeRingList(list)
	case ringwire.OpRingCount:
		if req.Count == nil {
			return nil, fmt.Errorf("opcode %d is RING_COUNT and takes the request under \"count\"", req.Opcode)
		}
		count, err := req.Count.ringCount()
		if err != nil {
			return nil, err
		}
		return ringwire.EncodeRingCount(count)
	case ringwire.OpRingAdmin:
		if req.Admin == nil {
			return nil, fmt.Errorf("opcode %d is RING_ADMIN and takes the request under \"admin\"", req.Opcode)
		}
		admin, err := req.Admin.ringAdmin()
		if err != nil {
			return nil, err
		}
		return ringwire.EncodeRingAdmin(admin)
	case ringwire.OpRingGetMany:
		reads := make([]ringwire.RingRead, len(req.Reads))
		for i, form := range req.Reads {
			read, err := form.ringRead()
			if err != nil {
				return nil, fmt.Errorf("read %d of %d: %w", i, len(req.Reads), err)
			}
			reads[i] = read
		}
		return ringwire.EncodeRingGetMany(ringwire.RingGetManyRequest{Prologue: req.Prologue.ringPrologue(), Reads: reads})
	case ringwire.OpRingBatch:
		ops := make([]ringwire.Op, len(req.Ops))
		for i, form := range req.Ops {
			op, err := form.ringOp()
			if err != nil {
				return nil, fmt.Errorf("operation %d of %d: %w", i, len(req.Ops), err)
			}
			ops[i] = op
		}
		return ringwire.EncodeRingBatch(ringwire.RingBatchRequest{Prologue: req.Prologue.ringPrologue(), Ops: ops})
	case ringwire.OpRingReply:
		return nil, fmt.Errorf("opcode %d is RING_REPLY, the server's answer to a ring request and never a request itself: send %d (RING_LIST), %d (RING_COUNT), %d (RING_KEYED), %d (RING_ADMIN), %d (RING_BATCH) or %d (RING_GET_MANY), and read a reply body with ringvec decode-reply",
			req.Opcode, ringwire.OpRingList, ringwire.OpRingCount, ringwire.OpRingKeyed, ringwire.OpRingAdmin, ringwire.OpRingBatch, ringwire.OpRingGetMany)
	case ringwire.OpRingTopology:
		return nil, fmt.Errorf("opcode %d is RING_TOPOLOGY and hoarddb-wire has no request codec for it, so neither ringvec nor X6 can form it: send %d (RING_LIST), %d (RING_COUNT), %d (RING_KEYED), %d (RING_ADMIN), %d (RING_BATCH) or %d (RING_GET_MANY)",
			req.Opcode, ringwire.OpRingList, ringwire.OpRingCount, ringwire.OpRingKeyed, ringwire.OpRingAdmin, ringwire.OpRingBatch, ringwire.OpRingGetMany)
	}
	return nil, fmt.Errorf("opcode %d is not a ring request ringvec encodes: send %d (RING_LIST), %d (RING_COUNT), %d (RING_KEYED), %d (RING_ADMIN), %d (RING_BATCH) or %d (RING_GET_MANY)",
		req.Opcode, ringwire.OpRingList, ringwire.OpRingCount, ringwire.OpRingKeyed, ringwire.OpRingAdmin, ringwire.OpRingBatch, ringwire.OpRingGetMany)
}

// readRequest decodes one input object strictly and holds it to the members its
// opcode takes, so a misspelled or misplaced member is refused by name instead
// of encoded away.
func readRequest(input []byte) (request, error) {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(input, &members); err != nil {
		return request{}, fmt.Errorf("the input is not one request object: %w", err)
	}
	var req request
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return request{}, fmt.Errorf("the input holds a member no form takes, or a member of the wrong type: %w. The members of every form are the contract in docs/specs/ring-py-node-drivers-design.md section 2b", err)
	}
	if err := decoder.Decode(&json.RawMessage{}); !errors.Is(err, io.EOF) {
		return request{}, errors.New("encode reads one request object and nothing after it, and this input holds more than one")
	}
	allowed, ok := formMembers[req.Opcode]
	if !ok {
		// The opcode refusal below names the requests ringvec takes.
		return req, nil
	}
	var extra []string
	for name := range members {
		if !slices.Contains(allowed, name) {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	for _, name := range extra {
		if name == "prologue" {
			return request{}, fmt.Errorf("opcode %d (%s) carries no prologue and this input sends one: drop \"prologue\". A prologue belongs to RING_KEYED, RING_BATCH and RING_GET_MANY",
				req.Opcode, opcodeNames[req.Opcode])
		}
		return request{}, fmt.Errorf("opcode %d (%s) takes %s and no other form member; this input also sends %q. Drop %q, or send the opcode that form belongs to",
			req.Opcode, opcodeNames[req.Opcode], memberPhrase(allowed), name, name)
	}
	return req, nil
}

// memberPhrase names the form members an opcode takes, the way a sentence
// reads: "\"prologue\", \"read\" or \"write\"".
func memberPhrase(allowed []string) string {
	forms := slices.DeleteFunc(slices.Clone(allowed), func(name string) bool { return name == "opcode" })
	quoted := make([]string, len(forms))
	for i, name := range forms {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	switch len(quoted) {
	case 0:
		return "no form member"
	case 1:
		return quoted[0]
	}
	return strings.Join(quoted[:len(quoted)-1], ", ") + " or " + quoted[len(quoted)-1]
}

// encodeKeyed returns the RING_KEYED payload of a read or a write, exactly one.
func encodeKeyed(req request) ([]byte, error) {
	switch {
	case req.Read == nil && req.Write == nil:
		return nil, fmt.Errorf("opcode %d is RING_KEYED and takes a read under \"read\" or a write under \"write\"", req.Opcode)
	case req.Read != nil && req.Write != nil:
		return nil, fmt.Errorf("a RING_KEYED is a read or a write, not both: drop \"write\"")
	case req.Read != nil:
		read, err := req.Read.ringRead()
		if err != nil {
			return nil, err
		}
		return ringwire.EncodeRingKeyed(ringwire.RingKeyedRequest{Prologue: req.Prologue.ringPrologue(), Read: &read})
	}
	op, err := req.Write.ringOp()
	if err != nil {
		return nil, err
	}
	return ringwire.EncodeRingKeyed(ringwire.RingKeyedRequest{Prologue: req.Prologue.ringPrologue(), Op: op})
}

func (p prologueForm) ringPrologue() ringwire.RingPrologue {
	return ringwire.RingPrologue{Epoch: p.Epoch, StateSeq: p.StateSeq, DeadlineMs: p.DeadlineMs}
}

func (f readForm) ringRead() (ringwire.RingRead, error) {
	id, err := hexOf("id_hex", f.IDHex)
	if err != nil {
		return ringwire.RingRead{}, err
	}
	after, err := hexOf("after_hex", f.AfterHex)
	if err != nil {
		return ringwire.RingRead{}, err
	}
	read := ringwire.RingRead{
		Class: f.Class, BucketID: f.BucketID, DeclVersion: f.DeclVersion, SchemaVersionSeen: f.SchemaSeen,
		EncID: id, FieldID: f.FieldID, Limit: f.Limit, After: after, Key: f.Key, Fields: f.Fields,
	}
	if f.LeaseHex != "" {
		lease, err := leaseOf(f.LeaseHex)
		if err != nil {
			return ringwire.RingRead{}, err
		}
		read.Lease = lease
	}
	if f.Nearest != nil {
		vector, err := hexOf("nearest.vector_hex", f.Nearest.VectorHex)
		if err != nil {
			return ringwire.RingRead{}, err
		}
		read.Nearest = &ringwire.RingNearest{Metric: f.Nearest.Metric, K: f.Nearest.K, Field: f.Nearest.Field, Vector: vector}
	}
	if f.Aggregate != nil {
		read.Aggregate = &ringwire.RingAggregateRead{Kind: f.Aggregate.Kind, Field: f.Aggregate.Field, By: f.Aggregate.By}
	}
	if f.Options != nil {
		options, err := f.Options.ringOptions()
		if err != nil {
			return ringwire.RingRead{}, err
		}
		read.Options = options
	}
	return read, nil
}

func (f writeForm) ringOp() (ringwire.Op, error) {
	id, err := hexOf("id_hex", f.IDHex)
	if err != nil {
		return ringwire.Op{}, err
	}
	idValue, err := hexOf("id_value_hex", f.IDValueHex)
	if err != nil {
		return ringwire.Op{}, err
	}
	fields := make([]ringwire.FieldOp, len(f.Fields))
	for i, field := range f.Fields {
		value, err := hexOf(fmt.Sprintf("fields[%d].value_hex", i), field.ValueHex)
		if err != nil {
			return ringwire.Op{}, err
		}
		fields[i] = ringwire.FieldOp{FieldID: field.ID, OpCode: field.Op, Verb: field.Verb, Value: value}
	}
	guard := make([]ringwire.GuardTerm, len(f.Guard))
	for i, term := range f.Guard {
		value, err := hexOf(fmt.Sprintf("guard[%d].value_hex", i), term.ValueHex)
		if err != nil {
			return ringwire.Op{}, err
		}
		guard[i] = ringwire.GuardTerm{FieldID: term.ID, Value: value}
	}
	op := ringwire.Op{
		Class: f.Class, BucketID: f.BucketID, DeclVersion: f.DeclVersion, SchemaVersionSeen: f.SchemaSeen,
		EncID: id, IDValue: idValue, Fields: fields, Count: f.Count, Now: f.Now, Until: f.Until,
		Guard: guard, Return: f.Return,
	}
	if f.NonceHex != "" {
		nonce, err := leaseOf(f.NonceHex)
		if err != nil {
			return ringwire.Op{}, fmt.Errorf("nonce_hex: %w", err)
		}
		op.Nonce = nonce
	}
	if f.LeaseHex != "" {
		lease, err := leaseOf(f.LeaseHex)
		if err != nil {
			return ringwire.Op{}, err
		}
		op.LeaseID = lease
	}
	return op, nil
}

func (f listForm) ringList() (ringwire.RingListRequest, error) {
	filter, err := documentOf("list.filter", f.Filter)
	if err != nil {
		return ringwire.RingListRequest{}, err
	}
	cursor, err := hexOf("list.cursor_hex", f.CursorHex)
	if err != nil {
		return ringwire.RingListRequest{}, err
	}
	options, err := optionsForm{Local: f.Local, Expand: f.Expand}.ringOptions()
	if err != nil {
		return ringwire.RingListRequest{}, err
	}
	return ringwire.RingListRequest{
		Database: f.Database, Bucket: f.Bucket, Filter: filter, Fields: f.Fields,
		Limit: f.Limit, Cursor: cursor, Epoch: f.Epoch, Options: options,
	}, nil
}

func (f countForm) ringCount() (ringwire.RingCountRequest, error) {
	filter, err := documentOf("count.filter", f.Filter)
	if err != nil {
		return ringwire.RingCountRequest{}, err
	}
	return ringwire.RingCountRequest{Database: f.Database, Bucket: f.Bucket, Filter: filter}, nil
}

func (f optionsForm) ringOptions() (ringwire.RingReadOptions, error) {
	options := ringwire.RingReadOptions{Local: f.Local}
	for i, form := range f.Expand {
		spec, err := form.ringExpand()
		if err != nil {
			return ringwire.RingReadOptions{}, fmt.Errorf("expand[%d]: %w", i, err)
		}
		options.Expand = append(options.Expand, spec)
	}
	return options, nil
}

func (f expandForm) ringExpand() (ringwire.RingExpandSpec, error) {
	where, err := documentOf("where", f.Where)
	if err != nil {
		return ringwire.RingExpandSpec{}, err
	}
	return ringwire.RingExpandSpec{Field: f.Field, Database: f.Database, Bucket: f.Bucket, Fields: f.Fields, Where: where}, nil
}

func (f adminForm) ringAdmin() (ringwire.RingAdminRequest, error) {
	cursor, err := hexOf("admin.cursor_hex", f.CursorHex)
	if err != nil {
		return ringwire.RingAdminRequest{}, err
	}
	admin := ringwire.RingAdminRequest{
		Cmd: f.Cmd, Database: f.Database, Name: f.Name, Bucket: f.Bucket, RF: f.RF, Storage: f.Storage,
		Fields: adminFieldsOf(f.Fields), Aggregates: aggregateDeclsOf(f.Aggregates), Add: adminFieldsOf(f.Add), Drop: f.Drop,
		ExpireField: f.ExpireField, ExpirePaths: f.ExpirePaths, DropExpire: f.DropExpire, Slot: f.Slot, Anyway: f.Anyway,
		ExpectBucketID: f.ExpectBucketID, Limit: f.Limit, Cursor: cursor,
		NodeID: f.NodeID, DataAddr: f.DataAddr, Witness: f.Witness,
	}
	return admin, nil
}

func adminFieldsOf(forms []adminFieldForm) []ringwire.RingAdminField {
	if forms == nil {
		return nil
	}
	fields := make([]ringwire.RingAdminField, len(forms))
	for i, form := range forms {
		fields[i] = ringwire.RingAdminField{Name: form.Name, Type: form.Type, Of: form.Of}
	}
	return fields
}

func aggregateDeclsOf(forms []aggregateDeclForm) []ringwire.AggregateDecl {
	if forms == nil {
		return nil
	}
	decls := make([]ringwire.AggregateDecl, len(forms))
	for i, form := range forms {
		decls[i] = ringwire.AggregateDecl{Store: form.Store, Kind: form.Kind, Field: form.Field, By: form.By}
	}
	return decls
}

// decodeReply returns the reply document as canonical Extended JSON, the form
// the vectors carry a reply in. It is the whole document, the batch and get-many
// answers included: the row's payload_json holds them, so the tool prints them.
func decodeReply(payload []byte) ([]byte, error) {
	reply, err := ringwire.DecodeRingReply(payload)
	if err != nil {
		if body, decoder, ok := otherReplyBody(payload); ok {
			return nil, fmt.Errorf("the payload is %s, not the reply to RING_LIST, RING_COUNT, RING_KEYED, RING_BATCH or RING_GET_MANY: %s: %w", body, decoder, err)
		}
		return nil, fmt.Errorf("the payload is not a RING_REPLY: %w", err)
	}
	doc, err := ringwire.ReplyDocWithAnswers(reply)
	if err != nil {
		return nil, fmt.Errorf("the payload's answers cannot be read back: %w", err)
	}
	text, err := bson.MarshalExtJSON(doc, true, false)
	if err != nil {
		return nil, fmt.Errorf("writing the reply as Extended JSON: %w", err)
	}
	return text, nil
}

// otherReplyBody names a body decodeReply does not read. It is asked only after
// DecodeRingReply refused the bytes, so a body of the reply's own kind is never taken for
// one of these: the message says which body it is, and what decodes it.
func otherReplyBody(payload []byte) (body, decoder string, ok bool) {
	elements, err := bson.Raw(payload).Elements()
	if err != nil || len(elements) == 0 {
		return "", "", false
	}
	switch elements[0].Key() {
	case "cmd":
		body = "a RING_ADMIN reply body"
		if cmd, isString := elements[0].Value().StringValueOK(); isString && cmd != "" {
			body = fmt.Sprintf("a RING_ADMIN reply body (the reply to the admin command %q)", cmd)
		}
		return body, "decode it with ringwire.DecodeRingAdminReply (clients/wire/ringadmin.go), not with decode-reply", true
	case "epoch":
		if len(elements) > 1 && elements[1].Key() == "state_seq" {
			return "a RING_TOPOLOGY reply body (the cluster view, whose first members are epoch and state_seq)",
				"hoarddb-wire has no topology reply decoder in this build, so no ringvec command decodes it", true
		}
	}
	return "", "", false
}

// refusalOut is the error payload as data: the code, its registered name, the
// message and the detail a driver acts on.
type refusalOut struct {
	Code    int        `json:"code"`
	Name    string     `json:"name"`
	Message string     `json:"message"`
	Detail  *detailOut `json:"detail,omitempty"`
}

type detailOut struct {
	Owner        *string `json:"owner,omitempty"`
	OwnerAddr    *string `json:"owner_addr,omitempty"`
	Node         *string `json:"node,omitempty"`
	Epoch        *uint64 `json:"epoch,omitempty"`
	Slot         *uint64 `json:"slot,omitempty"`
	Voters       *int    `json:"voters,omitempty"`
	Answered     *int    `json:"answered,omitempty"`
	Rf           *int    `json:"rf,omitempty"`
	DataMembers  *int    `json:"data_members,omitempty"`
	MissingSlots *int    `json:"missing_slots,omitempty"`
	LapsedMs     *int    `json:"lapsed_ms,omitempty"`
	Expired      *bool   `json:"expired,omitempty"`
}

func decodeError(payload []byte) ([]byte, error) {
	refusal, err := ringwire.DecodeRingError(payload)
	if err != nil {
		return nil, fmt.Errorf("the payload is not an error: %w", err)
	}
	out := refusalOut{Code: int(refusal.Code), Name: refusal.Code.String(), Message: refusal.Message}
	if detail := refusal.Detail; detail != nil {
		carried, err := carriedDetailMembers(payload)
		if err != nil {
			return nil, fmt.Errorf("reading the members of the error's detail: %w", err)
		}
		out.Detail = detailOutOf(*detail, carried)
	}
	text, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("writing the error as JSON: %w", err)
	}
	return text, nil
}

// carriedDetailMembers is the names of the detail members the payload itself carries. The
// decoded Detail cannot tell a member the payload wrote as its zero from one it left out,
// so the payload is read for the members to print.
func carriedDetailMembers(payload []byte) (map[string]bool, error) {
	value, err := bson.Raw(payload).LookupErr("detail")
	if err != nil {
		return nil, fmt.Errorf("detail: %w", err)
	}
	detail, ok := value.DocumentOK()
	if !ok {
		return nil, errors.New("detail is not a document")
	}
	elements, err := detail.Elements()
	if err != nil {
		return nil, fmt.Errorf("detail: %w", err)
	}
	carried := make(map[string]bool, len(elements))
	for _, element := range elements {
		carried[element.Key()] = true
	}
	return carried, nil
}

// detailOutOf prints the members the payload carried, in the field order of the shape's
// table, a zero value included.
func detailOutOf(detail ringwire.Detail, carried map[string]bool) *detailOut {
	out := &detailOut{}
	if carried["owner"] {
		out.Owner = &detail.Owner
	}
	if carried["owner_addr"] {
		out.OwnerAddr = &detail.OwnerAddr
	}
	if carried["node"] {
		out.Node = &detail.Node
	}
	if carried["epoch"] {
		out.Epoch = &detail.Epoch
	}
	if carried["slot"] {
		out.Slot = &detail.Slot
	}
	if carried["voters"] {
		out.Voters = &detail.Voters
	}
	if carried["answered"] {
		out.Answered = &detail.Answered
	}
	if carried["rf"] {
		out.Rf = &detail.Rf
	}
	if carried["data_members"] {
		out.DataMembers = &detail.DataMembers
	}
	if carried["missing_slots"] {
		out.MissingSlots = &detail.MissingSlots
	}
	if carried["lapsed_ms"] {
		out.LapsedMs = &detail.LapsedMs
	}
	if carried["expired"] {
		out.Expired = &detail.Expired
	}
	return out
}

// hexOf decodes one hex member, naming the member in a refusal.
func hexOf(member, text string) ([]byte, error) {
	if text == "" {
		return nil, nil
	}
	raw, err := hex.DecodeString(text)
	if err != nil {
		return nil, fmt.Errorf("%s is not hex: %w", member, err)
	}
	return raw, nil
}

// leaseOf decodes a 16-byte lease id or nonce, naming the member in a refusal.
func leaseOf(text string) ([16]byte, error) {
	var id [16]byte
	raw, err := hexOf("a lease id", text)
	if err != nil {
		return id, err
	}
	if len(raw) != len(id) {
		return id, fmt.Errorf("a lease id is %d bytes and this one is %d", len(id), len(raw))
	}
	copy(id[:], raw)
	return id, nil
}

// documentOf returns the BSON bytes of one Extended JSON document member, or
// nil when the member is absent.
func documentOf(member string, text json.RawMessage) ([]byte, error) {
	if len(text) == 0 || string(text) == "null" {
		return nil, nil
	}
	var doc bson.Raw
	if err := bson.UnmarshalExtJSON(text, false, &doc); err != nil {
		return nil, fmt.Errorf("%s is not an Extended JSON document: %w", member, err)
	}
	return doc, nil
}
