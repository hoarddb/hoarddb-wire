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
	"fmt"
	"regexp"
	"sort"
)

// Condition names one text of one code. A code that serves two facts has two
// conditions; the values are unique across codes, so a condition passed to
// the wrong code is refused rather than read as another text.
type Condition uint8

// One constant per condition of every code. The names that carry a code
// already (CondOwnerDown) have one text; NOT_FOUND, INVALID_ARGUMENT and
// UNAVAILABLE have several.
const (
	CondBucketNotFound Condition = iota + 1
	CondDocumentNotFound
	CondBadCursor
	CondLimitAboveMax
	CondLimitBelowOne
	CondSortNotSupported
	CondReadPreference
	CondPoisonedLog
	CondDeclarationWriteFailed
	CondResultTooLarge
	CondFieldNotDeclared
	CondDocumentExists
	CondNotOwner
	CondStaleEpoch
	CondOwnerDown
	CondNoQuorum
	CondRFUnsupported
	CondLeaseLapsed
	CondEpochFenced
	CondSeqGap
	CondLogBelowHorizon
	CondStateMoved
	CondConflict
	CondClusterMismatch
	CondNotADataNode
	CondSchemaNotCurrent
	CondClientDeclarationStale
	CondSchemaNotCurrentUnnamed
	CondBucketDropping
	CondDeclUnavailable
	CondBucketExists
	CondReservedName
	CondBucketIDCollision
	CondSeedNotHeld
	CondSeedNotHeldForget
	CondReplaceTargetHasData
	CondMintFailed
	CondCorruptKey
	CondSeedHeld
	CondForwardWriteUnreachable
	CondForwardReadUnreachable
	CondRFUnsupportedCreateVoters
	CondRFUnsupportedCreateData
	CondSeedHeldOnlyCopy
	CondExpandTooMany
	CondExpandDepth
	CondExpandStoreType
	CondExpandNoBucket
	CondOfBucketNotFound
	CondOfTypeWord
	CondRefIDType
	CondExpandFieldProjected
	CondExpandWhereField
	CondExpandTooManyReads
	CondExpandFieldType
)

// Kind says what a text's fix is, so the grammar check knows what to hold it
// to. Only KindCLI texts are checked against the CLI grammar.
type Kind uint8

// The kinds of fix a text can name.
const (
	// KindCLI quotes statements of the CLI grammar.
	KindCLI Kind = iota + 1
	// KindAPI quotes a driver call such as db.<bucket>.list().
	KindAPI
	// KindConfig quotes a configuration key such as cluster.stale_reads.
	KindConfig
	// KindInternal is node to node only and never shown to a client.
	KindInternal
	// KindNone names no command: the fix is to resend, wait or restart.
	KindNone
)

// String returns the kind's name in lower case, for example cli.
func (k Kind) String() string {
	switch k {
	case KindCLI:
		return "cli"
	case KindAPI:
		return "api"
	case KindConfig:
		return "config"
	case KindInternal:
		return "internal"
	case KindNone:
		return "none"
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

type conditionText struct {
	kind     Kind
	template string
}

// refusalTexts is the one table of exact texts. Placeholders are written
// <name> and are replaced from Text's args; the names are the conventions
// table's, with the few that were not identifiers (<e′>, <a, b, c>, a bare
// <n> used for two facts) given one.
var refusalTexts = map[Code]map[Condition]conditionText{
	CodeNotFound: {
		CondBucketNotFound:   {KindCLI, `bucket "<bucket>" not found in database "<db>": the declaration fetch returned no document under that name in <db>._buckets. Run 'show buckets in <db>' to list what exists, or 'create bucket <bucket> { <Field> string }' to create it`},
		CondOfBucketNotFound: {KindCLI, `field "<field>" of bucket "<bucket>" is declared of "<target>", which does not exist in database "<tdb>". Nothing was created. Run 'show buckets in <tdb>' to list what exists, or 'create bucket <target> in <tdb> { <Field> string }' first, then run this create again`},
		CondDocumentNotFound: {KindAPI, `document "<id>" is not in <db>.<bucket>. Run 'db.<bucket>.list({limit: 10})' to see what exists, or 'db.<bucket>.insert({_id_: "<id>", <field>: <value>})' to create it`},
	},
	CodeInvalidArgument: {
		CondBadCursor:            {KindAPI, `cursor is not a valid list cursor for <db>.<bucket>: <reason>, so it cannot say where the last page ended. Start again with 'db.<bucket>.list()'`},
		CondLimitAboveMax:        {KindAPI, `limit <n> is above this release's maximum page of <max>. Run 'db.<bucket>.list({limit: <max>})' and continue with the cursor it prints`},
		CondLimitBelowOne:        {KindAPI, `limit <n> is not a page size: leave it out, or write 0, for the default <default>. Run 'db.<bucket>.list({limit: <default>})'`},
		CondSortNotSupported:     {KindAPI, `sort is not in this release, so the clause would be ignored and the result would look sorted when it is not. Run 'db.<bucket>.list({limit: 100})' — the order is (slot, id): stable, pageable, not meaningful — and sort the page in your program`},
		CondReadPreference:       {KindConfig, `read_preference is not an option: stale reads are a node setting, not a request. Ask the operator of this node to set 'cluster.stale_reads: true', or read from a node that can see a majority of voters ('nodes' shows which are up)`},
		CondExpandTooMany:        {KindAPI, `expand names <n> fields and a call takes at most 8. Run the list with fewer fields in expand, or run it twice`},
		CondExpandDepth:          {KindAPI, `expand goes one level: "<bucket>" is itself expanded here. Nothing was read. Remove the inner expand, and read the next level with its own list`},
		CondExpandStoreType:      {KindAPI, `expand on a store list needs element keys, and a <type> has none. Nothing was read. Use a hashStore or a btreeStore, or read the ids with list and fetch them with get`},
		CondExpandNoBucket:       {KindAPI, `field "<field>" is not declared of any bucket. Run 'describe bucket <bucket> in <db>', or name the bucket: expand: {<field>: "<Bucket>"}`},
		CondOfTypeWord:           {KindCLI, `field "<field>" of bucket "<bucket>" is declared <type> of "<target>", and a reference holds the target's _id_, which is text in this build. Run 'create bucket <bucket> in <db> { <field> string of <target> }' and declare the reference as a string`},
		CondExpandFieldProjected: {KindAPI, `expand reads field "<field>" of <db>.<bucket>, and this call's fields leave it out, so every row would expand to nothing. Nothing was read. Add "<field>" to the fields of the call, or drop fields, and run it again`},
		CondExpandWhereField:     {KindAPI, `where on "<field>" cannot match: <why>. Nothing was read. Run 'describe bucket <bucket> in <db>' to list the declared fields, and name only those in where and in the fields of the expansion`},
		CondExpandTooManyReads:   {KindAPI, `this call would read <n> distinct documents to expand, and a call reads at most <max>. Nothing was read. Run the call with a smaller limit, or expand fewer fields`},
		CondExpandFieldType:      {KindAPI, `expand reads field "<field>" of <db>.<bucket> as the far id, and <why>. Nothing was read. Run 'describe bucket <bucket> in <db>' to list the fields and expand a string field; to expand the keys of a store, list the store with expand`},
		CondRefIDType:            {KindAPI, `field <field> of <db>.<bucket> refers to <target> and holds ids as text; this value is not text. Nothing was written. Send the target's _id_ as a string`},
	},
	CodeUnavailable: {
		CondPoisonedLog:             {KindCLI, `writes to "<db>.<bucket>" are paused on <node>: the write at position <epoch>:<seq> could not be appended to the retained log (<cause>). Nothing was acknowledged and no position was consumed; the slot resumes after the node restarts and replays. Run 'show replication for <db>.<bucket> slot <slot>' to list the paused slots, and 'nodes' to see the node`},
		CondDeclarationWriteFailed:  {KindCLI, `<op> <db>.<bucket>: the declaration write was confirmed by <c> of <rf> copies, and a declaration must be durable on every copy that is ready to serve it before a schema change is published, so the change was not committed and nothing was altered. Nothing was changed. Retry in a few seconds; 'show replication for <db>._buckets slot <slot>' lists the copies that are behind and 'nodes' shows whether they are down, and a database whose replication factor is above the copies that can be ready is set back with 'alter database <db> { replication_factor: <n> }'`},
		CondForwardWriteUnreachable: {KindCLI, `this node forwarded the write to <owner>, which owns its slot, and could not reach <owner> (the connection failed, or <owner> is in backoff after a recent failure). Whether the write was applied is UNKNOWN: <owner> may have applied it before the connection broke. Run 'nodes' to see <owner>, then retry with the same request id, which is safe`},
		CondForwardReadUnreachable:  {KindCLI, `this node forwarded the read to <owner>, which owns its slot, and could not reach <owner> (the connection failed, or <owner> is in backoff after a recent failure). Nothing was changed. Run 'nodes' to see <owner>, then retry in a few seconds`},
	},
	CodeResultTooLarge: {
		CondResultTooLarge: {KindAPI, `list <db>.<bucket>: the first document alone is <n> bytes and a document is never split, so the page cannot be built. Read it by key: 'db.<bucket>["<id>"].get()'`},
	},
	CodeFieldType: {
		CondFieldNotDeclared: {KindCLI, `field "<field>" is not declared in <db>.<bucket>, and a bucket stores only declared fields (declared: <declared>). Run 'alter bucket <bucket> { add <field> <type> }' to declare it, or correct the field name`},
	},
	CodeDocumentExists: {
		CondDocumentExists: {KindAPI, `document "<id>" is already in <db>.<bucket>, and insert never replaces. Run 'db.<bucket>.upsert({_id_: "<id>", …})' to replace it, or 'db.<bucket>["<id>"].get()' to read what is there`},
	},
	CodeNotOwner: {
		CondNotOwner: {KindNone, `node <node> is not the owner of "<id>" in <db>.<bucket> at epoch <e>: <owner> owns slot <slot>. Nothing was written. Send it to <owner>, or to any node and let it forward`},
	},
	CodeStaleEpoch: {
		CondStaleEpoch: {KindCLI, `slot <slot> of <db> is owned by <owner> at epoch <e> and this request carried epoch <e_seen>, so the two sides disagree and nothing was applied (a table older than the node, and a node that learned a higher epoch from a frame, both land here). Refresh with 'show placement for <id> in <db>.<bucket>' and retry`},
	},
	CodeOwnerDown: {
		CondOwnerDown: {KindCLI, `<op> <db>.<bucket>: <owner> owns <n> of the 8192 slots this call must read and did not answer, so a partial result would look complete. Nothing was returned. Run 'nodes' to see <owner>; retry in a few seconds, or start its replacement with 'replace-node <owner> --with <owner>@<addr>'`},
	},
	CodeNoQuorum: {
		CondNoQuorum: {KindCLI, `this node cannot see a majority of the cluster's <v> voters (<n> answered), so it refuses reads and writes: what it holds may be missing acknowledged writes. Run 'nodes' to see which voters are down, and retry when a majority answers`},
	},
	CodeRFUnsupported: {
		CondRFUnsupported: {KindCLI, `replication factor <rf> needs at least 3 voters, at least <rf> of them holding data; this cluster has <v> voters and <d> data members. Run 'nodes' to see the members, add a data node with 'join <node>@<addr>', or set 'alter database <db> { replication_factor: <n> }'`},
		// A create that is refused did not make the database, so the alter form cannot be
		// its fix: the two create texts name only the command that works, and only the
		// condition that failed.
		CondRFUnsupportedCreateVoters: {KindCLI, `replication factor <rf> needs at least 3 voters, and this cluster has <v>. Nothing was created. Run 'nodes' to see the members, add a voter with 'join <node>@<addr>', then re-run 'create database <db>'`},
		CondRFUnsupportedCreateData:   {KindCLI, `replication factor <rf> needs at least <rf> data members, and this cluster has <d>. Nothing was created. Run 'nodes' to see the members, add a data node with 'join <node>@<addr>', then re-run 'create database <db>'`},
	},
	CodeLeaseLapsed: {
		CondLeaseLapsed: {KindCLI, `node <node> stopped serving reads and writes <lapsed_ms> ms ago because it could not renew its lease (step_down_ms <step_down_ms>). Nothing was written. Run 'nodes': the cluster elects an owner for the slot, then retry; 'show placement for <id> in <db>.<bucket>' names it`},
	},
	CodeEpochFenced: {
		CondEpochFenced: {KindInternal, `refused <kind> from <node> at epoch <e>: this node is at epoch <e_seen>`},
	},
	CodeSeqGap: {
		CondSeqGap: {KindInternal, `refused position <e>:<n> for slot <slot> of <db> from <node>: this copy is at <e>:<m>, and <e>:<next>…<e>:<last> are missing. Catching up; no client action`},
	},
	CodeLogBelowHorizon: {
		CondLogBelowHorizon: {KindNone, `cannot catch <node> up for slot <slot> of <db>: it needs <need_pos> and the log's oldest record for the slot is <oldest_pos> (<seconds> s kept, <bytes> cap). The copy stays behind until the slot is re-seeded, which this build does not start by itself; the slot is served by its other copies`},
	},
	CodeStateMoved: {
		CondStateMoved: {KindCLI, `refused: the cluster state moved to state_seq <n> while this command was built on <m>. Nothing was changed. Re-run it ('nodes' prints the current state_seq)`},
	},
	CodeConflict: {
		CondConflict: {KindCLI, `node "<node>" is still <state> at epoch <e>, so a replacement cannot take its identity: two nodes writing as <node> would both own its slots. Stop it and wait for it to be declared down ('nodes' shows the state), then 'replace-node <node> --with <node>@<addr>'`},
	},
	CodeClusterMismatch: {
		CondClusterMismatch: {KindCLI, `node "<node>" belongs to cluster <node_cluster>… but this node belongs to cluster <this_cluster>…. Start it with 'join <node>@<seed_addr>'; its own state is discarded and its documents are re-seeded`},
	},
	CodeNotADataNode: {
		CondNotADataNode: {KindCLI, `node "<node>" is a witness: it holds the cluster state and a vote, and no documents. Send this to a data node ('nodes' marks each node data or witness)`},
	},
	CodeSchemaNotCurrent: {
		// The re-run is quoted as a complete alter statement; a bare
		// 'alter bucket <bucket> in <db>' is not one the grammar accepts.
		CondSchemaNotCurrent: {KindCLI, `<db>.<bucket> is not writable: its declaration is at schema version <d> and the database's schema_version is <s>, so a field may have been added, retired or removed since it was read and this operation would use the wrong ones. Nothing was written. The owner already re-read the declaration once and the bucket's own table moved, so retrying will not help: run 'describe bucket <bucket> in <db>' to see the declaration in force, and if an alter of the bucket was interrupted, run the same 'alter bucket <bucket> { add <field> <type> }' again to finish it`},
		// The same refusal when the node can no longer name the bucket or its database (the cache
		// dropped the entry, or the bucket was dropped meanwhile): it still carries both versions
		// and says what to do, and names no id because an id is not something a developer typed.
		CondSchemaNotCurrentUnnamed: {KindNone, `a bucket this operation addressed is not writable: its declaration is at schema version <d> and the database's schema_version is <s>, so a field may have been added, retired or removed since it was read and this operation would use the wrong ones. Nothing was written. This node cannot name the bucket now, which usually means it was dropped; list your database's buckets with show buckets, check the one you wrote to with describe bucket, and send the operation again`},
		// The ring handler's own refusal of a write built on a declaration the bucket has since left
		// behind: the client learns the bucket again and sends the write once more.
		CondClientDeclarationStale: {KindCLI, `<db>.<bucket> changed after this client read it: the client holds declaration version <d> and the table in force is version <s>, so a field may have been added or retired and this write might use the wrong ones. Nothing was written. Read the bucket again and send the write once more, which the hoardDB drivers do by themselves: 'describe bucket <bucket> in <db>' shows the fields in force`},
	},
	CodeBucketDropping: {
		CondBucketDropping: {KindCLI, `bucket "<bucket>" in database "<db>" is being dropped (<n> of 8192 slots swept), and a dropping bucket accepts no reads or writes. Run 'show buckets in <db>' to watch it leave the list; the name is free when the sweep finishes and the declaration is deleted`},
	},
	CodeDeclUnavailable: {
		CondDeclUnavailable: {KindCLI, `cannot read the declaration of bucket "<bucket>" in database "<db>": the node that stores it did not answer in time (it may be busy, or down), and this node holds no cached declaration for that name. Nothing was written. Run 'nodes': if every member is active, run 'status' on each node: Serving: ok means the read was only slow, so send the write again; a member that is not active is the one to bring back`},
	},
	CodeBucketExists: {
		CondBucketExists: {KindCLI, `bucket "<bucket>" already exists in database "<db>" with id <id16>, state <state>. Run 'show buckets in <db>' to see it, or 'describe bucket <bucket> in <db>' to see its fields and the version it was created with; if the field table differs, this retry is old — re-read the declaration before trying again`},
	},
	CodeReservedName: {
		CondReservedName: {KindCLI, `"<name>" cannot be a bucket name: names beginning with "_" are reserved for system buckets, and every database has the system bucket _buckets, which holds the declaration of each bucket. Run 'show buckets in <db>' to list the buckets you can create, or choose a name without a leading underscore`},
	},
	CodeBucketIDCollision: {
		CondBucketIDCollision: {KindCLI, `could not claim a free bucket id for "<bucket>" in database "<db>" after 3 draws; nothing was created. Retry 'create bucket <bucket> in <db> { … }': every attempt draws fresh ids`},
	},
	CodeSeedNotHeld: {
		CondSeedNotHeld:       {KindCLI, `slot <slot> of database "<db>" is <state> on this node, and only a seeding or stalled slot can be restarted, so nothing was changed. Run 'status seeding <db> <slot>' to see it, or 'nodes' to see which node owns the slot`},
		CondSeedNotHeldForget: {KindCLI, `slot <slot> of database "<db>" has no record on this node, so there is nothing to forget and nothing was changed. Run 'status seeding' to list the slots this node keeps a record of, or 'nodes' to see which node owns the slot`},
	},
	CodeReplaceTargetHasData: {
		CondReplaceTargetHasData: {KindCLI, `node "<node>" cannot be replaced without --clear: its data directory holds <n> bytes of documents for this identity, and a replacement that starts with documents of its own would claim copies it never seeded. Run 'replace-node <node> --with <node>@<addr> --clear' to discard them and seed this identity's slots from their current owners`},
	},
	CodeMintFailed: {
		CondMintFailed: {KindAPI, `insert could not mint an _id_: <reason>. Nothing was written. Retry, or supply one: 'db.<bucket>.insert({_id_: "<id>", <field>: <value>})'`},
	},
	CodeSeedHeld: {
		CondSeedHeldOnlyCopy: {KindCLI, `slot <slot> of database "<db>" is retired on this node and no other member has confirmed a copy covering the fence (yet), so this node may hold the only copy of its data, and forgetting it lets the next seed of this slot delete the range. Nothing was changed. Run 'status seeding <db> <slot>' to see what the deletion gate waits for, or 'seeding forget <db> <slot> anyway' to forget it regardless`},
		CondSeedHeld:         {KindCLI, `slot <slot> of database "<db>" is <state> on this node, and a forget removes only a retired record, so nothing was changed. Run 'status seeding <db> <slot>' to see it, or 'seeding retry <db> <slot>' to start the slot again from a fresh cut`},
	},
	CodeCorruptKey: {
		CondCorruptKey: {KindNone, `the store holds a key in bucket id <bucket_id> whose id component does not decode: unknown tag 0x<tag> at offset <offset>. Stop this node, move <data_dir>/store aside and keep it (its files may be the only copy), then restart and let the node be seeded`},
	},
}

var placeholder = regexp.MustCompile(`<[A-Za-z][A-Za-z0-9_]*>`)

// Template returns the text of a condition with its placeholders still in
// place. ok is false when the code has no such condition.
func Template(c Code, cond Condition) (string, bool) {
	entry, ok := refusalTexts[c][cond]
	return entry.template, ok
}

// KindOf returns what the condition's fix is. ok is false when the code has
// no such condition.
func KindOf(c Code, cond Condition) (Kind, bool) {
	entry, ok := refusalTexts[c][cond]
	return entry.kind, ok
}

// Conditions returns the conditions of a code in ascending order.
func Conditions(c Code) []Condition {
	conds := make([]Condition, 0, len(refusalTexts[c]))
	for cond := range refusalTexts[c] {
		conds = append(conds, cond)
	}
	sort.Slice(conds, func(i, j int) bool { return conds[i] < conds[j] })
	return conds
}

// Text returns the exact text of one condition of the code, with every
// <name> replaced from args. A placeholder with no argument is left as
// written, so a missing argument shows in the output instead of hiding. A
// condition the code does not have returns one fixed text naming the code.
func (c Code) Text(cond Condition, args map[string]string) string {
	entry, ok := refusalTexts[c][cond]
	if !ok {
		return fmt.Sprintf("%s has no such condition: this is a hoardDB bug, please report it with the command you ran", c)
	}
	return placeholder.ReplaceAllStringFunc(entry.template, func(token string) string {
		if argument, given := args[token[1:len(token)-1]]; given {
			return argument
		}
		return token
	})
}
