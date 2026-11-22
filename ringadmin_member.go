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
	"net"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The cmd values of the membership commands.
const (
	// AdminJoin registers a node as a non-voting member.
	AdminJoin = "join"
	// AdminPromote makes a joined non-voter a voter.
	AdminPromote = "promote"
	// AdminLeave asks a member to leave the cluster in an orderly way.
	AdminLeave = "leave"
	// AdminRemoveNode removes a member that is down or dead.
	AdminRemoveNode = "remove_node"
	// AdminReplaceNode puts a new machine in the place of a down or dead member.
	AdminReplaceNode = "replace_node"
)

// MaxMemberResult bounds the result line of a membership reply, in bytes.
const MaxMemberResult = 512

func isMembershipCmd(cmd string) bool {
	switch cmd {
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return true
	}
	return false
}

// RingAdminMembership is the reply of the five membership commands.
type RingAdminMembership struct {
	// Epoch and StateSeq are the committed position of the change, both 0 or more.
	Epoch, StateSeq int64
	// Result is one line, 1 to 512 bytes of UTF-8 with no control character, saying what was committed.
	Result string
}

type adminMemberJoinDoc struct {
	Cmd      string `bson:"cmd"`
	NodeID   string `bson:"node_id"`
	DataAddr string `bson:"data_addr"`
	Witness  bool   `bson:"witness,omitempty"`
}

type adminMemberNodeDoc struct {
	Cmd    string `bson:"cmd"`
	NodeID string `bson:"node_id"`
}

type adminMemberReplaceDoc struct {
	Cmd      string `bson:"cmd"`
	NodeID   string `bson:"node_id"`
	DataAddr string `bson:"data_addr"`
}

type adminMemberReplyDoc struct {
	Cmd      string `bson:"cmd"`
	Epoch    int64  `bson:"epoch"`
	StateSeq int64  `bson:"state_seq"`
	Result   string `bson:"result"`
}

// checkMemberText bounds a node id or a data address: 1 to 256 bytes of UTF-8 with no control character.
func checkMemberText(cmd, member, text string) error {
	if len(text) == 0 || len(text) > MaxNodeText {
		fix := "Send the node id that 'nodes' lists"
		if member == "data_addr" {
			fix = "Send host:port, the address the node serves the cluster on, for example node-b.example:4433"
		}
		return Refusal("%s: %s is %d bytes and it is 1 to %d. %s", cmd, member, len(text), MaxNodeText, fix)
	}
	if !utf8.ValidString(text) {
		return Refusal("%s: %s is not valid UTF-8. Send it as UTF-8 text", cmd, member)
	}
	if at := strings.IndexFunc(text, unicode.IsControl); at >= 0 {
		return Refusal("%s: %s has a control character at byte %d, which none may hold. Send printable characters", cmd, member, at)
	}
	return nil
}

// checkMemberAddr checks a data address: text, then host:port with a host and a port from 1 to 65535.
func checkMemberAddr(cmd, addr string) error {
	if err := checkMemberText(cmd, "data_addr", addr); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return Refusal("%s: data_addr %q is not host:port. Send the address the node serves the cluster on, for example node-b.example:4433", cmd, addr)
	}
	if host == "" {
		return Refusal("%s: data_addr %q has no host. Send host:port, for example node-b.example:4433", cmd, addr)
	}
	if number, err := strconv.ParseUint(port, 10, 16); err != nil || number == 0 {
		return Refusal("%s: data_addr %q has port %q and a port is 1 to 65535. Send host:port, for example node-b.example:4433", cmd, addr, port)
	}
	return nil
}

// checkMembershipRequest checks the members of the five membership requests.
func checkMembershipRequest(req RingAdminRequest) error {
	if err := adminStray(req, ""); err != nil {
		return err
	}
	if err := checkMemberText(req.Cmd, "node_id", req.NodeID); err != nil {
		return err
	}
	takesAddr := req.Cmd == AdminJoin || req.Cmd == AdminReplaceNode
	switch {
	case takesAddr:
		if err := checkMemberAddr(req.Cmd, req.DataAddr); err != nil {
			return err
		}
	case req.DataAddr != "":
		return Refusal("%s takes only node_id and cmd, and this request also sets data_addr. Remove data_addr", req.Cmd)
	}
	if req.Witness && req.Cmd != AdminJoin {
		return Refusal("%s takes no witness, which only join takes. Remove witness; a node is made a witness when it joins", req.Cmd)
	}
	return nil
}

func membershipDocOf(req RingAdminRequest) any {
	switch req.Cmd {
	case AdminJoin:
		return adminMemberJoinDoc{Cmd: req.Cmd, NodeID: req.NodeID, DataAddr: req.DataAddr, Witness: req.Witness}
	case AdminReplaceNode:
		return adminMemberReplaceDoc{Cmd: req.Cmd, NodeID: req.NodeID, DataAddr: req.DataAddr}
	}
	return adminMemberNodeDoc{Cmd: req.Cmd, NodeID: req.NodeID}
}

// membershipMembers lists the members each membership command takes besides cmd.
var membershipMembers = map[string][]string{
	AdminJoin:        {"node_id", "data_addr", "witness"},
	AdminPromote:     {"node_id"},
	AdminLeave:       {"node_id"},
	AdminRemoveNode:  {"node_id"},
	AdminReplaceNode: {"node_id", "data_addr"},
}

func unpackMembershipRequest(cmd string, raw []byte) (RingAdminRequest, error) {
	bad := func(err error) (RingAdminRequest, error) {
		return RingAdminRequest{}, Refusal("a %s request is not the document of that command (its members, each of its stated type): %v", cmd, err)
	}
	// The typed unmarshal below drops a member it has no field for, so name one here, as the
	// describe_node arm does, instead of leaving the re-encode compare to refuse it unnamed.
	members, err := bson.Raw(raw).Elements()
	if err != nil {
		return bad(err)
	}
	for _, member := range members {
		if key := member.Key(); key != "cmd" && !slices.Contains(membershipMembers[cmd], key) {
			return RingAdminRequest{}, Refusal("%s takes only %s and cmd, and this request also sets %s. Remove %s",
				cmd, strings.Join(membershipMembers[cmd], ", "), key, key)
		}
	}
	switch cmd {
	case AdminJoin:
		var doc adminMemberJoinDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminRequest{Cmd: cmd, NodeID: doc.NodeID, DataAddr: doc.DataAddr, Witness: doc.Witness}, nil
	case AdminReplaceNode:
		var doc adminMemberReplaceDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminRequest{Cmd: cmd, NodeID: doc.NodeID, DataAddr: doc.DataAddr}, nil
	}
	var doc adminMemberNodeDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return bad(err)
	}
	return RingAdminRequest{Cmd: cmd, NodeID: doc.NodeID}, nil
}

// checkMembershipReply checks the reply of a membership command; zeroRest says no other member is set.
func checkMembershipReply(reply RingAdminReply, zeroRest bool) error {
	if reply.Membership == nil || !zeroRest {
		return Refusal("a %s reply is epoch, state_seq and result, and nothing else", reply.Cmd)
	}
	membership := reply.Membership
	if membership.Epoch < 0 || membership.StateSeq < 0 {
		return Refusal("epoch is %d and state_seq %d, each 0 or more", membership.Epoch, membership.StateSeq)
	}
	if len(membership.Result) == 0 || len(membership.Result) > MaxMemberResult {
		return Refusal("a result line is %d bytes and must be 1 to %d", len(membership.Result), MaxMemberResult)
	}
	if !utf8.ValidString(membership.Result) || strings.IndexFunc(membership.Result, unicode.IsControl) >= 0 {
		return Refusal("a result line is one line of printable UTF-8")
	}
	return nil
}

func unpackMembershipReply(cmd string, raw []byte) (RingAdminReply, error) {
	var doc adminMemberReplyDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingAdminReply{}, Refusal("a %s reply is not the document of that command (its members, each of its stated type): %v", cmd, err)
	}
	return RingAdminReply{Cmd: cmd, Membership: &RingAdminMembership{Epoch: doc.Epoch, StateSeq: doc.StateSeq, Result: doc.Result}}, nil
}
