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
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
)

const (
	// MaxFields is the most names a projection holds.
	MaxFields = 64
	// ringMaxFieldName is the longest field name a projection carries; a declared field
	// name is at most this long.
	ringMaxFieldName = 255
	// ringMaxFieldsBytes bounds the BSON array of a list request's fields member, checked
	// before the array is unpacked: 64 names of 255 bytes with their element headers.
	ringMaxFieldsBytes = MaxFields*(ringMaxFieldName+8) + 64
)

func projectionRefusal(format string, args ...any) error {
	return Refusal(format, args...)
}

// CheckRingGetFields says whether names are a fields list a get_fields carries: 1 to 64 names, each
// valid UTF-8 of 1 to 256 bytes, none twice. A client calls it to refuse one key before it builds
// a frame, with the text the server's codec would give.
func CheckRingGetFields(names []string) error { return CheckFieldNames("fields", names) }

// CheckFieldNames validates the names of a projection as they ride on the wire. member
// is the wire member's name, for the message.
func CheckFieldNames(member string, names []string) error {
	switch {
	case len(names) == 0:
		return projectionRefusal("%s is empty. Send at least one field name such as [\"Name\"], or leave %s out to get every field", member, member)
	case len(names) > MaxFields:
		return projectionRefusal("%s has %d names and at most %d are taken. Send the names you need, or leave %s out to get every field", member, len(names), MaxFields, member)
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		switch {
		case len(name) == 0 || len(name) > ringMaxFieldName:
			return projectionRefusal("a name in %s is %d bytes and a field name is 1 to %d. Send names that 'describe bucket <name>' lists", member, len(name), ringMaxFieldName)
		case !utf8.ValidString(name):
			return projectionRefusal("a name in %s is not valid UTF-8. Send field names as text", member)
		}
		if _, twice := seen[name]; twice {
			return projectionRefusal("%s names %q twice. Send each field name once", member, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// rawFieldNames reads the fields member of a list request, which has been through the
// bounded validator. present is false when the member is absent. Its size is bounded
// before it is unpacked and every element must be a string, so what comes back is what
// CheckFieldNames can judge.
func rawFieldNames(raw []byte) (names []string, present bool, err error) {
	value, lookupErr := bson.Raw(raw).LookupErr("fields")
	if lookupErr != nil {
		return nil, false, nil
	}
	array, isArray := value.ArrayOK()
	if !isArray {
		return nil, true, projectionRefusal("fields is a %s and must be an array of field names such as [\"Name\", \"Age\"]", value.Type)
	}
	if len(array) > ringMaxFieldsBytes {
		return nil, true, projectionRefusal("fields is %d bytes and at most %d are taken (%d names of %d bytes). Send the names you need, or leave fields out to get every field", len(array), ringMaxFieldsBytes, MaxFields, ringMaxFieldName)
	}
	elements, elementsErr := array.Elements()
	if elementsErr != nil {
		return nil, true, projectionRefusal("fields is not a valid array: %v. Send an array of field names such as [\"Name\", \"Age\"]", elementsErr)
	}
	if len(elements) > MaxFields {
		return nil, true, projectionRefusal("fields has %d names and at most %d are taken. Send the names you need, or leave fields out to get every field", len(elements), MaxFields)
	}
	names = make([]string, 0, len(elements))
	for position, element := range elements {
		name, isString := element.Value().StringValueOK()
		if !isString {
			return nil, true, projectionRefusal("fields must hold strings and element %d is a %s. Send field names such as [\"Name\", \"Age\"]", position, element.Value().Type)
		}
		names = append(names, name)
	}
	return names, true, nil
}
