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
	"strings"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
)

const (
	// MaxVerbArgBytes is the largest STORE_VERB arg, 1 MiB.
	MaxVerbArgBytes = 1 << 20
	// VerbArgDepth is the deepest nesting a verb arg may have: the arg layouts are flat
	// documents whose element is a binary, so 8 is generous.
	VerbArgDepth = 8
	// MaxVerbKeyBytes is the longest k or e of a STORE_VERB arg.
	MaxVerbKeyBytes = 1024
	// maxIncPaths is the most dotted paths one inc names; a counter hour has a handful.
	maxIncPaths = 64
	// maxIncDepth is the deepest dotted path an inc names: d.14.t is three levels.
	maxIncDepth = 3
)

// The wire documents. Field order is the canonical byte layout.
type (
	// PopArgDoc is the {n} arg of a pop.
	PopArgDoc struct {
		N int32 `bson:"n"`
	}
	// ElementArgDoc is the {v} arg of a push to a fifoStore or lifoStore.
	ElementArgDoc struct {
		V []byte `bson:"v"`
	}
	// PutArgDoc is the {k, v} arg of a put.
	PutArgDoc struct {
		K string `bson:"k"`
		V []byte `bson:"v"`
	}
	// TakeKeyArgDoc is the {k} arg of a take from a hashStore or btreeStore.
	TakeKeyArgDoc struct {
		K string `bson:"k"`
	}
	// IncArgDoc is the {k, v[, r]} arg of an inc.
	IncArgDoc struct {
		K string `bson:"k"`
		V []byte `bson:"v"`
		// R is omitted when false: an arg with r: false would name the same increment as one without it.
		R bool `bson:"r,omitempty"`
	}
)

// VerbArgRefusal is an INVALID_ARGUMENT refusal of a STORE_VERB arg.
func VerbArgRefusal(format string, args ...any) error {
	return &Error{Code: CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

// CheckVerbKey refuses a k or e that is empty, too long or not valid UTF-8; name says which.
func CheckVerbKey(name, key string) error {
	if len(key) < 1 || len(key) > MaxVerbKeyBytes {
		return VerbArgRefusal("%q is %d bytes and must be 1 to %d bytes. Nothing was written", name, len(key), MaxVerbKeyBytes)
	}
	// A key comes back in the keys member of a reply, a BSON string, which is UTF-8 (build
	// SPEC-GAP U85-5).
	if !utf8.ValidString(key) {
		return VerbArgRefusal("%q is not valid UTF-8. Nothing was written. Send a key and an entry id as text; encode binary values as hex or base64 first", name)
	}
	return nil
}

// CheckVerbElement refuses an element that is not one BSON document.
func CheckVerbElement(element []byte) error {
	if len(element) < 1 {
		return VerbArgRefusal("%q is empty and must be at least 1 byte (a BSON document). Nothing was written", "v")
	}
	return nil
}

// EncodeVerbDoc is doc as the bytes of a STORE_VERB arg, refused when over the arg cap.
func EncodeVerbDoc(doc any) ([]byte, error) {
	arg, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encoding a store verb arg: %w", err)
	}
	if len(arg) > MaxVerbArgBytes {
		return nil, VerbArgRefusal("the arg is %d bytes and at most %d bytes are taken. Nothing was written", len(arg), MaxVerbArgBytes)
	}
	return arg, nil
}

// EncodeElementArg is the {v} arg of a push to a fifoStore or lifoStore.
func EncodeElementArg(element []byte) ([]byte, error) {
	if err := CheckVerbElement(element); err != nil {
		return nil, err
	}
	return EncodeVerbDoc(ElementArgDoc{V: element})
}

// EncodePutArg is the {k, v} arg of a put into a hashStore or btreeStore.
func EncodePutArg(key string, element []byte) ([]byte, error) {
	if err := CheckVerbKey("k", key); err != nil {
		return nil, err
	}
	if err := CheckVerbElement(element); err != nil {
		return nil, err
	}
	return EncodeVerbDoc(PutArgDoc{K: key, V: element})
}

// EncodeTakeKeyArg is the {k} arg of a take from a hashStore or btreeStore.
func EncodeTakeKeyArg(key string) ([]byte, error) {
	if err := CheckVerbKey("k", key); err != nil {
		return nil, err
	}
	return EncodeVerbDoc(TakeKeyArgDoc{K: key})
}

// EncodePopArg is the {n} arg of a pop that takes up to n elements.
func EncodePopArg(n int) ([]byte, error) {
	if err := CheckPopCount(int64(n)); err != nil {
		return nil, err
	}
	return EncodeVerbDoc(PopArgDoc{N: int32(n)})
}

// CheckPopCount refuses a pop count outside 1 to MaxPeekCount.
func CheckPopCount(n int64) error {
	if n < 1 || n > MaxPeekCount {
		return VerbArgRefusal("%q is %d and a pop takes 1 to %d elements. Nothing was removed", "n", n, MaxPeekCount)
	}
	return nil
}

// IncArg is the {k, v[, r]} arg of an inc on a hashStore or btreeStore: key names the element,
// increments is a BSON document of dotted key paths to numbers, and returnAfter asks for the
// element as applied. r is written only when true, so an arg has one byte form.
func IncArg(key string, increments []byte, returnAfter bool) ([]byte, error) {
	if err := CheckVerbKey("k", key); err != nil {
		return nil, err
	}
	if err := CheckIncrements(increments); err != nil {
		return nil, err
	}
	return EncodeVerbDoc(IncArgDoc{K: key, V: increments, R: returnAfter})
}

// CheckIncrements refuses an increments document the apply could not add, before a position is
// taken: a path of more than maxIncDepth levels or with an empty level, a value that is not an
// int32, an int64 or a finite double, more than maxIncPaths paths, and no path at all.
func CheckIncrements(increments []byte) error {
	if _, err := bson.Validate(increments, bson.Limits{Depth: VerbArgDepth, Size: MaxVerbArgBytes}); err != nil {
		return VerbArgRefusal("inc's increments are not a valid BSON document: %v. Nothing was written", err)
	}
	elements, err := bson.Raw(increments).Elements()
	if err != nil {
		return VerbArgRefusal("inc's increments are not a BSON document: %v. Nothing was written", err)
	}
	if len(elements) == 0 {
		return VerbArgRefusal("inc takes at least one path and its number, and the increments are empty. Nothing was written. Send one, for example db.User[\"ada\"].Friends.inc(\"grace\", {visits: 1}) (help inc)")
	}
	if len(elements) > maxIncPaths {
		return VerbArgRefusal("inc takes at most %d paths in one call, and this one has %d. Nothing was written; split it into more calls", maxIncPaths, len(elements))
	}
	for _, element := range elements {
		if err := checkIncPath(element.Key()); err != nil {
			return err
		}
		if err := checkIncNumber(element.Key(), element.Value()); err != nil {
			return err
		}
	}
	return checkIncConflicts(elements)
}

// checkIncConflicts refuses a path given twice and a path that is a parent of another: the add
// would otherwise be applied twice or to a number that is also a document. At most maxIncPaths paths, so the pairwise walk is bounded.
func checkIncConflicts(elements []bson.RawElement) error {
	for i, first := range elements {
		for _, second := range elements[i+1:] {
			a, b := first.Key(), second.Key()
			switch {
			case a == b:
				return VerbArgRefusal("path %q is given twice and would be added twice. Nothing was written; give each path once with the total amount", a)
			case strings.HasPrefix(a, b+"."):
				return VerbArgRefusal("paths %q and %q conflict, as %q is inside %q. Nothing was written; inc one or the other in a call", b, a, a, b)
			case strings.HasPrefix(b, a+"."):
				return VerbArgRefusal("paths %q and %q conflict, as %q is inside %q. Nothing was written; inc one or the other in a call", a, b, b, a)
			}
		}
	}
	return nil
}

// checkIncPath refuses a dotted path of more than maxIncDepth levels, or with an empty level.
func checkIncPath(path string) error {
	levels := strings.Split(path, ".")
	if len(levels) > maxIncDepth {
		return VerbArgRefusal("path %q is %d levels deep and inc takes at most %d. Nothing was written", path, len(levels), maxIncDepth)
	}
	for _, level := range levels {
		if level == "" {
			return VerbArgRefusal("path %q has an empty level. Nothing was written; a path is names joined by dots, such as \"d.14.t\"", path)
		}
	}
	return nil
}

// checkIncNumber refuses a value that is not an int32, an int64 or a finite double. A decimal is
// refused by name: money belongs in integer minor units, which an inc can add exactly.
func checkIncNumber(path string, value bson.RawValue) error {
	switch value.Type {
	case bsontype.Int32, bsontype.Int64:
		return nil
	case bsontype.Double:
		if number, ok := ReadNumber(value.Type, value.Value); ok && IsFinite(number.Real) {
			return nil
		}
		return VerbArgRefusal("%q is not a finite double. Nothing was written", path)
	case bsontype.Decimal128:
		return VerbArgRefusal("inc takes numbers; a decimal is refused, store money in integer minor units. %q is a decimal. Nothing was written", path)
	}
	return VerbArgRefusal("inc takes an int32, an int64 or a finite double, and %q is a %s. Nothing was written", path, value.Type)
}

// FirstError returns the first error of errs that is not nil.
func FirstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
