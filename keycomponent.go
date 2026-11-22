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
	"fmt"
)

// Key component type tags. They live here, a leaf package, so
// the operation codec can measure a component without importing store; store's
// own tag constants are defined from these. Order between types is the tag
// byte: arbitrary but fixed.
const (
	TagNull     byte = 0x10
	TagBool     byte = 0x20
	TagNumber   byte = 0x30
	TagDateTime byte = 0x40
	TagString   byte = 0x50
	TagBinary   byte = 0x60
	// TagObjectID is the seventh `_id_` tag: the tag, then twelve raw bytes.
	TagObjectID byte = 0x70
)

// EncodedComponentLen returns the length of the one component that body begins,
// read from its tag, so a caller can step over a component inside a longer key.
// A body that does not decode is an error, never a guessed length: a wrong
// length moves the reader into a neighbouring key.
func EncodedComponentLen(body []byte) (int, error) {
	if len(body) == 0 {
		return 0, errors.New("an empty key component")
	}
	switch body[0] {
	case TagString, TagBinary:
		return escapedComponentLen(body)
	}
	width, ok := fixedComponentWidth(body[0])
	if !ok {
		return 0, fmt.Errorf("unknown key component tag 0x%02x", body[0])
	}
	if len(body) < width {
		return 0, fmt.Errorf("a key component of tag 0x%02x is %d bytes, want %d", body[0], len(body), width)
	}
	return width, nil
}

// EncodedComponentLenDesc is EncodedComponentLen for a component written with
// every byte inverted, as the descending encoder writes it. It reads the tag
// inverted and measures the inverted escapes without un-inverting the body, so
// the walk never copies a component to measure it.
func EncodedComponentLenDesc(body []byte) (int, error) {
	if len(body) == 0 {
		return 0, errors.New("an empty descending key component")
	}
	tag := ^body[0]
	switch tag {
	case TagString, TagBinary:
		return invertedEscapedComponentLen(body)
	}
	width, ok := fixedComponentWidth(tag)
	if !ok {
		return 0, fmt.Errorf("unknown descending key component tag 0x%02x", body[0])
	}
	if len(body) < width {
		return 0, fmt.Errorf("a descending key component of tag 0x%02x is %d bytes, want %d", body[0], len(body), width)
	}
	return width, nil
}

// fixedComponentWidth is the width of a component whose length its tag fixes;
// string and binary are variable-length and report false.
func fixedComponentWidth(tag byte) (int, bool) {
	switch tag {
	case TagNull:
		return 1, true
	case TagBool:
		return 2, true
	case TagDateTime:
		return 9, true
	case TagNumber:
		return 18, true
	case TagObjectID:
		return 13, true
	}
	return 0, false
}

// escapedComponentLen measures a string or binary component: its escaped
// payload runs to the first 0x00 0x00 terminator, and each 0x00 inside the
// payload is written 0x00 0xFF by appendEscaped.
func escapedComponentLen(body []byte) (int, error) {
	for i := 1; i < len(body); i++ {
		if body[i] != 0x00 {
			continue
		}
		if i+1 == len(body) {
			break
		}
		switch body[i+1] {
		case 0x00:
			return i + 2, nil
		case 0xFF:
			i++
		default:
			return 0, fmt.Errorf("a key component of tag 0x%02x has a 0x00 followed by 0x%02x, which no encoder writes", body[0], body[i+1])
		}
	}
	return 0, fmt.Errorf("a key component of tag 0x%02x has no terminator", body[0])
}

// invertedEscapedComponentLen measures a descending string or binary component:
// every byte is inverted, so the terminator 0x00 0x00 reads 0xFF 0xFF and the
// escape 0x00 0xFF reads 0xFF 0x00.
func invertedEscapedComponentLen(body []byte) (int, error) {
	for i := 1; i < len(body); i++ {
		if body[i] != 0xFF {
			continue
		}
		if i+1 == len(body) {
			break
		}
		switch body[i+1] {
		case 0xFF:
			return i + 2, nil
		case 0x00:
			i++
		default:
			return 0, fmt.Errorf("a descending key component of tag 0x%02x has a 0xFF followed by 0x%02x, which no encoder writes", body[0], body[i+1])
		}
	}
	return 0, fmt.Errorf("a descending key component of tag 0x%02x has no terminator", body[0])
}
