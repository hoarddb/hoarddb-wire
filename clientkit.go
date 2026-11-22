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

// ElementArg is the arg of a push to a fifoStore or lifoStore: element is one BSON document.
func ElementArg(element []byte) ([]byte, error) { return EncodeElementArg(element) }

// PutArg is the arg of a put into a hashStore or btreeStore.
func PutArg(key string, element []byte) ([]byte, error) { return EncodePutArg(key, element) }

// TakeKeyArg is the arg of a take from a hashStore or btreeStore.
func TakeKeyArg(key string) ([]byte, error) { return EncodeTakeKeyArg(key) }

// PopArg is the arg of a pop that takes up to n elements, n from 1 to 100. A pop with no
// arg takes one.
func PopArg(n int) ([]byte, error) { return EncodePopArg(n) }
