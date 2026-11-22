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

// Package ringwire holds the ring wire contract that the server and the
// drivers share: the bytes of every ring request, reply and stored operation,
// and the codes and wording of every refusal.
//
// The module is Apache-2.0 and must stay importable by a driver, so it imports
// no server package; TestModuleImportsNoServerPackage enforces that.
package ringwire
