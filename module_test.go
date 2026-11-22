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
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// serverPackagePath matches an import path inside the server module, whose
// module path is the bare word "hoardDB". The server is BSL 1.1; this module is
// Apache-2.0, so one such import would put BSL code behind an Apache licence.
var serverPackagePath = regexp.MustCompile(`^hoardDB(/|$)`)

// TestModuleImportsNoServerPackage lists every package this module builds or
// tests against, tests included, and fails on any that lives in the server
// module.
func TestModuleImportsNoServerPackage(t *testing.T) {
	var stderr bytes.Buffer
	list := exec.Command("go", "list", "-deps", "-test", "./...")
	list.Stderr = &stderr
	listed, err := list.Output()
	if err != nil {
		t.Fatalf("go list -deps -test ./...: %v\n%s", err, stderr.String())
	}
	importPaths := strings.Fields(string(listed))
	if len(importPaths) == 0 {
		t.Fatal("go list -deps -test ./... printed no package, so the check proved nothing")
	}
	for _, importPath := range importPaths {
		if serverPackagePath.MatchString(importPath) {
			t.Errorf("this module depends on the server package %q; move the code it needs into clients/wire instead of importing the server", importPath)
		}
	}
}
