// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigtable

// Shared configuration for tests that hit the vRPC sandbox instance.
// Kept as package-private constants so every session-related test targets
// the same project/instance/table/endpoint — read_session_test.go and
// session_data_validation_test.go both consume these.
//
// The sandbox is the only environment where the SessionClient (vRPC) path
// is deployed at time of writing; production Bigtable endpoints do not
// yet accept OpenSession, so tests that need vRPC MUST use these values.
const (
	sessionSandboxProject  = "autonomous-mote-782"
	sessionSandboxInstance = "test-sushanb"
	sessionSandboxTable    = "sushanb"
	sessionSandboxFamily   = "cf12"
	sessionSandboxEndpoint = "test-bigtable.sandbox.googleapis.com:443"
)
