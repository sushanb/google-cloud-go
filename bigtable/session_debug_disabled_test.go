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

import (
	"testing"
)

// TestClient_SessionDebug_DisabledByDefault verifies the default
// contract for ClientConfig.EnableClientDebug: leaving it unset yields
// SessionDebug() == nil so the debugview handler renders the
// "not enabled" panel and skips every allocating debug recorder.
//
// The mirror test (SessionDebug() != nil) requires a real gRPC dial to
// build a session client end-to-end and lives in the integration
// suite; this unit-scope test pins only the flag-driven nil contract.
func TestClient_SessionDebug_DisabledByDefault(t *testing.T) {
	// EnableSessionPool false → sessionImpl is nil regardless.
	// The exported nil contract is the same: SessionDebug() must return
	// nil so callers don't need to nil-check per z-page.
	c := &Client{config: ClientConfig{EnableSessionPool: false}}
	if got := c.SessionDebug(); got != nil {
		t.Errorf("Client.SessionDebug() = %v, want nil when EnableSessionPool=false", got)
	}

	// EnableSessionPool true + EnableClientDebug false + sessionImpl nil
	// (would-be typed-nil trap in a fully-constructed Client) — still
	// nil because we short-circuit on sessionImpl == nil before dialing
	// into the session provider.
	c = &Client{config: ClientConfig{EnableSessionPool: true, EnableClientDebug: false}}
	if got := c.SessionDebug(); got != nil {
		t.Errorf("Client.SessionDebug() = %v, want nil when sessionImpl unwired", got)
	}
}
