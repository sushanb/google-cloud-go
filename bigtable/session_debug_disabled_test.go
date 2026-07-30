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

// TestClient_SessionDebug_NilWhenSessionImplUnwired pins the nil
// contract that debugview.Handler relies on: a Client whose session
// backend was never constructed (sessionImpl==nil) must return nil
// from SessionDebug() so the /debug/ index renders the "not enabled"
// panel and skips every provider fan-out. The corresponding "enabled
// and populated" test requires a real gRPC dial + session handshake,
// so it lives in the integration suite.
func TestClient_SessionDebug_NilWhenSessionImplUnwired(t *testing.T) {
	c := &Client{} // sessionImpl left nil, mimicking a hand-built Client
	if got := c.SessionDebug(); got != nil {
		t.Errorf("Client.SessionDebug() = %v, want nil when sessionImpl is nil", got)
	}
}

// TestClient_TCPStats_NilWhenDisabled pins that a Client whose
// EnableClientDebug was false returns nil from TCPStats(). Callers
// pass the result straight to debugview.Handler; nil is the "no
// collector" sentinel that renders the tcpz "not attached" panel.
func TestClient_TCPStats_NilWhenDisabled(t *testing.T) {
	c := &Client{} // tcpStats left nil, mimicking EnableClientDebug=false
	if got := c.TCPStats(); got != nil {
		t.Errorf("Client.TCPStats() = %v, want nil when EnableClientDebug=false", got)
	}
}

// TestClient_TCPStats_NilReceiverSafe defends against callers that
// do client.TCPStats() on a *Client returned nil from a constructor
// error path — the getter must not panic.
func TestClient_TCPStats_NilReceiverSafe(t *testing.T) {
	var c *Client
	if got := c.TCPStats(); got != nil {
		t.Errorf("(*Client)(nil).TCPStats() = %v, want nil", got)
	}
}
