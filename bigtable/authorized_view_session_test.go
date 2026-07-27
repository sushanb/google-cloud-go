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
	"context"
	"testing"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestAuthorizedViewSessionSandbox drives the SessionClient's
// OpenAuthorizedView path end-to-end against the sandbox: creates an
// AuthorizedView on the sandbox table if missing, opens it via the
// session-forced client, writes a row through the AV, reads it back.
// Failure indicates a session/AV wiring regression: the AV path is
// distinct from the plain-table path (different RPC, different proto,
// different session pool per (av, permission) tuple).
func TestAuthorizedViewSessionSandbox(t *testing.T) {
	const (
		authorizedViewID = "session-test-av"
		rowPrefix        = "session-av:"
		qualifier        = "colq1"
	)

	// Client-side session pool enabled via ClientConfig below. To see
	// per-session PeerInfo (transport_type, afe) at OpenSession
	// handshake time, run the test with CBT_ENABLE_DEBUG=true.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Setup: create the AV via AdminClient. Point admin at the sandbox
	// admin endpoint (mirrors the data endpoint's host pattern). Skip
	// cleanly if admin creds aren't available.
	const sandboxAdminEndpoint = "test-bigtableadmin.sandbox.googleapis.com:443"
	adminClient, err := NewAdminClient(ctx, sessionSandboxProject, sessionSandboxInstance,
		option.WithEndpoint(sandboxAdminEndpoint))
	if err != nil {
		t.Skipf("admin client unavailable, skipping AV sandbox test: %v", err)
	}
	defer adminClient.Close()

	conf := &AuthorizedViewConf{
		TableID:          sessionSandboxTable,
		AuthorizedViewID: authorizedViewID,
		AuthorizedView: &SubsetViewConf{
			RowPrefixes: [][]byte{[]byte(rowPrefix)},
			FamilySubsets: map[string]FamilySubset{
				sessionSandboxFamily: {
					Qualifiers: [][]byte{[]byte(qualifier)},
				},
			},
		},
	}
	if err := adminClient.CreateAuthorizedView(ctx, conf); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("CreateAuthorizedView: %v", err)
		}
		t.Logf("AV %s already exists — reusing", authorizedViewID)
	} else {
		t.Logf("created AV %s", authorizedViewID)
	}

	cfg := ClientConfig{EnableSessionPool: true}
	client, err := NewClientWithConfig(ctx, sessionSandboxProject, sessionSandboxInstance, cfg,
		option.WithEndpoint(sessionSandboxEndpoint))
	if err != nil {
		t.Fatalf("NewClientWithConfig: %v", err)
	}
	defer client.Close()

	av := client.OpenAuthorizedView(sessionSandboxTable, authorizedViewID)

	// Give the session pool a moment to warm up so the first data call
	// isn't racing pool cold-start (matches TestReadSessionSandbox).
	t.Logf("warming up session pool...")
	time.Sleep(3 * time.Second)

	rowKey := rowPrefix + "test-row"
	value := []byte("av-session-value")

	t.Logf("writing row %q via session AV path...", rowKey)
	mut := NewMutation()
	mut.Set(sessionSandboxFamily, qualifier, ServerTime, value)
	if err := av.Apply(ctx, rowKey, mut); err != nil {
		t.Fatalf("Apply via AV failed: %v", err)
	}

	t.Logf("reading row %q via session AV path...", rowKey)
	row, err := av.ReadRow(ctx, rowKey)
	if err != nil {
		t.Fatalf("ReadRow via AV failed: %v", err)
	}
	if row == nil {
		t.Fatal("row not found after write via AV")
	}
	items, ok := row[sessionSandboxFamily]
	if !ok || len(items) == 0 {
		families := make([]string, 0, len(row))
		for k := range row {
			families = append(families, k)
		}
		t.Fatalf("family %s missing from row, got families = %v", sessionSandboxFamily, families)
	}
	got := string(items[0].Value)
	if got != string(value) {
		t.Errorf("read value = %q, want %q", got, value)
	}
	t.Logf("AV read/write succeeded: family=%s column=%s value=%s",
		sessionSandboxFamily, items[0].Column, got)
}
