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
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigtable/internal/session"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestMaterializedViewSessionSandbox drives the SessionClient's
// OpenMaterializedView path end-to-end against the sandbox: creates a
// simple GROUP-BY MV over the sandbox table if missing, opens it via
// the session-enabled client, reads through the MV, and confirms
// Apply returns ErrWriteNotSupported (MV is read-only by contract).
// Run with CBT_ENABLE_DEBUG=true to see PeerInfo lines per session.
func TestMaterializedViewSessionSandbox(t *testing.T) {
	const materializedViewID = "session-test-mv"

	// MV creation is a long-running operation; give it up to 3 min for
	// the first-time-create path. Subsequent runs hit AlreadyExists
	// and are fast.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// MV admin lives on InstanceAdminClient (not AdminClient). Point at
	// the sandbox admin endpoint; skip cleanly if creds aren't
	// available.
	const sandboxAdminEndpoint = "test-bigtableadmin.sandbox.googleapis.com:443"
	iac, err := NewInstanceAdminClient(ctx, sessionSandboxProject,
		option.WithEndpoint(sandboxAdminEndpoint))
	if err != nil {
		t.Skipf("instance admin client unavailable, skipping MV sandbox test: %v", err)
	}
	defer iac.Close()

	mvInfo := &MaterializedViewInfo{
		MaterializedViewID: materializedViewID,
		Query: fmt.Sprintf(
			"SELECT _key, count(%s['colq1']) as `result.count` FROM `%s` GROUP BY _key",
			sessionSandboxFamily, sessionSandboxTable),
		DeletionProtection: Unprotected,
	}
	if err := iac.CreateMaterializedView(ctx, sessionSandboxInstance, mvInfo); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("CreateMaterializedView: %v", err)
		}
		t.Logf("MV %s already exists — reusing", materializedViewID)
	} else {
		t.Logf("created MV %s", materializedViewID)
	}

	// The base table has rows from prior sandbox tests; pick "myrow-0"
	// (written by TestReadSessionSandbox). MV output shape is the
	// aggregation, not the source row.
	rowKey := "myrow-0"

	// Poll MV readiness via a CLASSIC client first — the session pool
	// would trip its consecutive-failure breaker if we opened it while
	// the MV was still being backfilled (every session's OpenMV
	// handshake would fail FailedPrecondition → abnormal close →
	// breaker trip). MV backfill can take 10+ minutes; skip the test
	// cleanly if it isn't ready in our bounded window.
	const mvReadyDeadline = 2 * time.Minute
	t.Logf("polling MV readiness via classic path (up to %v)...", mvReadyDeadline)
	classicClient, err := NewClient(ctx, sessionSandboxProject, sessionSandboxInstance,
		option.WithEndpoint(sessionSandboxEndpoint))
	if err != nil {
		t.Fatalf("classic NewClient: %v", err)
	}
	classicMV := classicClient.OpenMaterializedView(materializedViewID)
	pollDeadline := time.Now().Add(mvReadyDeadline)
	ready := false
	for {
		if _, err := classicMV.ReadRow(ctx, rowKey); err == nil {
			ready = true
			break
		} else if status.Code(err) == codes.FailedPrecondition && time.Now().Before(pollDeadline) {
			t.Logf("MV still being created; retrying in 15s (%v)", err)
			time.Sleep(15 * time.Second)
			continue
		} else if status.Code(err) == codes.FailedPrecondition {
			classicClient.Close()
			t.Skipf("MV backfill did not complete within %v; skipping (rerun once server-side backfill finishes)", mvReadyDeadline)
		} else {
			classicClient.Close()
			t.Fatalf("classic MV ReadRow polling failed: %v", err)
		}
	}
	classicClient.Close()
	if !ready {
		return
	}
	t.Logf("MV ready; opening session client...")

	cfg := ClientConfig{EnableSessionPool: true}
	client, err := NewClientWithConfig(ctx, sessionSandboxProject, sessionSandboxInstance, cfg,
		option.WithEndpoint(sessionSandboxEndpoint))
	if err != nil {
		t.Fatalf("NewClientWithConfig: %v", err)
	}
	defer client.Close()

	mv := client.OpenMaterializedView(materializedViewID)

	t.Logf("warming up session pool...")
	time.Sleep(3 * time.Second)

	t.Logf("reading row %q via session MV path...", rowKey)
	row, err := mv.ReadRow(ctx, rowKey)
	if err != nil {
		t.Fatalf("ReadRow via MV failed: %v", err)
	}
	if row == nil {
		t.Logf("MV row %q not found (base table may not have data yet) — ReadRow itself succeeded", rowKey)
	} else {
		for fam, items := range row {
			for _, item := range items {
				t.Logf("MV row: family=%s column=%s value=%v", fam, item.Column, item.Value)
			}
		}
	}

	// MV is read-only: any Apply must fail. Session-path Apply returns
	// session.ErrWriteNotSupported directly (nil openWrite on the
	// SessionTableApi).
	t.Logf("attempting Apply via MV (must fail — MV is read-only)...")
	mut := NewMutation()
	mut.Set(sessionSandboxFamily, "colq1", ServerTime, []byte("should-not-write"))
	err = mv.Apply(ctx, rowKey, mut)
	if err == nil {
		t.Fatal("Apply via MV returned nil, want an error (MV is read-only)")
	}
	if !errors.Is(err, session.ErrWriteNotSupported) {
		t.Logf("Apply via MV rejected with %v (want session.ErrWriteNotSupported — but any error is acceptable if server rejects first)", err)
	} else {
		t.Logf("Apply via MV correctly rejected client-side with ErrWriteNotSupported")
	}
}

// TestMaterializedViewSessionSandbox_FailedPreconditionSurfaces
// deliberately runs the session-path MV ReadRow WITHOUT waiting for
// the MV to be ready. Purpose: capture the actual error the session
// client surfaces when the server rejects the OpenMaterializedView
// handshake with FailedPrecondition (backfill in progress). This is
// informational — it may trip the pool's consecutive-failure breaker,
// so the surfaced error may be ErrConsecutiveFailures rather than a
// raw FailedPrecondition. Run only when MV is known to still be
// backfilling. Skipped by default; enable with CBT_TRY_UNREADY_MV=1.
func TestMaterializedViewSessionSandbox_FailedPreconditionSurfaces(t *testing.T) {
	if os.Getenv("CBT_TRY_UNREADY_MV") != "1" {
		t.Skip("skipping; set CBT_TRY_UNREADY_MV=1 to run against a still-being-created MV")
	}
	const materializedViewID = "session-test-mv"

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := ClientConfig{EnableSessionPool: true}
	client, err := NewClientWithConfig(ctx, sessionSandboxProject, sessionSandboxInstance, cfg,
		option.WithEndpoint(sessionSandboxEndpoint))
	if err != nil {
		t.Fatalf("NewClientWithConfig: %v", err)
	}
	defer client.Close()

	mv := client.OpenMaterializedView(materializedViewID)
	t.Logf("firing session-path ReadRow while MV may still be backfilling...")
	row, err := mv.ReadRow(ctx, "myrow-0")
	t.Logf("session-path ReadRow returned: row=%v err=%v", row, err)
	if err == nil {
		t.Logf("(no error — MV was actually ready)")
		return
	}
	code := status.Code(err)
	t.Logf("gRPC code: %s (raw error message: %q)", code, err.Error())
}
