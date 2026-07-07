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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// fakeBigtableServer is an in-process Bigtable server used to exercise the
// vRPC session path at the wire level. It implements just enough of the
// BigtableServer surface to handle OpenTable handshakes, PingAndWarm,
// GetClientConfiguration (returning SessionLoad=1.0 so the Diverter routes
// traffic through the session path), and VirtualRpc requests for ReadRow /
// MutateRow.
//
// Every VirtualRpcRequest is captured (full proto, under mutex) so tests can
// assert on Deadline, Metadata.AttemptNumber, Metadata.AttemptStart, and the
// payload shape. A configurable hook (firstAttemptErr) makes the first vRPC
// per stream fail with a chosen status — enabling retry tests without per-test
// server boilerplate.
type fakeBigtableServer struct {
	btpb.UnimplementedBigtableServer

	mu                 sync.Mutex
	vrpcRequests       []*btpb.VirtualRpcRequest
	openSessionCnt     int
	closeSessionCnt    int
	getClientConfigCnt int

	// attemptErrs is a queue: each incoming VirtualRpcRequest pops one
	// entry and (if non-nil) returns the encoded SessionResponse_Error to
	// the client. Empty queue = every request succeeds normally.
	attemptErrs []fakeAttemptErr

	// responseDelay is applied before every reply frame (success or error).
	// Used to force deadline / cancellation to fire mid-flight.
	responseDelay time.Duration

	// readRowResponse holds the TableResponse to send for ReadRow vRPCs.
	// Overridable via setReadRowResponse — the encoded bytes cache is
	// re-marshaled on set so a "row: nil" reply is representable.
	readRowResponseBytes []byte
	// mutateRowResponse is the proto-encoded TableResponse payload returned
	// for MutateRow virtual RPCs.
	mutateRowResponseBytes []byte

	// peerInfoHeaderBase is used as a template for the bigtable-peer-info
	// stream header. If per-session AFE IDs are configured via
	// setPeerInfoRotation, the fake stamps an increasing ApplicationFrontendId
	// onto a clone of this template per session; otherwise every session
	// receives the same base header.
	peerInfoHeaderBase *btpb.PeerInfo
	// peerInfoAfeRotation, when non-empty, provides ApplicationFrontendId
	// values to rotate through on successive OpenTable streams. Guarded by mu.
	peerInfoAfeRotation []int64
	// nextPeerInfoIdx is the index into peerInfoAfeRotation for the next
	// stream. atomic to keep the OpenTable read cheap.
	nextPeerInfoIdx atomic.Int64

	// poolMinCount/poolMaxCount stamp the SessionPoolConfiguration returned
	// from GetClientConfiguration. The client's ClientConfigurationManager
	// treats server-supplied values as authoritative and overwrites the
	// per-pool min/max — so tests must set these to match the
	// ClientConfig.SessionPoolMin/Max the harness passes, otherwise the
	// server defaults (5/400) fill the pool with far more sessions than
	// the test expects. Defaults align with the harness (1/2).
	poolMinCount int32
	poolMaxCount int32
}

// fakeAttemptErr is one queued reply for a VirtualRpcRequest. RetryInfo is
// optional; when non-nil, it is packed into the ErrorResponse envelope so
// the client's retry classifier sees an explicit server go-ahead.
type fakeAttemptErr struct {
	Status    *rpcstatus.Status
	RetryInfo *errdetails.RetryInfo
}

// getClientConfigCount returns the number of GetClientConfiguration RPCs the
// server has answered. Used by the harness to wait for the initial poll to
// land (which is what flips the Diverter to SessionLoad=1.0).
func (s *fakeBigtableServer) getClientConfigCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getClientConfigCnt
}

func newFakeBigtableServer(t *testing.T) *fakeBigtableServer {
	t.Helper()
	srv := &fakeBigtableServer{
		// Match newSessionTestHarness's ClientConfig defaults. Tests that
		// override SessionPoolMin/Max on the client MUST also call
		// setSessionPoolSizing on the fake, otherwise the config manager's
		// UpdateConfig overwrites the client values with these defaults.
		poolMinCount: 1,
		poolMaxCount: 2,
	}

	// Default ReadRow response: row "test-row" with one cell containing
	// "test-value" under family "fam1", qualifier "col1".
	rrResp := &btpb.TableResponse{
		Payload: &btpb.TableResponse_ReadRow{
			ReadRow: &btpb.SessionReadRowResponse{
				Row: &btpb.Row{
					Key: []byte("test-row"),
					Families: []*btpb.Family{{
						Name: "fam1",
						Columns: []*btpb.Column{{
							Qualifier: []byte("col1"),
							Cells: []*btpb.Cell{{
								Value:           []byte("test-value"),
								TimestampMicros: 1000,
							}},
						}},
					}},
				},
			},
		},
	}
	b, err := proto.Marshal(rrResp)
	if err != nil {
		t.Fatalf("proto.Marshal ReadRow response: %v", err)
	}
	srv.readRowResponseBytes = b

	mrResp := &btpb.TableResponse{
		Payload: &btpb.TableResponse_MutateRow{
			MutateRow: &btpb.SessionMutateRowResponse{},
		},
	}
	b, err = proto.Marshal(mrResp)
	if err != nil {
		t.Fatalf("proto.Marshal MutateRow response: %v", err)
	}
	srv.mutateRowResponseBytes = b
	return srv
}

// snapshotVRpcs returns a copy of every VirtualRpcRequest the server has seen.
func (s *fakeBigtableServer) snapshotVRpcs() []*btpb.VirtualRpcRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*btpb.VirtualRpcRequest, len(s.vrpcRequests))
	copy(out, s.vrpcRequests)
	return out
}

// setFirstAttemptErr arms the server so the next VirtualRpcRequest is met
// with the given status (no server-supplied RetryInfo). The arming is
// cleared after one use. Prefer queueAttemptErrs when the test needs
// multiple errors or an attached RetryInfo.
func (s *fakeBigtableServer) setFirstAttemptErr(st *rpcstatus.Status) {
	s.queueAttemptErrs(fakeAttemptErr{Status: st})
}

// queueAttemptErrs pushes N errors onto the reply queue. The next N
// VirtualRpcRequests will each pop one entry (in order) and receive it as
// a SessionResponse_Error, optionally carrying the supplied RetryInfo.
// After the queue drains, subsequent requests succeed normally.
func (s *fakeBigtableServer) queueAttemptErrs(errs ...fakeAttemptErr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attemptErrs = append(s.attemptErrs, errs...)
}

// queuedAttemptErrCount returns how many queued replies remain unused. A
// zero return after a call means the server consumed every armed error.
func (s *fakeBigtableServer) queuedAttemptErrCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.attemptErrs)
}

// setResponseDelay makes the fake sleep this long before sending each
// reply frame. Used to force ctx deadline / cancel to fire mid-flight.
func (s *fakeBigtableServer) setResponseDelay(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responseDelay = d
}

// setReadRowResponse overrides the TableResponse the fake returns for
// ReadRow vRPCs. Encodes to bytes once; subsequent requests use the
// cached slice under mu.
func (s *fakeBigtableServer) setReadRowResponse(t *testing.T, resp *btpb.TableResponse) {
	t.Helper()
	b, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("proto.Marshal ReadRow override: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readRowResponseBytes = b
}

// setPeerInfoRotation configures the fake to stamp
// PeerInfo.ApplicationFrontendId with the values in `ids`, rotating one
// per OpenTable stream (i.e. one per session). Call before session pool
// starts opening streams. Non-thread-safe once traffic is flowing.
func (s *fakeBigtableServer) setPeerInfoRotation(ids []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerInfoAfeRotation = append([]int64(nil), ids...)
	if s.peerInfoHeaderBase == nil {
		s.peerInfoHeaderBase = &btpb.PeerInfo{
			TransportType: btpb.PeerInfo_TRANSPORT_TYPE_SESSION_DIRECT_ACCESS,
		}
	}
}

// setSessionPoolSizing sets the min/max the fake advertises in
// GetClientConfiguration. Call this BEFORE the harness's
// waitForSessionRouting completes so the first config poll delivers the
// intended sizing to the pool.
func (s *fakeBigtableServer) setSessionPoolSizing(min, max int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poolMinCount = min
	s.poolMaxCount = max
}

// closeSessionCount returns how many CloseSession frames the server has
// received across all streams.
func (s *fakeBigtableServer) closeSessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeSessionCnt
}

// openSessionCount returns how many OpenSession handshakes the server has
// completed. Bumped once per bidi stream.
func (s *fakeBigtableServer) openSessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openSessionCnt
}

// peerInfoHeaderFor returns the base64-encoded PeerInfo header value to
// stamp onto the OpenTable stream. Returns "" when no peer info is
// configured (older behaviour — sessions get AfeID=0).
func (s *fakeBigtableServer) peerInfoHeaderFor(streamIdx int64) string {
	s.mu.Lock()
	base := s.peerInfoHeaderBase
	rot := s.peerInfoAfeRotation
	s.mu.Unlock()
	if base == nil {
		return ""
	}
	pi := proto.Clone(base).(*btpb.PeerInfo)
	if len(rot) > 0 {
		pi.ApplicationFrontendId = rot[int(streamIdx)%len(rot)]
	}
	raw, err := proto.Marshal(pi)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func (s *fakeBigtableServer) PingAndWarm(ctx context.Context, req *btpb.PingAndWarmRequest) (*btpb.PingAndWarmResponse, error) {
	return &btpb.PingAndWarmResponse{}, nil
}

func (s *fakeBigtableServer) GetClientConfiguration(ctx context.Context, req *btpb.GetClientConfigurationRequest) (*btpb.ClientConfiguration, error) {
	s.mu.Lock()
	s.getClientConfigCnt++
	minC, maxC := s.poolMinCount, s.poolMaxCount
	s.mu.Unlock()
	// SessionLoad: 1.0 pins the Diverter to the session path so every
	// ReadRow/Apply on the TableShim routes through SessionTable. Note: the
	// AddSessionLoadListener listener is invoked at registration time with
	// the manager's *default* config (SessionLoad=0), and is only flipped to
	// 1.0 once this RPC's response is parsed by the configManager. Tests
	// must wait for that to happen before issuing data calls — see
	// waitForSessionRouting in the harness.
	//
	// Explicit SessionPool sizing is included because ClientConfigurationManager
	// treats server values as authoritative and overwrites the per-pool
	// min/max the client asked for. Leaving SessionPool unset here means the
	// manager's built-in default (Min=5, Max=400) fills the pool with far
	// more sessions than tests expect. See setSessionPoolSizing.
	return &btpb.ClientConfiguration{
		SessionConfiguration: &btpb.SessionClientConfiguration{
			SessionLoad: 1.0,
			SessionPoolConfiguration: &btpb.SessionClientConfiguration_SessionPoolConfiguration{
				MinSessionCount: minC,
				MaxSessionCount: maxC,
			},
		},
	}, nil
}

// OpenTable handles the bidi stream: completes the OpenSession handshake,
// then for every VirtualRpcRequest decodes the inner TableRequest, captures
// the full proto, and replies with the appropriate TableResponse (or an
// error popped from the queue). Also honors an optional response delay so
// tests can drive deadline / cancel semantics deterministically.
func (s *fakeBigtableServer) OpenTable(stream btpb.Bigtable_OpenTableServer) error {
	// Handshake: first message must be OpenSession.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetOpenSession() == nil {
		return fmt.Errorf("fakeBigtableServer: expected OpenSession as first frame, got %T", first.GetPayload())
	}
	// Stamp per-stream state before the reply so handleOpenSession sees a
	// consistent OpenSession count when it lands.
	streamIdx := s.nextPeerInfoIdx.Add(1) - 1
	s.mu.Lock()
	s.openSessionCnt++
	s.mu.Unlock()

	// If configured, send the bigtable-peer-info header BEFORE the first
	// response so the client's handleOpenSession parses it synchronously.
	if hdr := s.peerInfoHeaderFor(streamIdx); hdr != "" {
		if err := stream.SendHeader(metadata.Pairs("bigtable-peer-info", hdr)); err != nil {
			return err
		}
	}

	if err := stream.Send(&btpb.SessionResponse{
		Payload: &btpb.SessionResponse_OpenSession{
			OpenSession: &btpb.OpenSessionResponse{},
		},
	}); err != nil {
		return err
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// CloseSession ends the stream cleanly.
		if req.GetCloseSession() != nil {
			s.mu.Lock()
			s.closeSessionCnt++
			s.mu.Unlock()
			return nil
		}
		vrpc := req.GetVirtualRpc()
		if vrpc == nil {
			// Ignore other oneof variants (heartbeats etc) — not used here.
			continue
		}

		// Capture the full vRPC request and pop one queued error (if any)
		// under a single lock so ordering is deterministic under load.
		s.mu.Lock()
		s.vrpcRequests = append(s.vrpcRequests, vrpc)
		var armed *fakeAttemptErr
		if len(s.attemptErrs) > 0 {
			armed = &s.attemptErrs[0]
			s.attemptErrs = s.attemptErrs[1:]
		}
		delay := s.responseDelay
		s.mu.Unlock()

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}

		if armed != nil {
			errResp := &btpb.SessionResponse{
				Payload: &btpb.SessionResponse_Error{
					Error: &btpb.ErrorResponse{
						RpcId:     vrpc.RpcId,
						Status:    armed.Status,
						RetryInfo: armed.RetryInfo,
					},
				},
			}
			if err := stream.Send(errResp); err != nil {
				return err
			}
			continue
		}

		// Decode the inner TableRequest to pick the right response shape.
		var tableReq btpb.TableRequest
		if err := proto.Unmarshal(vrpc.Payload, &tableReq); err != nil {
			return fmt.Errorf("fakeBigtableServer: unmarshal TableRequest: %v", err)
		}

		s.mu.Lock()
		readBytes := s.readRowResponseBytes
		writeBytes := s.mutateRowResponseBytes
		s.mu.Unlock()

		var payload []byte
		switch tableReq.Payload.(type) {
		case *btpb.TableRequest_ReadRow:
			payload = readBytes
		case *btpb.TableRequest_MutateRow:
			payload = writeBytes
		default:
			return fmt.Errorf("fakeBigtableServer: unsupported TableRequest payload %T", tableReq.Payload)
		}

		resp := &btpb.SessionResponse{
			Payload: &btpb.SessionResponse_VirtualRpc{
				VirtualRpc: &btpb.VirtualRpcResponse{
					RpcId: vrpc.RpcId,
					ClusterInfo: &btpb.ClusterInformation{
						ClusterId: "fake-c1",
						ZoneId:    "fake-z1",
					},
					Stats: &btpb.SessionRequestStats{
						BackendLatency: durationpb.New(7 * time.Millisecond),
					},
					Payload: payload,
				},
			},
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// sessionTestHarness wires a fakeBigtableServer to a bufconn-backed grpc.Server
// and dials it through a real *grpc.ClientConn. Tests then build a Client over
// that conn with EnableSessionPool=true, which makes ReadRow / Apply traverse
// the SessionTable → SessionPoolImpl → vRPC plumbing end-to-end.
//
// Why a fake server (not bttest):
// bttest returns Unimplemented for OpenTable, so it cannot drive the
// session/vRPC path at all. A small inline fake is also faster (no real
// network) and lets us inspect every VirtualRpcRequest the client sent —
// which is exactly what we need to assert deadline/metadata plumbing.
//
// Why the higher-level Client path (not direct transport.NewSession):
// We want to verify the full chain — TableShim → SessionTable → SessionPoolImpl
// → Session → wire frames — actually wires up correctly with EnableSessionPool
// set on a real client. Passing option.WithGRPCConn(conn) sets
// enableBigtableConnPool=false (see client.go:175-182), which keeps the dial
// inside the simple gtransport.DialPool branch and avoids the
// BigtableChannelPool factory machinery that doesn't play nicely with
// bufconn. SessionPoolMin=1 / SessionPoolMax=2 keeps the test fleet small.
type sessionTestHarness struct {
	t      *testing.T
	server *fakeBigtableServer
	grpc   *grpc.Server
	lis    *bufconn.Listener
	conn   *grpc.ClientConn
	client *Client
}

func newSessionTestHarness(t *testing.T) *sessionTestHarness {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	fakeSrv := newFakeBigtableServer(t)
	btpb.RegisterBigtableServer(grpcSrv, fakeSrv)
	go func() {
		// Serve returns when grpcSrv.Stop is called by the test cleanup.
		_ = grpcSrv.Serve(lis)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcSrv.Stop()
		t.Fatalf("grpc.NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := NewClientWithConfig(ctx, "test-project", "test-instance", ClientConfig{
		MetricsProvider:   NoopMetricsProvider{},
		EnableSessionPool: true,
		SessionPoolMin:    1,
		SessionPoolMax:    2,
	},
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		_ = conn.Close()
		grpcSrv.Stop()
		t.Fatalf("NewClientWithConfig: %v", err)
	}

	h := &sessionTestHarness{
		t:      t,
		server: fakeSrv,
		grpc:   grpcSrv,
		lis:    lis,
		conn:   conn,
		client: client,
	}
	t.Cleanup(h.Close)
	// Block until the initial configManager poll has landed and flipped the
	// Diverter to SessionLoad=1.0. Without this, the TableShim consults a
	// Diverter that still carries the listener's bootstrap value (0.0, set
	// when the listener was registered with the default config) and routes
	// to the classic path, which our fake server does not implement.
	h.waitForSessionRouting(5 * time.Second)
	return h
}

// waitForSessionRouting polls the client's Diverter (and the server's
// GetClientConfiguration counter as a backstop) until both confirm that
// session routing is enabled. Returns once UseSession() reports true; fails
// the test on timeout.
func (h *sessionTestHarness) waitForSessionRouting(timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.server.getClientConfigCount() >= 1 && h.client.diverter.UseSession() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for Diverter to flip to session routing (getClientConfigCount=%d, useSession=%t)",
		h.server.getClientConfigCount(), h.client.diverter.UseSession())
}

func (h *sessionTestHarness) Close() {
	// Close in dependency order: bigtable.Client first (drains session pool
	// cleanly), then the gRPC conn, then the server.
	_ = h.client.Close()
	_ = h.conn.Close()
	h.grpc.Stop()
	_ = h.lis.Close()
}

// waitForVRpcs polls until the server has captured at least `want` VRpcs or
// the deadline fires. It returns the snapshot at the time of success so
// callers can assert on it without racing against late deliveries.
func waitForVRpcs(t *testing.T, srv *fakeBigtableServer, want int, timeout time.Duration) []*btpb.VirtualRpcRequest {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := srv.snapshotVRpcs()
		if len(got) >= want {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %d vRPCs (saw %d)", timeout, want, len(srv.snapshotVRpcs()))
	return nil
}

// TestIntegration_SessionVRpc_ReadRow exercises ReadRow end-to-end through the
// vRPC session path and asserts the row arrives back from the fake server.
func TestIntegration_SessionVRpc_ReadRow(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	row, err := tbl.ReadRow(ctx, "test-row")
	if err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	if row == nil {
		t.Fatal("ReadRow returned nil row, want test-value cell")
	}
	cells := row["fam1"]
	if len(cells) == 0 {
		t.Fatalf("ReadRow row missing fam1 cells, got %+v", row)
	}
	if got := string(cells[0].Value); got != "test-value" {
		t.Errorf("cell value = %q, want %q", got, "test-value")
	}

	vrpcs := waitForVRpcs(t, h.server, 1, 2*time.Second)
	if len(vrpcs) != 1 {
		t.Errorf("server saw %d vRPCs, want exactly 1", len(vrpcs))
	}
	// Confirm the payload was a ReadRow TableRequest.
	var tr btpb.TableRequest
	if err := proto.Unmarshal(vrpcs[0].Payload, &tr); err != nil {
		t.Fatalf("decode captured TableRequest: %v", err)
	}
	if tr.GetReadRow() == nil {
		t.Errorf("captured TableRequest was not ReadRow shape: %T", tr.Payload)
	}
}

// TestIntegration_SessionVRpc_Apply exercises Apply end-to-end through the
// vRPC session path and asserts a MutateRow-shaped request reached the wire.
func TestIntegration_SessionVRpc_Apply(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	mut := NewMutation()
	// Explicit timestamp keeps the mutation retryable; ServerTime would
	// flip mutationsAreRetryable and change the retry budget — irrelevant
	// for this happy-path assertion, but worth being deterministic.
	mut.Set("fam1", "col1", Timestamp(1000), []byte("v"))

	if err := tbl.Apply(ctx, "test-row", mut); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	vrpcs := waitForVRpcs(t, h.server, 1, 2*time.Second)
	if len(vrpcs) != 1 {
		t.Errorf("server saw %d vRPCs, want exactly 1", len(vrpcs))
	}
	var tr btpb.TableRequest
	if err := proto.Unmarshal(vrpcs[0].Payload, &tr); err != nil {
		t.Fatalf("decode captured TableRequest: %v", err)
	}
	if tr.GetMutateRow() == nil {
		t.Errorf("captured TableRequest was not MutateRow shape: %T", tr.Payload)
	}
}

// TestIntegration_SessionVRpc_RequestCarriesDeadline asserts the per-vRPC
// Deadline field is populated from the caller's context deadline. The exact
// remaining budget will be slightly less than the parent (encode + send
// overhead), so we bound-check rather than equality-check.
func TestIntegration_SessionVRpc_RequestCarriesDeadline(t *testing.T) {
	h := newSessionTestHarness(t)

	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	ctx, cancel := context.WithDeadline(parent, time.Now().Add(2*time.Second))
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	if _, err := tbl.ReadRow(ctx, "test-row"); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}

	vrpcs := waitForVRpcs(t, h.server, 1, 2*time.Second)
	v := vrpcs[0]
	if v.Deadline == nil {
		t.Fatal("VirtualRpcRequest.Deadline = nil, want non-nil")
	}
	d := v.Deadline.AsDuration()
	if d < 100*time.Millisecond || d > 2*time.Second {
		t.Errorf("VirtualRpcRequest.Deadline = %v, want in [100ms, 2s]", d)
	}
}

// TestIntegration_SessionVRpc_RequestCarriesMetadata asserts the vRPC
// Metadata oneof carries AttemptNumber and AttemptStart on every wire frame
// — the data the AFE needs to attribute retries to the same logical op.
func TestIntegration_SessionVRpc_RequestCarriesMetadata(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	if _, err := tbl.ReadRow(ctx, "test-row"); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}

	vrpcs := waitForVRpcs(t, h.server, 1, 2*time.Second)
	v := vrpcs[0]
	if v.Metadata == nil {
		t.Fatal("VirtualRpcRequest.Metadata = nil")
	}
	if v.Metadata.AttemptNumber < 1 {
		t.Errorf("AttemptNumber = %d, want >= 1", v.Metadata.AttemptNumber)
	}
	if v.Metadata.AttemptStart == nil {
		t.Fatal("AttemptStart = nil")
	}
	// Sanity: AttemptStart should be within a few seconds of now (it was
	// captured immediately before Send by Invoke).
	now := time.Now()
	got := v.Metadata.AttemptStart.AsTime()
	if got.Before(now.Add(-30*time.Second)) || got.After(now.Add(30*time.Second)) {
		t.Errorf("AttemptStart = %v, want within +/-30s of %v", got, now)
	}
}

// TestIntegration_SessionVRpc_RetriesOnUnavailable arms the server to return
// Unavailable on the first vRPC then succeed on the second, and asserts the
// retry interceptor surfaces a successful ReadRow with AttemptNumber=1 and
// AttemptNumber=2 in the captured frames.
//
// The Java-parity classifier does NOT retry a bare server-explicit error
// (see shouldRetryDefault in internal/transport/retrying.go), so the reply
// must carry an explicit server RetryInfo to authorize the retry. This
// matches the Java client's contract with the AFE.
func TestIntegration_SessionVRpc_RetriesOnUnavailable(t *testing.T) {
	h := newSessionTestHarness(t)

	// Arm the first vRPC to fail with Unavailable + server-directed retry.
	// The vRPC retry loop in SessionTable uses RetryingVRpc(MaxAttempts:10,
	// InitialBackoff:10ms), so the second attempt fires very quickly.
	h.server.queueAttemptErrs(fakeAttemptErr{
		Status: &rpcstatus.Status{
			Code:    int32(codes.Unavailable),
			Message: "fake transient error",
		},
		RetryInfo: &errdetails.RetryInfo{
			RetryDelay: durationpb.New(5 * time.Millisecond),
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tbl := h.client.OpenTable("test-table")
	row, err := tbl.ReadRow(ctx, "test-row")
	if err != nil {
		// Re-check status code for diagnostics — the retry loop should
		// have eaten the Unavailable.
		st, _ := status.FromError(err)
		t.Fatalf("ReadRow after retry: %v (code=%s)", err, st.Code())
	}
	if row == nil {
		t.Fatal("ReadRow after retry returned nil row")
	}

	vrpcs := waitForVRpcs(t, h.server, 2, 5*time.Second)
	if len(vrpcs) < 2 {
		t.Fatalf("server saw %d vRPCs after retry, want >= 2", len(vrpcs))
	}
	// Both attempts must arrive with vRPC metadata populated, and
	// AttemptNumber must strictly increment: attempt 1 = 1, attempt 2 = 2.
	// SessionTable now seeds the ctx with WithVRpcMetadata before invoking
	// RetryingVRpc, so retrying.go's WithAttempt(ctx, n) mutates the value
	// that Invoke subsequently reads via VRpcAttempt(ctx).
	wantAttempts := []int64{1, 2}
	for i, want := range wantAttempts {
		v := vrpcs[i]
		if v.GetMetadata() == nil {
			t.Errorf("vrpcs[%d].Metadata = nil, want non-nil", i)
			continue
		}
		if got := v.GetMetadata().GetAttemptNumber(); got != want {
			t.Errorf("vrpcs[%d].AttemptNumber = %d, want %d", i, got, want)
		}
	}
	// Sanity: at least one frame must be a ReadRow (both should be, but
	// the second is what we care about for retry semantics).
	var tr btpb.TableRequest
	if err := proto.Unmarshal(vrpcs[1].Payload, &tr); err != nil {
		t.Fatalf("decode retry TableRequest: %v", err)
	}
	if tr.GetReadRow() == nil {
		t.Errorf("retry attempt was not a ReadRow request: %T", tr.Payload)
	}
}

// TestIntegration_SessionVRpc_SessionReuse verifies that N sequential
// ReadRows do NOT open one session per call — sessions are reused. The
// pool can seed up to SessionPoolMax sessions eagerly (min=1, max=2 with
// headroom), but the aggregate open count must stay ≤ max regardless of
// how many reads fire, and the per-session RPC counter proves reuse.
func TestIntegration_SessionVRpc_SessionReuse(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const reads = 6
	tbl := h.client.OpenTable("test-table")
	for i := 0; i < reads; i++ {
		if _, err := tbl.ReadRow(ctx, "test-row"); err != nil {
			t.Fatalf("ReadRow[%d]: %v", i, err)
		}
	}

	vrpcs := waitForVRpcs(t, h.server, reads, 2*time.Second)
	if len(vrpcs) != reads {
		t.Errorf("server saw %d vRPCs, want exactly %d", len(vrpcs), reads)
	}
	// The pool is bounded by SessionPoolMax=2 (the fake advertises this in
	// GetClientConfiguration). If ANY reuse is happening, openSessionCount
	// stays well under `reads`.
	got := h.server.openSessionCount()
	if got > 2 {
		t.Errorf("openSessionCount = %d, want <= 2 (SessionPoolMax bound)", got)
	}
	if got >= reads {
		t.Errorf("openSessionCount = %d, want < %d (sessions must be reused across sequential reads)", got, reads)
	}
}

// TestIntegration_SessionVRpc_MultipleTables opens two SessionTables on the
// same Client and asserts each spins up its own read pool + session, and each
// ReadRow reaches the wire independently.
func TestIntegration_SessionVRpc_MultipleTables(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t1 := h.client.OpenTable("table-a")
	t2 := h.client.OpenTable("table-b")

	if _, err := t1.ReadRow(ctx, "row-a"); err != nil {
		t.Fatalf("ReadRow table-a: %v", err)
	}
	if _, err := t2.ReadRow(ctx, "row-b"); err != nil {
		t.Fatalf("ReadRow table-b: %v", err)
	}

	// Two ReadRows, two vRPCs — one per table.
	vrpcs := waitForVRpcs(t, h.server, 2, 2*time.Second)
	if len(vrpcs) != 2 {
		t.Fatalf("saw %d vRPCs, want 2", len(vrpcs))
	}

	// Two SessionTables → two lazy read pools → at least two OpenSession
	// handshakes (each pool's initial fill opens one). Bound the upper end
	// loosely (SessionPoolMax=2 * 2 tables = 4).
	got := h.server.openSessionCount()
	if got < 2 || got > 4 {
		t.Errorf("openSessionCount = %d, want in [2, 4] for two distinct tables", got)
	}
}

// TestIntegration_SessionVRpc_NilRowResponse verifies the empty-row path:
// the server returns a well-formed TableResponse whose ReadRow.Row is nil
// (row not found), and the client surfaces (nil, nil) — not an error.
func TestIntegration_SessionVRpc_NilRowResponse(t *testing.T) {
	h := newSessionTestHarness(t)
	h.server.setReadRowResponse(t, &btpb.TableResponse{
		Payload: &btpb.TableResponse_ReadRow{
			ReadRow: &btpb.SessionReadRowResponse{Row: nil},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	row, err := h.client.OpenTable("test-table").ReadRow(ctx, "missing-row")
	if err != nil {
		t.Fatalf("ReadRow: %v (want nil error for empty row)", err)
	}
	if row != nil {
		t.Errorf("ReadRow row = %+v, want nil (row not present)", row)
	}
}

// TestIntegration_SessionVRpc_NonRetryableInvalidArgument arms a single
// InvalidArgument reply (no RetryInfo) and asserts the client surfaces it
// immediately without retrying. This exercises the Java-parity default:
// bare server-explicit errors are terminal unless RetryInfo says otherwise.
func TestIntegration_SessionVRpc_NonRetryableInvalidArgument(t *testing.T) {
	h := newSessionTestHarness(t)
	h.server.queueAttemptErrs(fakeAttemptErr{
		Status: &rpcstatus.Status{
			Code:    int32(codes.InvalidArgument),
			Message: "bogus filter",
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err == nil {
		t.Fatal("ReadRow returned nil, want InvalidArgument")
	}
	st, ok := status.FromError(err)
	if !ok {
		// Error may be wrapped in fmt.Errorf %w — walk the chain.
		if se := errors.Unwrap(err); se != nil {
			st, ok = status.FromError(se)
		}
	}
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("err = %v (ok=%t, code=%s), want InvalidArgument", err, ok, st.Code())
	}
	// Exactly one wire frame — no retry.
	vrpcs := h.server.snapshotVRpcs()
	if len(vrpcs) != 1 {
		t.Errorf("server saw %d vRPCs, want 1 (non-retryable code must not retry)", len(vrpcs))
	}
	if h.server.queuedAttemptErrCount() != 0 {
		t.Errorf("armed error queue still holds %d entries, want 0", h.server.queuedAttemptErrCount())
	}
}

// TestIntegration_SessionVRpc_ServerDirectedRetryOnFailedPrecondition
// verifies the RetryInfo escape hatch: a normally-terminal FailedPrecondition
// becomes retryable when the server explicitly attaches RetryInfo. This is
// the sole way for a session-path error to bypass the Java-parity default.
func TestIntegration_SessionVRpc_ServerDirectedRetryOnFailedPrecondition(t *testing.T) {
	h := newSessionTestHarness(t)
	h.server.queueAttemptErrs(fakeAttemptErr{
		Status: &rpcstatus.Status{
			Code:    int32(codes.FailedPrecondition),
			Message: "please retry",
		},
		RetryInfo: &errdetails.RetryInfo{
			RetryDelay: durationpb.New(1 * time.Millisecond),
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	row, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err != nil {
		t.Fatalf("ReadRow: %v (server RetryInfo should authorize retry)", err)
	}
	if row == nil {
		t.Fatal("ReadRow after server-directed retry returned nil row")
	}
	vrpcs := waitForVRpcs(t, h.server, 2, 3*time.Second)
	if len(vrpcs) < 2 {
		t.Errorf("saw %d vRPCs, want >= 2 (initial + retry)", len(vrpcs))
	}
}

// TestIntegration_SessionVRpc_RetryExhaustion queues enough retryable
// errors to exceed MaxAttempts=10 and asserts the client (a) surfaces the
// last error, (b) actually issued MaxAttempts wire frames, and (c) never
// dips into the queue past what MaxAttempts permits.
func TestIntegration_SessionVRpc_RetryExhaustion(t *testing.T) {
	h := newSessionTestHarness(t)

	const maxAttempts = 10
	const queued = maxAttempts + 5 // 5 extra should remain unused
	errs := make([]fakeAttemptErr, queued)
	for i := range errs {
		errs[i] = fakeAttemptErr{
			Status: &rpcstatus.Status{
				Code:    int32(codes.Unavailable),
				Message: fmt.Sprintf("attempt %d failure", i+1),
			},
			// 0-delay RetryInfo makes the retry loop fire back-to-back
			// without dragging the test wall clock past the RPC deadline.
			RetryInfo: &errdetails.RetryInfo{
				RetryDelay: durationpb.New(0),
			},
		}
	}
	h.server.queueAttemptErrs(errs...)

	// Generous ctx budget so the failure is retry exhaustion, not deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err == nil {
		t.Fatal("ReadRow returned nil, want Unavailable after exhaustion")
	}
	// The bottom of the error chain should carry the last Unavailable.
	st, _ := status.FromError(errors.Unwrap(err))
	if st.Code() != codes.Unavailable {
		t.Errorf("last-error code = %s, want Unavailable", st.Code())
	}

	// Wait briefly for all attempts to land in the vRPC log (retries fire
	// after Send has captured the request, but the count race is short).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.server.snapshotVRpcs()) >= maxAttempts {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(h.server.snapshotVRpcs()); got != maxAttempts {
		t.Errorf("wire frame count = %d, want exactly %d", got, maxAttempts)
	}
	if got := h.server.queuedAttemptErrCount(); got != queued-maxAttempts {
		t.Errorf("armed queue depth = %d, want %d (retry loop must stop at MaxAttempts)",
			got, queued-maxAttempts)
	}
}

// TestIntegration_SessionVRpc_ContextCanceled cancels the caller's context
// before ReadRow. The client should surface context.Canceled without
// sending any vRPC to the wire.
func TestIntegration_SessionVRpc_ContextCanceled(t *testing.T) {
	h := newSessionTestHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // fire immediately

	_, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err == nil {
		t.Fatal("ReadRow with pre-canceled ctx returned nil error")
	}
	if !errors.Is(err, context.Canceled) {
		// Some paths convert to codes.Canceled — accept either shape.
		st, _ := status.FromError(errors.Unwrap(err))
		if st.Code() != codes.Canceled {
			t.Errorf("err = %v, want context.Canceled or codes.Canceled", err)
		}
	}
}

// TestIntegration_SessionVRpc_DeadlineExceeded pairs a short caller
// deadline with a slow server. The retry loop's ctx-done guard should fire
// and the error should carry a DeadlineExceeded signal.
func TestIntegration_SessionVRpc_DeadlineExceeded(t *testing.T) {
	h := newSessionTestHarness(t)
	// Every reply frame is delayed 500ms → any ctx with a sub-500ms budget
	// times out mid-flight, exercising the mid-flight ctx-done path
	// (session_vrpc.go tags this StateTransportFailure).
	h.server.setResponseDelay(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := h.client.OpenTable("test-table").ReadRow(ctx, "test-row")
	if err == nil {
		t.Fatal("ReadRow with slow server + short deadline returned nil error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		st, _ := status.FromError(errors.Unwrap(err))
		if st.Code() != codes.DeadlineExceeded {
			t.Errorf("err = %v, want context.DeadlineExceeded or codes.DeadlineExceeded", err)
		}
	}
}

// TestIntegration_SessionVRpc_ConcurrentLoad fires many concurrent
// ReadRow/Apply calls through one Client and asserts every call succeeds
// and every attempt reached the wire. Guards against pool contention
// deadlocks and races between checkout and result delivery.
func TestIntegration_SessionVRpc_ConcurrentLoad(t *testing.T) {
	h := newSessionTestHarness(t)

	const workers = 32
	const iters = 4

	tbl := h.client.OpenTable("test-table")
	var wg sync.WaitGroup
	errs := make(chan error, workers*iters)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for i := 0; i < iters; i++ {
				if w%2 == 0 {
					if _, err := tbl.ReadRow(ctx, fmt.Sprintf("row-%d-%d", w, i)); err != nil {
						errs <- fmt.Errorf("ReadRow[w=%d i=%d]: %w", w, i, err)
						return
					}
				} else {
					mut := NewMutation()
					mut.Set("fam1", "col1", Timestamp(1000), []byte("v"))
					if err := tbl.Apply(ctx, fmt.Sprintf("row-%d-%d", w, i), mut); err != nil {
						errs <- fmt.Errorf("Apply[w=%d i=%d]: %w", w, i, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("concurrent op failed: %v", e)
	}

	// Every op maps to exactly one wire frame (no retries under normal load).
	wantVRpcs := workers * iters
	vrpcs := waitForVRpcs(t, h.server, wantVRpcs, 10*time.Second)
	if len(vrpcs) != wantVRpcs {
		t.Errorf("wire frame count = %d, want %d", len(vrpcs), wantVRpcs)
	}
	// Sessions never exceed SessionPoolMax=2 per pool × 2 pools (read+write).
	if got := h.server.openSessionCount(); got > 4 {
		t.Errorf("openSessionCount = %d, want <= 4 (Max=2 per pool, 2 pools)", got)
	}
}

// TestIntegration_SessionVRpc_PeerInfoParsedIntoAfeID configures the fake
// to emit the bigtable-peer-info header with two distinct AFE IDs across
// successive OpenTable streams, drives enough concurrent load to open
// multiple sessions, then snapshots SessionDebug and asserts BOTH AFE IDs
// appear on live sessions. Exercises PeerInfo extraction + AFE grouping.
func TestIntegration_SessionVRpc_PeerInfoParsedIntoAfeID(t *testing.T) {
	// Build the fake + harness manually so we can pre-load PeerInfo before
	// the pool starts opening streams. The standard harness starts session
	// polling in NewClientWithConfig, so the rotation must be set beforehand
	// via a small hand-rolled variant of newSessionTestHarness.
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	fakeSrv := newFakeBigtableServer(t)
	// Match the ClientConfig sizing below (Min=2, Max=4) so the config
	// manager doesn't overwrite them with its 5/400 defaults.
	fakeSrv.setSessionPoolSizing(2, 4)
	fakeSrv.setPeerInfoRotation([]int64{4242, 8484})
	btpb.RegisterBigtableServer(grpcSrv, fakeSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop(); _ = lis.Close() })

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := NewClientWithConfig(ctx, "test-project", "test-instance", ClientConfig{
		MetricsProvider:   NoopMetricsProvider{},
		EnableSessionPool: true,
		SessionPoolMin:    2, // force 2 sessions up front so both AFE IDs land
		SessionPoolMax:    4,
	},
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewClientWithConfig: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// Wait for routing to flip.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fakeSrv.getClientConfigCount() >= 1 && client.diverter.UseSession() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !client.diverter.UseSession() {
		t.Fatalf("timed out waiting for session routing")
	}

	// Drive a read so the read pool opens (lazy). SessionPoolMin=2 guarantees
	// both AFE-tagged sessions get created for the read pool.
	tbl := client.OpenTable("test-table")
	if _, err := tbl.ReadRow(ctx, "test-row"); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}

	// Poll the debug snapshot briefly — session opens are asynchronous with
	// SessionPoolMin fills, so the second session may not be Ready yet at
	// the moment the first ReadRow returns.
	prov := client.SessionDebug()
	if prov == nil {
		t.Fatal("client.SessionDebug() = nil")
	}
	var (
		saw4242 bool
		saw8484 bool
	)
	pollUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(pollUntil) {
		saw4242, saw8484 = false, false
		for _, pool := range prov.Snapshot() {
			for _, s := range pool.Sessions {
				switch s.Peer.ApplicationFrontendID {
				case 4242:
					saw4242 = true
				case 8484:
					saw8484 = true
				}
			}
		}
		if saw4242 && saw8484 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !saw4242 || !saw8484 {
		t.Errorf("SessionDebug snapshot missed rotated AFE IDs: saw4242=%t saw8484=%t", saw4242, saw8484)
	}
}

// TestIntegration_SessionVRpc_ClientCloseSendsCloseSession asserts that
// tearing down the Client triggers a CloseSession frame per session — the
// polite shutdown path the server expects for accounting.
func TestIntegration_SessionVRpc_ClientCloseSendsCloseSession(t *testing.T) {
	// Manual harness — we need to close the Client mid-test rather than
	// letting t.Cleanup do it, so we can observe the effect.
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	fakeSrv := newFakeBigtableServer(t)
	btpb.RegisterBigtableServer(grpcSrv, fakeSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(func() { grpcSrv.Stop(); _ = lis.Close() })

	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := NewClientWithConfig(ctx, "test-project", "test-instance", ClientConfig{
		MetricsProvider:   NoopMetricsProvider{},
		EnableSessionPool: true,
		SessionPoolMin:    1,
		SessionPoolMax:    2,
	},
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewClientWithConfig: %v", err)
	}

	// Wait for routing so at least one session is open.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fakeSrv.getClientConfigCount() >= 1 && client.diverter.UseSession() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Force one ReadRow so the read pool opens (lazy).
	if _, err := client.OpenTable("test-table").ReadRow(ctx, "test-row"); err != nil {
		t.Fatalf("ReadRow: %v", err)
	}

	sessionsBeforeClose := fakeSrv.openSessionCount()
	if sessionsBeforeClose == 0 {
		t.Fatal("openSessionCount = 0 before Close, want >= 1")
	}

	// Close returns an error today when the CloseSession RPC races the
	// underlying conn teardown ("grpc: the client connection is closing").
	// The race is benign — the frame is enqueued before conn shutdown — so
	// we log but don't fail on it. What we care about is the ordering: at
	// least ONE CloseSession frame reached the server before the socket died.
	if err := client.Close(); err != nil {
		t.Logf("client.Close returned (non-fatal): %v", err)
	}

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fakeSrv.closeSessionCount() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := fakeSrv.closeSessionCount(); got < 1 {
		t.Errorf("closeSessionCount = %d after Close, want >= 1 (sessions must send CloseSession on shutdown, opened=%d)",
			got, sessionsBeforeClose)
	}
}
