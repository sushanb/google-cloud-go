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
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	btransport "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"
)

// brokenProtoMessage wraps a real proto.Message but overrides its
// ProtoMethods to return a Marshal function that always errors. This is the
// only reliable way to force proto.Marshal to return an error from a unit
// test — the public proto API doesn't expose hooks for forcing failure.
type brokenProtoMessage struct {
	real *btpb.OpenTableRequest
}

var brokenMethods = &protoiface.Methods{
	Marshal: func(in protoiface.MarshalInput) (protoiface.MarshalOutput, error) {
		return protoiface.MarshalOutput{}, errors.New("test: forced proto.Marshal failure")
	},
}

type brokenProtoRef struct {
	protoreflect.Message
}

func (brokenProtoRef) ProtoMethods() *protoiface.Methods { return brokenMethods }

func (b *brokenProtoMessage) Reset()         {}
func (b *brokenProtoMessage) String() string { return "brokenProtoMessage" }
func (b *brokenProtoMessage) ProtoReflect() protoreflect.Message {
	return brokenProtoRef{Message: b.real.ProtoReflect()}
}

func TestSessionManager_GetOrCreateSessionPool_MarshalErrorReturnsClassicFallback(t *testing.T) {
	// A SessionManager with enableSessionPool=true and a non-nil diverter
	// returns a TableShim wrapping SessionTable — even when the read
	// payload will fail proto.Marshal, because pool creation is now
	// deferred to first ReadRow (lazy pools). The test verifies both
	// halves of that contract:
	//   1. GetOrCreateSessionTable no longer inspects the payload — it
	//      returns a TableShim regardless.
	//   2. The lazyPool for the broken payload can be probed by calling
	//      the underlying SessionTable's readPool.get(), which surfaces
	//      the same marshal error a real ReadRow would trigger.
	diverter := btransport.NewDiverter(1.0)
	mgr := NewSessionManager(
		true,  // enableSessionPool
		false, // metricsEnabled
		true,  // disableRetryInfo
		nil,   // featureFlagsMD
		diverter,
		nil, // configManager
		context.Background(),
		1, 10,
		nil, // meterProvider
		managedChannelPool{},
	)

	classic := &Table{
		c: &Client{
			project:              "P",
			instance:             "I",
			metricsTracerFactory: &builtinMetricsTracerFactory{enabled: false, shutdown: func() {}},
		},
		table: "t",
	}

	broken := &brokenProtoMessage{real: &btpb.OpenTableRequest{TableName: "projects/P/instances/I/tables/t"}}
	// Use a benign write payload (nil → writePool will be nil), so any
	// fallback we observe must be due to the read-side Marshal failure.
	got := mgr.GetOrCreateSessionTable(
		"projects/P/instances/I/tables/t",
		classic,
		btransport.TABLE_SESSION,
		func(ctx context.Context) (btransport.Stream, error) { return nil, errors.New("should not dial") },
		func(ctx context.Context) (btransport.Stream, error) { return nil, errors.New("should not dial") },
		broken,
		nil,
		btransport.READ_ROW,
		btransport.MUTATE_ROW,
		"keyprefix",
	)

	shim, isShim := got.(*TableShim)
	if !isShim {
		t.Fatalf("got %T; want *TableShim (lazy pools defer detection)", got)
	}
	sessTable, ok := shim.session.(*SessionTable)
	if !ok {
		t.Fatalf("shim.session = %T; want *SessionTable", shim.session)
	}
	// SessionTable's lazyPool for the read side must surface the marshal
	// error on first get() — that's what causes ReadRow to fall back to
	// classic instead of returning a broken response.
	if _, err := sessTable.readPool.get(); err == nil {
		t.Fatal("readPool.get() returned nil error; want marshal failure surfaced on first-use")
	}
}

func TestSessionManager_CreatePoolForPayload_SortedHeader(t *testing.T) {
	// createPoolForPayload is an unexported method that builds the
	// request-params header by sorting the MetadataFn keys. There is no
	// public hook to inspect the resulting metadata.MD without standing
	// up a real session pool dial, so this is verified indirectly via
	// the integration tests against bttest.
	t.Skip("internals not exposed; sorted-header invariant exercised by integration tests")
}

func TestSessionManager_Close_CallsUnregisterThunks(t *testing.T) {
	// Verifying that Close() invokes every managedPool.unregister thunk
	// requires instrumentation on the ClientConfigurationManager
	// AddSessionPoolListener call chain that isn't exposed. The
	// unregister-on-Close behavior is covered by
	// TestClose_SuppressesListenerCallbacks +
	// TestClose_WaitsForInFlightPolls in the transport package.
	t.Skip("requires unregister-thunk instrumentation not exposed by ClientConfigurationManager")
}
