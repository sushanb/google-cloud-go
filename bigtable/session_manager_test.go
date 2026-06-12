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
	// would normally return a TableShim wrapping SessionTable. If
	// proto.Marshal on the read payload fails, the manager must fall back
	// to a plain tableImpl rather than panicking or returning a partially-
	// wired TableShim.
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

	// On marshal failure, the manager returns a plain tableImpl, not a
	// TableShim. Assert the concrete type — TableShim would silently route
	// reads through a half-built session pool and that's the whole bug.
	if _, isShim := got.(*TableShim); isShim {
		t.Fatal("got *TableShim; want *tableImpl fallback after read-payload Marshal failure")
	}
	if _, isImpl := got.(*tableImpl); !isImpl {
		t.Errorf("got %T; want *tableImpl fallback", got)
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
