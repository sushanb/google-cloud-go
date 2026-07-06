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

package metrics

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetricsProvider is a wrapper for the built-in metrics meter
// provider. Callers pass an implementation to ClientConfig; today the
// only concrete implementations are NoopMetricsProvider (opt out of
// built-in metrics) and nil (opt in). Re-exported from the bigtable
// package as bigtable.MetricsProvider / bigtable.NoopMetricsProvider
// via type alias.
type MetricsProvider interface {
	isMetricsProvider()
}

// NoopMetricsProvider disables the built-in metrics.
type NoopMetricsProvider struct{}

// isMetricsProvider marks NoopMetricsProvider as a MetricsProvider.
func (NoopMetricsProvider) isMetricsProvider() {}

// methodNameReadRows is the method label used on operation-latencies /
// first-response-latencies when the caller is ReadRows. Duplicated
// from bigtable.methodNameReadRows so the tracer can name-match
// without importing bigtable.
const methodNameReadRows = "ReadRows"

// convertToGrpcStatusErr mirrors bigtable.convertToGrpcStatusErr —
// tracer paths need the (code, err) shape at attempt/operation
// completion. Duplicated to avoid an import cycle; keep in sync with
// the bigtable-side helper.
func convertToGrpcStatusErr(err error) (codes.Code, error) {
	if err == nil {
		return codes.OK, nil
	}
	if errStatus, ok := status.FromError(err); ok {
		return errStatus.Code(), status.Error(errStatus.Code(), errStatus.Message())
	}
	ctxStatus := status.FromContextError(err)
	if ctxStatus.Code() != codes.Unknown {
		return ctxStatus.Code(), status.Error(ctxStatus.Code(), ctxStatus.Message())
	}
	return codes.Unknown, err
}
