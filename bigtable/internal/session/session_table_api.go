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

package session

import (
	"context"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// SessionTableApi is the per-table, proto-native API exposed to callers. The
// concrete implementation routes ReadRow over a READ session pool and
// MutateRow over a separate WRITE session pool — callers do not see the
// distinction.
type SessionTableApi interface {
	ReadRow(ctx context.Context, req *btpb.SessionReadRowRequest) (*btpb.SessionReadRowResponse, error)
	MutateRow(ctx context.Context, req *btpb.SessionMutateRowRequest) (*btpb.SessionMutateRowResponse, error)

	// Close releases this table's underlying read+write session pools.
	// Independent from SessionClient.Close — closing the table does not close
	// the channel pool.
	Close() error
}
