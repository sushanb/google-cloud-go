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

import btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"

// serverTime mirrors bigtable.ServerTime — the sentinel TimestampMicros value
// meaning "server picks the timestamp at write time." Duplicated here so the
// internal/session package does not import the root bigtable package.
const serverTime int64 = -1

// mutationsAreRetryable reports whether all mutations are idempotent and
// therefore safe to retry. A SetCell with TimestampMicros == serverTime is
// non-idempotent — a retry would produce a duplicate cell with a different
// server-assigned timestamp.
func mutationsAreRetryable(muts []*btpb.Mutation) bool {
	for _, mut := range muts {
		if sc := mut.GetSetCell(); sc != nil && sc.TimestampMicros == serverTime {
			return false
		}
	}
	return true
}
