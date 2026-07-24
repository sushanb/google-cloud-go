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

package internal

import (
	"fmt"
	"strings"

	spb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/protobuf/proto"
)

// resourceLeaf returns the last segment of a slash-separated resource path
// — e.g. "sushanb" from "projects/p/instances/i/tables/sushanb". Empty
// string when the path is empty.
func resourceLeaf(path string) string {
	if path == "" {
		return ""
	}
	if i := strings.LastIndex(path, "/"); i >= 0 && i < len(path)-1 {
		return path[i+1:]
	}
	return path
}

// SessionType represents the protocol target session type.
type SessionType int

const (
	// SessionTypeTable indicates standard table session type.
	SessionTypeTable SessionType = iota
	// SessionTypeAuthorizedView indicates authorized view session type.
	SessionTypeAuthorizedView
	// SessionTypeMaterializedView indicates materialized view session type.
	SessionTypeMaterializedView
)

func (t SessionType) String() string {
	switch t {
	case SessionTypeTable:
		return "table"
	case SessionTypeAuthorizedView:
		return "authorized_view"
	case SessionTypeMaterializedView:
		return "materialized_view"
	default:
		return "unknown"
	}
}

// ProtoName returns the bare name of the inner OpenSessionRequest proto for
// this session type — e.g. "OpenTable" — used to build human-readable
// pool identifiers ("OpenTablePool-3 [READ]") in the debug UI.
func (t SessionType) ProtoName() string {
	switch t {
	case SessionTypeTable:
		return "OpenTable"
	case SessionTypeAuthorizedView:
		return "OpenAuthorizedView"
	case SessionTypeMaterializedView:
		return "OpenMaterializedView"
	default:
		return "OpenSession"
	}
}

// SessionDescriptor models a dynamic envelope handshake parameters compiler.
type SessionDescriptor struct {
	Type       SessionType
	MethodName string
	HeaderKeys []string
	LogNameFn  func(req proto.Message) string
	MetadataFn func(req proto.Message) map[string]string // Dynamically populates handshake metadata headers E2E!
}

var (
	// TABLE_SESSION manages standard table scoped connection streams.
	TABLE_SESSION = &SessionDescriptor{
		Type:       SessionTypeTable,
		MethodName: "OpenTable",
		HeaderKeys: []string{"table_name", "app_profile_id", "permission"},
		LogNameFn: func(req proto.Message) string {
			r := req.(*spb.OpenTableRequest)
			return fmt.Sprintf("TableSession(table=%s, app_profile=%s, perm=%s)", r.TableName, r.AppProfileId, r.Permission.String())
		},
		MetadataFn: func(req proto.Message) map[string]string {
			r := req.(*spb.OpenTableRequest)
			return map[string]string{
				"open_session.payload.table_name":     r.TableName,
				"open_session.payload.app_profile_id": r.AppProfileId,
				"open_session.payload.permission":     r.Permission.String(),
			}
		},
	}

	// AUTHORIZED_VIEW_SESSION manages authorized view scoped connection streams.
	AUTHORIZED_VIEW_SESSION = &SessionDescriptor{
		Type:       SessionTypeAuthorizedView,
		MethodName: "OpenAuthorizedView",
		HeaderKeys: []string{"authorized_view_name", "app_profile_id", "permission"},
		LogNameFn: func(req proto.Message) string {
			r := req.(*spb.OpenAuthorizedViewRequest)
			return fmt.Sprintf("AuthorizedViewSession(view=%s, app_profile=%s, perm=%s)", r.AuthorizedViewName, r.AppProfileId, r.Permission.String())
		},
		MetadataFn: func(req proto.Message) map[string]string {
			r := req.(*spb.OpenAuthorizedViewRequest)
			return map[string]string{
				"open_session.payload.authorized_view_name": r.AuthorizedViewName,
				"open_session.payload.app_profile_id":       r.AppProfileId,
				"open_session.payload.permission":           r.Permission.String(),
			}
		},
	}

	// MATERIALIZED_VIEW_SESSION manages materialized view scoped connection streams (Read-Only).
	MATERIALIZED_VIEW_SESSION = &SessionDescriptor{
		Type:       SessionTypeMaterializedView,
		MethodName: "OpenMaterializedView",
		HeaderKeys: []string{"materialized_view_name", "app_profile_id", "permission"},
		LogNameFn: func(req proto.Message) string {
			r := req.(*spb.OpenMaterializedViewRequest)
			return fmt.Sprintf("MaterializedViewSession(view=%s, app_profile=%s, perm=%s)", r.MaterializedViewName, r.AppProfileId, r.Permission.String())
		},
		MetadataFn: func(req proto.Message) map[string]string {
			r := req.(*spb.OpenMaterializedViewRequest)
			return map[string]string{
				"open_session.payload.materialized_view_name": r.MaterializedViewName,
				"open_session.payload.app_profile_id":         r.AppProfileId,
				"open_session.payload.permission":             r.Permission.String(),
			}
		},
	}
)