// Copyright 2025 Google LLC
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
	"context"
	"fmt"
	"log"
	"time"

	internal "cloud.google.com/go/bigtable/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/alts"
	"google.golang.org/grpc/metadata"
)

// CallSingleIP connects to a specific Bigtable endpoint (IP:Port) using ALTS
// and performs a PingAndWarm request.
// CallSingleIP connects to a specific Bigtable endpoint (IP:Port) using ALTS
// and performs a PingAndWarm request with a 10-second timeout.
func CallSingleIP(ctx context.Context, fullInstanceName, appProfile string, host string, port int) error {
	target := fmt.Sprintf("%s:%d", host, port)

	log.Printf("Connecting to %s via ALTS...", target)

	// Create ALTS credentials
	altsCreds := alts.NewClientCreds(alts.DefaultClientOptions())

	// 1. Create the gRPC Client
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(altsCreds),
	)
	if err != nil {
		return fmt.Errorf("failed to create client for single IP %s: %w", target, err)
	}
	defer conn.Close()

	// 2. Wrap it in the internal BigtableConn
	btc := internal.NewBigtableConn(conn)

	// 3. Use Prime() to ping and warm the connection
	log.Println("Calling internal.BigtableConn.Prime()...")

	// Set 10s timeout
	primeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = btc.Prime(primeCtx, fullInstanceName, appProfile, metadata.MD{})
	if err != nil {
		return fmt.Errorf("Prime() failed: %w", err)
	}

	// 4. Use the Getter to verify ALTS
	if btc.IsDirectAccess() {
		log.Println("ALTS Check: Passed")
	} else {
		log.Println("ALTS Check: Failed")
	}

	log.Println("Prime() successful.")
	return nil
}
