// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"google.golang.org/grpc/credentials/google"
	"google.golang.org/grpc/metadata"
)

// CallSingleChannel connects to Bigtable using the DirectPath C2P scheme
// and verifies ALTS context in the headers/peer info using PingAndWarm with a 10-second timeout.
func CallSingleChannel(ctx context.Context, project, instance, appProfile string) error {
	fullInstanceName := fmt.Sprintf("projects/%s/instances/%s", project, instance)

	// DirectPath specific target
	const host = "bigtable.googleapis.com"
	target := "google-c2p:///" + host

	log.Printf("Creating single channel to %s", target)

	// 1. Create the gRPC Client (No custom interceptor needed now)
	conn, err := grpc.NewClient(target,
		grpc.WithCredentialsBundle(google.NewDefaultCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to create client for C2P target %s: %w", target, err)
	}
	defer conn.Close()

	// 2. Wrap it in the internal BigtableConn
	btc := internal.NewBigtableConn(conn)

	// 3. Reuse Prime() from connpool.go
	log.Println("Calling internal.BigtableConn.Prime()...")

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

type SingleIPConnectivityCheck struct {
	InstanceName string
	AppProfile   string
	Host         string
	Port         int
}

func (c *SingleIPConnectivityCheck) Name() string {
	return fmt.Sprintf("Single IP Connectivity (%s:%d)", c.Host, c.Port)
}
func (c *SingleIPConnectivityCheck) Stage() string { return "Connectivity" }
func (c *SingleIPConnectivityCheck) Execute() (bool, error) {
	ctx := context.Background()
	if err := CallSingleIP(ctx, c.InstanceName, c.AppProfile, c.Host, c.Port); err != nil {
		return false, err
	}
	return true, nil
}
