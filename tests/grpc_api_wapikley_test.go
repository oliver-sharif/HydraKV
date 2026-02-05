package tests

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"hydrakv/envhandler"
	"hydrakv/server"
	"hydrakv/server/hydrakv/proto/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ... existing code ...

func BenchmarkGRPC_WithAPIKey_RPS(b *testing.B) {
	// Silence logs during benchmark
	log.SetOutput(io.Discard)

	// API Key Engine aktivieren
	oldVal := *envhandler.ENV.APIKEY_ENABLED
	*envhandler.ENV.APIKEY_ENABLED = true
	defer func() {
		*envhandler.ENV.APIKEY_ENABLED = oldVal
	}()

	// Setup server and client
	s := server.NewServer(0, "127.0.0.1")
	gs := server.NewGRPCServer(s)

	port := 9293
	tmpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err == nil {
		port = tmpLis.Addr().(*net.TCPAddr).Port
		tmpLis.Close()
	}

	go gs.Start("127.0.0.1", port)
	// Wait for server to be ready
	var conn *grpc.ClientConn
	timeout := time.After(3 * time.Second)
	for {
		var err error
		conn, err = grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(200*time.Millisecond),
		)
		if err == nil {
			break
		}
		select {
		case <-timeout:
			b.Fatalf("timed out waiting for gRPC server to start")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	defer conn.Close()
	defer gs.Stop()

	client := kvpb.NewKVServiceClient(conn)
	dbName := "benchdbapikey"

	createCtx, createCancel := context.WithTimeout(context.Background(), 2*time.Second)
	createResp, err := client.CreateDB(createCtx, &kvpb.CreateDBRequest{Name: dbName})
	createCancel()
	if err != nil {
		// Try again with a short wait if it fails initially
		time.Sleep(100 * time.Millisecond)

		createCtx2, createCancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		createResp, err = client.CreateDB(createCtx2, &kvpb.CreateDBRequest{Name: dbName})
		createCancel2()
		if err != nil {
			b.Fatalf("CreateDB in benchmark failed: %v", err)
		}
	}
	apiKey := createResp.Apikey

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			// Small write and read
			_, _ = client.Set(ctx, &kvpb.SetRequest{
				Db:     dbName,
				Key:    key,
				Value:  "value",
				Apikey: apiKey,
			})
			_, _ = client.Get(ctx, &kvpb.GetRequest{
				Db:     dbName,
				Key:    key,
				Apikey: apiKey,
			})
			cancel()
			i++
		}
	})
	b.StopTimer()

	// Calculate RPS
	totalRequests := b.N * 2
	duration := b.Elapsed()
	rps := float64(totalRequests) / duration.Seconds()
	fmt.Printf("\nBenchmarkGRPC_WithAPIKey_RPS: Total Requests: %d, Time: %v, Max RPS: %.2f\n", totalRequests, duration, rps)
}
