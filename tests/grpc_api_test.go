package tests

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"hydrakv/server"
	"hydrakv/server/hydrakv/proto/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newGRPCServer(t *testing.T) (kvpb.KVServiceClient, func()) {
	t.Helper()

	// Initialize the actual KV logic
	s := server.NewServer(0, "127.0.0.1")

	// Create gRPC server
	gs := server.NewGRPCServer(s)

	// Pick a port
	port := 9292 // Default gRPC port or any other
	// Try to find a free port
	tmpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err == nil {
		port = tmpLis.Addr().(*net.TCPAddr).Port
		tmpLis.Close()
	}

	// Run server in background
	go gs.Start("127.0.0.1", port)

	// Wait for server to be ready
	var conn *grpc.ClientConn
	timeout := time.After(2 * time.Second)
	tick := time.Tick(50 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting for gRPC server to start")
		case <-tick:
			var err error
			conn, err = grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(), // This makes Dial wait for the connection to be established
				grpc.WithTimeout(100*time.Millisecond),
			)
			if err == nil {
				goto ready
			}
		}
	}

ready:
	client := kvpb.NewKVServiceClient(conn)

	cleanup := func() {
		conn.Close()
		gs.Stop()
	}

	return client, cleanup
}

func TestGRPC_SequentialCRUD(t *testing.T) {
	client, cleanup := newGRPCServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbName := "grpcdb"

	// 1) Create DB
	createResp, err := client.CreateDB(ctx, &kvpb.CreateDBRequest{Name: dbName})
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}
	if !createResp.Created && !createResp.Exists {
		t.Fatalf("DB not created and doesn't exist")
	}

	// 2) Set
	setResp, err := client.Set(ctx, &kvpb.SetRequest{
		Db:    dbName,
		Key:   "k1",
		Value: "v1",
		Ttl:   0,
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if !setResp.Ok {
		t.Fatalf("Set returned ok=false")
	}

	// 3) Get
	getResp, err := client.Get(ctx, &kvpb.GetRequest{
		Db:  dbName,
		Key: "k1",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !getResp.Found || getResp.Value != "v1" {
		t.Fatalf("Get unexpected response: found=%v, value=%s", getResp.Found, getResp.Value)
	}

	// 4) SetNX
	setNXResp, err := client.SetNX(ctx, &kvpb.SetRequest{
		Db:    dbName,
		Key:   "k1",
		Value: "v2",
	})
	if err != nil {
		t.Fatalf("SetNX failed: %v", err)
	}
	if setNXResp.Ok {
		t.Fatalf("SetNX should have failed for existing key")
	}

	// 5) Incr
	_, _ = client.Set(ctx, &kvpb.SetRequest{Db: dbName, Key: "counter", Value: "10"})
	incrResp, err := client.Incr(ctx, &kvpb.IncrRequest{
		Db:     dbName,
		Key:    "counter",
		Amount: "5",
	})
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	if !incrResp.Ok {
		t.Fatalf("Incr returned ok=false")
	}
	getIncrResp, _ := client.Get(ctx, &kvpb.GetRequest{Db: dbName, Key: "counter"})
	if getIncrResp.Value != "15" {
		t.Fatalf("Incr expected 15, got %s", getIncrResp.Value)
	}

	// 6) Exists
	existsResp, err := client.Exists(ctx, &kvpb.ExistsRequest{
		Db: dbName,
	})
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !existsResp.Exists {
		t.Fatalf("Exists returned false for existing DB")
	}

	// 7) Delete
	delResp, err := client.Delete(ctx, &kvpb.DeleteRequest{
		Db:  dbName,
		Key: "k1",
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !delResp.Ok {
		t.Fatalf("Delete returned ok=false")
	}

	// 5) Get after delete
	getResp2, err := client.Get(ctx, &kvpb.GetRequest{
		Db:  dbName,
		Key: "k1",
	})
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if getResp2.Found {
		t.Fatalf("Get after delete should not find the key")
	}
}

func BenchmarkGRPC_RPS(b *testing.B) {
	// Silence logs during benchmark
	log.SetOutput(io.Discard)

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
	ctx := context.Background()
	dbName := "benchdb"
	_, _ = client.CreateDB(ctx, &kvpb.CreateDBRequest{Name: dbName})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			// Small write and read
			_, _ = client.Set(ctx, &kvpb.SetRequest{
				Db:    dbName,
				Key:   key,
				Value: "value",
			})
			_, _ = client.Get(ctx, &kvpb.GetRequest{
				Db:  dbName,
				Key: key,
			})
			i++
		}
	})
	b.StopTimer()

	// Calculate RPS
	// b.N is the total number of operations
	// Each operation in our loop is 1 Set + 1 Get = 2 requests
	totalRequests := b.N * 2
	duration := b.Elapsed()
	rps := float64(totalRequests) / duration.Seconds()
	fmt.Printf("\nBenchmarkGRPC_RPS: Total Requests: %d, Time: %v, Max RPS: %.2f\n", totalRequests, duration, rps)
}
