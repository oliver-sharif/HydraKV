package tests

import (
	"context"
	"fmt"
	"hydrakv/server"
	"hydrakv/server/hydrakv/proto/kvpb"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupFiFoLiFoGRPC(t *testing.T) (kvpb.KVServiceClient, *server.Server, func()) {
	t.Helper()
	s := server.NewServer(0, "127.0.0.1")
	gs := server.NewGRPCServer(s)

	tmpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	port := tmpLis.Addr().(*net.TCPAddr).Port
	tmpLis.Close()

	go gs.Start("127.0.0.1", port)

	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := kvpb.NewKVServiceClient(conn)

	cleanup := func() {
		conn.Close()
		gs.Stop()
	}

	return client, s, cleanup
}

func TestFiFoLiFoGRPC(t *testing.T) {
	client, s, cleanup := setupFiFoLiFoGRPC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	name := "testfifo"
	dbName := "TESTDB"
	limit := 10

	// 0. Create DB
	s.NewDB(dbName)

	// 1. Create FiFoLiFo via Server (not gRPC, as requested)
	err := s.AddFifoLifo(dbName, name, limit)
	if err != nil {
		t.Fatalf("Failed to create FiFoLiFo: %v", err)
	}

	// 2. Push via gRPC
	pushResp, err := client.FiFoLiFoPush(ctx, &kvpb.FiFoLiFoPushRequest{
		Db:    dbName,
		Name:  name,
		Value: "first",
	})
	if err != nil {
		t.Fatalf("FiFoLiFoPush failed: %v", err)
	}
	if !pushResp.Ok {
		t.Fatalf("FiFoLiFoPush returned ok=false")
	}

	_, _ = client.FiFoLiFoPush(ctx, &kvpb.FiFoLiFoPushRequest{Db: dbName, Name: name, Value: "second"})

	// 3. FPop via gRPC (FIFO)
	fpopResp, err := client.FiFoLiFoFPop(ctx, &kvpb.FiFoLiFoPopRequest{Db: dbName, Name: name})
	if err != nil {
		t.Fatalf("FiFoLiFoFPop failed: %v", err)
	}
	if fpopResp.Value != "first" {
		t.Fatalf("FiFoLiFoFPop expected 'first', got '%s'", fpopResp.Value)
	}

	// 4. LPop via gRPC (LIFO)
	// Add another one: "second" is still there, add "third"
	_, _ = client.FiFoLiFoPush(ctx, &kvpb.FiFoLiFoPushRequest{Db: dbName, Name: name, Value: "third"})
	// Queue is now: second, third
	lpopResp, err := client.FiFoLiFoLPop(ctx, &kvpb.FiFoLiFoPopRequest{Db: dbName, Name: name})
	if err != nil {
		t.Fatalf("FiFoLiFoLPop failed: %v", err)
	}
	if lpopResp.Value != "third" {
		t.Fatalf("FiFoLiFoLPop expected 'third', got '%s'", lpopResp.Value)
	}

	// 5. Delete via gRPC
	delResp, err := client.FiFoLiFoDelete(ctx, &kvpb.FiFoLiFoDeleteRequest{Db: dbName, Name: name})
	if err != nil {
		t.Fatalf("FiFoLiFoDelete failed: %v", err)
	}
	if !delResp.Ok {
		t.Fatalf("FiFoLiFoDelete returned ok=false")
	}

	// 6. Verify it's gone
	_, err = client.FiFoLiFoPush(ctx, &kvpb.FiFoLiFoPushRequest{Db: dbName, Name: name, Value: "fail"})
	if err == nil {
		t.Fatalf("FiFoLiFoPush should have failed for deleted queue")
	}
}
