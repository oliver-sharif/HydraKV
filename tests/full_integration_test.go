package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"hydrakv/server"
	"hydrakv/server/hydrakv/proto/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupFullServer(t *testing.T) (*server.Server, string, kvpb.KVServiceClient, func()) {
	t.Helper()

	// 1. Setup Server
	s := server.NewServer(0, "127.0.0.1")

	// Create a listener for REST to get a dynamic port
	restLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for REST: %v", err)
	}
	restPort := restLis.Addr().(*net.TCPAddr).Port
	restLis.Close()

	// Update server port (though it might not be used if we use httptest or manual listen)
	// Actually server.Start() uses the port from NewServer.
	// Let's create a new server with the correct port.
	s = server.NewServer(restPort, "127.0.0.1")
	go s.Start()

	restBase := fmt.Sprintf("http://127.0.0.1:%d", restPort)

	// 2. Setup gRPC
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for gRPC: %v", err)
	}
	grpcPort := grpcLis.Addr().(*net.TCPAddr).Port
	grpcLis.Close()

	gs := server.NewGRPCServer(s)
	go gs.Start("127.0.0.1", grpcPort)

	// 3. gRPC Client
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := kvpb.NewKVServiceClient(conn)

	cleanup := func() {
		conn.Close()
		gs.Stop()
		// Shut down REST server
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Server.Shutdown(ctx)
		s.CloseDbs()
	}

	// Wait a bit for servers to start
	time.Sleep(100 * time.Millisecond)

	return s, restBase, client, cleanup
}

func doRequest(t *testing.T, method, url string, body any) (*http.Response, []byte) {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("REST request failed: %v", err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, data
}

func TestFullIntegration(t *testing.T) {
	s, restBase, grpcClient, cleanup := setupFullServer(t)
	defer cleanup()

	restDB := "fullrestdb"
	grpcDB := "fullgrpcdb"

	dbsToCleanup := []string{restDB, grpcDB}
	defer func() {
		for _, db := range dbsToCleanup {
			if s.DBExists(db) {
				s.DBDelete(db)
			}
		}
	}()

	// --- REST TESTS ---
	t.Run("REST_Operations", func(t *testing.T) {
		// Create DB
		resp, body := doRequest(t, http.MethodPost, restBase+"/create", server.NewDB{Name: restDB})
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
			t.Errorf("REST CreateDB failed: %d, body: %s", resp.StatusCode, string(body))
		}

		// Set
		resp, _ = doRequest(t, http.MethodPut, restBase+"/db/"+restDB, server.Set{Key: "restk", Value: "restv"})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("REST Set failed: %d", resp.StatusCode)
		}

		// Get
		resp, body = doRequest(t, http.MethodPost, restBase+"/db/"+restDB+"/keys", server.Key{Key: "restk"})
		var val server.Value
		json.Unmarshal(body, &val)
		if val.Value != "restv" {
			t.Errorf("REST Get failed: expected restv, got %s", val.Value)
		}

		// SetNX (Conflict)
		resp, _ = doRequest(t, http.MethodPost, restBase+"/db/"+restDB, server.Set{Key: "restk", Value: "newv"})
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("REST SetNX expected conflict, got: %d", resp.StatusCode)
		}

		// Incr
		doRequest(t, http.MethodPut, restBase+"/db/"+restDB, server.Set{Key: "counter", Value: "10"})
		resp, _ = doRequest(t, http.MethodPatch, restBase+"/db/"+restDB, server.Set{Key: "counter", Value: "5"})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("REST Incr failed: %d", resp.StatusCode)
		}
		_, body = doRequest(t, http.MethodPost, restBase+"/db/"+restDB+"/keys", server.Key{Key: "counter"})
		json.Unmarshal(body, &val)
		if val.Value != "15" {
			t.Errorf("REST Incr check failed: expected 15, got %s", val.Value)
		}

		// FiFoLiFo REST
		qName := "restqueue"
		// REST endpoints for FiFoLiFo now include the DB name in the path
		doRequest(t, http.MethodPost, restBase+"/db/"+restDB+"/fifolifo", server.NewLiFoFifo{Name: qName, Limit: 10})
		doRequest(t, http.MethodPut, restBase+"/db/"+restDB+"/fifolifo", server.PushFiFoLiFo{Name: qName, Value: "q1"})
		doRequest(t, http.MethodPut, restBase+"/db/"+restDB+"/fifolifo", server.PushFiFoLiFo{Name: qName, Value: "q2"})

		_, body = doRequest(t, http.MethodPost, restBase+"/db/"+restDB+"/fifo", server.PopFiFoLiFo{Name: qName})
		var qVal string
		json.Unmarshal(body, &qVal)
		if qVal != "q1" {
			t.Errorf("REST FiFo failed: expected q1, got %s", qVal)
		}

		// Delete DB via REST
		resp, _ = doRequest(t, http.MethodDelete, restBase+"/db/"+restDB, server.DeleteDB{Name: restDB})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("REST DeleteDB failed: %d", resp.StatusCode)
		}
	})

	// --- gRPC TESTS ---
	t.Run("gRPC_Operations", func(t *testing.T) {
		// Add deadline context
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Create DB
		cResp, err := grpcClient.CreateDB(ctx, &kvpb.CreateDBRequest{Name: grpcDB})
		if err != nil || (!cResp.Created && !cResp.Exists) {
			t.Errorf("gRPC CreateDB failed: err=%v, resp=%+v", err, cResp)
		}

		// Set
		sResp, err := grpcClient.Set(ctx, &kvpb.SetRequest{Db: grpcDB, Key: "grpck", Value: "grpcv"})
		if err != nil || !sResp.Ok {
			t.Errorf("gRPC Set failed: %v", err)
		}

		// Get
		gResp, err := grpcClient.Get(ctx, &kvpb.GetRequest{Db: grpcDB, Key: "grpck"})
		if err != nil || gResp.Value != "grpcv" {
			t.Errorf("gRPC Get failed: expected grpcv, got %s", gResp.Value)
		}

		// SetNX
		snxResp, err := grpcClient.SetNX(ctx, &kvpb.SetRequest{Db: grpcDB, Key: "grpck", Value: "newv"})
		if err != nil || snxResp.Ok {
			t.Errorf("gRPC SetNX should have failed")
		}

		// Incr
		grpcClient.Set(ctx, &kvpb.SetRequest{Db: grpcDB, Key: "counter", Value: "100"})
		iResp, err := grpcClient.Incr(ctx, &kvpb.IncrRequest{Db: grpcDB, Key: "counter", Amount: "50"})
		if err != nil || !iResp.Ok {
			t.Errorf("gRPC Incr failed: %v", err)
		}
		gResp, _ = grpcClient.Get(ctx, &kvpb.GetRequest{Db: grpcDB, Key: "counter"})
		if gResp.Value != "150" {
			t.Errorf("gRPC Incr check failed: expected 150, got %s", gResp.Value)
		}

		// FiFoLiFo gRPC
		qName := "grpcqueue"
		// Create via Server because gRPC doesn't have CreateFiFoLiFo in the interface we saw
		s.AddFifoLifo(grpcDB, qName, 10)

		grpcClient.FiFoLiFoPush(ctx, &kvpb.FiFoLiFoPushRequest{Db: grpcDB, Name: qName, Value: "gq1"})
		grpcClient.FiFoLiFoPush(ctx, &kvpb.FiFoLiFoPushRequest{Db: grpcDB, Name: qName, Value: "gq2"})

		popResp, err := grpcClient.FiFoLiFoFPop(ctx, &kvpb.FiFoLiFoPopRequest{Db: grpcDB, Name: qName})
		if err != nil || popResp.Value != "gq1" {
			t.Errorf("gRPC FiFo Pop failed: expected gq1, got %s", popResp.Value)
		}

		// Delete via gRPC
		delResp, err := grpcClient.FiFoLiFoDelete(ctx, &kvpb.FiFoLiFoDeleteRequest{Db: grpcDB, Name: qName})
		if err != nil || !delResp.Ok {
			t.Errorf("gRPC FiFo Delete failed: %v", err)
		}
	})
}
