package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"hydrakv/server"
)

func newRESTServer(t *testing.T) (*httptest.Server, *http.Client, string) {
	t.Helper()
	s := server.NewServer(0, "127.0.0.1")
	ts := httptest.NewServer(s.Handler())
	return ts, ts.Client(), ts.URL
}

func doRESTJSON(t testing.TB, client *http.Client, method, url string, body any) (*http.Response, []byte) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	resp.Body.Close()
	return resp, data
}

func TestREST_SequentialCRUD(t *testing.T) {
	ts, client, base := newRESTServer(t)
	defer ts.Close()

	dbName := "restdb"

	// 1) Create DB
	resp, _ := doRESTJSON(t, client, http.MethodPost, base+"/create", server.NewDB{Name: dbName})
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		t.Fatalf("CreateDB failed: expected 201 or 409, got %d", resp.StatusCode)
	}

	// 2) Set
	setPayload := server.Set{Key: "k1", Value: "v1"}
	resp, body := doRESTJSON(t, client, http.MethodPut, base+"/db/"+dbName, setPayload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Set failed: expected 200, got %d, body=%s", resp.StatusCode, string(body))
	}
	var ok server.OK
	if err := json.Unmarshal(body, &ok); err != nil {
		t.Fatalf("decode ok: %v", err)
	}
	if !ok.OK {
		t.Fatalf("Set returned ok=false")
	}

	// 3) Get
	resp, body = doRESTJSON(t, client, http.MethodPost, base+"/db/"+dbName+"/keys", server.Key{Key: "k1"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Get failed: expected 200, got %d, body=%s", resp.StatusCode, string(body))
	}
	var v server.Value
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("decode value: %v", err)
	}
	if !v.Found || v.Value != "v1" {
		t.Fatalf("Get unexpected response: found=%v, value=%s", v.Found, v.Value)
	}

	// 4) SetNX (Set with POST on /db/{name} usually acts as SetNX based on api_test.go)
	resp, _ = doRESTJSON(t, client, http.MethodPost, base+"/db/"+dbName, setPayload)
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("SetNX should have failed for existing key")
	}

	// 5) Incr (PATCH)
	_, _ = doRESTJSON(t, client, http.MethodPut, base+"/db/"+dbName, server.Set{Key: "counter", Value: "10"})
	resp, body = doRESTJSON(t, client, http.MethodPatch, base+"/db/"+dbName, server.Set{Key: "counter", Value: "5"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Incr failed: status %d, body %s", resp.StatusCode, string(body))
	}

	resp, body = doRESTJSON(t, client, http.MethodPost, base+"/db/"+dbName+"/keys", server.Key{Key: "counter"})
	var val server.Value
	json.Unmarshal(body, &val)
	if val.Value != "15" {
		t.Fatalf("Incr expected 15, got %s", val.Value)
	}

	// 6) Exists (Check DB existence)
	// There isn't a direct "Exists" for DB in REST but we can try to GET keys or similar
	// api_test.go doesn't seem to test DB existence explicitly other than create conflict
	resp, _ = doRESTJSON(t, client, http.MethodPost, base+"/create", server.NewDB{Name: dbName})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("DB should exist and return conflict on create")
	}

	// 7) Delete
	resp, body = doRESTJSON(t, client, http.MethodDelete, base+"/db/"+dbName+"/keys", server.Key{Key: "k1"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Delete failed: expected 200, got %d, body=%s", resp.StatusCode, string(body))
	}

	// 8) Get after delete
	resp, _ = doRESTJSON(t, client, http.MethodPost, base+"/db/"+dbName+"/keys", server.Key{Key: "k1"})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Get after delete should return 404, got %d", resp.StatusCode)
	}
}

func BenchmarkREST_RPS(b *testing.B) {
	// Silence logs during benchmark
	log.SetOutput(io.Discard)

	s := server.NewServer(0, "127.0.0.1")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	client := ts.Client()
	base := ts.URL
	dbName := "benchdb"

	// Create DB
	_, _ = doRESTJSON(b, client, http.MethodPost, base+"/create", server.NewDB{Name: dbName})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			// Small write
			setPayload := server.Set{Key: key, Value: "value"}
			_, _ = doRESTJSON(b, client, http.MethodPut, base+"/db/"+dbName, setPayload)

			// Small read
			_, _ = doRESTJSON(b, client, http.MethodPost, base+"/db/"+dbName+"/keys", server.Key{Key: key})
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
	fmt.Printf("\nBenchmarkREST_RPS: Total Requests: %d, Time: %v, Max RPS: %.2f\n", totalRequests, duration, rps)
}
