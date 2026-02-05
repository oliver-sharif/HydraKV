package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	serverpkg "hydrakv/server"
)

// small helpers
func doJSON(t *testing.T, client *http.Client, method, url string, body any) (*http.Response, []byte) {
	t.Helper()
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

func newAPIServer(t *testing.T) (*httptest.Server, *http.Client, string) {
	t.Helper()
	s := serverpkg.NewServer(0, "127.0.0.1")
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, ts.Client(), ts.URL
}

func TestAPI_SequentialCRUD(t *testing.T) {
	ts, client, base := newAPIServer(t)

	// 1) Create DB
	resp, _ := doJSON(t, client, http.MethodPost, base+"/create", serverpkg.NewDB{Name: "testdb"})
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		t.Fatalf("create db: unexpected status %d", resp.StatusCode)
	}

	// 2) Set
	setPayload := serverpkg.Set{Key: "user:1", Value: "Alice"}
	resp, body := doJSON(t, client, http.MethodPut, base+"/db/testdb", setPayload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set: expected 200, got %d, body=%s", resp.StatusCode, string(body))
	}
	var ok serverpkg.OK
	if err := json.Unmarshal(body, &ok); err != nil {
		t.Fatalf("decode ok: %v", err)
	}
	if !ok.OK {
		t.Fatalf("set: ok=false")
	}

	// 3) Get
	resp, body = doJSON(t, client, http.MethodPost, base+"/db/testdb/keys", serverpkg.Key{Key: "user:1"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get: expected 200, got %d, body=%s", resp.StatusCode, string(body))
	}
	var v serverpkg.Value
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("decode value: %v", err)
	}
	if !v.Found || v.Value != "Alice" {
		t.Fatalf("get: unexpected value: %+v", v)
	}

	// 4) SetNX should fail (key exists)
	resp, _ = doJSON(t, client, http.MethodPost, base+"/db/testdb", setPayload)
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("setnx: expected conflict-like status, got 200")
	}

	// 5) Delete
	resp, body = doJSON(t, client, http.MethodDelete, base+"/db/testdb/keys", serverpkg.Key{Key: "user:1"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("del: expected 200, got %d, body=%s", resp.StatusCode, string(body))
	}

	// 6) Get after delete should be 404
	resp, _ = doJSON(t, client, http.MethodPost, base+"/db/testdb/keys", serverpkg.Key{Key: "user:1"})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get after del: expected 404, got %d", resp.StatusCode)
	}

	// 7) Delete non-existing key should also be 200
	resp, body = doJSON(t, client, http.MethodDelete, base+"/db/testdb/keys", serverpkg.Key{Key: "non-existing"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("del non-existing: expected 200, got %d", resp.StatusCode)
	}
	var ok2 serverpkg.OK
	if err := json.Unmarshal(body, &ok2); err != nil {
		t.Fatalf("decode ok non-existing: %v", err)
	}
	if ok2.OK {
		t.Fatalf("del non-existing: ok=true, want false")
	}

	_ = ts // silence unused (though Cleanup closes it)
}

func TestAPI_Concurrency_NoLostRequests(t *testing.T) {
	_, client, base := newAPIServer(t)

	// Create DB
	resp, _ := doJSON(t, client, http.MethodPost, base+"/create", serverpkg.NewDB{Name: "cdb"})
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		t.Fatalf("create db: unexpected status %d", resp.StatusCode)
	}

	// Concurrent writers and readers
	const writers = 50
	const readers = 50

	var wg sync.WaitGroup
	errs := make(chan error, writers+readers)

	// writers: set distinct keys
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k:" + strconv.Itoa(i)
			payload := serverpkg.Set{Key: key, Value: "v:" + strconv.Itoa(i)}
			r, b := doJSON(t, client, http.MethodPut, base+"/db/cdb", payload)
			if r.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("writer status=%d body=%s", r.StatusCode, string(b))
			}
		}()
	}

	// readers: read keys (some may not exist yet; ensure server still responds reliably)
	for i := 0; i < readers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k:" + strconv.Itoa(i%writers)
			r, _ := doJSON(t, client, http.MethodPost, base+"/db/cdb/keys", serverpkg.Key{Key: key})
			// Allow either 200 (found) or 404 (not yet written), but anything else is an error
			if r.StatusCode != http.StatusOK && r.StatusCode != http.StatusNotFound {
				errs <- fmt.Errorf("reader unexpected status=%d", r.StatusCode)
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrency error: %v", err)
		}
	}

	// Verify final state: all writer keys should be present
	for i := 0; i < writers; i++ {
		key := "k:" + strconv.Itoa(i)
		r, body := doJSON(t, client, http.MethodPost, base+"/db/cdb/keys", serverpkg.Key{Key: key})
		if r.StatusCode != http.StatusOK {
			t.Fatalf("final get %s: expected 200, got %d (body=%s)", key, r.StatusCode, string(body))
		}
		var v serverpkg.Value
		if err := json.Unmarshal(body, &v); err != nil {
			t.Fatalf("decode value: %v", err)
		}
		if !v.Found {
			t.Fatalf("final get %s: not found", key)
		}
	}
}

func TestAPI_Incr(t *testing.T) {
	_, client, base := newAPIServer(t)

	// Create DB
	doJSON(t, client, http.MethodPost, base+"/create", serverpkg.NewDB{Name: "incdb"})

	// 1. Incr on new key (PATCH)
	payload := serverpkg.Set{Key: "counter", Value: "100"}
	resp, body := doJSON(t, client, http.MethodPatch, base+"/db/incdb", payload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Incr failed: status %d, body %s", resp.StatusCode, string(body))
	}

	// 2. Verify value
	resp, body = doJSON(t, client, http.MethodPost, base+"/db/incdb/keys", serverpkg.Key{Key: "counter"})
	var val serverpkg.Value
	json.Unmarshal(body, &val)
	if val.Value != "100" {
		t.Fatalf("Expected 100, got %s", val.Value)
	}

	// 3. Incr existing key
	payload = serverpkg.Set{Key: "counter", Value: "50"}
	doJSON(t, client, http.MethodPatch, base+"/db/incdb", payload)

	// 4. Verify value
	resp, body = doJSON(t, client, http.MethodPost, base+"/db/incdb/keys", serverpkg.Key{Key: "counter"})
	json.Unmarshal(body, &val)
	if val.Value != "150" {
		t.Fatalf("Expected 150, got %s", val.Value)
	}

	// 5. Incr with negative value
	payload = serverpkg.Set{Key: "counter", Value: "-10"}
	doJSON(t, client, http.MethodPatch, base+"/db/incdb", payload)

	// 6. Verify value
	resp, body = doJSON(t, client, http.MethodPost, base+"/db/incdb/keys", serverpkg.Key{Key: "counter"})
	json.Unmarshal(body, &val)
	if val.Value != "140" {
		t.Fatalf("Expected 140, got %s", val.Value)
	}

	// 7. Incr non-numeric value (should return 409 Conflict based on route logic)
	doJSON(t, client, http.MethodPut, base+"/db/incdb", serverpkg.Set{Key: "alpha", Value: "abc"})
	resp, body = doJSON(t, client, http.MethodPatch, base+"/db/incdb", serverpkg.Set{Key: "alpha", Value: "1"})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("Expected 409 for non-numeric Incr, got %d", resp.StatusCode)
	}
}

func TestAPI_TTL(t *testing.T) {
	_, client, base := newAPIServer(t)

	// Create DB
	doJSON(t, client, http.MethodPost, base+"/create", serverpkg.NewDB{Name: "ttldb"})

	// 1. Set value with 1s TTL
	payload := serverpkg.Set{Key: "ttl-key", Value: "ttl-val", Ttl: 1}
	resp, body := doJSON(t, client, http.MethodPut, base+"/db/ttldb", payload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Set with TTL failed: status %d, body %s", resp.StatusCode, string(body))
	}

	// 2. Immediate check
	resp, body = doJSON(t, client, http.MethodPost, base+"/db/ttldb/keys", serverpkg.Key{Key: "ttl-key"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected key to be present, got %d", resp.StatusCode)
	}

	// 3. Wait for TTL (2.5 seconds to be safe)
	time.Sleep(2500 * time.Millisecond)

	// 4. Check if key is gone
	resp, body = doJSON(t, client, http.MethodPost, base+"/db/ttldb/keys", serverpkg.Key{Key: "ttl-key"})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for expired key, got %d", resp.StatusCode)
	}
}
