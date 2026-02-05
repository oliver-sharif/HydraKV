package tests

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"testing"

	serverpkg "hydrakv/server"
)

// This new test creates a new DB, concurrently sets several hundred entries,
// concurrently verifies gets, deletes a subset, then SetNX re-inserts the
// deleted ones, and finally validates counts and values.
func TestAPI_BulkSetGetDeleteSetNX(t *testing.T) {
	ts, client, base := newAPIServer(t)
	_ = ts

	// Create DB
	resp, _ := doJSON(t, client, http.MethodPost, base+"/create", serverpkg.NewDB{Name: "bulkdb"})
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		t.Fatalf("create db: unexpected status %d", resp.StatusCode)
	}

	const N = 600 // several hundreds

	// 1) Concurrent PUT set of N unique keys
	var wg sync.WaitGroup
	setErrs := make(chan string, N)
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k:" + strconv.Itoa(i)
			val := "v:" + strconv.Itoa(i)
			payload := serverpkg.Set{Key: key, Value: val}
			r, body := doJSON(t, client, http.MethodPut, base+"/db/bulkdb", payload)
			if r.StatusCode != http.StatusOK {
				setErrs <- "PUT status=" + strconv.Itoa(r.StatusCode) + " body=" + string(body)
			}
		}()
	}
	wg.Wait()
	close(setErrs)
	for e := range setErrs {
		t.Fatalf("set error: %s", e)
	}

	// 2) Concurrent GET verification of all keys
	getErrs := make(chan string, N)
	wg = sync.WaitGroup{}
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k:" + strconv.Itoa(i)
			r, body := doJSON(t, client, http.MethodPost, base+"/db/bulkdb/keys", serverpkg.Key{Key: key})
			if r.StatusCode != http.StatusOK {
				getErrs <- "GET status=" + strconv.Itoa(r.StatusCode) + " key=" + key
				return
			}
			var v serverpkg.Value
			if err := json.Unmarshal(body, &v); err != nil {
				getErrs <- "unmarshal: " + err.Error()
				return
			}
			if !v.Found || v.Value != "v:"+strconv.Itoa(i) {
				getErrs <- "unexpected value for " + key + ": " + v.Value
			}
		}()
	}
	wg.Wait()
	close(getErrs)
	for e := range getErrs {
		t.Fatalf("get error: %s", e)
	}

	// 3) Concurrent DELETE every 10th key
	delEvery := 10
	delErrs := make(chan string, N/delEvery+1)
	deletedKeys := make(map[int]struct{})
	var mu sync.Mutex
	wg = sync.WaitGroup{}
	for i := 0; i < N; i += delEvery {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k:" + strconv.Itoa(i)
			r, _ := doJSON(t, client, http.MethodDelete, base+"/db/bulkdb/keys", serverpkg.Key{Key: key})
			if r.StatusCode != http.StatusOK {
				delErrs <- "DEL status=" + strconv.Itoa(r.StatusCode) + " key=" + key
				return
			}
			mu.Lock()
			deletedKeys[i] = struct{}{}
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(delErrs)
	for e := range delErrs {
		t.Fatalf("delete error: %s", e)
	}

	// 4) Concurrent SetNX for ALL keys; only deleted ones should succeed
	setnxOK := make(chan int, len(deletedKeys))
	setnxErrs := make(chan string, N)
	wg = sync.WaitGroup{}
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k:" + strconv.Itoa(i)
			val := "nx:" + strconv.Itoa(i)
			payload := serverpkg.Set{Key: key, Value: val}
			r, body := doJSON(t, client, http.MethodPost, base+"/db/bulkdb", payload)
			// Expect 200 for previously deleted keys, 409 for existing keys
			if _, wasDeleted := func() (struct{}, bool) { mu.Lock(); v, ok := deletedKeys[i]; mu.Unlock(); return v, ok }(); wasDeleted {
				if r.StatusCode != http.StatusOK {
					setnxErrs <- "SetNX expected 200 for deleted key, got " + strconv.Itoa(r.StatusCode) + " key=" + key + " body=" + string(body)
					return
				}
				setnxOK <- i
			} else {
				if r.StatusCode != http.StatusConflict {
					setnxErrs <- "SetNX expected 409 for existing key, got " + strconv.Itoa(r.StatusCode) + " key=" + key
				}
			}
		}()
	}
	wg.Wait()
	close(setnxOK)
	close(setnxErrs)
	for e := range setnxErrs {
		t.Fatalf("setnx error: %s", e)
	}

	// Ensure the number of successful SetNX equals number of deletions
	okCount := 0
	for range setnxOK {
		okCount++
	}
	if okCount != len(deletedKeys) {
		t.Fatalf("SetNX okCount=%d != deleted=%d", okCount, len(deletedKeys))
	}

	// 5) Final verification: all N keys should be present, and values match expectation
	finalErrs := make(chan string, N)
	wg = sync.WaitGroup{}
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k:" + strconv.Itoa(i)
			r, body := doJSON(t, client, http.MethodPost, base+"/db/bulkdb/keys", serverpkg.Key{Key: key})
			if r.StatusCode != http.StatusOK {
				finalErrs <- "final GET status=" + strconv.Itoa(r.StatusCode) + " key=" + key
				return
			}
			var v serverpkg.Value
			if err := json.Unmarshal(body, &v); err != nil {
				finalErrs <- "unmarshal: " + err.Error()
				return
			}
			if !v.Found {
				finalErrs <- "final not found: " + key
				return
			}
			// Determine expected value
			mu.Lock()
			_, wasDeleted := deletedKeys[i]
			mu.Unlock()
			expected := "v:" + strconv.Itoa(i)
			if wasDeleted {
				expected = "nx:" + strconv.Itoa(i)
			}
			if v.Value != expected {
				finalErrs <- "unexpected final value for " + key + ": got=" + v.Value + " want=" + expected
			}
		}()
	}
	wg.Wait()
	close(finalErrs)
	for e := range finalErrs {
		t.Fatalf("final check error: %s", e)
	}
}
