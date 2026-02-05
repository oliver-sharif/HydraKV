package hashMap

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// helper to create a unique AOF Name per test and ensure cleanup
func uniqueAOFName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test_%s_%d", t.Name(), time.Now().UnixNano())
}

func removeAOF(t *testing.T, name string) {
	t.Helper()
	// Files live under Aof/<Name>.bin relative to project root
	_ = os.Remove(filepath.Join("Aof", name+".bin"))
}

func TestHashMap_SetGetDel(t *testing.T) {
	name := uniqueAOFName(t)
	hm, err := NewHashMap(name)
	if err != nil {
		t.Fatalf("NewHashMap error: %v", err)
	}
	t.Cleanup(func() {
		_ = hm.Close()
		removeAOF(t, name)
	})

	const n = 100
	for i := 0; i < n; i++ {
		k := "k-" + strconv.Itoa(i)
		v := "v-" + strconv.Itoa(i)
		if ok := hm.Set(0, k, v); !ok {
			t.Fatalf("Set failed for key %s", k)
		}
	}

	if got := hm.GetEntries(); got != n {
		t.Fatalf("Entries mismatch: got %d want %d", got, n)
	}

	for i := 0; i < n; i++ {
		k := "k-" + strconv.Itoa(i)
		ok, v := hm.Get(k)
		if !ok {
			t.Fatalf("Get missing key %s", k)
		}
		want := "v-" + strconv.Itoa(i)
		if v != want {
			t.Fatalf("Get wrong value for %s: got %s want %s", k, v, want)
		}
	}

	// delete half
	for i := 0; i < n; i += 2 {
		k := "k-" + strconv.Itoa(i)
		if ok := hm.Del(k); !ok {
			t.Fatalf("Del failed for key %s", k)
		}
	}

	if got := hm.GetEntries(); got != n/2 {
		t.Fatalf("Entries after delete: got %d want %d", got, n/2)
	}

	for i := 0; i < n; i++ {
		k := "k-" + strconv.Itoa(i)
		ok, _ := hm.Get(k)
		if i%2 == 0 {
			if ok {
				t.Fatalf("expected key %s to be deleted", k)
			}
		} else if !ok {
			t.Fatalf("expected key %s to exist", k)
		}
	}
}

func TestHashMap_ResizeOnLoadFactor(t *testing.T) {
	name := uniqueAOFName(t)
	hm, err := NewHashMap(name)
	if err != nil {
		t.Fatalf("NewHashMap error: %v", err)
	}
	t.Cleanup(func() {
		_ = hm.Close()
		removeAOF(t, name)
	})

	// Initially DefaultBasketSize buckets
	if got := hm.GetBasketNum(); got != DefaultBasketSize {
		t.Fatalf("unexpected initial basket size: got %d want %d", got, DefaultBasketSize)
	}

	// Insert entries so that load factor reaches 0.75 before the insert happens,
	// which should trigger a resize from 4 -> 8.
	for i := 0; i < DefaultBasketSize; i++ {
		k := fmt.Sprintf("k-%d", i)
		v := fmt.Sprintf("v-%d", i)
		hm.Set(0, k, v)
	}

	// Give the async ResizeChecker some time or trigger manually
	hm.CheckResize()

	// After adding 4th element (with DefaultBasketSize=4), resize should have occurred to 8
	if got := hm.GetBasketNum(); got != DefaultBasketSize*2 {
		t.Fatalf("expected resize to %d baskets, got %d", DefaultBasketSize*2, got)
	}
}

func TestAOF_ConsistencyReplay(t *testing.T) {
	name := uniqueAOFName(t)

	// Phase 1: write data
	{
		hm, err := NewHashMap(name)
		if err != nil {
			t.Fatalf("NewHashMap error: %v", err)
		}
		const N = 200
		for i := 0; i < N; i++ {
			k := "key-" + strconv.Itoa(i)
			v := "value-" + strconv.Itoa(i)
			hm.Set(0, k, v)
		}
		if err := hm.Close(); err != nil {
			t.Fatalf("Close error: %v", err)
		}
	}

	// Phase 2: reopen and validate replay
	{
		hm, err := NewHashMap(name)
		if err != nil {
			t.Fatalf("NewHashMap reopen error: %v", err)
		}
		t.Cleanup(func() {
			_ = hm.Close()
			removeAOF(t, name)
		})

		const N = 200
		for i := 0; i < N; i++ {
			k := "key-" + strconv.Itoa(i)
			ok, v := hm.Get(k)
			if !ok {
				t.Fatalf("missing key after replay: %s", k)
			}
			want := "value-" + strconv.Itoa(i)
			if v != want {
				t.Fatalf("wrong value after replay for %s: got %s want %s", k, v, want)
			}
		}
		if hm.GetEntries() != 200 {
			t.Fatalf("entries after replay: got %d want %d", hm.GetEntries(), 200)
		}
	}
}

func TestHashMap_Incr(t *testing.T) {
	name := uniqueAOFName(t)
	hm, err := NewHashMap(name)
	if err != nil {
		t.Fatalf("NewHashMap error: %v", err)
	}
	t.Cleanup(func() {
		_ = hm.Close()
		removeAOF(t, name)
	})

	// 1. Incr on non-existing key
	if ok := hm.Incr(0, "c1", "10"); !ok {
		t.Fatal("Incr on new key failed")
	}
	if ok, v := hm.Get("c1"); !ok || v != "10" {
		t.Fatalf("Expected 10, got %s (ok=%v)", v, ok)
	}

	// 2. Incr on existing key
	if ok := hm.Incr(0, "c1", "5"); !ok {
		t.Fatal("Incr on existing key failed")
	}
	if ok, v := hm.Get("c1"); !ok || v != "15" {
		t.Fatalf("Expected 15, got %s", v)
	}

	// 3. Incr with negative value (Decr)
	if ok := hm.Incr(0, "c1", "-7"); !ok {
		t.Fatal("Incr with negative value failed")
	}
	if ok, v := hm.Get("c1"); !ok || v != "8" {
		t.Fatalf("Expected 8, got %s", v)
	}

	// 4. Incr on non-numeric value (should fail)
	hm.Set(0, "alpha", "not-a-number")
	if ok := hm.Incr(0, "alpha", "1"); ok {
		t.Fatal("Incr on non-numeric value should have failed")
	}

	// 5. Incr with non-numeric amount (should fail)
	if ok := hm.Incr(0, "c1", "abc"); ok {
		t.Fatal("Incr with non-numeric amount should have failed")
	}

	// 6. Incr with TTL
	if ok := hm.Incr(1, "c_ttl", "100"); !ok {
		t.Fatal("Incr with TTL failed")
	}
	if ok, v := hm.Get("c_ttl"); !ok || v != "100" {
		t.Fatalf("Expected 100 with TTL, got %s", v)
	}
}

func TestHashMap_TTL(t *testing.T) {
	name := uniqueAOFName(t)
	hm, err := NewHashMap(name)
	if err != nil {
		t.Fatalf("NewHashMap error: %v", err)
	}
	t.Cleanup(func() {
		_ = hm.Close()
		removeAOF(t, name)
	})

	// 1. Set with short TTL (1 second)
	key := "ttl-key"
	value := "ttl-value"
	if ok := hm.Set(1, key, value); !ok {
		t.Fatal("Set with TTL failed")
	}

	// 2. Immediate check
	if ok, v := hm.Get(key); !ok || v != value {
		t.Fatalf("Expected key to be present, ok=%v, v=%s", ok, v)
	}

	// 3. Wait for TTL to expire (more than 1 second)
	// TTLManager runs every second, so we might need to wait up to 2 seconds to be sure
	time.Sleep(2500 * time.Millisecond)

	// 4. Check if key is gone
	if ok, v := hm.Get(key); ok {
		t.Fatalf("Expected key to be deleted, but it still exists with value %s", v)
	}

	// 5. Check entries count
	if got := hm.GetEntries(); got != 0 {
		t.Fatalf("Expected 0 entries, got %d", got)
	}

	// 6. Multiple keys with different TTLs
	hm.Set(1, "short", "val1")
	hm.Set(3, "long", "val2")

	time.Sleep(1500 * time.Millisecond)

	if ok, _ := hm.Get("short"); ok {
		t.Fatal("short key should be gone")
	}
	if ok, v := hm.Get("long"); !ok || v != "val2" {
		t.Fatal("long key should still be there")
	}

	time.Sleep(2000 * time.Millisecond)

	if ok, _ := hm.Get("long"); ok {
		t.Fatal("long key should be gone now")
	}

	if got := hm.GetEntries(); got != 0 {
		t.Fatalf("Expected 0 entries after all TTLs expired, got %d", got)
	}
}

// Benchmarks: measure latency of Set and Get operations
func BenchmarkHashMap_Set(b *testing.B) {
	name := fmt.Sprintf("bench_set_%d", time.Now().UnixNano())
	hm, err := NewHashMap(name)
	if err != nil {
		b.Fatalf("NewHashMap error: %v", err)
	}
	b.Cleanup(func() {
		_ = hm.Close()
		removeAOF(&testing.T{}, name)
	})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := "k-" + strconv.Itoa(i)
		v := "v-" + strconv.Itoa(i)
		if !hm.Set(0, k, v) {
			b.Fatalf("Set failed at %d", i)
		}
	}
}

func BenchmarkHashMap_Get(b *testing.B) {
	name := fmt.Sprintf("bench_get_%d", time.Now().UnixNano())
	hm, err := NewHashMap(name)
	if err != nil {
		b.Fatalf("NewHashMap error: %v", err)
	}
	b.Cleanup(func() {
		_ = hm.Close()
		removeAOF(&testing.T{}, name)
	})

	// prefill
	const N = 10000
	for i := 0; i < N; i++ {
		_ = hm.Set(0, "k-"+strconv.Itoa(i), "v-"+strconv.Itoa(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "k-" + strconv.Itoa(i%N)
		ok, _ := hm.Get(key)
		if !ok {
			b.Fatalf("Get failed for key %s", key)
		}
	}
}
