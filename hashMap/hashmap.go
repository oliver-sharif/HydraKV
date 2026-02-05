package hashMap

import (
	"bufio"
	"errors"
	"hydrakv/envhandler"
	"hydrakv/xxhash64"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	DefaultBasketSize = 2048
)

type HashMap struct {
	table          []*Basket
	keyCount       int64
	mutex          sync.RWMutex
	xxhash         *xxhash64.XXHash64
	Entries        atomic.Uint64
	Name           string
	Aof            *AOF
	reset          bool
	basketLocks    []sync.RWMutex
	cpuCount       int
	resizeCheck    chan struct{}
	deletedEntries atomic.Int64
	done           chan struct{}
	TTlManager     *TTLManager
	basketNum      int
	basketLockNum  int
}

// Metrics for Prometheus in Hashmap
var (
	// Counter for total KV operations
	kvOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kv_operations_total",
			Help: "Total number of KV operations",
		},
		[]string{"operation", "status"},
	)

	// Histogram for KV operation durations
	kvOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kv_operation_duration_seconds",
			Help:    "Duration of KV operations in seconds",
			Buckets: prometheus.DefBuckets, // oder eigene Buckets: []float64{.001, .01, .1, 1}
		},
		[]string{"operation"},
	)

	// Gauge for current number of keys in storage
	kvStorageSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kv_storage_size",
			Help: "Current number of keys in storage",
		},
	)
)

// NewHashMap returns a new HashMap struct
func NewHashMap(name string) (*HashMap, error) {

	// Create a new HashMap
	hm := &HashMap{
		table: make([]*Basket, DefaultBasketSize), mutex: sync.RWMutex{}, xxhash: xxhash64.XXH,
		Name: strings.ToUpper(name), reset: true, cpuCount: runtime.NumCPU(),
		resizeCheck: make(chan struct{}, 1001), done: make(chan struct{}),
	}

	// Create TTL Manager for this HashMap
	hm.TTlManager = NewTTLManager(name, hm.Del)

	// create AOF to save data to disk
	aof, err := NewAOF(name, hm.GetAllEntriesAndCompress)
	if err != nil {
		return nil, err
	}

	hm.Aof = aof

	// init the Locks
	lpot := hm.TTlManager.LowerPowerOfTwo(uint64(hm.cpuCount * (*envhandler.ENV.CPU_MULTIPLIER)))
	log.Printf("Using %d basket locks", lpot)
	// lpot may not be gt int.Max
	if lpot >= math.MaxInt32 {
		lpot = math.MaxInt32
	}
	hm.basketLockNum = int(lpot)
	hm.basketLocks = make([]sync.RWMutex, lpot)

	// init the Baskets
	for i := 0; i < DefaultBasketSize; i++ {
		hm.table[i] = NewBasket()
	}
	hm.basketNum = DefaultBasketSize

	// start the resize checker
	go hm.ResizeChecker()

	// try to replay the AOF file
	err = hm.ReplayAOF()
	if err != nil {
		return nil, err
	}

	// set reset to false
	hm.reset = false

	// start the AOF loop
	if err := hm.Aof.Start(); err != nil {
		return nil, err
	}

	return hm, nil
}

// ReplayAOF replays the AOF file to restore the HashMap state
func (hm *HashMap) ReplayAOF() error {
	// if the bin file not exists we can return
	if _, err := os.Stat(hm.Aof.FileName); os.IsNotExist(err) {
		return nil
	}

	// open the file
	f, err := os.Open(hm.Aof.FileName)
	if err != nil {
		return err
	}
	defer f.Close()

	// Create buffered reader
	reader := bufio.NewReaderSize(f, 1024*64)

	// ... existing code ...
	for {
		var d Data
		err := hm.Aof.readFrame(reader, &d)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("AOF truncated for %s, stopping replay", hm.Name)
				break
			}
			return err
		}

		switch d.Action {
		case "set":
			hm.Set(d.Ttl, d.Key, d.Value)
			// ... existing code ...
		case "del":
			hm.Del(d.Key)
		case "incr":
			hm.Incr(d.Ttl, d.Key, d.Value)
		}
	}
	log.Printf("Replayed AOF for %s", hm.Name)
	return nil
}

// getIndex gets the Index of a Key
func (hm *HashMap) getIndex(key string) (int, uint64) {
	h := hm.xxhash.HashString(key)
	index := h & uint64(hm.basketNum-1)
	return int(index), h
}

// Set inserts or updates a key-value pair in the HashMap. Returns true if the operation is successful.
func (hm *HashMap) Set(ttl int64, key string, value string) bool {
	timer := prometheus.NewTimer(kvOperationDuration.WithLabelValues("set"))
	defer timer.ObserveDuration()

	// Write the AOF - this happens in a separate goroutine
	if !hm.reset {
		hm.Aof.com <- Data{Action: "set", Key: key, Value: value, Ttl: ttl}
	}

	// check resize
	select {
	case hm.resizeCheck <- struct{}{}:
	default:
	}

	// we need global read lock
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	// we need the index of the key
	index, hash := hm.getIndex(key)

	// we need a Basketlocal write lock
	hm.WLockBasketLock(hash)
	defer hm.WUnlockBasketLock(hash)

	// Get the basket which should hold / newly hold our entry
	basket := hm.table[index]

	// Does it exist? If yes - update value
	for item := basket.Items; item != nil; item = item.Next {
		if item.Key == key {
			item.Value = value
			// if there was a TTL add delete the entry from the TTLManager
			if item.Ttl != 0 {
				hm.TTlManager.delEntry(item, item.Ttl)
			}
			item.Ttl = ttl
			hm.TTlManager.addEntry(item)
			return true
		}
	}

	// If not - add it
	e := NewEntry(ttl, key, value, hash, hm.table[index].Items)
	hm.table[index].Items = e
	hm.TTlManager.addEntry(e)
	hm.Entries.Add(1)
	kvStorageSize.Set(float64(hm.Entries.Load()))
	kvOperations.WithLabelValues("set", "ok").Inc()
	return true
}

// Get retrieves the value associated with the given key from the HashMap. Returns an empty string if the key is not found.
func (hm *HashMap) Get(key string) (bool, string) {
	timer := prometheus.NewTimer(kvOperationDuration.WithLabelValues("get"))
	defer timer.ObserveDuration()

	// we need global read lock
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	// get the right index
	index, hash := hm.getIndex(key)
	basket := hm.table[index]

	// we need a Basketlocal write lock
	hm.RLockBasketLock(hash)
	defer hm.RUnlockBasketLock(hash)

	// Try to get the value in existing entries
	for item := basket.Items; item != nil; item = item.Next {
		if item.Key == key {
			kvOperations.WithLabelValues("get", "found").Inc()
			return true, item.Value
		}
	}

	// it doesent exist!
	kvOperations.WithLabelValues("get", "not_found").Inc()
	return false, ""
}

// Incr increments the value associated with the given key by the given amount. Returns the new value.
func (hm *HashMap) Incr(ttl int64, key, amount string) bool {
	timer := prometheus.NewTimer(kvOperationDuration.WithLabelValues("incr"))
	defer timer.ObserveDuration()
	// Writes the AOF - this happens in a separate goroutine
	if !hm.reset {
		hm.Aof.com <- Data{Action: "incr", Key: key, Value: amount}
	}

	// we need global read lock
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	// get the right index
	index, hash := hm.getIndex(key)
	basket := hm.table[index]

	// basketlocal write lock
	hm.WLockBasketLock(hash)
	defer hm.WUnlockBasketLock(hash)

	// we need the amount as int64
	for item := basket.Items; item != nil; item = item.Next {
		if item.Key == key {
			// make a number from item.Value and amount
			val, ok := hm.checkIsNumber(item.Value)
			if !ok {
				return false
			}

			add, ok := hm.checkIsNumber(amount)
			if !ok {
				return false
			}
			item.Value = strconv.FormatInt(val+add, 10)

			// if there was a TTL add delete the entry from the TTLManager
			if item.Ttl != 0 {
				hm.TTlManager.delEntry(item, item.Ttl)
			}
			item.Ttl = ttl
			hm.TTlManager.addEntry(item)
			kvOperations.WithLabelValues("incr", "ok").Inc()
			return true
		}
	}

	// if it not exists - set the value to the amount value
	e := NewEntry(ttl, key, amount, hash, basket.Items)
	basket.Items = e
	hm.TTlManager.addEntry(e)
	hm.Entries.Add(1)
	kvStorageSize.Set(float64(hm.Entries.Load()))
	kvOperations.WithLabelValues("incr", "ok").Inc()
	return true
}

// Del deletes the entry associated with the provided key from the HashMap.
// Returns true if the key was found and successfully removed; otherwise, returns false.
func (hm *HashMap) Del(key string) bool {
	timer := prometheus.NewTimer(kvOperationDuration.WithLabelValues("del"))
	defer timer.ObserveDuration()

	// Write the AOF - this happens in a separate goroutine
	if !hm.reset {
		hm.Aof.com <- Data{Action: "del", Key: key}
	}

	// we need global read lock
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	// Get index and right basket
	index, hash := hm.getIndex(key)
	basket := hm.table[index]

	// we need a Basketlocal write lock
	hm.WLockBasketLock(hash)
	defer hm.WUnlockBasketLock(hash)

	// Basket is empty
	if basket.Items == nil {
		return false
	}

	var prev *Entry

	// Search for the right key
	for item := basket.Items; item != nil; item = item.Next {
		if item.Key == key {
			// remove the entry from the TTLManager
			hm.TTlManager.delEntry(item, item.Ttl)
			if prev != nil {
				prev.Next = item.Next
			} else {
				basket.Items = item.Next
			}
			hm.Entries.Add(^uint64(0))
			hm.deletedEntries.Add(1)
			kvStorageSize.Set(float64(hm.Entries.Load()))
			kvOperations.WithLabelValues("del", "ok").Inc()
			return true
		}
		prev = item
	}
	kvOperations.WithLabelValues("del", "not_found").Inc()
	return false
}

// checkNewBasket checks if the load factor exceeds 0.75 and resizes the HashMap by doubling its capacity if necessary.
func (hm *HashMap) checkNewBasket() {
	newSize := len(hm.table) * 2
	newTable := make([]*Basket, newSize)

	for i := 0; i < newSize; i++ {
		newTable[i] = NewBasket()
	}

	for _, oldBucket := range hm.table {
		for item := oldBucket.Items; item != nil; {
			next := item.Next
			newIndex := int(item.Hash & uint64(newSize-1))
			item.Next = newTable[newIndex].Items
			newTable[newIndex].Items = item
			item = next
		}
	}
	hm.table = newTable
	hm.basketNum = newSize
}

// GetAllEntriesAndCompress returns a slice of all entries in the HashMap
// This is needed for compression of the AOF
func (hm *HashMap) GetAllEntriesAndCompress() []*AOFEntry {
	timer := prometheus.NewTimer(kvOperationDuration.WithLabelValues("compress"))
	defer timer.ObserveDuration()
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	var entries []*AOFEntry
	for _, bucket := range hm.table {
		for item := bucket.Items; item != nil; item = item.Next {
			d := &AOFEntry{Key: item.Key, Value: item.Value, Ttl: item.Ttl}
			entries = append(entries, d)
		}
	}
	return entries
}

// GetBasketNum returns the number of baskets in the HashMap
func (hm *HashMap) GetBasketNum() int {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()
	return len(hm.table)
}

// GetEntries returns the number of entries in the HashMap
func (hm *HashMap) GetEntries() int64 {
	return int64(hm.Entries.Load())
}

// Close Closes the AOF and Hashmap
func (hm *HashMap) Close() error {
	hm.TTlManager.Stop()
	err := hm.Aof.Close()
	close(hm.done)
	return err
}

// CheckResize locks the HashMap and checks if the load factor exceeds 0.75; triggers resizing if necessary.
func (hm *HashMap) CheckResize() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	if float64(hm.Entries.Load())/float64(len(hm.table)) > 0.75 {
		hm.checkNewBasket()
	}
}

// checkIsNumber checks if the given string is a number
func (hm *HashMap) checkIsNumber(s string) (int64, bool) {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, true
	}
	return 0, false
}

// WLockBasketLock write locks the basket at the given index
func (hm *HashMap) WLockBasketLock(index uint64) {
	hm.basketLocks[index&uint64(hm.basketLockNum-1)].Lock()
}

// WUnlockBasketLock write unlocks the basket at the given index
func (hm *HashMap) WUnlockBasketLock(index uint64) {
	hm.basketLocks[index&uint64(hm.basketLockNum-1)].Unlock()
}

// RLockBasketLock read locks the basket at the given index
func (hm *HashMap) RLockBasketLock(index uint64) {
	hm.basketLocks[index&uint64(hm.basketLockNum-1)].RLock()
}

// RUnlockBasketLock read unlocks the basket at the given index
func (hm *HashMap) RUnlockBasketLock(index uint64) {
	hm.basketLocks[index&uint64(hm.basketLockNum-1)].RUnlock()
}

// ResizeChecker processes resize check signals and triggers resize if a threshold of 1000 signals is met.
func (hm *HashMap) ResizeChecker() {
	inputs := 0
	resizeTicker := time.NewTicker(60 * time.Second)

	// on return clean up
	defer func() {
		log.Printf("MapSizeChecker stopped for DB %s stopped", hm.Name)
		resizeTicker.Stop()
	}()

	for {
		select {
		case <-hm.resizeCheck:
			inputs++
			if inputs%1000 == 0 {
				hm.CheckResize()
				inputs = 0
			}
		case <-resizeTicker.C:
			// this will compress the AOF file
			entries := hm.Entries.Load()
			deleted := hm.deletedEntries.Load()

			if (entries > 2 || deleted > 2) && deleted >= int64(entries)/2 {
				// this will compress the AOF file
				hm.Aof.compressing <- struct{}{}
				hm.deletedEntries.Store(0)
			}
		case <-hm.done:
			return
		}
	}
}
