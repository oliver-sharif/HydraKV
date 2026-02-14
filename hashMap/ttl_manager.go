package hashMap

import (
	"context"
	"hydrakv/envhandler"
	"log"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type TTLManager struct {
	List        []*TTLEntryManager
	lastDeleted atomic.Int64
	Name        string
	delCallback func(key string) bool
	numShards   int64
	cancel      context.CancelFunc
}

type TTLEntryManager struct {
	list map[int64]map[string]*Entry
	mut  sync.Mutex
}

// NewTTLManager creates a new TTLEntryManager
func NewTTLManager(name string, delFunc func(key string) bool) *TTLManager {
	log.Println("TTLManager initialized")
	// Create the TTLManager
	ttl := &TTLManager{lastDeleted: atomic.Int64{}, Name: name, delCallback: delFunc, List: make([]*TTLEntryManager, 0)}

	// set numshards
	ttl.numShards = int64(ttl.LowerPowerOfTwo(uint64(runtime.NumCPU() * (*envhandler.ENV.CPU_MULTIPLIER))))

	// Create the TTLEntryManagers
	for i := 0; i < int(ttl.numShards); i++ {
		ttl.newTTLEntryManager()
	}

	// init lastDeleted to
	ttl.lastDeleted.Store(time.Now().Unix())

	log.Println("TTLManager for DB " + name + " initialized..")
	return ttl
}

// Stop stops the TTLManager and all its managers
func (ttlm *TTLManager) Stop() {
	if ttlm.cancel == nil {
		return
	}
	ttlm.cancel()
	log.Println("TTLManager for DB " + ttlm.Name + " stopped..")
}

// newTTLEntryManager creates a new TTLEntryManager
func (ttlm *TTLManager) newTTLEntryManager() {
	tt := &TTLEntryManager{list: make(map[int64]map[string]*Entry), mut: sync.Mutex{}}
	ttlm.List = append(ttlm.List, tt)
}

// addEntry adds an entry to the TTLEntryManager
func (ttlm *TTLManager) addEntry(entry *Entry) {
	// return if unnecessary
	if entry.Ttl <= 0 {
		return
	}

	// ok we need to find the right TTLEntryManager
	em := ttlm.List[entry.Hash&uint64(ttlm.numShards-1)]

	// set the key to now + entry.ttl
	future := time.Now().Unix() + entry.Ttl

	em.mut.Lock()
	defer em.mut.Unlock()

	// for security reasons - return if the entry is already expired
	if future <= ttlm.lastDeleted.Load() {
		return
	}

	// if map already exist - add - else create new map and add
	if values, ok := em.list[future]; ok {
		values[entry.Key] = entry
	} else {
		em.list[future] = map[string]*Entry{entry.Key: entry}
	}
}

// delEntry deletes an entry from the TTLEntryManager
func (ttlm *TTLManager) delEntry(entry *Entry, ttl int64) {
	// get the TTLEntryManager
	em := ttlm.List[entry.Hash&uint64(ttlm.numShards-1)]
	em.mut.Lock()
	defer em.mut.Unlock()

	// Delete bucket if empty
	if bucket, ok := em.list[ttl]; ok {
		delete(bucket, entry.Key)
		if len(bucket) == 0 {
			delete(em.list, ttl)
		}
	}

}

// deleteEntries deletes expired entries (if there are some)
func (ttlm *TTLManager) delEntries(now int64) {
	last := ttlm.lastDeleted.Load()

	for i := last + 1; i <= now; i++ {
		for _, ttlEntry := range ttlm.List {
			ttlEntry.mut.Lock()
			entries, ok := ttlEntry.list[i]
			if ok {
				delete(ttlEntry.list, i)
			}
			ttlEntry.mut.Unlock()
			if ok {
				ttlm.delEntriesFromHashMap(entries)
			}
		}
	}
	ttlm.lastDeleted.Store(now)
}

// delEntriesFromHashMap deletes the entries from the HashMap
func (ttlm *TTLManager) delEntriesFromHashMap(entries map[string]*Entry) {
	for _, entry := range entries {
		ttlm.delCallback(entry.Key) // fire and forget
	}
}

// Start starts the TTLManager WatchDog
func (ttlm *TTLManager) Start() {
	// create a context with a cancel function to stop execution if necessary
	ctx, cancel := context.WithCancel(context.Background())
	ttlm.cancel = cancel

	// start the go routine
	go func() {
		for {
			// What we need is a Secondexact deletion of expired entries
			now := time.Now()
			next := now.Truncate(time.Second).Add(time.Second)

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Until(next)):
				ttlm.delEntries(next.Unix())
			}
		}
	}()
}

// LowerPowerOfTwo returns the lower power of two greater than or equal to shards
func (ttlm *TTLManager) LowerPowerOfTwo(shards uint64) uint64 {
	if shards <= 2 {
		return 2
	}
	return 1 << (bits.Len64(shards - 1))
}
