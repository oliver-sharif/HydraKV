package hashMap

type Entry struct {
	Hash  uint64
	Key   string
	Value string
	Next  *Entry
	Ttl   int64
}

// NewEntry creates a new Entry
func NewEntry(ttl int64, key string, value string, hash uint64, last *Entry) *Entry {
	return &Entry{Ttl: ttl, Key: key, Value: value, Hash: hash, Next: last}
}
