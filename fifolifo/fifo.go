package fifolifo

import (
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
)

// A FIFO queue
type FifoLifo struct {
	elements    *Element
	mut         *sync.RWMutex
	name        string
	maxEntries  int
	length      atomic.Int32
	lastElement *Element
}

// An Element of the queue
type Element struct {
	id     [16]byte
	entry  string
	before *Element
	next   *Element
}

// NewFifo creates a new FIFO queue
func NewFiFoLiFo(name string, maxEntries int) (*FifoLifo, error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("maxEntries must be positive, got %d", maxEntries)
	}
	return &FifoLifo{
		elements:   nil,
		mut:        &sync.RWMutex{},
		name:       name,
		maxEntries: maxEntries,
	}, nil
}

// FPush an entry to the queue
func (f *FifoLifo) Push(entry string) (bool, error) {
	if entry == "" || f.length.Load() >= int32(f.maxEntries) {
		return false, fmt.Errorf("entry cannot be empty or queue is full, maxEntries: %d, length: %d", f.maxEntries, f.length.Load())
	}

	// get Pseudo UUID
	b, err := f.PseudoUUID()
	if err != nil {
		return false, err
	}

	f.mut.Lock()
	defer f.mut.Unlock()

	if f.lastElement == nil {
		f.elements = &Element{
			id:    b,
			entry: entry,
			next:  nil,
		}
		f.lastElement = f.elements
	} else {
		elem := &Element{
			id:    b,
			entry: entry,
			next:  nil,
		}
		f.lastElement.next = elem
		elem.before = f.lastElement
		f.lastElement = elem
	}
	f.length.Add(1)
	return true, nil
}

// FPop an entry from the FIFO queue
func (f *FifoLifo) FPop() (string, error) {
	if f.length.Load() == 0 {
		return "", fmt.Errorf("queue is empty")
	}
	f.mut.Lock()
	defer f.mut.Unlock()
	data := f.elements.entry
	f.elements = f.elements.next

	// set lastElement to nil if queue is empty
	if f.elements == nil {
		f.lastElement = nil
	} else {
		f.elements.before = nil
	}
	f.length.Add(-1)
	return data, nil
}

// LPop an entry from the LIFO queue
func (f *FifoLifo) LPop() (string, error) {
	if f.length.Load() == 0 {
		return "", fmt.Errorf("queue is empty")
	}
	f.mut.Lock()
	defer f.mut.Unlock()
	data := f.lastElement.entry
	f.lastElement = f.lastElement.before

	// set elements to nil if queue is empty
	if f.lastElement == nil {
		f.elements = nil
	} else {
		f.lastElement.next = nil
	}
	f.length.Add(-1)
	return data, nil
}

// Len returns the length of the queue
func (f *FifoLifo) Len() int {
	return int(f.length.Load())
}

// PseudoUUID generates a pseudo-random UUID
func (f *FifoLifo) PseudoUUID() ([16]byte, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return [16]byte{}, err
	}
	return [16]byte(b), nil
}
