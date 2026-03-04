package tests

import (
	"hydrakv/fifolifo"
	"testing"
)

func TestFifoLifo(t *testing.T) {
	t.Run("FIFO Operations", func(t *testing.T) {
		q, err := fifolifo.NewFiFoLiFo("test-fifo", 10)
		if err != nil {
			t.Fatalf("Failed to create FIFO: %v", err)
		}

		entries := []string{"first", "second", "third"}
		for _, e := range entries {
			ok, err := q.Push(e)
			if !ok || err != nil {
				t.Errorf("Failed to push %s: %v", e, err)
			}
		}

		if q.Len() != 3 {
			t.Errorf("Expected length 3, got %d", q.Len())
		}

		for _, want := range entries {
			got, err := q.FPop()
			if err != nil {
				t.Errorf("Failed to FPop: %v", err)
			}
			if got != want {
				t.Errorf("FPop got %s, want %s", got, want)
			}
		}

		if q.Len() != 0 {
			t.Errorf("Expected empty queue after FPop, got length %d", q.Len())
		}
	})

	t.Run("LIFO Operations", func(t *testing.T) {
		q, err := fifolifo.NewFiFoLiFo("test-lifo", 10)
		if err != nil {
			t.Fatalf("Failed to create LIFO: %v", err)
		}

		entries := []string{"first", "second", "third"}
		for _, e := range entries {
			ok, err := q.Push(e)
			if !ok || err != nil {
				t.Errorf("Failed to push %s: %v", e, err)
			}
		}

		// LIFO should return third, second, first
		expected := []string{"third", "second", "first"}
		for _, want := range expected {
			got, err := q.LPop()
			if err != nil {
				t.Errorf("Failed to LPop: %v", err)
			}
			if got != want {
				t.Errorf("LPop got %s, want %s", got, want)
			}
		}

		if q.Len() != 0 {
			t.Errorf("Expected empty queue after LPop, got length %d", q.Len())
		}
	})

	t.Run("Mixed Operations", func(t *testing.T) {
		q, err := fifolifo.NewFiFoLiFo("test-mixed", 10)
		if err != nil {
			t.Fatalf("Failed to create Mixed: %v", err)
		}

		q.Push("A")
		q.Push("B")
		q.Push("C")

		// Pop "A" (FIFO)
		val, _ := q.FPop()
		if val != "A" {
			t.Errorf("Expected A, got %s", val)
		}

		// Pop "C" (LIFO)
		val, _ = q.LPop()
		if val != "C" {
			t.Errorf("Expected C, got %s", val)
		}

		// Pop "B" (FIFO/LIFO - only one left)
		val, _ = q.FPop()
		if val != "B" {
			t.Errorf("Expected B, got %s", val)
		}

		if q.Len() != 0 {
			t.Errorf("Expected empty queue, got %d", q.Len())
		}
	})
}
