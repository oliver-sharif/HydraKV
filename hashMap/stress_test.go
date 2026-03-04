package hashMap

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func BenchmarkHashMap_StressParallel(b *testing.B) {
	name := fmt.Sprintf("bench_stress_%d", time.Now().UnixNano())
	hm, err := NewHashMap(name)
	if err != nil {
		b.Fatalf("NewHashMap error: %v", err)
	}
	defer func() {
		_ = hm.Close()
		removeAOF(&testing.T{}, name)
	}()

	start := time.Now()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key-" + strconv.Itoa(i)
			val := "val-" + strconv.Itoa(i)

			// Stress both Set and Get
			hm.Set(0, key, val)
			hm.Get(key)

			i++
		}
	})

	b.StopTimer()
	duration := time.Since(start)
	totalOps := int64(b.N) * 2 // Set + Get

	if b.N > 1 {
		fmt.Printf("\n--- Stress Test Results ---\n")
		fmt.Printf("Total Operations: %d\n", totalOps)
		fmt.Printf("Total Duration:   %v\n", duration)
		fmt.Printf("Ops/sec:          %.2f\n", float64(totalOps)/duration.Seconds())
		fmt.Printf("Avg Latency/Op:   %v\n", duration/time.Duration(totalOps))
		fmt.Printf("---------------------------\n")
	}
}
