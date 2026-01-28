package main

import (
	"fmt"
	"time"

	"github.com/spaghetti-lover/go-db/pkg/kv"
)

func main() {
	engine, err := kv.NewWALBPTreeEngine("test.db", "test.wal")
	if err != nil {
		fmt.Println("Init error:", err)
		return
	}

	// Preload some data
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("val%d", i))
		engine.Set(key, val)
	}

	// Benchmark reads
	N := 100000
	start := time.Now()
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("key%d", i%1000))
		engine.Get(key)
	}
	elapsed := time.Since(start)
	fmt.Printf("Reads/sec: %.0f\n", float64(N)/elapsed.Seconds())
}
