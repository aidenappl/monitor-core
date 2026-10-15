package apikeys

import (
	"testing"
	"time"
)

func TestMarkUsedKeepsNewestPerKey(t *testing.T) {
	takeUsage() // start clean

	markUsed("k-1")
	first := usage["k-1"]
	time.Sleep(2 * time.Millisecond)
	markUsed("k-1")
	markUsed("k-2")

	batch := takeUsage()
	if len(batch) != 2 {
		t.Fatalf("batch has %d keys, want 2", len(batch))
	}
	if !batch["k-1"].After(first) {
		t.Errorf("k-1 = %v, want later than %v", batch["k-1"], first)
	}
	if again := takeUsage(); again != nil {
		t.Errorf("second take = %v, want nil after the swap", again)
	}
}
