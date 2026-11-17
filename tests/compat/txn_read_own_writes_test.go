package compat

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// TestExecReadsItsOwnWrites pins the read-your-writes half of the staged
// transaction: while EXEC buffers its writes in one Pebble batch, the queued
// reads have to see them. Large values and sparse aggregates live in Pebble
// rather than in the in-memory index, so they exercise the batch-aware read path
// (including merge fold and range scans).
func TestExecReadsItsOwnWrites(t *testing.T) {
	c := setup(t)

	big := strings.Repeat("x", 600)
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "SET", "big", big), "QUEUED")
	assertStr(t, do(t, c, "GET", "big"), "QUEUED")
	res := do(t, c, "EXEC").Array()
	if len(res) != 2 || res[1].String() != big {
		t.Fatalf("big value read inside EXEC: got %v", res)
	}

	// A counter written through the merge operator twice must read back as the
	// folded total, not as an operand.
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "INCR", "n"), "QUEUED")
	assertStr(t, do(t, c, "INCR", "n"), "QUEUED")
	assertStr(t, do(t, c, "GET", "n"), "QUEUED")
	res = do(t, c, "EXEC").Array()
	if len(res) != 3 || res[2].String() != "2" {
		t.Fatalf("counter inside EXEC: got %v, want last reply 2", res)
	}

	// A sparse hash (too big to be inline) is read back through the range scan.
	const fields = 120
	assertStr(t, do(t, c, "MULTI"), "OK")
	for i := 0; i < fields; i++ {
		assertStr(t, do(t, c, "HSET", "h", "f"+strconv.Itoa(i), "v"+strconv.Itoa(i)), "QUEUED")
	}
	assertStr(t, do(t, c, "HLEN", "h"), "QUEUED")
	assertStr(t, do(t, c, "HGET", "h", "f1"), "QUEUED")
	res = do(t, c, "EXEC").Array()
	if n := res[len(res)-2].Integer(); n != fields {
		t.Fatalf("HLEN inside EXEC: got %d, want %d", n, fields)
	}
	if v := res[len(res)-1].String(); v != "v1" {
		t.Fatalf("HGET inside EXEC: got %q, want v1", v)
	}

	// Deleting a key and reading it back in the same transaction sees the delete.
	preset(t, c, "gone", "here")
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "DEL", "gone"), "QUEUED")
	assertStr(t, do(t, c, "GET", "gone"), "QUEUED")
	assertStr(t, do(t, c, "EXISTS", "gone"), "QUEUED")
	res = do(t, c, "EXEC").Array()
	if !res[1].IsNull() {
		t.Fatalf("GET after DEL inside EXEC: got %v, want nil", res[1])
	}
	if res[2].Integer() != 0 {
		t.Fatalf("EXISTS after DEL inside EXEC: got %d, want 0", res[2].Integer())
	}

	// Everything above must also be committed, not just staged.
	if v := do(t, c, "GET", "big").String(); v != big {
		t.Fatalf("committed big = %q", v)
	}
	if v := do(t, c, "GET", "n").String(); v != "2" {
		t.Fatalf("committed n = %q, want 2", v)
	}
	if n := do(t, c, "HLEN", "h").Integer(); n != fields {
		t.Fatalf("committed HLEN = %d, want %d", n, fields)
	}
}

// TestExecWriteIsOneBatch checks the atomicity marker from the outside: the
// transaction's writes must not be visible to another connection before EXEC
// returns, and must all be visible right after.
func TestExecWriteIsOneBatch(t *testing.T) {
	c := setup(t)
	other := secondConn(t, c)

	assertStr(t, do(t, c, "MULTI"), "OK")
	for i := 0; i < 20; i++ {
		assertStr(t, do(t, c, "SET", fmt.Sprintf("tx:%d", i), "v"), "QUEUED")
	}
	assertStr(t, do(t, c, "SET", "tx:marker", "v"), "QUEUED")

	// Nothing is visible before EXEC.
	if n := do(t, other, "EXISTS", "tx:marker").Integer(); n != 0 {
		t.Fatalf("queued write leaked before EXEC: EXISTS = %d", n)
	}

	res := do(t, c, "EXEC").Array()
	if len(res) != 21 {
		t.Fatalf("EXEC replies = %d, want 21", len(res))
	}
	if n := do(t, other, "EXISTS", "tx:marker").Integer(); n != 1 {
		t.Fatalf("committed write missing after EXEC: EXISTS = %d", n)
	}
	if n := do(t, other, "DBSIZE").Integer(); n < 21 {
		t.Fatalf("DBSIZE after commit = %d, want >= 21", n)
	}
}
