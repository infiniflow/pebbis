package compat

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tidwall/resp"
)

// TestExecIsIsolated is the regression test for the transaction isolation bug:
// EXEC used to run its queued commands without holding the per-key locks, so a
// concurrent writer could land between two queued commands and the transaction
// observed a half-applied state. A transaction that SETs a key and reads it back
// must never see another client's value in between.
func TestExecIsIsolated(t *testing.T) {
	c := setup(t)
	writer := secondConn(t, c)

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// The writer must keep the connection in request/reply lockstep, but it
		// cannot call t.Fatalf from a non-test goroutine, so failures just end it.
		for !stop.Load() {
			if err := writer.WriteArray([]resp.Value{
				resp.StringValue("SET"), resp.StringValue("k"), resp.StringValue("intruder"),
			}); err != nil {
				return
			}
			if _, _, err := writer.ReadValue(); err != nil {
				return
			}
		}
	}()

	const rounds = 300
	for i := 0; i < rounds; i++ {
		mine := "txn-" + strconv.Itoa(i)
		assertStr(t, do(t, c, "MULTI"), "OK")
		assertStr(t, do(t, c, "SET", "k", mine), "QUEUED")
		assertStr(t, do(t, c, "GET", "k"), "QUEUED")

		res := do(t, c, "EXEC").Array()
		if len(res) != 2 {
			t.Fatalf("EXEC returned %d replies, want 2", len(res))
		}
		if got := res[1].String(); got != mine {
			t.Fatalf("round %d: GET inside EXEC saw %q, want %q (a writer interleaved)", i, got, mine)
		}
	}
	stop.Store(true)
	wg.Wait()
}

// TestExecLocksWatchedKey pins the other half of the fix: the WATCH version
// check now runs under the transaction's lock, so a write that lands between
// WATCH and EXEC can no longer be missed if it happens after the check.
func TestExecAbortsWhenAnotherClientWrites(t *testing.T) {
	c := setup(t)
	other := secondConn(t, c)

	preset(t, c, "w", "0")
	assertStr(t, do(t, c, "WATCH", "w"), "OK")
	assertStr(t, do(t, other, "SET", "w", "1"), "OK")

	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "SET", "w", "2"), "QUEUED")
	if v := do(t, c, "EXEC"); !v.IsNull() {
		t.Fatalf("EXEC should abort when a watched key changed, got %q", v.String())
	}
	assertStr(t, do(t, c, "GET", "w"), "1")
}
