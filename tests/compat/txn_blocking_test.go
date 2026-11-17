package compat

import (
	"testing"
	"time"

	"github.com/tidwall/resp"
)

// TestBlockingPopWakesAfterExecCommit covers the interaction between the
// transaction write gate and the blocking commands: a parked BLPOP holds the
// shared side of the gate only around each individual pop, so a MULTI/EXEC that
// pushes onto that list must commit (and then wake the waiter) instead of
// deadlocking with it.
func TestBlockingPopWakesAfterExecCommit(t *testing.T) {
	c := setup(t)
	blocker := secondConn(t, c)

	type result struct {
		v   resp.Value
		err error
	}
	ch := make(chan result, 1)
	go func() {
		if err := blocker.WriteArray([]resp.Value{
			resp.StringValue("BLPOP"), resp.StringValue("q"), resp.StringValue("2"),
		}); err != nil {
			ch <- result{err: err}
			return
		}
		v, _, err := blocker.ReadValue()
		ch <- result{v: v, err: err}
	}()

	// Let the waiter park before the transaction starts.
	time.Sleep(100 * time.Millisecond)

	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "LPUSH", "q", "first"), "QUEUED")
	if res := do(t, c, "EXEC").Array(); len(res) != 1 || res[0].Integer() != 1 {
		t.Fatalf("EXEC = %v", res)
	}

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("blpop: %v", r.err)
		}
		arr := r.v.Array()
		if len(arr) != 2 || arr[0].String() != "q" || arr[1].String() != "first" {
			t.Fatalf("blpop = %v, want [q first]", arr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("BLPOP never returned after the transaction committed (deadlock?)")
	}
}

// TestBlockingCommandInsideExecReturnsImmediately checks the queued blocking
// command path: inside EXEC it runs in non-blocking mode, and it must not try to
// re-take the write gate the transaction already holds.
func TestBlockingCommandInsideExecReturnsImmediately(t *testing.T) {
	c := setup(t)

	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "BLPOP", "empty-list", "0"), "QUEUED")
	assertStr(t, do(t, c, "SET", "after", "1"), "QUEUED")

	done := make(chan resp.Value, 1)
	go func() {
		// Raw write/read: t.Fatalf is not allowed off the test goroutine.
		if err := c.WriteArray([]resp.Value{resp.StringValue("EXEC")}); err != nil {
			done <- resp.Value{}
			return
		}
		v, _, err := c.ReadValue()
		if err != nil {
			done <- resp.Value{}
			return
		}
		done <- v
	}()
	select {
	case v := <-done:
		// go-redis-style clients see the multi-bulk array; tidwall/resp keeps it
		// as an array either way.
		arr := v.Array()
		if len(arr) != 2 {
			t.Fatalf("EXEC = %v, want 2 replies", v)
		}
		if !arr[0].IsNull() {
			t.Fatalf("BLPOP inside EXEC = %v, want nil", arr[0])
		}
		if arr[1].String() != "OK" {
			t.Fatalf("SET inside EXEC = %q, want OK", arr[1].String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("EXEC with a queued BLPOP never returned (deadlock?)")
	}
}
