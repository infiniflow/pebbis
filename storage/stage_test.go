package storage_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/pebbis/pebbis/config"
	"github.com/pebbis/pebbis/storage"
)

// openStageEngine opens a durable engine on dir.
func openStageEngine(t *testing.T, dir string) *storage.Engine {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Dir = dir
	cfg.SyncPolicy = config.SyncAlways
	eng, err := storage.Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return eng
}

// TestStageIsDiscardedOnReopen is the crash-atomicity guarantee: writes buffered
// in a stage that never commits leave no trace on disk, so a crash in the middle
// of a transaction is indistinguishable from the transaction never having run.
func TestStageIsDiscardedOnReopen(t *testing.T) {
	dir := t.TempDir()

	eng := openStageEngine(t, dir)
	if err := eng.Put([]byte("keep"), []byte("before")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	eng.BeginStage()
	if err := eng.Put([]byte("keep"), []byte("after")); err != nil {
		t.Fatalf("staged put: %v", err)
	}
	if err := eng.Put([]byte("added"), []byte("x")); err != nil {
		t.Fatalf("staged put: %v", err)
	}

	// Reads inside the stage must see the buffered writes.
	if got, err := eng.GetCopy([]byte("keep")); err != nil || !bytes.Equal(got, []byte("after")) {
		t.Fatalf("staged read: got %q err %v, want \"after\"", got, err)
	}
	if got, err := eng.GetCopy([]byte("added")); err != nil || !bytes.Equal(got, []byte("x")) {
		t.Fatalf("staged read: got %q err %v, want \"x\"", got, err)
	}

	// Simulate the crash: drop the stage and close without committing.
	eng.AbortStage()
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	eng = openStageEngine(t, dir)
	defer func() { _ = eng.Close() }()

	if got, err := eng.GetCopy([]byte("keep")); err != nil || !bytes.Equal(got, []byte("before")) {
		t.Fatalf("after reopen: keep = %q err %v, want \"before\"", got, err)
	}
	if _, err := eng.GetCopy([]byte("added")); err != storage.ErrNotFound {
		t.Fatalf("after reopen: added should be gone, err = %v", err)
	}
}

// TestStageCommitIsAllOrNothing checks the other side: a committed stage is
// durable as a whole.
func TestStageCommitIsAllOrNothing(t *testing.T) {
	dir := t.TempDir()

	eng := openStageEngine(t, dir)
	if err := eng.Put([]byte("old"), []byte("1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	eng.BeginStage()
	if err := eng.Put([]byte("a"), []byte("A")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := eng.Put([]byte("b"), []byte("B")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := eng.Delete([]byte("old")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := eng.CommitStage(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	eng = openStageEngine(t, dir)
	defer func() { _ = eng.Close() }()

	for k, want := range map[string]string{"a": "A", "b": "B"} {
		if got, err := eng.GetCopy([]byte(k)); err != nil || string(got) != want {
			t.Fatalf("%s = %q err %v, want %q", k, got, err, want)
		}
	}
	if _, err := eng.GetCopy([]byte("old")); err != storage.ErrNotFound {
		t.Fatalf("old should be deleted, err = %v", err)
	}
}

// TestStageScanMergesWrites makes sure range scans see staged writes, staged
// deletes and staged merge operands, in key order.
func TestStageScanMergesWrites(t *testing.T) {
	dir := t.TempDir()
	eng := openStageEngine(t, dir)
	defer func() { _ = eng.Close() }()

	for _, k := range []string{"k1", "k2", "k3", "k4"} {
		if err := eng.Put([]byte(k), []byte("v"+k)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := eng.Merge([]byte("k5"), storage.EncodeIntDelta(1)); err != nil {
		t.Fatalf("merge: %v", err)
	}

	eng.BeginStage()
	if err := eng.Put([]byte("k2"), []byte("new")); err != nil { // overwrite
		t.Fatalf("put: %v", err)
	}
	if err := eng.Delete([]byte("k3")); err != nil { // delete
		t.Fatalf("delete: %v", err)
	}
	if err := eng.Put([]byte("k0"), []byte("head")); err != nil { // new key, sorts first
		t.Fatalf("put: %v", err)
	}
	if err := eng.Merge([]byte("k5"), storage.EncodeIntDelta(2)); err != nil { // merge on top
		t.Fatalf("merge: %v", err)
	}

	// Plain Put values are raw; merge results carry the value header.
	payload := func(v []byte) string {
		if _, _, p, err := storage.DecodeValue(v); err == nil {
			return string(p)
		}
		return string(v)
	}

	var got []string
	if err := eng.Scan([]byte("k"), func(k, v []byte) error {
		got = append(got, fmt.Sprintf("%s=%s", k, payload(v)))
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	want := []string{"k0=head", "k1=vk1", "k2=new", "k4=vk4", "k5=3"}
	if len(got) != len(want) {
		t.Fatalf("scan = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}

	// The last key of the range must also come from the merged view.
	key, value, found, err := eng.LastInRange([]byte("k"), []byte("k9"))
	if err != nil || !found || string(key) != "k5" || payload(value) != "3" {
		t.Fatalf("LastInRange = %q %q %v %v, want k5 payload 3", key, value, found, err)
	}
}

// TestStageDeleteThenReadd covers the ordering trap: a staged delete followed by
// a staged re-add of the same key must be visible, not suppressed.
func TestStageDeleteThenReadd(t *testing.T) {
	dir := t.TempDir()
	eng := openStageEngine(t, dir)
	defer func() { _ = eng.Close() }()

	if err := eng.Put([]byte("k"), []byte("old")); err != nil {
		t.Fatalf("put: %v", err)
	}
	eng.BeginStage()
	if err := eng.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := eng.Put([]byte("k"), []byte("new")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got, err := eng.GetCopy([]byte("k")); err != nil || string(got) != "new" {
		t.Fatalf("get = %q err %v, want new", got, err)
	}
	if err := eng.Scan([]byte("k"), func(k, v []byte) error {
		if string(k) != "k" || string(v) != "new" {
			t.Fatalf("scan = %q=%q, want k=new", k, v)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
}
