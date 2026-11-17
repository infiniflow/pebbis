package storage

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pebbis/pebbis/config"
)

// ErrNotFound mirrors pebble.ErrNotFound so callers need not import pebble.
var ErrNotFound = pebble.ErrNotFound

// Engine owns a Pebble instance and the block cache that backs it.
//
// The block cache is the real "memory" of the store: value bytes are served from
// it, not from the Go heap. That is what makes MaxMemory a hard limit rather
// than a GC hint.
type Engine struct {
	dir      string
	db       *pebble.DB
	cache    *pebble.Cache
	writeOpt *pebble.WriteOptions
	inMemory bool
	// staged, when non-nil, is an indexed batch that absorbs every write and is
	// the first place reads look. It is what makes MULTI/EXEC crash-atomic: all
	// of a transaction's writes land in one Pebble batch, so a crash either
	// applies the whole transaction or none of it. Only one stage may be open at
	// a time; the store serialises the writers that would otherwise share it.
	staged atomic.Pointer[pebble.Batch]
	// merger is the counter merge operator, kept so a staged Merge can be folded
	// by hand (an indexed batch does not fold operands on read).
	merger *pebble.Merger
}

// Open creates or reopens a Pebble store described by cfg.
// An empty cfg.Dir yields a memory-backed store.
func Open(cfg *config.Config) (*Engine, error) {
	merger := &pebble.Merger{
		Merge: counterMerge{}.Merge,
		Name:  "pebbis.counter",
	}
	opts := &pebble.Options{
		Cache:                       pebble.NewCache(cfg.BlockCacheSize),
		MemTableSize:                cfg.MemTableSize,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       cfg.L0CompactionThreshold,
		L0StopWritesThreshold:       cfg.L0StopWritesThreshold,
		MaxConcurrentCompactions:    func() int { return min(8, max(2, runtime.NumCPU()/2)) },
		Levels:                      levelOptions(),
		// The commutative counter class (INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT)
		// sinks its read-modify-write into Pebble's merge operator, so increments
		// on the same key never serialise on a mutex. See storage/merge.go.
		Merger: merger,
	}

	inMemory := cfg.Dir == ""
	if inMemory {
		opts.FS = vfs.NewMem()
		opts.DisableWAL = true
		cfg.Dir = "" // pebble requires an empty dir when using an in-memory FS
	} else {
		opts.FS = vfs.Default
	}
	opts.WALMinSyncInterval = func() time.Duration { return cfg.WALMinSyncInterval }
	opts.EnsureDefaults()

	db, err := pebble.Open(cfg.Dir, opts)
	if err != nil {
		opts.Cache.Unref()
		return nil, fmt.Errorf("open pebble at %q: %w", cfg.Dir, err)
	}

	var wo *pebble.WriteOptions
	switch cfg.SyncPolicy {
	case config.SyncAlways:
		wo = pebble.Sync
	case config.SyncNever, config.SyncGroup:
		// SyncGroup relies on WALMinSyncInterval to batch fsyncs; the write
		// itself is still appended to the WAL synchronously.
		wo = pebble.NoSync
	}
	if inMemory {
		wo = pebble.NoSync
	}

	return &Engine{
		dir:      cfg.Dir,
		db:       db,
		cache:    opts.Cache,
		writeOpt: wo,
		inMemory: inMemory,
		merger:   merger,
	}, nil
}

// levelOptions configures block size, compression and bloom filters per level.
// Bloom filters matter a lot here: the dict misses go straight to Pebble, and
// most of them are for keys that were never written.
func levelOptions() []pebble.LevelOptions {
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		l := &levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		l.Compression = pebble.SnappyCompression
	}
	return levels
}

// Get returns the value stored at key. The returned slice is only valid until
// release is called; callers that need to keep it must copy.
func (e *Engine) Get(key []byte) (value []byte, release func(), err error) {
	if st := e.staged.Load(); st != nil {
		// An indexed batch reads through to the DB for keys it does not hold,
		// so staged writes and the committed state are one view.
		v, closer, err := st.Get(key)
		if err != nil {
			return nil, nil, err
		}
		return v, func() { _ = closer.Close() }, nil
	}
	v, closer, err := e.db.Get(key)
	if err != nil {
		return nil, nil, err
	}
	return v, func() { _ = closer.Close() }, nil
}

// GetCopy returns a heap copy of the value stored at key.
func (e *Engine) GetCopy(key []byte) ([]byte, error) {
	v, release, err := e.Get(key)
	if err != nil {
		return nil, err
	}
	defer release()
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// BeginStage opens a write transaction: from here until CommitStage or
// AbortStage, every write on this Engine is buffered instead of applied, reads
// see the buffered writes, and nothing reaches Pebble until CommitStage applies
// the whole buffer in one atomic batch.
//
// Callers must guarantee exclusivity: no other goroutine may write while a stage
// is open (the store holds its write gate for this).
func (e *Engine) BeginStage() {
	e.staged.Store(e.db.NewIndexedBatch())
}

// CommitStage applies the buffered writes as a single Pebble batch. A crash
// before this call leaves no trace of the transaction; after it, all of it.
func (e *Engine) CommitStage() error {
	st := e.staged.Swap(nil)
	if st == nil {
		return nil
	}
	defer func() { _ = st.Close() }()
	if st.Empty() {
		return nil
	}
	return e.db.Apply(st, e.writeOpt)
}

// AbortStage throws the buffered writes away.
func (e *Engine) AbortStage() {
	if st := e.staged.Swap(nil); st != nil {
		_ = st.Close()
	}
}

// Staged reports whether a write transaction is open.
func (e *Engine) Staged() bool { return e.staged.Load() != nil }

// Put writes a single key.
func (e *Engine) Put(key, value []byte) error {
	if st := e.staged.Load(); st != nil {
		return st.Set(key, value, nil)
	}
	return e.db.Set(key, value, e.writeOpt)
}

// Delete removes a single key.
func (e *Engine) Delete(key []byte) error {
	if st := e.staged.Load(); st != nil {
		return st.Delete(key, nil)
	}
	return e.db.Delete(key, e.writeOpt)
}

// Batch returns a fresh write batch. The caller must Close it.
func (e *Engine) Batch() *Batch {
	return &Batch{b: e.db.NewBatch()}
}

// Apply commits a batch atomically. Inside a stage the batch is merged into the
// open transaction instead, so its writes commit (or vanish) with it.
func (e *Engine) Apply(b *Batch) error {
	if st := e.staged.Load(); st != nil {
		return st.Apply(b.b, nil)
	}
	return e.db.Apply(b.b, e.writeOpt)
}

// Merge appends a merge operand for key. Used by the commutative counter class so
// increments on a key are resolved associatively by the merge operator instead of
// serialising on a per-key lock. See storage/merge.go.
func (e *Engine) Merge(key, value []byte) error {
	st := e.staged.Load()
	if st == nil {
		return e.db.Merge(key, value, e.writeOpt)
	}
	// An indexed batch does not fold merge operands when reading, so fold here
	// and stage a plain Set: whatever the transaction reads back is the folded
	// value, exactly what the committed state will produce.
	var base []byte
	v, closer, err := st.Get(key)
	switch {
	case err == nil:
		base = append([]byte(nil), v...)
		_ = closer.Close()
	case err == pebble.ErrNotFound:
		base = nil
	default:
		return err
	}
	vm, err := e.merger.Merge(key, base)
	if err != nil {
		return err
	}
	if err := vm.MergeNewer(value); err != nil {
		return err
	}
	merged, mcloser, err := vm.Finish(true)
	if mcloser != nil {
		_ = mcloser.Close()
	}
	if err != nil {
		return err
	}
	return st.Set(key, merged, nil)
}

// DeleteRange removes every key in [start, end).
func (e *Engine) DeleteRange(start, end []byte) error {
	if st := e.staged.Load(); st != nil {
		return st.DeleteRange(start, end, nil)
	}
	return e.db.DeleteRange(start, end, e.writeOpt)
}

// Scan visits every key/value under prefix in lexicographic order. It stops
// early if fn returns an error. The value slice is only valid during the call.
func (e *Engine) Scan(prefix []byte, fn func(key, value []byte) error) error {
	return e.ScanRange(prefix, prefixEnd(prefix), fn)
}

// ScanRange visits every key/value in [start, end).
func (e *Engine) ScanRange(start, end []byte, fn func(key, value []byte) error) error {
	return e.scanRange(start, end, fn)
}

// scanRange is the merged view of the DB and the open stage. While a
// transaction is staging writes, a plain DB iterator would miss them (and would
// still show keys the transaction deleted), so the two sides are merged in key
// order and every key is resolved through the batch, which applies deletes and
// merge operands consistently.
func (e *Engine) scanRange(start, end []byte, fn func(key, value []byte) error) error {
	opts := &pebble.IterOptions{LowerBound: start, UpperBound: end}
	dbIter, err := e.db.NewIter(opts)
	if err != nil {
		return err
	}
	defer func() { _ = dbIter.Close() }()

	st := e.staged.Load()
	if st == nil {
		for valid := dbIter.First(); valid; valid = dbIter.Next() {
			if err := fn(dbIter.Key(), dbIter.Value()); err != nil {
				return err
			}
		}
		return dbIter.Error()
	}

	bIter, err := st.NewIter(opts)
	if err != nil {
		return err
	}
	defer func() { _ = bIter.Close() }()

	// emit resolves key through the stage: a staged delete hides it, a staged
	// merge operand is folded in, and a staged write is returned as-is.
	emit := func(key []byte) error {
		v, closer, err := st.Get(key)
		if err == pebble.ErrNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		defer func() { _ = closer.Close() }()
		return fn(key, v)
	}

	bValid := bIter.First()
	for dValid := dbIter.First(); dValid; dValid = dbIter.Next() {
		dk := dbIter.Key()
		for bValid && bytes.Compare(bIter.Key(), dk) < 0 {
			if err := emit(bIter.Key()); err != nil {
				return err
			}
			bValid = bIter.Next()
		}
		if bValid && bytes.Equal(bIter.Key(), dk) {
			if err := emit(dk); err != nil {
				return err
			}
			bValid = bIter.Next()
			continue
		}
		if err := emit(dk); err != nil {
			return err
		}
	}
	for ; bValid; bValid = bIter.Next() {
		if err := emit(bIter.Key()); err != nil {
			return err
		}
	}
	if err := dbIter.Error(); err != nil {
		return err
	}
	return bIter.Error()
}

// LastInRange returns the greatest key/value in [start, end] (both inclusive)
// via a reverse seek. This is what makes XADD's ID generation O(logN) instead
// of a scan of the whole stream: "the newest entry" is one SeekLT away.
// The returned key and value are copies; release is nil-safe.
func (e *Engine) LastInRange(start, end []byte) (key, value []byte, found bool, err error) {
	st := e.staged.Load()
	if st == nil {
		iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: prefixEnd(end)})
		if err != nil {
			return nil, nil, false, err
		}
		defer func() { _ = iter.Close() }()

		if valid := iter.Last(); valid {
			key = append([]byte(nil), iter.Key()...)
			value = append([]byte(nil), iter.Value()...)
			found = true
		}
		return key, value, found, iter.Error()
	}

	// Staged: start from the greatest key either side holds and walk down while
	// the transaction deleted it. Normally this is one step.
	opts := &pebble.IterOptions{LowerBound: start, UpperBound: prefixEnd(end)}
	dbIter, err := e.db.NewIter(opts)
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { _ = dbIter.Close() }()
	bIter, err := st.NewIter(opts)
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { _ = bIter.Close() }()

	var cur []byte
	if dbIter.Last() {
		cur = append([]byte(nil), dbIter.Key()...)
	}
	if bIter.Last() {
		if bk := bIter.Key(); cur == nil || bytes.Compare(bk, cur) > 0 {
			cur = append([]byte(nil), bk...)
		}
	}
	for cur != nil {
		v, closer, gerr := st.Get(cur)
		if gerr == nil {
			out := append([]byte(nil), v...)
			_ = closer.Close()
			return cur, out, true, nil
		}
		if gerr != pebble.ErrNotFound {
			return nil, nil, false, gerr
		}
		// Deleted by the transaction: step to the next smaller key.
		var next []byte
		if dbIter.SeekLT(cur) {
			next = append([]byte(nil), dbIter.Key()...)
		}
		if bIter.SeekLT(cur) {
			if bk := bIter.Key(); next == nil || bytes.Compare(bk, next) > 0 {
				next = append([]byte(nil), bk...)
			}
		}
		cur = next
	}
	return nil, nil, false, nil
}

// ScanKeys visits only the keys under prefix, avoiding value materialisation.
func (e *Engine) ScanKeys(prefix []byte, fn func(key []byte) error) error {
	if e.staged.Load() == nil {
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixEnd(prefix),
		})
		if err != nil {
			return err
		}
		defer func() { _ = iter.Close() }()

		for valid := iter.First(); valid; valid = iter.Next() {
			if err := fn(iter.Key()); err != nil {
				return err
			}
		}
		return iter.Error()
	}
	return e.scanRange(prefix, prefixEnd(prefix), func(key, _ []byte) error {
		return fn(key)
	})
}

// ScanKeysFrom is like ScanKeys but resumes strictly after startKey, so a
// caller can page through the keyspace without re-scanning already-returned
// keys. A nil startKey begins at the first key under the prefix. It backs
// SCAN's cursor: the cursor encodes the last key returned, and the next call
// seeks to the first key greater than it. This terminates even while the
// keyspace grows, because iteration always advances to the lexicographic end.
func (e *Engine) ScanKeysFrom(prefix, startKey []byte, fn func(key []byte) error) error {
	if e.staged.Load() != nil {
		return e.scanRange(prefix, prefixEnd(prefix), func(key, _ []byte) error {
			if len(startKey) > 0 && bytes.Compare(key, startKey) <= 0 {
				return nil
			}
			return fn(key)
		})
	}
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	var valid bool
	if len(startKey) == 0 {
		valid = iter.First()
	} else {
		// Seek to the first key strictly greater than startKey. Appending 0x00
		// yields a key that sits just above startKey (no key can fall in the
		// open interval (startKey, startKey+0x00)), so SeekGE lands past it.
		seek := make([]byte, len(startKey)+1)
		copy(seek, startKey)
		seek[len(startKey)] = 0x00
		valid = iter.SeekGE(seek)
	}
	for ; valid; valid = iter.Next() {
		if err := fn(iter.Key()); err != nil {
			return err
		}
	}
	return iter.Error()
}

// Checkpoint creates a consistent hard-link snapshot of the store in destDir.
func (e *Engine) Checkpoint(destDir string) error {
	return e.db.Checkpoint(destDir)
}

// Flush forces memtables to stable storage.
func (e *Engine) Flush() error {
	return e.db.Flush()
}

// Metrics exposes Pebble's internal counters for the INFO command.
func (e *Engine) Metrics() *pebble.Metrics {
	return e.db.Metrics()
}

// BlockCacheBytes returns the current block cache usage.
func (e *Engine) BlockCacheBytes() int64 {
	if e.cache == nil {
		return 0
	}
	return e.cache.Size()
}

// MemTableBytes returns the bytes currently held by the memtables. It walks
// Pebble's metrics, so it is meant for a periodic sampler, not the write path.
func (e *Engine) MemTableBytes() int64 {
	m := e.db.Metrics()
	return int64(m.MemTable.Size)
}

// DiskUsageBytes returns an estimate of the on-disk footprint.
func (e *Engine) DiskUsageBytes() uint64 {
	m := e.db.Metrics()
	var total uint64
	for _, lm := range m.Levels {
		total += uint64(lm.Size)
	}
	total += uint64(m.WAL.Size)
	return total
}

// Close releases the store. It must not be called concurrently with other
// Engine methods.
func (e *Engine) Close() error {
	err := e.db.Close()
	if e.cache != nil {
		e.cache.Unref()
		e.cache = nil
	}
	return err
}

// Batch is a thin wrapper that keeps callers from importing pebble directly.
type Batch struct {
	b *pebble.Batch
}

// Set stages a write.
func (b *Batch) Set(key, value []byte) error {
	return b.b.Set(key, value, nil)
}

// Delete stages a deletion.
func (b *Batch) Delete(key []byte) error {
	return b.b.Delete(key, nil)
}

// DeleteRange stages a range deletion.
func (b *Batch) DeleteRange(start, end []byte) error {
	return b.b.DeleteRange(start, end, nil)
}

// Empty reports whether nothing has been staged yet.
func (b *Batch) Empty() bool {
	return b.b.Empty()
}

// Reset clears the batch so it can be reused.
func (b *Batch) Reset() {
	b.b.Reset()
}

// Close releases the batch.
func (b *Batch) Close() error {
	return b.b.Close()
}

// prefixEnd returns the lexicographic successor of a prefix, i.e. the exclusive
// upper bound that covers exactly the keys carrying that prefix.
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // prefix is all 0xff; no upper bound
}

// ErrStop is a sentinel that Scan callers may return to stop early without
// reporting a failure.
var ErrStop = errors.New("Pebbis: stop iteration")

// IsStop reports whether err is ErrStop.
func IsStop(err error) bool {
	return errors.Is(err, ErrStop)
}
