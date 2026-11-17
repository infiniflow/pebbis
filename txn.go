package pebbis

import (
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

// Transactions and authentication.

// requiresAuth reports whether connections must authenticate first.
func (s *Store) requiresAuth() bool {
	return s.cfg.Load().RequirePass != ""
}

// checkPassword compares against the configured password.
func (s *Store) checkPassword(pass string) bool {
	c := s.cfg.Load()
	return c.RequirePass != "" && c.RequirePass == pass
}

func cmdAuth(c *Ctx, args [][]byte) error {
	if len(args) < 1 || len(args) > 2 {
		return WrongArgs("auth")
	}
	pass := string(args[0])
	// AUTH <user> <pass> is accepted; the user is ignored, matching a server
	// without ACLs.
	if len(args) == 2 {
		pass = string(args[1])
	}
	cs := c.Client()
	if !c.Store.requiresAuth() {
		return &protoError{"ERR Client sent AUTH, but no password is set"}
	}
	if !c.Store.checkPassword(pass) {
		if cs != nil {
			cs.authed = false
		}
		return &protoError{"WRONGPASS invalid username-password pair or user is disabled."}
	}
	if cs != nil {
		cs.authed = true
	}
	c.writeOK()
	return nil
}

func cmdMulti(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	cs := c.Client()
	if cs == nil {
		return &protoError{"ERR MULTI is only available on a server connection"}
	}
	if cs.inTxn {
		return &protoError{"ERR MULTI calls can not be nested"}
	}
	cs.inTxn = true
	cs.queue = nil
	cs.dirtyTxn = false
	c.writeOK()
	return nil
}

func cmdDiscard(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	cs := c.Client()
	if cs == nil || !cs.inTxn {
		return &protoError{"ERR DISCARD without MULTI"}
	}
	abortTxn(cs)
	c.writeOK()
	return nil
}

func cmdWatch(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	cs := c.Client()
	if cs == nil {
		return &protoError{"ERR WATCH is only available on a server connection"}
	}
	if cs.inTxn {
		return &protoError{"ERR WATCH inside MULTI is not allowed"}
	}
	if cs.watched == nil {
		cs.watched = map[string]uint64{}
	}
	if cs.watchedExpiry == nil {
		cs.watchedExpiry = map[string]int64{}
	}
	if cs.watchedStale == nil {
		cs.watchedStale = map[string]bool{}
	}
	now := c.Store.clock.NowMilli()
	for _, a := range args {
		key := string(a)
		if _, seen := cs.watched[key]; seen {
			continue
		}
		ver := uint64(0)
		exp := int64(-1) // -1: the key did not exist at WATCH time
		stale := false
		if e, ok := c.Store.dict.Lookup(c.DB, key); ok {
			ver = e.Version()
			exp = e.Expiry()
			// Already logically expired when WATCHed?
			stale = exp > 0 && exp <= now
		}
		cs.watched[key] = ver
		cs.watchedExpiry[key] = exp
		cs.watchedStale[key] = stale
	}
	c.writeOK()
	return nil
}

func cmdUnwatch(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	cs := c.Client()
	if cs != nil {
		cs.watched = nil
		cs.watchedExpiry = nil
		cs.watchedStale = nil
	}
	c.writeOK()
	return nil
}

// validateQueued checks a command before it is queued inside MULTI: Redis
// rejects unknown commands and arity violations at queue time and marks the
// transaction dirty, so that EXEC reports EXECABORT.
func validateQueued(name string, argc int) error {
	cmd, ok := commands[name]
	if !ok {
		return UnknownCommand(name)
	}
	if cmd.arity >= 0 {
		if argc != cmd.arity {
			return WrongArgs(name)
		}
	} else if argc < -cmd.arity {
		return WrongArgs(name)
	}
	return nil
}

func abortTxn(cs *connState) {
	cs.inTxn = false
	cs.queue = nil
	cs.dirtyTxn = false
	cs.watched = nil
	cs.watchedExpiry = nil
	cs.watchedStale = nil
}

// cmdExec runs the queued commands.
//
// The reply is an array of the individual replies, or nil when a WATCHed key
// changed (Redis' way of reporting an aborted optimistic transaction).
func cmdExec(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	cs := c.Client()
	if cs == nil || !cs.inTxn {
		return &protoError{"ERR EXEC without MULTI"}
	}

	// Lock every key the transaction will touch, plus the watched ones, for the
	// whole EXEC. This is what makes the queued commands indivisible: another
	// client's command on one of those keys now blocks until the transaction
	// has finished, instead of slipping in between two queued commands. The
	// locks are taken in a canonical order (ascending database, then the shard
	// order LockKeys already uses), so two transactions can never deadlock.
	queue := cs.queue
	unlockTxn := c.Store.lockTxnKeys(c.DB, queue, cs.watched)
	defer unlockTxn()

	// Stage the transaction's storage writes: every Put/Delete/Merge/Apply while
	// the stage is open lands in one Pebble batch, so a crash either applies the
	// whole transaction or none of it. The exclusive side of the write gate
	// keeps single writes (and the background expires/eviction passes) out while
	// the stage is open, which is what makes the staging safe to be global.
	staged := transactionHasWrites(queue)
	if staged {
		c.Store.writeGate.Lock()
		defer c.Store.writeGate.Unlock()
		c.Store.eng.BeginStage()
		// No-op once CommitStage has taken the buffer.
		defer c.Store.eng.AbortStage()
	}

	// Optimistic concurrency check, expiry-aware: a watched key that expired
	// after WATCH counts as modified, while a key that was already stale
	// (logically expired) at WATCH time is treated as non-existent, so its
	// removal does not abort the transaction. It runs under the lock taken
	// above, so nothing can change between the check and the writes.
	if len(cs.watched) > 0 {
		now := c.Store.clock.NowMilli()
		abort := false
		for key, ver := range cs.watched {
			we := int64(-1)
			if cs.watchedExpiry != nil {
				we = cs.watchedExpiry[key]
			}
			staleAtWatch := cs.watchedStale != nil && cs.watchedStale[key]
			if e, ok := c.Store.dict.Lookup(c.DB, key); ok {
				if we < 0 || e.Version() != ver {
					// Created after WATCH, or written to.
					abort = true
					break
				}
				if !staleAtWatch && e.Expiry() > 0 && e.Expiry() <= now {
					// Live when watched, expired since the WATCH without any
					// user write: expiry counts as a modification.
					abort = true
					break
				}
			} else if we >= 0 && !staleAtWatch {
				// A live key was deleted (or expired) after WATCH.
				abort = true
				break
			}
		}
		if abort {
			abortTxn(cs)
			c.writeNull()
			return nil
		}
	}
	if cs.dirtyTxn {
		abortTxn(cs)
		return &protoError{"EXECABORT Transaction discarded because of previous errors."}
	}

	cs.inTxn = false
	cs.queue = nil
	cs.watched = nil
	cs.dirtyTxn = false

	c.w.WriteArray(len(queue))
	curDB := c.DB
	for _, line := range queue {
		if len(line) == 0 {
			continue
		}
		upper := strings.ToUpper(line[0])
		cm, ok := lookupCommand(upper)
		if !ok {
			// Should not happen: the command was validated when queued.
			c.w.WriteError(UnknownCommand(strings.ToLower(line[0])).Error())
			continue
		}
		if cm.write && c.Store.memoryFull() {
			c.w.WriteError(OOM(upper).Error())
			continue
		}
		byteArgs := make([][]byte, 0, len(line)-1)
		for _, a := range line[1:] {
			byteArgs = append(byteArgs, []byte(a))
		}
		sub := &Ctx{
			Store:      c.Store,
			DB:         curDB,
			Name:       upper,
			MaxBulkLen: c.MaxBulkLen,
			Args:       line,
			state:      cs,
			srv:        c.srv,
			noBlock:    true, // blocking commands run non-blocking inside MULTI
			w:          c.w,
		}
		if err := cm.fn(sub, byteArgs); err != nil {
			c.w.WriteError(err.Error())
		}
		// A SELECT queued in the transaction switches the database for the
		// commands after it (and for the connection afterwards), as in Redis.
		if upper == "SELECT" {
			curDB = sub.DB
		}
	}
	// The connection keeps whatever database the transaction ended on; the
	// dispatch loop mirrors c.DB back into the connection state.
	c.DB = curDB
	cs.db = curDB

	// Commit the staged writes as one atomic Pebble batch. The replies are
	// already buffered on the connection, so a commit failure cannot be reported
	// as a protocol error without corrupting the stream: log it and drop the
	// connection instead.
	if staged {
		if err := c.Store.eng.CommitStage(); err != nil {
			log.Printf("pebbis: EXEC commit failed, closing connection: %v", err)
			if c.conn != nil {
				_ = c.conn.Close()
			}
			return nil
		}
	}
	return nil
}

// transactionHasWrites reports whether any queued command writes. A read-only
// transaction needs neither the write gate nor a staging batch.
func transactionHasWrites(queue [][]string) bool {
	for _, line := range queue {
		if len(line) == 0 {
			continue
		}
		if cm, ok := lookupCommand(strings.ToUpper(line[0])); ok && cm.write {
			return true
		}
	}
	return false
}

// txnKeyGroup is one database's worth of keys touched by a transaction.
type txnKeyGroup struct {
	db   uint16
	keys []string
}

// lockTxnKeys takes the exclusive per-key lock on everything the transaction
// touches and returns the release function. Databases are locked in ascending
// order and each database's shards in LockKeys' canonical order, so the lock
// acquisition order is identical for every transaction and cannot deadlock.
func (s *Store) lockTxnKeys(startDB uint16, queue [][]string, watched map[string]uint64) func() {
	groups := transactionKeys(startDB, queue, watched)
	unlocks := make([]func(), 0, len(groups))
	for _, g := range groups {
		unlocks = append(unlocks, s.LockKeys(g.db, g.keys, true))
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
}

// transactionKeys walks the queued commands the way EXEC will run them, tracking
// SELECT so every key is attributed to the database it will be written in, and
// returns the keys grouped by database in ascending order.
func transactionKeys(startDB uint16, queue [][]string, watched map[string]uint64) []txnKeyGroup {
	db := startDB
	byDB := make(map[uint16][]string)
	seen := make(map[uint16]map[string]struct{})
	add := func(d uint16, key string) {
		m, ok := seen[d]
		if !ok {
			m = make(map[string]struct{})
			seen[d] = m
		}
		if _, dup := m[key]; dup {
			return
		}
		m[key] = struct{}{}
		byDB[d] = append(byDB[d], key)
	}

	for _, line := range queue {
		if len(line) == 0 {
			continue
		}
		upper := strings.ToUpper(line[0])
		args := make([][]byte, 0, len(line)-1)
		for _, a := range line[1:] {
			args = append(args, []byte(a))
		}
		if upper == "SELECT" && len(args) == 1 {
			if idx, err := strconv.ParseInt(string(args[0]), 10, 64); err == nil && idx >= 0 && idx <= 0xffff {
				db = uint16(idx)
			}
			continue
		}
		for _, k := range commandKeys(upper, args) {
			add(db, k)
		}
	}
	// A transaction aborts when a watched key changed, so the watched keys must
	// be locked too: otherwise the version could move right after the check.
	for k := range watched {
		add(startDB, k)
	}

	dbs := make([]int, 0, len(byDB))
	for d := range byDB {
		dbs = append(dbs, int(d))
	}
	sort.Ints(dbs)
	out := make([]txnKeyGroup, 0, len(dbs))
	for _, d := range dbs {
		out = append(out, txnKeyGroup{db: uint16(d), keys: byDB[uint16(d)]})
	}
	return out
}

// compile-time check that the transaction path stays compatible with redcon's
// writer, which is what Ctx.w holds on the server path.
var _ Writer = redcon.Conn(nil)
