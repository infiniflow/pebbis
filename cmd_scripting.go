package pebbis

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	lua "github.com/yuin/gopher-lua"
)

// Scripting: EVAL, EVALSHA and SCRIPT.
//
// The scripts run on gopher-lua with a deliberately small library set: base,
// table, string and math, minus the base functions that touch the filesystem.
// Only redis.call / redis.pcall / redis.error_reply / redis.status_reply /
// redis.sha1hex / redis.log are exposed through the `redis` table.
//
// Atomicity follows the MULTI/EXEC approach rather than a new mechanism: the
// declared KEYS are locked exclusively for the whole script, and the nested
// commands run without taking locks of their own (the key table is not
// reentrant). A script that touches a key it did not declare therefore gets
// only the per-command locking that a plain command would, which is the same
// trade-off Redis documents for scripts in non-cluster mode.

// scriptCache maps a lower-case SHA1 to its source, mirroring Redis' script
// cache. It is process-wide, as in Redis, so EVALSHA works across connections.
var scriptCache sync.Map

// scriptDisallowed are commands a script may not call, either because they
// would deadlock against the script's own key locks or because they cannot mean
// anything inside a script.
var scriptDisallowed = map[string]bool{
	"EVAL": true, "EVALSHA": true, "SCRIPT": true,
	"MULTI": true, "EXEC": true, "DISCARD": true, "WATCH": true, "UNWATCH": true,
	"SUBSCRIBE": true, "UNSUBSCRIBE": true, "PSUBSCRIBE": true, "PUNSUBSCRIBE": true,
	"SSUBSCRIBE": true, "SUNSUBSCRIBE": true, "MONITOR": true, "SYNC": true,
	"SHUTDOWN": true, "SAVE": true, "BGSAVE": true, "SELECT": true, "QUIT": true,
	"AUTH": true, "HELLO": true, "RESET": true, "CLIENT": true,
}

func scriptSha(src string) string {
	sum := sha1.Sum([]byte(src))
	return hex.EncodeToString(sum[:])
}

// cmdEval runs a script given inline.
func cmdEval(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	return evalScript(c, string(args[0]), args[1:])
}

// cmdEvalSha runs a script from the cache; go-redis sends this first and falls
// back to EVAL on NOSCRIPT.
func cmdEvalSha(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	sha := strings.ToLower(string(args[0]))
	src, ok := scriptCache.Load(sha)
	if !ok {
		return fmt.Errorf("NOSCRIPT No matching script. Please use EVAL.")
	}
	return evalScript(c, src.(string), args[1:])
}

// cmdScript implements the SCRIPT subcommands clients actually use to warm and
// probe the cache.
func cmdScript(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	switch strings.ToUpper(string(args[0])) {
	case "LOAD":
		if len(args) != 2 {
			return WrongArgs(c.Name)
		}
		src := string(args[1])
		sha := scriptSha(src)
		scriptCache.Store(sha, src)
		c.w.WriteBulkString(sha)
		return nil
	case "EXISTS":
		c.w.WriteArray(len(args) - 1)
		for _, a := range args[1:] {
			if _, ok := scriptCache.Load(strings.ToLower(string(a))); ok {
				c.w.WriteInt(1)
			} else {
				c.w.WriteInt(0)
			}
		}
		return nil
	case "FLUSH":
		scriptCache.Range(func(k, _ any) bool {
			scriptCache.Delete(k)
			return true
		})
		c.writeOK()
		return nil
	default:
		return fmt.Errorf("ERR Unknown SCRIPT subcommand or wrong number of arguments for '%s'",
			strings.ToLower(string(args[0])))
	}
}

// evalScript is the shared body of EVAL and EVALSHA.
func evalScript(c *Ctx, src string, rest [][]byte) error {
	if len(rest) < 1 {
		return WrongArgs(c.Name)
	}
	numKeys, err := strconv.Atoi(string(rest[0]))
	if err != nil {
		return fmt.Errorf("ERR value is not an integer or out of range")
	}
	if numKeys < 0 {
		return fmt.Errorf("ERR Number of keys can't be negative")
	}
	if numKeys > len(rest)-1 {
		return fmt.Errorf("ERR Number of keys can't be greater than number of args")
	}
	keys := rest[1 : 1+numKeys]
	argv := rest[1+numKeys:]

	// EVAL caches the script too, so a later EVALSHA of the same body works.
	scriptCache.Store(scriptSha(src), src)

	keyNames := make([]string, len(keys))
	for i, k := range keys {
		keyNames[i] = string(k)
	}
	unlock := c.Store.LockKeys(c.DB, keyNames, true)
	defer unlock()

	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()
	openScriptLibs(L)

	L.SetGlobal("KEYS", luaStringSlice(L, keys))
	L.SetGlobal("ARGV", luaStringSlice(L, argv))

	bridge := &scriptBridge{parent: c, L: L}
	L.SetGlobal("redis", bridge.redisTable())

	fn, err := L.LoadString(src)
	if err != nil {
		return fmt.Errorf("ERR Error compiling script: %v", err)
	}
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		return luaErrorReply(err)
	}
	writeLuaValue(c.w, L.Get(-1))
	return nil
}

// openScriptLibs loads the libraries a Redis script may rely on. loadfile and
// dofile are removed: a script has no business reading the host filesystem, and
// Redis does not give it one either.
func openScriptLibs(L *lua.LState) {
	for _, open := range []struct {
		name string
		fn   lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
	} {
		L.Push(L.NewFunction(open.fn))
		L.Push(lua.LString(open.name))
		_ = L.PCall(1, 0, nil)
	}
	L.SetGlobal("dofile", lua.LNil)
	L.SetGlobal("loadfile", lua.LNil)
	L.SetGlobal("print", lua.LNil)
}

func luaStringSlice(L *lua.LState, items [][]byte) *lua.LTable {
	t := L.NewTable()
	for i, item := range items {
		t.RawSetInt(i+1, lua.LString(item))
	}
	return t
}

// scriptBridge exposes the redis table to a running script.
type scriptBridge struct {
	parent *Ctx
	L      *lua.LState
}

func (b *scriptBridge) redisTable() *lua.LTable {
	t := b.L.NewTable()
	t.RawSetString("call", b.L.NewFunction(b.callFn(true)))
	t.RawSetString("pcall", b.L.NewFunction(b.callFn(false)))
	t.RawSetString("error_reply", b.L.NewFunction(func(L *lua.LState) int {
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString(L.CheckString(1)))
		L.Push(tbl)
		return 1
	}))
	t.RawSetString("status_reply", b.L.NewFunction(func(L *lua.LState) int {
		tbl := L.NewTable()
		tbl.RawSetString("ok", lua.LString(L.CheckString(1)))
		L.Push(tbl)
		return 1
	}))
	t.RawSetString("sha1hex", b.L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString(scriptSha(L.CheckString(1))))
		return 1
	}))
	t.RawSetString("log", b.L.NewFunction(func(L *lua.LState) int { return 0 }))
	return t
}

// callFn builds the redis.call / redis.pcall implementation. raise selects
// between them: redis.call turns a command error into a script error, pcall
// returns it as a {err=...} table.
func (b *scriptBridge) callFn(raise bool) lua.LGFunction {
	return func(L *lua.LState) int {
		top := L.GetTop()
		if top == 0 {
			L.Error(lua.LString("ERR wrong number of arguments for 'call'"), 0)
			return 0
		}
		args := make([]string, top)
		for i := 1; i <= top; i++ {
			v := L.Get(i)
			switch v.Type() {
			case lua.LTString:
				args[i-1] = string(v.(lua.LString))
			case lua.LTNumber:
				// Redis converts a numeric argument the way lua_tostring does
				// ("%.14g"), so 3.7 arrives as "3.7". Truncating here would
				// silently quantise values scripts store in hashes, such as a
				// token bucket's fractional remainder.
				args[i-1] = v.String()
			default:
				L.Error(lua.LString("ERR Lua redis lib command arguments must be strings or integers"), 0)
				return 0
			}
		}

		reply, err := b.parent.Store.execScriptCommand(b.parent, args)
		if err != nil {
			if raise {
				// Level 0 keeps Redis' error text verbatim: gopher-lua would
				// otherwise prefix it with "<string>:1:", and the RESP error
				// code (WRONGTYPE, ...) has to survive so the client sees it.
				L.Error(lua.LString(err.Error()), 0)
				return 0
			}
			tbl := L.NewTable()
			tbl.RawSetString("err", lua.LString(err.Error()))
			L.Push(tbl)
			return 1
		}
		L.Push(replyToLua(L, reply))
		return 1
	}
}

// execScriptCommand runs one command for a script, capturing its reply instead
// of writing it to a connection.
//
// Nested calls deliberately skip the key locks (the script already holds them)
// and the write gate (which is not reentrant); the transaction staging state is
// inherited from the parent context so a script queued inside MULTI still lands
// in the transaction's batch.
func (s *Store) execScriptCommand(parent *Ctx, args []string) (scriptReply, error) {
	if len(args) == 0 {
		return scriptReply{}, errors.New("ERR wrong number of arguments")
	}
	upper := strings.ToUpper(args[0])
	cm, ok := lookupCommand(upper)
	if !ok {
		return scriptReply{}, UnknownCommand(strings.ToLower(args[0]))
	}
	if scriptDisallowed[upper] || blockingCmds[upper] {
		return scriptReply{}, fmt.Errorf("ERR This Redis command is not allowed from script: %s", upper)
	}
	if err := checkArity(cm.arity, len(args)); err != nil {
		return scriptReply{}, err
	}

	byteArgs := make([][]byte, 0, len(args)-1)
	for _, a := range args[1:] {
		byteArgs = append(byteArgs, []byte(a))
	}

	w := &captureWriter{}
	c := &Ctx{
		Store:      s,
		DB:         parent.DB,
		Name:       upper,
		MaxBulkLen: parent.MaxBulkLen,
		Args:       args,
		state:      parent.state,
		// A script never holds the write gate and never blocks, so the nested
		// command takes no gate of its own and its writes go straight through.
		noBlock: true,
		w:       w,
	}
	if err := runCommand(c, cm, byteArgs); err != nil {
		return scriptReply{}, err
	}
	return w.single()
}

// checkArity mirrors the check dispatch performs for a client command.
func checkArity(arity, argc int) error {
	if arity >= 0 {
		if argc != arity {
			return errors.New("ERR wrong number of arguments")
		}
		return nil
	}
	if argc < -arity {
		return errors.New("ERR wrong number of arguments")
	}
	return nil
}

// luaErrorReply turns a Lua runtime error into the error reply EVAL returns.
//
// gopher-lua's ApiError.Error() appends a Go-side stack trace; only the Lua
// object is meaningful to a client, so the traceback is dropped. A message that
// already carries a Redis error code (WRONGTYPE, NOSCRIPT, or a raised ERR) is
// passed through untouched instead of being wrapped twice.
func luaErrorReply(err error) error {
	msg := err.Error()
	var apiErr *lua.ApiError
	if errors.As(err, &apiErr) {
		msg = apiErr.Object.String()
	}
	if msg == "" {
		msg = "unknown error"
	}
	if respErrorCode(msg) {
		return errors.New(msg)
	}
	return fmt.Errorf("ERR Error running script: %s", msg)
}

// respErrorCode reports whether msg already starts with a Redis error prefix
// such as "WRONGTYPE" or "NOSCRIPT", which must survive unchanged.
func respErrorCode(msg string) bool {
	sp := strings.IndexByte(msg, ' ')
	if sp <= 0 || sp > 16 {
		return false
	}
	code := msg[:sp]
	for _, r := range code {
		if !(r >= 'A' && r <= 'Z') {
			return false
		}
	}
	return true
}

// scriptReply is one captured RESP reply.
type scriptReply struct {
	kind replyKind
	str  string
	num  int64
	bulk []byte
	arr  []scriptReply
}

type replyKind int

const (
	replyNull replyKind = iota
	replyStatus
	replyError
	replyBulk
	replyInt
	replyArray
)

// captureWriter rebuilds a reply tree from the flat Writer interface. WriteArray
// opens a frame that the following writes fill; once a frame is complete it is
// attached to its parent, so nesting works without the command knowing it is
// being captured.
type captureWriter struct {
	stack []captureFrame
	root  []scriptReply
}

type captureFrame struct {
	reply     scriptReply
	remaining int
}

func (w *captureWriter) WriteString(str string) { w.emit(scriptReply{kind: replyStatus, str: str}) }
func (w *captureWriter) WriteBulk(bulk []byte) {
	w.emit(scriptReply{kind: replyBulk, bulk: bulk})
}
func (w *captureWriter) WriteBulkString(bulk string) {
	w.emit(scriptReply{kind: replyBulk, bulk: []byte(bulk)})
}
func (w *captureWriter) WriteInt(num int)     { w.emit(scriptReply{kind: replyInt, num: int64(num)}) }
func (w *captureWriter) WriteInt64(num int64) { w.emit(scriptReply{kind: replyInt, num: num}) }
func (w *captureWriter) WriteNull()           { w.emit(scriptReply{kind: replyNull}) }
func (w *captureWriter) WriteError(msg string) {
	w.emit(scriptReply{kind: replyError, str: msg})
}

func (w *captureWriter) WriteArray(count int) {
	if count < 0 {
		w.emit(scriptReply{kind: replyNull})
		return
	}
	if count == 0 {
		w.emit(scriptReply{kind: replyArray})
		return
	}
	w.stack = append(w.stack, captureFrame{
		reply:     scriptReply{kind: replyArray, arr: make([]scriptReply, 0, count)},
		remaining: count,
	})
}

func (w *captureWriter) emit(r scriptReply) {
	for len(w.stack) > 0 {
		top := &w.stack[len(w.stack)-1]
		top.reply.arr = append(top.reply.arr, r)
		top.remaining--
		if top.remaining > 0 {
			return
		}
		done := top.reply
		w.stack = w.stack[:len(w.stack)-1]
		r = done
	}
	w.root = append(w.root, r)
}

// single returns the one top-level reply the command produced.
func (w *captureWriter) single() (scriptReply, error) {
	if len(w.root) == 0 {
		return scriptReply{kind: replyNull}, nil
	}
	return w.root[0], nil
}

// replyToLua converts a captured reply into the value redis.call returns.
func replyToLua(L *lua.LState, r scriptReply) lua.LValue {
	switch r.kind {
	case replyStatus:
		t := L.NewTable()
		t.RawSetString("ok", lua.LString(r.str))
		return t
	case replyError:
		t := L.NewTable()
		t.RawSetString("err", lua.LString(r.str))
		return t
	case replyBulk:
		return lua.LString(r.bulk)
	case replyInt:
		return lua.LNumber(r.num)
	case replyArray:
		t := L.NewTable()
		for i, item := range r.arr {
			t.RawSetInt(i+1, replyToLua(L, item))
		}
		return t
	default:
		return lua.LFalse
	}
}

// writeLuaValue writes a script's return value as a RESP reply, following the
// Redis conversion rules: numbers truncate to integers, tables become arrays
// unless they carry an err/ok field, and false becomes a null bulk reply.
func writeLuaValue(w Writer, v lua.LValue) {
	switch v.Type() {
	case lua.LTNil:
		w.WriteNull()
	case lua.LTBool:
		if lua.LVAsBool(v) {
			w.WriteInt64(1)
			return
		}
		w.WriteNull()
	case lua.LTNumber:
		w.WriteInt64(int64(float64(v.(lua.LNumber))))
	case lua.LTString:
		w.WriteBulkString(string(v.(lua.LString)))
	case lua.LTTable:
		t := v.(*lua.LTable)
		if errVal := t.RawGetString("err"); errVal != lua.LNil {
			w.WriteError(errVal.String())
			return
		}
		if okVal := t.RawGetString("ok"); okVal != lua.LNil {
			w.WriteString(okVal.String())
			return
		}
		w.WriteArray(t.Len())
		for i := 1; i <= t.Len(); i++ {
			writeLuaValue(w, t.RawGetInt(i))
		}
	default:
		w.WriteNull()
	}
}
