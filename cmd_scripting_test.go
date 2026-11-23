package pebbis

import (
	"strings"
	"testing"
)

// execArgs runs one command in-process and returns the captured reply, which is
// the same path a client connection takes minus the socket.
func execArgs(t *testing.T, s *Store, args ...string) (*captureWriter, error) {
	t.Helper()
	upper := strings.ToUpper(args[0])
	cm, ok := lookupCommand(upper)
	if !ok {
		t.Fatalf("command %s not registered", upper)
	}
	byteArgs := make([][]byte, 0, len(args)-1)
	for _, a := range args[1:] {
		byteArgs = append(byteArgs, []byte(a))
	}
	w := &captureWriter{}
	c := &Ctx{Store: s, Name: upper, Args: args, MaxBulkLen: 512 << 20, w: w}
	return w, runCommand(c, cm, byteArgs)
}

func mustExec(t *testing.T, s *Store, args ...string) scriptReply {
	t.Helper()
	w, err := execArgs(t, s, args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	r, err := w.single()
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	if r.kind == replyError {
		t.Fatalf("%v: unexpected error reply %q", args, r.str)
	}
	return r
}

// The three scripts below are copied verbatim from the RAGFlow server: they are
// the reason EVAL has to work here, so parity is asserted against the real
// bodies rather than invented ones.

const ragflowDeleteIfEqual = `
	local current_value = redis.call('get', KEYS[1])
	if current_value and current_value == ARGV[1] then
		redis.call('del', KEYS[1])
		return 1
	end
	return 0
`

func TestEvalDeleteIfEqual(t *testing.T) {
	s := testStore(t, nil)
	mustExec(t, s, "SET", "lock:doc", "owner-1")

	if r := mustExec(t, s, "EVAL", ragflowDeleteIfEqual, "1", "lock:doc", "owner-2"); r.kind != replyInt || r.num != 0 {
		t.Fatalf("wrong owner delete = %+v, want 0", r)
	}
	if r := mustExec(t, s, "GET", "lock:doc"); r.kind != replyBulk || string(r.bulk) != "owner-1" {
		t.Fatalf("value after failed delete = %+v, want owner-1", r)
	}
	if r := mustExec(t, s, "EVAL", ragflowDeleteIfEqual, "1", "lock:doc", "owner-1"); r.kind != replyInt || r.num != 1 {
		t.Fatalf("owner delete = %+v, want 1", r)
	}
	if r := mustExec(t, s, "GET", "lock:doc"); r.kind != replyNull {
		t.Fatalf("value after delete = %+v, want null", r)
	}
}

const ragflowTokenBucket = `
	local key       = KEYS[1]
	local capacity  = tonumber(ARGV[1])
	local rate      = tonumber(ARGV[2])
	local now       = tonumber(ARGV[3])
	local cost      = tonumber(ARGV[4])

	local data = redis.call("HMGET", key, "tokens", "timestamp")
	local tokens = tonumber(data[1])
	local last_ts = tonumber(data[2])

	if tokens == nil then
		tokens = capacity
		last_ts = now
	end

	local delta = math.max(0, now - last_ts)
	tokens = math.min(capacity, tokens + delta * rate)

	if tokens < cost then
		return {0, tokens}
	end

	tokens = tokens - cost

	redis.call("HMSET", key,
		"tokens", tokens,
		"timestamp", now
	)

	redis.call("EXPIRE", key, math.ceil(capacity / rate * 2))

	return {1, tokens}
`

func TestEvalTokenBucket(t *testing.T) {
	s := testStore(t, nil)

	for i := 0; i < 2; i++ {
		r := mustExec(t, s, "EVAL", ragflowTokenBucket, "1", "tb:webhook", "2", "0.1", "1700000000", "1")
		if r.kind != replyArray || len(r.arr) != 2 || r.arr[0].num != 1 {
			t.Fatalf("call %d = %+v, want [1, tokens]", i+1, r)
		}
	}
	r := mustExec(t, s, "EVAL", ragflowTokenBucket, "1", "tb:webhook", "2", "0.1", "1700000000", "1")
	if r.kind != replyArray || len(r.arr) != 2 || r.arr[0].num != 0 {
		t.Fatalf("call 3 = %+v, want [0, tokens]", r)
	}
	if r.arr[1].kind != replyInt {
		t.Fatalf("remaining tokens should be an integer, got %+v", r.arr[1])
	}
}

const ragflowMediaPut = `
	local media_key  = KEYS[1]
	local digest_key = KEYS[2]
	local state_key  = KEYS[3]
	local digest     = ARGV[1]
	local value      = ARGV[2]
	local max_images = tonumber(ARGV[3])
	local max_bytes  = tonumber(ARGV[4])
	local ttl        = tonumber(ARGV[5])
	local manifest_key = ARGV[6]
	local manifest_member = ARGV[7]

	local function track_manifest()
		if manifest_key ~= "" then
			redis.call("SADD", manifest_key, manifest_member)
			redis.call("EXPIRE", manifest_key, ttl)
		end
	end

	if redis.call("SISMEMBER", digest_key, digest) == 1 then
		if redis.call("EXISTS", media_key) == 0 then
			redis.call("SET", media_key, value, "EX", ttl)
			redis.call("EXPIRE", digest_key, ttl)
			redis.call("EXPIRE", state_key, ttl)
			track_manifest()
		end
		return {1, 0}
	end

	local images = redis.call("SCARD", digest_key)
	local bytes  = tonumber(redis.call("HGET", state_key, "bytes") or "0")
	if max_images > 0 and images >= max_images then
		return {0, 0}
	end
	if max_bytes > 0 and #value > max_bytes - bytes then
		return {0, 0}
	end

	redis.call("SET", media_key, value, "EX", ttl)
	track_manifest()
	redis.call("SADD", digest_key, digest)
	redis.call("HINCRBY", state_key, "bytes", #value)
	redis.call("EXPIRE", digest_key, ttl)
	redis.call("EXPIRE", state_key, ttl)
	return {2, 1}
`

func TestEvalMediaPut(t *testing.T) {
	s := testStore(t, nil)

	// First insert: {2, 1}.
	r := mustExec(t, s, "EVAL", ragflowMediaPut, "3", "mc:a", "mc:digests", "mc:state",
		"digest-a", "abc", "1", "3", "3600", "mc:manifest", "mc:a")
	if r.kind != replyArray || r.arr[0].num != 2 || r.arr[1].num != 1 {
		t.Fatalf("first put = %+v, want [2,1]", r)
	}
	// Same digest again: {1, 0} and the payload is still there.
	r = mustExec(t, s, "EVAL", ragflowMediaPut, "3", "mc:a", "mc:digests", "mc:state",
		"digest-a", "abc", "1", "3", "3600", "mc:manifest", "mc:a")
	if r.kind != replyArray || r.arr[0].num != 1 || r.arr[1].num != 0 {
		t.Fatalf("duplicate put = %+v, want [1,0]", r)
	}
	// A second distinct digest exceeds max_images=1: {0, 0}.
	r = mustExec(t, s, "EVAL", ragflowMediaPut, "3", "mc:b", "mc:digests", "mc:state",
		"digest-b", "b", "1", "3", "3600", "mc:manifest", "mc:b")
	if r.kind != replyArray || r.arr[0].num != 0 {
		t.Fatalf("over-quota put = %+v, want [0,0]", r)
	}
	// The manifest was charged once.
	if r := mustExec(t, s, "SCARD", "mc:manifest"); r.num != 1 {
		t.Fatalf("manifest size = %d, want 1", r.num)
	}
}

func TestEvalShaAndScript(t *testing.T) {
	s := testStore(t, nil)

	sha := scriptSha(ragflowDeleteIfEqual)
	r := mustExec(t, s, "SCRIPT", "LOAD", ragflowDeleteIfEqual)
	if r.kind != replyBulk || string(r.bulk) != sha {
		t.Fatalf("SCRIPT LOAD = %+v, want %q", r, sha)
	}
	r = mustExec(t, s, "SCRIPT", "EXISTS", sha, "0000000000000000000000000000000000000000")
	if r.kind != replyArray || r.arr[0].num != 1 || r.arr[1].num != 0 {
		t.Fatalf("SCRIPT EXISTS = %+v, want [1,0]", r)
	}

	mustExec(t, s, "SET", "k", "v")
	if r := mustExec(t, s, "EVALSHA", sha, "1", "k", "v"); r.num != 1 {
		t.Fatalf("EVALSHA = %+v, want 1", r)
	}

	// An unknown digest must answer NOSCRIPT so go-redis falls back to EVAL.
	_, err := execArgs(t, s, "EVALSHA", strings.Repeat("a", 40), "0")
	if err == nil || !strings.HasPrefix(err.Error(), "NOSCRIPT") {
		t.Fatalf("unknown EVALSHA error = %v, want NOSCRIPT", err)
	}
}

func TestEvalRejectsNestedEval(t *testing.T) {
	s := testStore(t, nil)
	_, err := execArgs(t, s, "EVAL", `return redis.call("EVAL", "return 1", "0")`, "0")
	if err == nil || !strings.Contains(err.Error(), "not allowed from script") {
		t.Fatalf("nested EVAL error = %v, want a rejection", err)
	}
}

func TestEvalReturnsNilAndFalse(t *testing.T) {
	s := testStore(t, nil)
	if r := mustExec(t, s, "EVAL", `return nil`, "0"); r.kind != replyNull {
		t.Fatalf("nil = %+v, want null", r)
	}
	if r := mustExec(t, s, "EVAL", `return false`, "0"); r.kind != replyNull {
		t.Fatalf("false = %+v, want null", r)
	}
	if r := mustExec(t, s, "EVAL", `return {ok = "FINE"}`, "0"); r.kind != replyStatus || r.str != "FINE" {
		t.Fatalf("status table = %+v, want FINE", r)
	}
	if r := mustExec(t, s, "EVAL", `return 3.7`, "0"); r.num != 3 {
		t.Fatalf("float = %+v, want truncated 3", r)
	}
	// A command error inside redis.call aborts the script.
	mustExec(t, s, "SET", "str", "x")
	_, err := execArgs(t, s, "EVAL", `return redis.call("INCR", "str")`, "0")
	if err == nil || !strings.Contains(err.Error(), "not an integer") {
		t.Fatalf("non-integer error = %v, want ERR value is not an integer", err)
	}
	// A key of the wrong type propagates the WRONGTYPE prefix unchanged.
	mustExec(t, s, "RPUSH", "list", "a")
	_, err = execArgs(t, s, "EVAL", `return redis.call("INCR", "list")`, "0")
	if err == nil || !strings.HasPrefix(err.Error(), "WRONGTYPE") {
		t.Fatalf("wrongtype error = %v, want WRONGTYPE", err)
	}
	// redis.pcall hands the same error back as a table instead of aborting.
	if r := mustExec(t, s, "EVAL",
		`local e = redis.pcall("INCR", "list"); return {ok = "caught"}`,
		"0"); r.kind != replyStatus || r.str != "caught" {
		t.Fatalf("pcall did not swallow the error: %+v", r)
	}
}

func TestEvalScriptCannotTouchFiles(t *testing.T) {
	s := testStore(t, nil)
	_, err := execArgs(t, s, "EVAL", `return loadfile("/etc/passwd")`, "0")
	if err == nil {
		t.Fatal("loadfile must not be available to a script")
	}
}

// The run tracker's lease scripts (internal/agent/canvas/run_tracker.go) are
// the other RAGFlow call site. They compare HGET results against ARGV, which
// only works when a missing field arrives as false, and they lean on PEXPIRE
// and SET ... PX inside a script.
const (
	ragflowRegisterActiveSession = `
if redis.call("EXISTS", KEYS[1]) == 1 then return 0 end
redis.call("DEL", KEYS[2])
redis.call("HSET", KEYS[1],
  "session_id", ARGV[1], "token", ARGV[2], "user_id", ARGV[3],
  "canvas_id", ARGV[4], "run_id", ARGV[5], "started_at", ARGV[6])
redis.call("PEXPIRE", KEYS[1], ARGV[7])
return 1`

	ragflowRefreshActiveSession = `if redis.call("HGET", KEYS[1], "token") == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) end return 0`

	ragflowReleaseActiveSession = `
if redis.call("HGET", KEYS[1], "token") ~= ARGV[1] then return 0 end
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[1])
return 1`

	ragflowRequestCancel = `
if redis.call("HGET", KEYS[1], "token") ~= ARGV[1] then return 0 end
redis.call("SET", KEYS[2], "x", "PX", ARGV[2])
return 1`
)

func TestEvalRunTrackerLease(t *testing.T) {
	s := testStore(t, nil)
	active, cancel := "run:session:1", "run:cancel:1"

	register := func(token string) scriptReply {
		return mustExec(t, s, "EVAL", ragflowRegisterActiveSession, "2", active, cancel,
			"session-1", token, "user-1", "canvas-1", "run-1", "1700000000000", "30000")
	}

	if r := register("tok-a"); r.num != 1 {
		t.Fatalf("first register = %+v, want 1", r)
	}
	// A second owner must not steal a live lease.
	if r := register("tok-b"); r.num != 0 {
		t.Fatalf("second register = %+v, want 0", r)
	}
	if r := mustExec(t, s, "HGET", active, "token"); string(r.bulk) != "tok-a" {
		t.Fatalf("token after contention = %q, want tok-a", r.bulk)
	}
	if r := mustExec(t, s, "PTTL", active); r.num <= 0 {
		t.Fatalf("lease has no TTL: %+v", r)
	}

	// Refresh only succeeds for the current owner.
	if r := mustExec(t, s, "EVAL", ragflowRefreshActiveSession, "1", active, "tok-b", "30000"); r.num != 0 {
		t.Fatalf("refresh by stranger = %+v, want 0", r)
	}
	if r := mustExec(t, s, "EVAL", ragflowRefreshActiveSession, "1", active, "tok-a", "30000"); r.num != 1 {
		t.Fatalf("refresh by owner = %+v, want 1", r)
	}
	// A missing lease compares unequal to any token instead of erroring.
	if r := mustExec(t, s, "EVAL", ragflowRefreshActiveSession, "1", "run:absent", "tok-a", "30000"); r.num != 0 {
		t.Fatalf("refresh of a missing lease = %+v, want 0", r)
	}

	// Cancel writes the marker with a PX expiry, again only for the owner.
	if r := mustExec(t, s, "EVAL", ragflowRequestCancel, "2", active, cancel, "tok-b", "15000"); r.num != 0 {
		t.Fatalf("cancel by stranger = %+v, want 0", r)
	}
	if r := mustExec(t, s, "EVAL", ragflowRequestCancel, "2", active, cancel, "tok-a", "15000"); r.num != 1 {
		t.Fatalf("cancel by owner = %+v, want 1", r)
	}
	if r := mustExec(t, s, "GET", cancel); string(r.bulk) != "x" {
		t.Fatalf("cancel marker = %+v, want x", r)
	}

	// Release drops both keys for the owner and nothing for a stranger.
	if r := mustExec(t, s, "EVAL", ragflowReleaseActiveSession, "2", active, cancel, "tok-b"); r.num != 0 {
		t.Fatalf("release by stranger = %+v, want 0", r)
	}
	if r := mustExec(t, s, "EXISTS", active); r.num != 1 {
		t.Fatalf("stranger release removed the lease")
	}
	if r := mustExec(t, s, "EVAL", ragflowReleaseActiveSession, "2", active, cancel, "tok-a"); r.num != 1 {
		t.Fatalf("release by owner = %+v, want 1", r)
	}
	if r := mustExec(t, s, "EXISTS", active, cancel); r.num != 0 {
		t.Fatalf("release left keys behind: %+v", r)
	}
}

// A fractional value passed to redis.call reaches the store as text, exactly as
// Redis' lua_tostring conversion does, instead of being truncated.
func TestEvalNumericArgumentsKeepFractions(t *testing.T) {
	s := testStore(t, nil)
	if r := mustExec(t, s, "EVAL",
		`redis.call("SET", KEYS[1], 1.5); return redis.call("GET", KEYS[1])`,
		"1", "frac"); r.kind != replyBulk || string(r.bulk) != "1.5" {
		t.Fatalf("fractional argument = %+v, want bulk 1.5", r)
	}
	if r := mustExec(t, s, "EVAL", `return 1.9`, "0"); r.num != 1 {
		t.Fatalf("script return value = %+v, want truncation to 1", r)
	}
}
