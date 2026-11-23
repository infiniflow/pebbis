package goclient

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These bodies are the RAGFlow server's own scripts. The client-side point of
// this file is the path production takes: redis.NewScript issues EVALSHA first,
// falls back to EVAL (and SCRIPT LOAD) on NOSCRIPT, and the reply is decoded by
// the typed helpers the server calls (.Int64(), .Slice()).
const (
	scriptDeleteIfEqual = `
	local current_value = redis.call('get', KEYS[1])
	if current_value and current_value == ARGV[1] then
		redis.call('del', KEYS[1])
		return 1
	end
	return 0
`

	scriptTokenBucket = `
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

	scriptRegisterActiveSession = `
if redis.call("EXISTS", KEYS[1]) == 1 then return 0 end
redis.call("DEL", KEYS[2])
redis.call("HSET", KEYS[1],
  "session_id", ARGV[1], "token", ARGV[2], "user_id", ARGV[3],
  "canvas_id", ARGV[4], "run_id", ARGV[5], "started_at", ARGV[6])
redis.call("PEXPIRE", KEYS[1], ARGV[7])
return 1`
)

func TestScriptRunFallsBackAfterScriptFlush(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	script := redis.NewScript(scriptDeleteIfEqual)

	if err := c.Set(ctx, "lock:doc", "owner-1", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := script.Run(ctx, c, []string{"lock:doc"}, "owner-1").Int64()
	if err != nil || got != 1 {
		t.Fatalf("owner delete = %d, %v; want 1", got, err)
	}
	// The digest is cached now; flushing forces the NOSCRIPT fallback that a
	// restarted server would hand the client.
	if err := c.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("script flush: %v", err)
	}
	if err := c.Set(ctx, "lock:doc", "owner-2", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err = script.Run(ctx, c, []string{"lock:doc"}, "owner-2").Int64()
	if err != nil || got != 1 {
		t.Fatalf("delete after flush = %d, %v; want 1", got, err)
	}
	if err := c.Get(ctx, "lock:doc").Err(); err != redis.Nil {
		t.Fatalf("key survived the delete: %v", err)
	}
}

func TestScriptTokenBucketTypedReply(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	script := redis.NewScript(scriptTokenBucket)

	for i := 0; i < 2; i++ {
		reply, err := script.Run(ctx, c, []string{"tb:webhook"},
			"2", "0.1", "1700000000", "1").Slice()
		if err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
		if len(reply) != 2 || reply[0] != int64(1) {
			t.Fatalf("call %d = %v, want [1, tokens]", i+1, reply)
		}
	}

	// Five seconds of refill at 0.1/s plus a fractional cost must leave a
	// fraction in the hash: a truncated numeric argument would store 0.
	reply, err := script.Run(ctx, c, []string{"tb:webhook"},
		"2", "0.1", "1700000005", "0.1").Slice()
	if err != nil {
		t.Fatalf("refill call: %v", err)
	}
	if len(reply) != 2 || reply[0] != int64(1) {
		t.Fatalf("refill = %v, want [1, tokens]", reply)
	}
	if got := c.HGet(ctx, "tb:webhook", "tokens").Val(); got != "0.4" {
		t.Fatalf("stored tokens = %q, want 0.4", got)
	}
}

func TestEvalRunTrackerLeaseOverClient(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	keys := []string{"run:session:1", "run:cancel:1"}

	register := func(token string) int64 {
		t.Helper()
		got, err := c.Eval(ctx, scriptRegisterActiveSession, keys,
			"session-1", token, "user-1", "canvas-1", "run-1",
			"1700000000000", "30000").Int64()
		if err != nil {
			t.Fatalf("register %q: %v", token, err)
		}
		return got
	}
	if got := register("tok-a"); got != 1 {
		t.Fatalf("first register = %d, want 1", got)
	}
	if got := register("tok-b"); got != 0 {
		t.Fatalf("second register = %d, want 0", got)
	}
	if ttl := c.PTTL(ctx, keys[0]).Val(); ttl <= 0 || ttl > 30*time.Second {
		t.Fatalf("lease TTL = %v", ttl)
	}
	if owner := c.HGet(ctx, keys[0], "token").Val(); owner != "tok-a" {
		t.Fatalf("lease owner = %q, want tok-a", owner)
	}
}
