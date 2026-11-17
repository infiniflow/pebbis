// Command drill is a restart drill: it starts a real pebbisd, writes a mixed
// workload, SIGKILLs it (no graceful shutdown, no Pebble close), restarts it on
// the same directory and checks that every acknowledged write came back —
// including large values that live in Pebble rather than the in-memory index,
// counters kept through the merge operator, transactions, TTLs and the stream
// pending-entries list.
//
//	go run ./tests/drill -bin ./bin/pebbisd
//
// The drill runs with -appendfsync always so "acknowledged" really means
// durable; with the default group policy a few hundred microseconds of
// acknowledged writes may legitimately be lost on SIGKILL.
package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/tidwall/resp"
)

const (
	stringsCount = 1000
	hashFields   = 100
	listLen      = 50
	zsetLen      = 50
	streamLen    = 20
	streamReads  = 10
	txnKeys      = 20
	ttlKeys      = 20
	incrCount    = 100
)

func main() {
	bin := flag.String("bin", "./bin/pebbisd", "path to the pebbisd binary")
	dir := flag.String("dir", "", "data directory (default: a fresh temp dir)")
	keep := flag.Bool("keep", false, "keep the data directory")
	flag.Parse()

	dataDir := *dir
	if dataDir == "" {
		tmp, err := os.MkdirTemp("", "pebbis-drill-")
		if err != nil {
			fail("temp dir: %v", err)
		}
		dataDir = tmp
		if !*keep {
			defer os.RemoveAll(tmp)
		}
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		fail("mkdir: %v", err)
	}

	rd := &drill{bin: *bin, dir: dataDir}
	rd.run()
}

type drill struct {
	bin  string
	dir  string
	cmd  *exec.Cmd
	port int
}

func (d *drill) run() {
	fmt.Printf("drill: binary=%s dir=%s\n", d.bin, d.dir)

	d.start()
	defer func() { d.stopGraceful() }()

	fmt.Println("phase 1: writing workload")
	d.write()

	if err := d.verify(); err != nil {
		fail("verify before crash: %v", err)
	}
	fmt.Println("phase 1: ok")

	fmt.Println("phase 2: SIGKILL (simulating a crash, no flush, no close)")
	d.kill()

	fmt.Println("phase 3: restarting on the same directory")
	d.start()

	if err := d.verify(); err != nil {
		fail("verify after restart: %v", err)
	}
	fmt.Println("phase 3: ok")

	fmt.Println("\nDRILL PASSED: every acknowledged write survived SIGKILL + restart")
}

func (d *drill) start() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail("pick port: %v", err)
	}
	d.port = ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	cmd := exec.Command(d.bin,
		"-addr", fmt.Sprintf("127.0.0.1:%d", d.port),
		"-dir", d.dir,
		"-appendfsync", "always",
		"-protected-mode=false", // loopback anyway, but keep the drill explicit
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fail("start server: %v", err)
	}
	d.cmd = cmd

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c, closeFn, err := dial(d.addr())
		if err == nil {
			_, err = cmdOf(c, "PING")
			closeFn()
			if err == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	fail("server did not become ready on %s", d.addr())
}

func (d *drill) addr() string { return fmt.Sprintf("127.0.0.1:%d", d.port) }

// dial connects and wraps the socket in a blocking RESP client. resp.Conn does
// not expose Close, so the socket close function is returned alongside.
func dial(addr string) (*resp.Conn, func(), error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, nil, err
	}
	return resp.NewConn(conn), func() { _ = conn.Close() }, nil
}

func (d *drill) client() (*resp.Conn, func()) {
	c, closeFn, err := dial(d.addr())
	if err != nil {
		fail("dial: %v", err)
	}
	return c, closeFn
}

// kill is a hard SIGKILL: no signal handler, no Pebble close, no WAL flush.
func (d *drill) kill() {
	if d.cmd == nil {
		return
	}
	if err := d.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		fail("kill: %v", err)
	}
	_ = d.cmd.Wait()
	d.cmd = nil
}

func (d *drill) stopGraceful() {
	if d.cmd == nil {
		return
	}
	_ = d.cmd.Process.Signal(syscall.SIGTERM)
	_, _ = d.cmd.Process.Wait()
	d.cmd = nil
}

func (d *drill) write() {
	c, closeFn := d.client()
	defer closeFn()

	// Plain strings, one of them too big to live inline in the index.
	for i := 0; i < stringsCount; i++ {
		must(c, "SET", fmt.Sprintf("k:%d", i), fmt.Sprintf("v:%d", i))
	}
	must(c, "SET", "big", strings.Repeat("x", 600))

	// Sparse hash (does not fit inline, so it is read back through a scan).
	for i := 0; i < hashFields; i++ {
		must(c, "HSET", "h", fmt.Sprintf("f%d", i), fmt.Sprintf("v%d", i))
	}

	for i := 0; i < listLen; i++ {
		must(c, "RPUSH", "l", fmt.Sprintf("e%d", i))
	}
	for i := 0; i < zsetLen; i++ {
		must(c, "ZADD", "z", fmt.Sprint(i), fmt.Sprintf("m%d", i))
	}

	// Stream with a consumer group that never acks: the pending entries list
	// must survive the restart.
	for i := 0; i < streamLen; i++ {
		must(c, "XADD", "st", "*", "i", fmt.Sprint(i))
	}
	must(c, "XGROUP", "CREATE", "st", "g", "0")
	res, err := cmdOf(c, "XREADGROUP", "GROUP", "g", "c1", "COUNT", fmt.Sprint(streamReads), "STREAMS", "st", ">")
	if err != nil {
		fail("xreadgroup: %v", err)
	}
	if len(res.Array()) != 1 {
		fail("xreadgroup returned %v", res)
	}

	// Counters go through the merge operator.
	for i := 0; i < incrCount; i++ {
		must(c, "INCR", "counter")
	}

	// TTLs.
	for i := 0; i < ttlKeys; i++ {
		must(c, "SET", fmt.Sprintf("ttl:%d", i), "v")
		must(c, "EXPIRE", fmt.Sprintf("ttl:%d", i), "600")
	}

	// A transaction: all or nothing.
	must(c, "MULTI")
	for i := 0; i < txnKeys; i++ {
		queued(c, "SET", fmt.Sprintf("tx:%d", i), fmt.Sprintf("v%d", i))
	}
	if v, err := cmdOf(c, "EXEC"); err != nil || len(v.Array()) != txnKeys {
		fail("exec: %v %v", v, err)
	}
}

func (d *drill) verify() error {
	c, closeFn := d.client()
	defer closeFn()

	for i := 0; i < stringsCount; i++ {
		k := fmt.Sprintf("k:%d", i)
		if got := must(c, "GET", k).String(); got != fmt.Sprintf("v:%d", i) {
			return fmt.Errorf("%s = %q", k, got)
		}
	}
	if got := must(c, "GET", "big").String(); got != strings.Repeat("x", 600) {
		return errors.New("large value lost or truncated")
	}

	if n := must(c, "HLEN", "h").Integer(); n != hashFields {
		return fmt.Errorf("HLEN = %d, want %d", n, hashFields)
	}
	if got := must(c, "HGET", "h", "f99").String(); got != "v99" {
		return fmt.Errorf("HGET h f99 = %q", got)
	}

	lr := must(c, "LRANGE", "l", "0", "-1").Array()
	if len(lr) != listLen || lr[0].String() != "e0" || lr[listLen-1].String() != fmt.Sprintf("e%d", listLen-1) {
		return fmt.Errorf("list = %d entries", len(lr))
	}

	if n := must(c, "ZCARD", "z").Integer(); n != zsetLen {
		return fmt.Errorf("ZCARD = %d, want %d", n, zsetLen)
	}
	if first := must(c, "ZRANGE", "z", "0", "0").Array(); len(first) != 1 || first[0].String() != "m0" {
		return fmt.Errorf("ZRANGE first = %v", first)
	}

	if n := must(c, "XLEN", "st").Integer(); n != streamLen {
		return fmt.Errorf("XLEN = %d, want %d", n, streamLen)
	}
	pending := must(c, "XPENDING", "st", "g").Array()
	if len(pending) == 0 || pending[0].Integer() != streamReads {
		return fmt.Errorf("XPENDING = %v, want %d pending entries", pending, streamReads)
	}

	if got := must(c, "GET", "counter").String(); got != fmt.Sprint(incrCount) {
		return fmt.Errorf("counter = %q, want %d", got, incrCount)
	}

	for i := 0; i < ttlKeys; i++ {
		k := fmt.Sprintf("ttl:%d", i)
		if got := must(c, "GET", k).String(); got != "v" {
			return fmt.Errorf("%s = %q after restart", k, got)
		}
		if ttl := must(c, "TTL", k).Integer(); ttl <= 0 {
			return fmt.Errorf("%s TTL = %d, want a positive remaining TTL", k, ttl)
		}
	}

	for i := 0; i < txnKeys; i++ {
		k := fmt.Sprintf("tx:%d", i)
		if got := must(c, "GET", k).String(); got != fmt.Sprintf("v%d", i) {
			return fmt.Errorf("%s = %q (transaction not durable)", k, got)
		}
	}
	return nil
}

func cmdOf(c *resp.Conn, args ...string) (resp.Value, error) {
	vals := make([]resp.Value, len(args))
	for i, a := range args {
		vals[i] = resp.StringValue(a)
	}
	if err := c.WriteArray(vals); err != nil {
		return resp.Value{}, err
	}
	v, _, err := c.ReadValue()
	if err != nil {
		return resp.Value{}, err
	}
	if v.Type() == resp.Error {
		return v, fmt.Errorf("%s", v.String())
	}
	return v, nil
}

func must(c *resp.Conn, args ...string) resp.Value {
	v, err := cmdOf(c, args...)
	if err != nil {
		fail("%v: %v", strings.Join(args, " "), err)
	}
	return v
}

// queued expects the QUEUED marker of a command inside MULTI.
func queued(c *resp.Conn, args ...string) {
	if v := must(c, args...); v.String() != "QUEUED" {
		fail("%v: %v", strings.Join(args, " "), v)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "DRILL FAILED: "+format+"\n", args...)
	os.Exit(1)
}
