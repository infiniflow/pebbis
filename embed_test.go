package pebbis

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/resp"
)

// dialEmbedded connects a RESP client to the embedded endpoint.
func dialEmbedded(t *testing.T, addr string) *resp.Conn {
	t.Helper()
	nc, err := net.DialTimeout("unix", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	return resp.NewConn(nc)
}

func doCmd(t *testing.T, c *resp.Conn, args ...string) resp.Value {
	t.Helper()
	vals := make([]resp.Value, len(args))
	for i, a := range args {
		vals[i] = resp.StringValue(a)
	}
	if err := c.WriteArray(vals); err != nil {
		t.Fatalf("write %v: %v", args, err)
	}
	v, _, err := c.ReadValue()
	if err != nil {
		t.Fatalf("read %v: %v", args, err)
	}
	return v
}

// TestOpenEmbeddedServesRESP covers the point of the helper: existing RESP
// clients talk to a store that lives inside the current process, with the full
// command surface (aggregates and transactions included).
func TestOpenEmbeddedServesRESP(t *testing.T) {
	e, err := OpenEmbedded(EmbedOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open embedded: %v", err)
	}
	defer func() { _ = e.Close() }()

	if !strings.HasPrefix(filepath.Base(e.Addr()), "pebbis-") || !strings.HasSuffix(e.Addr(), ".sock") {
		t.Fatalf("socket = %q", e.Addr())
	}
	if len(e.Addr()) >= maxUnixSocketPath {
		t.Fatalf("default socket path too long: %d bytes", len(e.Addr()))
	}
	c := dialEmbedded(t, e.Addr())

	if v := doCmd(t, c, "SET", "k", "v"); v.String() != "OK" {
		t.Fatalf("SET = %v", v)
	}
	if v := doCmd(t, c, "GET", "k"); v.String() != "v" {
		t.Fatalf("GET = %v", v)
	}
	if v := doCmd(t, c, "HSET", "h", "f", "1"); v.Integer() != 1 {
		t.Fatalf("HSET = %v", v)
	}
	if v := doCmd(t, c, "HGET", "h", "f"); v.String() != "1" {
		t.Fatalf("HGET = %v", v)
	}
	if v := doCmd(t, c, "INCR", "n"); v.Integer() != 1 {
		t.Fatalf("INCR = %v", v)
	}

	if v := doCmd(t, c, "MULTI"); v.String() != "OK" {
		t.Fatalf("MULTI = %v", v)
	}
	if v := doCmd(t, c, "SET", "tx", "10"); v.String() != "QUEUED" {
		t.Fatalf("queued SET = %v", v)
	}
	if v := doCmd(t, c, "INCR", "tx"); v.String() != "QUEUED" {
		t.Fatalf("queued INCR = %v", v)
	}
	res := doCmd(t, c, "EXEC").Array()
	if len(res) != 2 || res[0].String() != "OK" || res[1].Integer() != 11 {
		t.Fatalf("EXEC = %v", res)
	}

	// The direct API and the socket see the same data.
	if v, err := e.Store().Get("k"); err != nil || string(v) != "v" {
		t.Fatalf("Store().Get = %q err %v", v, err)
	}
}

// TestOpenEmbeddedReopenKeepsData is the restart story: closing the endpoint
// closes Pebble, and opening the same directory again replays the WAL.
func TestOpenEmbeddedReopenKeepsData(t *testing.T) {
	dir := t.TempDir()

	e, err := OpenEmbedded(EmbedOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open embedded: %v", err)
	}
	c := dialEmbedded(t, e.Addr())
	for i := 0; i < 50; i++ {
		doCmd(t, c, "RPUSH", "list", "e"+string(rune('a'+i%26)))
	}
	if v := doCmd(t, c, "SET", "big", strings.Repeat("x", 600)); v.String() != "OK" {
		t.Fatalf("SET big = %v", v)
	}
	sock := e.Addr()
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Fatalf("socket %s still exists after Close", sock)
	}

	e2, err := OpenEmbedded(EmbedOptions{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = e2.Close() }()

	c2 := dialEmbedded(t, e2.Addr())
	if v := doCmd(t, c2, "LLEN", "list"); v.Integer() != 50 {
		t.Fatalf("LLEN after reopen = %v, want 50", v)
	}
	if v := doCmd(t, c2, "GET", "big"); len(v.String()) != 600 {
		t.Fatalf("large value after reopen: %d bytes", len(v.String()))
	}
}

// TestOpenEmbeddedSecondInstanceFails pins the single-owner rule: two processes
// (or two calls) cannot share one data directory.
func TestOpenEmbeddedSecondInstanceFails(t *testing.T) {
	dir := t.TempDir()
	e, err := OpenEmbedded(EmbedOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open embedded: %v", err)
	}
	defer func() { _ = e.Close() }()

	if _, err := OpenEmbedded(EmbedOptions{Dir: dir}); err == nil {
		t.Fatal("opening the same directory twice must fail")
	}
}

// TestOpenEmbeddedRejectsNonSocketPath makes a stale non-socket file an error
// instead of something the helper silently deletes.
func TestOpenEmbeddedRejectsNonSocketPath(t *testing.T) {
	// A short directory: the explicit socket path has to fit sockaddr_un.
	dir, err := os.MkdirTemp("", "pb")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "x.sock")
	if err := os.WriteFile(sock, []byte("not a socket"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}

	_, err = OpenEmbedded(EmbedOptions{Dir: dir, Socket: sock})
	if err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("err = %v, want a 'not a socket' error", err)
	}
	// The helper must not have deleted the user's file.
	if _, statErr := os.Stat(sock); statErr != nil {
		t.Fatalf("file was removed: %v", statErr)
	}
}
// TestOpenEmbeddedTCPServesRESP covers the cross-platform listener: platforms
// where a unix socket is awkward (Windows cannot chmod one) get the same
// endpoint over loopback TCP, and Addr reports the resolved ephemeral port.
func TestOpenEmbeddedTCPServesRESP(t *testing.T) {
	dir := t.TempDir()
	e, err := OpenEmbedded(EmbedOptions{Dir: dir, Network: "tcp"})
	if err != nil {
		t.Fatalf("open embedded: %v", err)
	}
	defer func() { _ = e.Close() }()

	if e.Network() != "tcp" {
		t.Fatalf("network = %q", e.Network())
	}
	host, port, err := net.SplitHostPort(e.Addr())
	if err != nil {
		t.Fatalf("Addr = %q: %v", e.Addr(), err)
	}
	if host != "127.0.0.1" || port == "0" {
		t.Fatalf("Addr = %q, want a resolved loopback port", e.Addr())
	}

	nc, err := net.DialTimeout("tcp", e.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", e.Addr(), err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	c := resp.NewConn(nc)

	if v := doCmd(t, c, "SET", "k", "v"); v.String() != "OK" {
		t.Fatalf("SET = %v", v)
	}
	if v := doCmd(t, c, "GET", "k"); v.String() != "v" {
		t.Fatalf("GET = %v", v)
	}
	if v := doCmd(t, c, "SELECT", "1"); v.String() != "OK" {
		t.Fatalf("SELECT = %v", v)
	}

	// Data written over TCP must survive a reopen on the same directory, and the
	// closed listener must not leave anything behind.
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	e2, err := OpenEmbedded(EmbedOptions{Dir: dir, Network: "tcp"})
	if err != nil {
		t.Fatalf("reopen embedded: %v", err)
	}
	defer func() { _ = e2.Close() }()
	nc2, err := net.DialTimeout("tcp", e2.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", e2.Addr(), err)
	}
	t.Cleanup(func() { _ = nc2.Close() })
	c2 := resp.NewConn(nc2)
	if v := doCmd(t, c2, "GET", "k"); v.String() != "v" {
		t.Fatalf("GET after reopen = %v", v)
	}
}

// TestOpenEmbeddedRejectsNonLoopbackAddr keeps the unauthenticated endpoint off
// the network.
func TestOpenEmbeddedRejectsNonLoopbackAddr(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", ":0", "8.8.8.8:6379"} {
		if _, err := OpenEmbedded(EmbedOptions{Dir: t.TempDir(), Network: "tcp", Addr: addr}); err == nil ||
			!strings.Contains(err.Error(), "loopback") {
			t.Fatalf("Addr %q: err = %v, want a loopback rejection", addr, err)
		}
	}
}
