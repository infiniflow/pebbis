package pebbis

import (
	"testing"

	"github.com/infiniflow/pebbis/config"
	"github.com/tidwall/redcon"
)

// fakeConn satisfies redcon.Conn for the admission check: only RemoteAddr and
// WriteError are ever reached, so the embedded nil interface covers the rest.
type fakeConn struct {
	redcon.Conn
	addr string
}

func (f fakeConn) RemoteAddr() string    { return f.addr }
func (f fakeConn) WriteError(msg string) {}

func newTestServer(t *testing.T, mutate func(*config.Config)) *Server {
	t.Helper()
	cfg := config.DefaultConfig()
	if mutate != nil {
		mutate(cfg)
	}
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewServer(store, ServerOptions{Addr: "127.0.0.1:0"})
}

func TestProtectedModeAdmission(t *testing.T) {
	srv := newTestServer(t, nil) // DefaultConfig: protected mode on, no password

	if !srv.allowConn(fakeConn{addr: "127.0.0.1:5000"}) {
		t.Error("loopback should be accepted in protected mode")
	}
	if !srv.allowConn(fakeConn{addr: "[::1]:5000"}) {
		t.Error("IPv6 loopback should be accepted in protected mode")
	}
	if srv.allowConn(fakeConn{addr: "10.1.2.3:5000"}) {
		t.Error("remote address must be refused while no password is set")
	}

	// A configured password lifts the restriction.
	srv = newTestServer(t, func(c *config.Config) { c.RequirePass = "s3cret" })
	if !srv.allowConn(fakeConn{addr: "10.1.2.3:5000"}) {
		t.Error("a password-authenticated deployment must accept remote clients")
	}

	// So does explicitly disabling protected mode.
	srv = newTestServer(t, func(c *config.Config) { c.ProtectedMode = false })
	if !srv.allowConn(fakeConn{addr: "10.1.2.3:5000"}) {
		t.Error("protected mode off must accept remote clients")
	}
}
