package pebbis

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pebbis/pebbis/config"
)

// Embedded is an in-process RESP endpoint: a Store plus a listener goroutine in
// the calling process. Nothing is forked, so the storage lifecycle is exactly
// the process lifecycle — when the host process restarts, the store restarts
// with it and Pebble replays its WAL.
//
// The endpoint speaks RESP over a unix socket, which is what lets existing Redis
// clients switch over by changing only the connection string. That also means a
// command still pays for RESP parsing and a socket round trip: it saves the
// separate process, not the protocol. Code that can call Store methods directly
// is faster and needs no listener at all — the two can be used side by side on
// the same Store.
type Embedded struct {
	store *Store
	srv   *Server
	sock  string

	closeOnce sync.Once
	closeErr  error
}

// EmbedOptions configures OpenEmbedded.
type EmbedOptions struct {
	// Dir is the data directory. Required: it is what makes the embedded store
	// durable across restarts. It must be owned by this process alone; Pebble
	// takes an exclusive lock on it.
	Dir string
	// Socket overrides the unix socket path. Default: <temp>/pebbis-<hash>.sock,
	// where the hash is derived from the data directory, so the path is stable
	// per directory and stays well under the platform's sockaddr_un limit. An
	// explicit path must fit that limit too (~100 bytes).
	Socket string
	// Config overrides the store configuration. Default: DefaultOptions() with
	// Dir set. It is copied, so the caller's value is not mutated.
	Config *config.Config
	// MaxClients caps concurrent client connections. Zero means unlimited.
	MaxClients int
}

// maxUnixSocketPath keeps some headroom under the platform limit (104 bytes on
// Darwin, 108 on Linux, both including the trailing NUL).
const maxUnixSocketPath = 100

// defaultSocketPath builds a short, stable socket path for a data directory. It
// deliberately lives in the temp directory rather than inside Dir: socket paths
// are capped at ~100 bytes, and a realistic data directory (CI temp dirs, deep
// deploy paths) blows past that immediately.
func defaultSocketPath(dir string) string {
	sum := sha256.Sum256([]byte(dir))
	return filepath.Join(os.TempDir(), fmt.Sprintf("pebbis-%x.sock", sum[:8]))
}

// OpenEmbedded opens a data directory and serves it on a unix socket inside the
// current process. The returned Embedded must be closed; closing it stops the
// listener, closes Pebble (so the WAL is complete) and removes the socket.
func OpenEmbedded(opts EmbedOptions) (*Embedded, error) {
	if opts.Dir == "" {
		return nil, errors.New("Pebbis: OpenEmbedded requires a data directory")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("Pebbis: create data dir: %w", err)
	}
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("Pebbis: resolve data dir: %w", err)
	}

	sock := opts.Socket
	if sock == "" {
		sock = defaultSocketPath(dir)
	}
	if len(sock) >= maxUnixSocketPath {
		return nil, fmt.Errorf("Pebbis: unix socket path is %d bytes, the limit is %d: %s "+
			"(set EmbedOptions.Socket to a shorter path)", len(sock), maxUnixSocketPath-1, sock)
	}

	cfg := opts.Config
	if cfg == nil {
		cfg = DefaultOptions()
	} else {
		cp := *cfg
		cfg = &cp
	}
	cfg.Dir = dir

	// Open first: a second process on the same directory must fail on Pebble's
	// lock rather than quietly sharing (or stealing) the socket.
	store, err := Open(cfg)
	if err != nil {
		return nil, err
	}

	if err := clearStaleSocket(sock); err != nil {
		_ = store.Close()
		return nil, err
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("Pebbis: listen on %s: %w", sock, err)
	}
	// The socket is a full protocol endpoint without authentication, so keep it
	// readable by the owning user only.
	if err := os.Chmod(sock, 0o600); err != nil {
		_ = ln.Close()
		_ = store.Close()
		return nil, fmt.Errorf("Pebbis: chmod socket: %w", err)
	}

	srv := NewServer(store, ServerOptions{
		Network:    "unix",
		Addr:       sock,
		MaxClients: opts.MaxClients,
	})
	go func() {
		err := srv.Serve(ln)
		// Serve returns when the listener is closed, which is the normal path;
		// anything else means the endpoint died while the process kept running.
		if err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("Pebbis: embedded listener stopped: %v", err)
		}
	}()

	return &Embedded{store: store, srv: srv, sock: sock}, nil
}

// clearStaleSocket removes the leftover socket file of a crashed process, but
// refuses to steal a path that something is actually listening on.
func clearStaleSocket(sock string) error {
	if _, err := os.Stat(sock); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("Pebbis: stat socket %s: %w", sock, err)
	}
	if fi, err := os.Stat(sock); err == nil && fi.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("Pebbis: %s exists and is not a socket", sock)
	}
	conn, err := net.DialTimeout("unix", sock, 200*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("Pebbis: %s is already served by another process", sock)
	}
	if err := os.Remove(sock); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Pebbis: remove stale socket %s: %w", sock, err)
	}
	return nil
}

// Addr is the unix socket path clients connect to.
func (e *Embedded) Addr() string { return e.sock }

// Store exposes the underlying store, so hot paths can call it directly and
// skip the protocol entirely.
func (e *Embedded) Store() *Store { return e.store }

// Close stops the listener, then closes the store so the WAL is complete, then
// removes the socket file. Connections that are mid-command when this runs will
// see an error rather than a reply: stop the listener first is deliberate, but
// there is no drain of in-flight requests.
func (e *Embedded) Close() error {
	e.closeOnce.Do(func() {
		if e.srv != nil {
			_ = e.srv.Close()
		}
		e.closeErr = e.store.Close()
		_ = os.Remove(e.sock)
	})
	return e.closeErr
}
