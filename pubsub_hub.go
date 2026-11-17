package pebbis

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

// Pub/Sub registry.
//
// redcon ships a PubSub registry, but it is not usable for PUBSUB CHANNELS /
// NUMSUB / NUMPAT: it keeps its subscriber table private and hands the detached
// connection to its own goroutine, so the server never learns that a client
// unsubscribed or went away. The bookkeeping here is therefore our own, which
// makes the counters exact:
//
//   - a channel disappears from PUBSUB CHANNELS as soon as its last subscriber
//     unsubscribes or disconnects;
//   - a pattern is dropped from NUMPAT at the same moment;
//   - the maps only ever hold live subscriptions, so a client cannot grow them
//     by subscribing to random channel names and vanishing.
//
// Flow control: the hub's mutex is never held while writing to a client socket,
// and a subscription lock is never held while touching the hub, so a slow
// subscriber cannot stall SUBSCRIBE/PUBLISH of everybody else or deadlock.

// pubsubHub owns every subscription of one Server.
type pubsubHub struct {
	mu     sync.Mutex
	nextID uint64
	// clients maps the connection to its subscriber state.
	clients map[redcon.Conn]*pubsubClient
	// chans and psubs are inverted indexes: channel/pattern -> subscribers.
	chans map[string]map[*pubsubClient]struct{}
	psubs map[string]map[*pubsubClient]struct{}
	// onClose is told when a subscriber-mode connection ends, so the server can
	// release the client slot it reserved at accept time (redcon does not run
	// its close hook for a detached connection).
	onClose func(redcon.Conn)
}

// pubsubClient is one connection in subscriber mode. It owns a detached
// connection and the read loop that serves it.
type pubsubClient struct {
	hub   *pubsubHub
	id    uint64
	conn  redcon.Conn
	dconn redcon.DetachedConn

	// mu guards the two sets and every write to dconn: PUBLISH from another
	// connection and the read loop both write replies here.
	mu       sync.Mutex
	channels map[string]struct{}
	patterns map[string]struct{}
}

func newPubsubHub() *pubsubHub {
	return &pubsubHub{
		clients: make(map[redcon.Conn]*pubsubClient),
		chans:   make(map[string]map[*pubsubClient]struct{}),
		psubs:   make(map[string]map[*pubsubClient]struct{}),
	}
}

// addIndexLocked inserts cl under name in the given index. Caller holds hub.mu.
func addIndexLocked(index map[string]map[*pubsubClient]struct{}, name string, cl *pubsubClient) {
	set, ok := index[name]
	if !ok {
		set = make(map[*pubsubClient]struct{}, 1)
		index[name] = set
	}
	set[cl] = struct{}{}
}

// removeIndexLocked deletes cl from the index, dropping an empty channel entry.
// Caller holds hub.mu.
func removeIndexLocked(index map[string]map[*pubsubClient]struct{}, name string, cl *pubsubClient) {
	set, ok := index[name]
	if !ok {
		return
	}
	delete(set, cl)
	if len(set) == 0 {
		delete(index, name)
	}
}

// subscribe registers conn on channel (or on a pattern) and answers with the
// confirmation frame. The first subscription detaches the connection and starts
// the subscriber-mode read loop.
func (h *pubsubHub) subscribe(conn redcon.Conn, name string, pattern bool) {
	h.mu.Lock()
	cl, exists := h.clients[conn]
	if !exists {
		h.nextID++
		cl = &pubsubClient{
			hub:      h,
			id:       h.nextID,
			conn:     conn,
			dconn:    conn.Detach(),
			channels: make(map[string]struct{}),
			patterns: make(map[string]struct{}),
		}
		h.clients[conn] = cl
	}
	if pattern {
		addIndexLocked(h.psubs, name, cl)
	} else {
		addIndexLocked(h.chans, name, cl)
	}
	h.mu.Unlock()

	cl.mu.Lock()
	if pattern {
		cl.patterns[name] = struct{}{}
	} else {
		cl.channels[name] = struct{}{}
	}
	kind := "subscribe"
	if pattern {
		kind = "psubscribe"
	}
	cl.dconn.WriteArray(3)
	cl.dconn.WriteBulkString(kind)
	cl.dconn.WriteBulkString(name)
	cl.dconn.WriteInt(cl.countLocked())
	cl.dconn.Flush()
	cl.mu.Unlock()

	if !exists {
		go cl.run()
	}
}

// countLocked is the subscription total Redis reports in every push frame:
// subscribed channels plus subscribed patterns.
func (cl *pubsubClient) countLocked() int {
	return len(cl.channels) + len(cl.patterns)
}

// unsubscribe drops one channel/pattern (or all of them when name is empty) and
// writes the matching confirmation frames. It reports how many subscriptions
// remain.
func (cl *pubsubClient) unsubscribe(name string, pattern, all bool) int {
	// Detach from the hub index first, then answer. The two locks are taken one
	// after the other so neither PUBLISH nor SUBSCRIBE can observe a half state.
	var removed []string
	if all {
		cl.mu.Lock()
		src := cl.channels
		if pattern {
			src = cl.patterns
		}
		for n := range src {
			removed = append(removed, n)
		}
		cl.mu.Unlock()
	} else {
		removed = []string{name}
	}

	if len(removed) == 0 {
		// UNSUBSCRIBE with nothing subscribed still gets exactly one reply.
		cl.mu.Lock()
		kind := "unsubscribe"
		if pattern {
			kind = "punsubscribe"
		}
		cl.dconn.WriteArray(3)
		cl.dconn.WriteBulkString(kind)
		cl.dconn.WriteNull()
		cl.dconn.WriteInt(cl.countLocked())
		cl.dconn.Flush()
		remaining := cl.countLocked()
		cl.mu.Unlock()
		return remaining
	}

	cl.hub.mu.Lock()
	for _, n := range removed {
		if pattern {
			removeIndexLocked(cl.hub.psubs, n, cl)
		} else {
			removeIndexLocked(cl.hub.chans, n, cl)
		}
	}
	cl.hub.mu.Unlock()

	cl.mu.Lock()
	for _, n := range removed {
		if pattern {
			delete(cl.patterns, n)
		} else {
			delete(cl.channels, n)
		}
	}
	kind := "unsubscribe"
	if pattern {
		kind = "punsubscribe"
	}
	for _, n := range removed {
		cl.dconn.WriteArray(3)
		cl.dconn.WriteBulkString(kind)
		cl.dconn.WriteBulkString(n)
		cl.dconn.WriteInt(cl.countLocked())
	}
	cl.dconn.Flush()
	remaining := cl.countLocked()
	cl.mu.Unlock()
	return remaining
}

// publish delivers message to every exact channel subscriber and every matching
// pattern subscriber, returning the number of deliveries (Redis counts one per
// delivered frame, so a client subscribed both directly and by pattern counts
// twice).
func (h *pubsubHub) publish(channel, message string) int {
	type target struct {
		cl      *pubsubClient
		pattern bool
		name    string
	}

	h.mu.Lock()
	targets := make([]target, 0, 4)
	for cl := range h.chans[channel] {
		targets = append(targets, target{cl: cl})
	}
	for pat, subs := range h.psubs {
		if !globMatch(pat, channel) {
			continue
		}
		for cl := range subs {
			targets = append(targets, target{cl: cl, pattern: true, name: pat})
		}
	}
	h.mu.Unlock()

	sent := 0
	for _, t := range targets {
		t.cl.mu.Lock()
		if t.pattern {
			t.cl.dconn.WriteArray(4)
			t.cl.dconn.WriteBulkString("pmessage")
			t.cl.dconn.WriteBulkString(t.name)
			t.cl.dconn.WriteBulkString(channel)
			t.cl.dconn.WriteBulkString(message)
		} else {
			t.cl.dconn.WriteArray(3)
			t.cl.dconn.WriteBulkString("message")
			t.cl.dconn.WriteBulkString(channel)
			t.cl.dconn.WriteBulkString(message)
		}
		t.cl.dconn.Flush()
		t.cl.mu.Unlock()
		sent++
	}
	return sent
}

// removeConn forgets a connection. It is the safety net for a connection that
// subscribed and then closed before the read loop noticed; the read loop itself
// calls removeClient.
func (h *pubsubHub) removeConn(conn redcon.Conn) {
	h.mu.Lock()
	cl := h.clients[conn]
	h.mu.Unlock()
	if cl != nil {
		h.removeClient(cl)
	}
}

// removeClient drops every subscription of cl. Safe to call twice: the read loop
// and the server's close hook may race.
func (h *pubsubHub) removeClient(cl *pubsubClient) {
	cl.mu.Lock()
	chans := make([]string, 0, len(cl.channels))
	for n := range cl.channels {
		chans = append(chans, n)
	}
	pats := make([]string, 0, len(cl.patterns))
	for n := range cl.patterns {
		pats = append(pats, n)
	}
	cl.channels = make(map[string]struct{})
	cl.patterns = make(map[string]struct{})
	cl.mu.Unlock()

	h.mu.Lock()
	delete(h.clients, cl.conn)
	for _, n := range chans {
		removeIndexLocked(h.chans, n, cl)
	}
	for _, n := range pats {
		removeIndexLocked(h.psubs, n, cl)
	}
	h.mu.Unlock()
}

// channelNames lists the live channels, optionally filtered by a glob.
func (h *pubsubHub) channelNames(pattern string) []string {
	h.mu.Lock()
	out := make([]string, 0, len(h.chans))
	for ch := range h.chans {
		if pattern != "" && !globMatch(pattern, ch) {
			continue
		}
		out = append(out, ch)
	}
	h.mu.Unlock()
	sort.Strings(out)
	return out
}

// chanCount is one PUBSUB NUMSUB entry.
type chanCount struct {
	name string
	n    int
}

// channelCounts reports subscriber counts in the caller's argument order.
func (h *pubsubHub) channelCounts(channels []string) []chanCount {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]chanCount, 0, len(channels))
	for _, ch := range channels {
		out = append(out, chanCount{ch, len(h.chans[ch])})
	}
	return out
}

// patternCount is PUBSUB NUMPAT: the number of distinct subscribed patterns.
func (h *pubsubHub) patternCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.psubs)
}

// run serves the detached connection in subscriber mode: it reads commands and
// answers the small subset Redis allows here.
func (cl *pubsubClient) run() {
	defer func() {
		cl.hub.removeClient(cl)
		cl.mu.Lock()
		_ = cl.dconn.Close()
		cl.mu.Unlock()
		if cl.hub.onClose != nil {
			cl.hub.onClose(cl.conn)
		}
	}()

	for {
		cmd, err := cl.dconn.ReadCommand()
		if err != nil {
			return
		}
		if len(cmd.Args) == 0 {
			continue
		}
		name := strings.ToLower(string(cmd.Args[0]))
		switch name {
		case "subscribe", "psubscribe":
			if len(cmd.Args) < 2 {
				cl.writeErrLocked(fmt.Sprintf("ERR wrong number of arguments for '%s'", name))
				continue
			}
			for _, a := range cmd.Args[1:] {
				cl.hub.subscribe(cl.conn, string(a), name == "psubscribe")
			}
		case "unsubscribe", "punsubscribe":
			pattern := name == "punsubscribe"
			if len(cmd.Args) == 1 {
				cl.unsubscribe("", pattern, true)
				continue
			}
			for _, a := range cmd.Args[1:] {
				cl.unsubscribe(string(a), pattern, false)
			}
		case "ping":
			cl.mu.Lock()
			switch len(cmd.Args) {
			case 1:
				cl.dconn.WriteArray(2)
				cl.dconn.WriteBulkString("pong")
				cl.dconn.WriteBulkString("")
			case 2:
				cl.dconn.WriteArray(2)
				cl.dconn.WriteBulkString("pong")
				cl.dconn.WriteBulkString(string(cmd.Args[1]))
			default:
				cl.dconn.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s'", name))
			}
			cl.dconn.Flush()
			cl.mu.Unlock()
		case "quit":
			cl.mu.Lock()
			cl.dconn.WriteString("OK")
			cl.dconn.Flush()
			cl.mu.Unlock()
			return
		default:
			cl.writeErrLocked(fmt.Sprintf("ERR Can't execute '%s': only (P)SUBSCRIBE / "+
				"(P)UNSUBSCRIBE / PING / QUIT are allowed in this context", string(cmd.Args[0])))
		}
	}
}

// writeErrLocked answers an error frame on the detached connection.
func (cl *pubsubClient) writeErrLocked(msg string) {
	cl.mu.Lock()
	cl.dconn.WriteError(msg)
	cl.dconn.Flush()
	cl.mu.Unlock()
}
