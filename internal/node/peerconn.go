package node

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"HM3/internal/protocol"
)

const (
	DefaultBufferSize = 1024
	retryDelay        = 20 * time.Millisecond
	readRetryDelay    = 50 * time.Millisecond
)

type PeerConn struct {
	peerID string
	addr   string

	mu           sync.Mutex
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	loopsStarted bool

	sendCh chan any
	done   chan struct{}

	onAck func(*protocol.ReplicationAck)
}

func NewPeerConn(peerID, addr string, onAck func(*protocol.ReplicationAck)) *PeerConn {
	return &PeerConn{
		peerID: peerID,
		addr:   addr,
		sendCh: make(chan any, DefaultBufferSize),
		done:   make(chan struct{}),
		onAck:  onAck,
	}
}

func (connection *PeerConn) Connect() error {
	if connection.isClosed() {
		return io.ErrClosedPipe
	}

	conn, err := net.Dial("tcp", connection.addr)
	if err != nil {
		return err
	}

	connection.mu.Lock()
	defer connection.mu.Unlock()

	if connection.conn != nil {
		_ = conn.Close()
		return nil
	}

	connection.conn = conn
	connection.reader = bufio.NewReader(conn)
	connection.writer = bufio.NewWriter(conn)

	if !connection.loopsStarted {
		connection.loopsStarted = true
		go connection.writerLoop()
		go connection.readerLoop()
	}

	return nil
}

func (connection *PeerConn) Close() {
	select {
	case <-connection.done:
		return
	default:
		close(connection.done)
	}

	connection.dropConn()
}

func (connection *PeerConn) Send(msg any) error {
	select {
	case <-connection.done:
		return io.ErrClosedPipe
	case connection.sendCh <- msg:
		return nil
	}
}

func (connection *PeerConn) writerLoop() {
	var pending any

	for {
		if connection.isClosed() {
			return
		}

		if pending == nil {
			select {
			case <-connection.done:
				return
			case pending = <-connection.sendCh:
			}
		}

		writer := connection.getWriter()
		if writer == nil {
			sleepOrDone(connection.done, retryDelay)
			continue
		}

		if err := writeJSON(writer, pending); err != nil {
			connection.dropConn()
			sleepOrDone(connection.done, retryDelay)
			continue
		}

		pending = nil
	}
}

func (connection *PeerConn) readerLoop() {
	for {
		if connection.isClosed() {
			return
		}

		reader := connection.getReader()
		if reader == nil {
			sleepOrDone(connection.done, readRetryDelay)
			continue
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			connection.dropConn()
			sleepOrDone(connection.done, retryDelay)
			continue
		}

		connection.handleIncoming(line)
	}
}

func (connection *PeerConn) handleIncoming(line []byte) {
	var env struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &env); err != nil {
		return
	}

	if env.Type != protocol.TypeReplicationAck || connection.onAck == nil {
		return
	}

	var ack protocol.ReplicationAck
	if err := json.Unmarshal(line, &ack); err != nil {
		return
	}

	slog.Info(
		"node handle request",
		"type", env.Type,
		"operation_id", ack.OperationID,
	)

	connection.onAck(&ack)
}

func (connection *PeerConn) dropConn() {
	connection.mu.Lock()
	defer connection.mu.Unlock()

	if connection.conn != nil {
		_ = connection.conn.Close()
	}

	connection.conn = nil
	connection.reader = nil
	connection.writer = nil
}

func (connection *PeerConn) getReader() *bufio.Reader {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.reader
}

func (connection *PeerConn) getWriter() *bufio.Writer {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.writer
}

func (connection *PeerConn) isClosed() bool {
	select {
	case <-connection.done:
		return true
	default:
		return false
	}
}

func writeJSON(w *bufio.Writer, msg any) error {
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		return err
	}
	return w.Flush()
}

func sleepOrDone(done <-chan struct{}, d time.Duration) {
	select {
	case <-done:
	case <-time.After(d):
	}
}
