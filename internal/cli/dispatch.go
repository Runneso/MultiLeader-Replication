package cli

import (
	"HM3/internal/protocol"
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"
)

const (
	defaultDialTimeout  = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
	defaultReadTimeout  = 10 * time.Second
)

func sendJSONLine(addr string, request any, response any) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := writeJSONLine(conn, request); err != nil {
		return err
	}

	if response == nil {
		return nil
	}

	if err := readJSONLine(conn, response); err != nil {
		return err
	}

	return nil
}

func dial(addr string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: defaultDialTimeout}

	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	return conn, nil
}

func writeJSONLine(conn net.Conn, value any) error {
	if err := conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}

	writer := bufio.NewWriter(conn)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush request: %w", err)
	}

	return nil
}

func readJSONLine(conn net.Conn, value any) error {
	if err := conn.SetReadDeadline(time.Now().Add(defaultReadTimeout)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if err := json.Unmarshal(line, value); err != nil {
		return fmt.Errorf("unmarshal response: %w (raw=%q)", err, string(line))
	}

	return nil
}

func addrOfNode(node protocol.NodeInfo) string {
	return net.JoinHostPort(node.Hostname, strconv.Itoa(node.Port))
}