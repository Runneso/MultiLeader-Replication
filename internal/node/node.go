package node

import (
	"HM3/internal/inmemory"
	"HM3/internal/protocol"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"

	"github.com/google/uuid"
)

type Node struct {
	id       string
	hostname string
	port     int

	storage *inmemory.Storage
	dedup   *inmemory.Deduplication

	peerManager   *PeerManager
	handlers      map[string]func([]byte, *bufio.Writer) (uuid.UUID, error)
	errorHandlers map[string]func(uuid.UUID, *bufio.Writer, error)
}

func NewNode(id, hostname string, port int) *Node {
	node := &Node{
		id:          id,
		hostname:    hostname,
		port:        port,
		storage:     inmemory.NewStorage(),
		dedup:       inmemory.NewDeduplication(),
		peerManager: NewPeerManager(protocol.NodeInfo{ID: id, Hostname: hostname, Port: port}),
	}
	node.handlers = map[string]func([]byte, *bufio.Writer) (uuid.UUID, error){
		protocol.TypeClientGetRequest:     node.handlerClientGetRequest,
		protocol.TypeClientPutRequest:     node.handlerClientPutRequest,
		protocol.TypeClientDumpRequest:    node.handlerClientDumpRequest,
		protocol.TypeReplicationPut:       node.handlerReplicationPut,
		protocol.TypeClusterUpdateRequest: node.handlerClusterUpdateRequest,
	}
	node.errorHandlers = map[string]func(uuid.UUID, *bufio.Writer, error){
		protocol.TypeClientGetRequest:     node.handlerClientError,
		protocol.TypeClientPutRequest:     node.handlerClientError,
		protocol.TypeClientDumpRequest:    node.handlerClientError,
		protocol.TypeClusterUpdateRequest: node.handlerClusterUpdateError,
	}
	return node

}

func (node *Node) Start() error {
	node.dedup.StartVacuum()

	address := net.JoinHostPort(node.hostname, strconv.Itoa(node.port))
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return fmt.Errorf("listen %s: %w", address, err)
	}

	slog.Info("node started", "id", node.id, "addr", address)

	for {
		connection, err := listener.Accept()
		if err != nil {
			slog.Error("failed to accept connection", "id", node.id, "err", err)
		}
		go node.handleConn(connection)
	}
}

func (node *Node) SelfInfo() protocol.NodeInfo {
	return protocol.NodeInfo{
		ID:       node.id,
		Hostname: node.hostname,
		Port:     node.port,
		Role:     node.peerManager.GetRole(),
	}
}

func (node *Node) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	defer writer.Flush()

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			slog.Error("read failed", "remote", conn.RemoteAddr().String(), "error", err)
			return
		}

		var env struct {
			Type string `json:"type"`
		}

		if err := json.Unmarshal(line, &env); err != nil {
			response := protocol.NewClientResponse(uuid.Nil, node.SelfInfo(), protocol.NewBadRequestError("bad json"))
			_ = node.writeJSONLine(writer, response)
			continue
		}

		if handler, ok := node.handlers[env.Type]; ok {
			if id, err := handler(line, writer); err != nil {
				if errorHandler, ok := node.errorHandlers[env.Type]; ok {
					errorHandler(id, writer, err)
					slog.Info("node handle error", "type", env.Type, "error", err)
				} else {
					slog.Warn("unknown handle error type", "type", env.Type)
				}
				slog.Warn("handle request failed", "type", env.Type, "error", err)
			}
		} else {
			slog.Warn("unknown handle type", "type", env.Type)
		}
	}
}

func (node *Node) writeJSONLine(writer *bufio.Writer, v any) error {
	bytes, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := writer.Write(bytes); err != nil {
		return err
	}
	if err := writer.WriteByte('\n'); err != nil {
		return err
	}
	return writer.Flush()
}

func (node *Node) handlerClientGetRequest(data []byte, writer *bufio.Writer) (uuid.UUID, error) {
	var request protocol.ClientGetRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return uuid.Nil, fmt.Errorf("unmarshal client get request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestUUID.String())

	value, exists := node.storage.Get(request.Key)

	response := protocol.NewClientGetResponse(
		request.RequestUUID,
		node.SelfInfo(),
		value,
		exists,
		nil)

	err := node.writeJSONLine(writer, response)
	if err != nil {
		return request.RequestUUID, fmt.Errorf("write response: %w", err)
	}

	return request.RequestUUID, nil
}

func (node *Node) handlerClientPutRequest(data []byte, writer *bufio.Writer) (uuid.UUID, error) {
	var request protocol.ClientPutRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return uuid.Nil, fmt.Errorf("unmarshal client put request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestUUID.String())

	if node.peerManager.GetRole() == protocol.RoleFollower {
		return request.RequestUUID, fmt.Errorf("not leader: %w", protocol.NewNotLeaderError())
	}

	version := protocol.Version{
		Lamport: node.peerManager.clock.Tick(),
		NodeId:  node.id,
	}

	node.storage.Put(request.Key, request.Value, version)

	err := node.peerManager.ReleaseMasters(request.Key, request.Value, version, uuid.New())
	if err != nil {
		return request.RequestUUID, fmt.Errorf("masters release request: %w", err)
	}

	err = node.peerManager.ReleaseFollowers(request.Key, request.Value, "-", version, request.RequestUUID)
	if err != nil {
		return request.RequestUUID, fmt.Errorf("followers release request: %w", err)
	}

	response := protocol.NewClientPutResponse(
		request.RequestUUID,
		node.SelfInfo(),
		nil,
	)

	err = node.writeJSONLine(writer, response)
	if err != nil {
		return request.RequestUUID, fmt.Errorf("write response: %w", err)
	}

	return request.RequestUUID, nil
}

func (node *Node) handlerClientDumpRequest(data []byte, writer *bufio.Writer) (uuid.UUID, error) {
	var request protocol.ClientDumpRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return uuid.Nil, fmt.Errorf("unmarshal client dump request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestUUID.String())

	dump := node.storage.Dump()

	response := protocol.NewClientDumpResponse(
		request.RequestUUID,
		node.SelfInfo(),
		dump,
		nil)

	err := node.writeJSONLine(writer, response)
	if err != nil {
		return request.RequestUUID, fmt.Errorf("write response: %w", err)
	}

	return request.RequestUUID, nil
}

func (node *Node) handlerReplicationPut(data []byte, writer *bufio.Writer) (uuid.UUID, error) {
	var request protocol.ReplicationPut
	if err := json.Unmarshal(data, &request); err != nil {
		return uuid.Nil, fmt.Errorf("unmarshal replication put request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "operation_id", request.OperationID.String(), "from", request.Node.ID, "key", request.Key)

	node.peerManager.clock.Update(request.Version.Lamport)

	isFresh := node.dedup.AddIfAbsent(request.OperationID)
	if !isFresh {
		response := protocol.NewReplicationAck(request.OperationID, node.SelfInfo())

		err := node.writeJSONLine(writer, response)
		if err != nil {
			return request.OperationID, fmt.Errorf("write response: %w", err)
		}
		return request.OperationID, nil
	}

	node.storage.Put(request.Key, request.Value, request.Version)

	if node.peerManager.GetRole() == protocol.RoleFollower {
		response := protocol.NewReplicationAck(request.OperationID, node.SelfInfo())

		err := node.writeJSONLine(writer, response)
		if err != nil {
			return request.OperationID, fmt.Errorf("write response: %w", err)
		}
		return request.OperationID, nil
	}

	err := node.peerManager.ReleaseMasters(request.Key, request.Value, request.Version, request.OperationID)
	if err != nil {
		return request.OperationID, fmt.Errorf("masters release request: %w", err)
	}

	err = node.peerManager.ReleaseFollowers(request.Key, request.Value, request.Node.ID, request.Version, request.OperationID)
	if err != nil {
		return request.OperationID, fmt.Errorf("followers release request: %w", err)
	}

	return request.OperationID, nil
}

func (node *Node) handlerClusterUpdateRequest(data []byte, writer *bufio.Writer) (uuid.UUID, error) {
	var request protocol.ClusterUpdateRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return uuid.Nil, fmt.Errorf("unmarshal cluster update request: %w", protocol.NewBadRequestError("bad json"))
	}

	slog.Info("node handle request", "type", request.Type, "request_id", request.RequestID.String())

	node.peerManager.ApplyClusterUpdate(&request)

	response := protocol.NewClusterUpdateResponse(
		request.RequestID,
		node.SelfInfo(),
		nil,
	)

	err := node.writeJSONLine(writer, response)
	if err != nil {
		return request.RequestID, fmt.Errorf("write response: %w", err)
	}

	return request.RequestID, nil
}

func (node *Node) handlerClientError(requestID uuid.UUID, writer *bufio.Writer, error error) {
	response := protocol.NewClientResponse(requestID, node.SelfInfo(), error)
	_ = node.writeJSONLine(writer, response)
}

func (node *Node) handlerClusterUpdateError(requestID uuid.UUID, writer *bufio.Writer, error error) {
	response := protocol.NewClusterUpdateResponse(requestID, node.SelfInfo(), error)
	_ = node.writeJSONLine(writer, response)
}
