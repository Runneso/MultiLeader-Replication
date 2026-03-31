package node

import (
	"HM3/internal/protocol"
	"HM3/pkg/random"
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultMinDelayMs = 0
	DefaultMaxDelayMs = 0
)

type ClusterRelease struct {
	onFinish  func()
	totalAcks int
	acked     map[string]struct{}
	cancel    context.CancelFunc
}

type PeerManager struct {
	mu sync.RWMutex

	self protocol.NodeInfo

	nodes map[string]protocol.NodeInfo
	peers map[string]*PeerConn

	config *ClusterConfig
	clock  *LogicalClock

	releases map[uuid.UUID]*ClusterRelease
}

func NewPeerManager(self protocol.NodeInfo) *PeerManager {
	return &PeerManager{
		self:     self,
		nodes:    make(map[string]protocol.NodeInfo),
		peers:    make(map[string]*PeerConn),
		clock:    NewLogicalClock(),
		releases: make(map[uuid.UUID]*ClusterRelease),
		config: NewClusterConfig(
			DefaultMinDelayMs,
			DefaultMaxDelayMs,
		),
	}
}

func (peerManager *PeerManager) GetRole() string {
	peerManager.mu.RLock()
	defer peerManager.mu.RUnlock()
	return peerManager.self.Role
}

func (peerManager *PeerManager) ApplyClusterUpdate(request *protocol.ClusterUpdateRequest) {
	peerManager.mu.Lock()
	defer peerManager.mu.Unlock()

	peerManager.config.Update(request)
	peerManager.nodes = make(map[string]protocol.NodeInfo)

	peerManager.self = request.Nodes[peerManager.self.ID]

	for _, node := range request.Followers[peerManager.self.ID] {
		peerManager.nodes[node.ID] = node
	}
	for _, node := range request.NextMasters[peerManager.self.ID] {
		peerManager.nodes[node.ID] = node
	}

	for id, connection := range peerManager.peers {
		connection.Close()
		delete(peerManager.peers, id)
	}

	for id, node := range peerManager.nodes {
		addr := net.JoinHostPort(node.Hostname, strconv.Itoa(node.Port))
		connection := NewPeerConn(id, addr, peerManager.onAck)
		peerManager.startReconnectLoop(connection)
		peerManager.peers[id] = connection
		go func(peer *PeerConn) {
			_ = peer.Connect()
		}(connection)
	}
}

func (peerManager *PeerManager) ReleaseMasters(key, value string, version protocol.Version, operationID uuid.UUID,
) error {
	peerManager.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())

	targets := make([]*PeerConn, 0, len(peerManager.peers))
	for _, connection := range peerManager.peers {
		if peerManager.nodes[connection.peerID].Role == protocol.RoleMaster {
			targets = append(targets, connection)
		}
	}

	if len(targets) == 0 {
		cancel()
		peerManager.mu.Unlock()
		return nil
	}

	release := &ClusterRelease{
		totalAcks: len(targets),
		onFinish:  func() {},
		acked:     make(map[string]struct{}),
		cancel:    cancel,
	}
	peerManager.releases[operationID] = release

	replicationRequest := protocol.NewReplicationPut(
		operationID,
		peerManager.self,
		key,
		value,
		version,
	)

	for _, connection := range targets {
		go peerManager.retrying(
			ctx,
			connection,
			replicationRequest,
			peerManager.config.delayMinMs,
			peerManager.config.delayMaxMs,
		)
	}

	peerManager.mu.Unlock()
	return nil
}

func (peerManager *PeerManager) ReleaseFollowers(key, value, nodeId string, version protocol.Version, backoff uuid.UUID) error {
	peerManager.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())

	operationID := uuid.New()
	targets := make([]*PeerConn, 0, len(peerManager.peers))

	for _, connection := range peerManager.peers {
		if peerManager.nodes[connection.peerID].Role == protocol.RoleFollower {
			targets = append(targets, connection)
		}
	}

	answerNode, found := peerManager.peers[nodeId]
	node := peerManager.self

	if len(targets) == 0 {
		cancel()
		peerManager.mu.Unlock()
		if !found {
			return nil
		}
		_ = answerNode.Send(
			protocol.NewReplicationAck(backoff, node),
		)
		return nil
	}

	release := &ClusterRelease{
		totalAcks: len(targets),
		onFinish: func() {
			if !found {
				return // начало цепочки репликации
			}
			_ = answerNode.Send(
				protocol.NewReplicationAck(backoff, node),
			)
		},
		acked:  make(map[string]struct{}),
		cancel: cancel,
	}

	peerManager.releases[operationID] = release

	replicationRequest := protocol.NewReplicationPut(
		operationID,
		peerManager.self,
		key,
		value,
		version,
	)

	for _, connection := range targets {
		go peerManager.retrying(
			ctx,
			connection,
			replicationRequest,
			peerManager.config.delayMinMs,
			peerManager.config.delayMaxMs,
		)
	}
	peerManager.mu.Unlock()

	return nil
}

func (peerManager *PeerManager) retrying(
	ctx context.Context,
	connection *PeerConn,
	request protocol.ReplicationPut,
	delayMinMs, delayMaxMs int,
) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(1000+delayMaxMs)) // avoid random choose in select
	defer ticker.Stop()
	peerManager.noiseSleep(delayMinMs, delayMaxMs)
	_ = connection.Send(request)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peerManager.noiseSleep(delayMinMs, delayMaxMs)
			_ = connection.Send(request)
		}
	}
}

func (peerManager *PeerManager) onAck(request *protocol.ReplicationAck) {
	peerManager.mu.Lock()

	release, ok := peerManager.releases[request.OperationID]
	if !ok {
		peerManager.mu.Unlock()
		return
	}

	release.acked[request.Node.ID] = struct{}{}
	if len(release.acked) != release.totalAcks {
		peerManager.mu.Unlock()
		return
	}

	release.cancel()
	delete(peerManager.releases, request.OperationID)
	onFinish := release.onFinish

	peerManager.mu.Unlock()

	onFinish()
}

func (peerManager *PeerManager) noiseSleep(delayMinMs, delayMaxMs int) {
	noise := random.RandInt(delayMinMs, delayMaxMs)
	duration := time.Duration(noise) * time.Millisecond
	time.Sleep(duration)
}

func (peerManager *PeerManager) startReconnectLoop(connection *PeerConn) {
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			select {
			case <-connection.done:
				return
			default:
			}
			connection.mu.Lock()
			alive := connection.conn != nil
			connection.mu.Unlock()
			if alive {
				continue
			}
			_ = connection.Connect()
		}
	}()
}
