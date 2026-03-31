package cli

import (
	"HM3/internal/protocol"
	"sort"

	"github.com/google/uuid"
)

const (
	TopologyMesh = "mesh"
	TopologyRing = "ring"
	TopologyStar = "star"
)

type State struct {
	nodes               map[string]protocol.NodeInfo
	strategiesOfMasters map[string]func() map[string][]protocol.NodeInfo

	topology   string
	starCenter string

	delayMinMs int
	delayMaxMs int
}

func NewState() *State {
	state := &State{
		nodes:    make(map[string]protocol.NodeInfo),
		topology: TopologyMesh,
	}
	state.strategiesOfMasters = map[string]func() map[string][]protocol.NodeInfo{
		TopologyMesh: state.buildMeshNextMasters,
		TopologyRing: state.buildRingNextMasters,
		TopologyStar: state.buildStarNextMasters,
	}
	return state
}

func (state *State) AddNode(id, host string, port int) {
	state.nodes[id] = protocol.NodeInfo{
		ID:       id,
		Hostname: host,
		Port:     port,
		Role:     protocol.RoleFollower,
	}
}

func (state *State) RemoveNode(id string) {
	delete(state.nodes, id)
	if state.starCenter == id {
		state.starCenter = ""
	}
}

func (state *State) SetRole(id, role string) {
	node := state.nodes[id]
	node.Role = role
	state.nodes[id] = node
}

func (state *State) SetTopology(topology string) {
	state.topology = topology
}

func (state *State) SetStarCenter(nodeID string) {
	state.starCenter = nodeID
}

func (state *State) SetDelayMs(minMs, maxMs int) {
	state.delayMinMs = minMs
	state.delayMaxMs = maxMs
}

func (state *State) SortedNodeIDs() []string {
	indexes := make([]string, 0, len(state.nodes))
	for id := range state.nodes {
		indexes = append(indexes, id)
	}
	sort.Strings(indexes)
	return indexes
}

func (state *State) leaders() []protocol.NodeInfo {
	result := make([]protocol.NodeInfo, 0)
	for _, id := range state.SortedNodeIDs() {
		node := state.nodes[id]
		if node.Role == protocol.RoleMaster {
			result = append(result, node)
		}
	}
	return result
}

func (state *State) followers() []protocol.NodeInfo {
	result := make([]protocol.NodeInfo, 0)
	for _, id := range state.SortedNodeIDs() {
		node := state.nodes[id]
		if node.Role == protocol.RoleFollower {
			result = append(result, node)
		}
	}
	return result
}

func (state *State) buildMeshNextMasters() map[string][]protocol.NodeInfo {
	result := make(map[string][]protocol.NodeInfo)

	leaders := state.leaders()

	for _, from := range leaders {
		for _, to := range leaders {
			if from.ID == to.ID {
				continue
			}
			result[from.ID] = append(result[from.ID], to)
		}
	}

	return result
}

func (state *State) buildRingNextMasters() map[string][]protocol.NodeInfo {
	result := make(map[string][]protocol.NodeInfo)

	leaders := state.leaders()

	for index := 0; index < len(leaders); index++ {
		curr := leaders[index]
		next := leaders[(index+1)%len(leaders)]
		if curr.ID != next.ID {
			result[curr.ID] = append(result[curr.ID], next)
		}
	}

	return result
}

func (state *State) buildStarNextMasters() map[string][]protocol.NodeInfo {
	result := make(map[string][]protocol.NodeInfo)

	if state.starCenter == "" {
		return result
	}

	center, ok := state.nodes[state.starCenter]
	if !ok || center.Role != protocol.RoleMaster {
		return result
	}

	for _, node := range state.leaders() {
		if node.ID == center.ID {
			continue
		}
		result[center.ID] = append(result[center.ID], node)
		result[node.ID] = append(result[node.ID], center)
	}

	return result
}

func (state *State) buildNextMasters() map[string][]protocol.NodeInfo {
	return state.strategiesOfMasters[state.topology]()
}

func (state *State) buildFollowers() map[string][]protocol.NodeInfo {
	result := make(map[string][]protocol.NodeInfo)

	leaders := state.leaders()
	followers := state.followers()

	if len(leaders) == 0 {
		return result
	}

	for index, follower := range followers {
		leader := leaders[index%len(leaders)]
		result[leader.ID] = append(result[leader.ID], follower)
	}

	return result
}

func (state *State) AsClusterUpdate() protocol.ClusterUpdateRequest {
	return protocol.NewClusterUpdateRequest(
		uuid.New(),
		state.nodes,
		state.buildNextMasters(),
		state.buildFollowers(),
		state.delayMinMs,
		state.delayMaxMs,
	)
}
