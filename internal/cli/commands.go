package cli

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"HM3/internal/protocol"

	"github.com/google/uuid"
)

type Command func(state *State, args []string, out io.Writer) error

func Commands() map[string]Command {
	return map[string]Command{
		"help":                  cmdHelp,
		"addNode":               cmdAddNode,
		"removeNode":            cmdRemoveNode,
		"listNodes":             cmdListNodes,
		"setRole":               cmdSetRole,
		"setTopology":           cmdSetTopology,
		"setStarCenter":         cmdSetStarCenter,
		"setReplicationDelayMs": cmdSetDelay,
		"syncCluster":           cmdSyncCluster,
		"put":                   cmdPut,
		"get":                   cmdGet,
		"dump":                  cmdDump,
		"getAll":                cmdGetAll,
		"clusterDump":           cmdClusterDump,
	}
}

func ExecuteLine(state *State, line string, out io.Writer) error {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}

	command, ok := Commands()[fields[0]]
	if !ok {
		return fmt.Errorf("unknown command: %s (try 'help')", fields[0])
	}

	return command(state, fields[1:], out)
}

func cmdHelp(_ *State, _ []string, out io.Writer) error {
	_, _ = fmt.Fprintln(out, "Cluster commands:")
	_, _ = fmt.Fprintln(out, "  addNode <nodeId> <host> <port>")
	_, _ = fmt.Fprintln(out, "  removeNode <nodeId>")
	_, _ = fmt.Fprintln(out, "  listNodes")
	_, _ = fmt.Fprintln(out, "  setRole <nodeId> master|follower")
	_, _ = fmt.Fprintln(out, "  setTopology mesh|ring|star")
	_, _ = fmt.Fprintln(out, "  setStarCenter <nodeId>")
	_, _ = fmt.Fprintln(out, "  setReplicationDelayMs <min> <max>")
	_, _ = fmt.Fprintln(out, "  syncCluster")
	_, _ = fmt.Fprintln(out, "")
	_, _ = fmt.Fprintln(out, "User commands:")
	_, _ = fmt.Fprintln(out, "  put <key> <value> [--target <nodeId>] [--client <clientId>]")
	_, _ = fmt.Fprintln(out, "  get <key> [--target <nodeId>] [--client <clientId>]")
	_, _ = fmt.Fprintln(out, "  dump [--target <nodeId>]")
	_, _ = fmt.Fprintln(out, "")
	_, _ = fmt.Fprintln(out, "Debug commands:")
	_, _ = fmt.Fprintln(out, "  getAll <key>")
	_, _ = fmt.Fprintln(out, "  clusterDump")
	return nil
}

func validateStateConfig(state *State) error {
	if len(state.nodes) == 0 {
		return nil
	}

	if state.delayMinMs < 0 || state.delayMaxMs < 0 || state.delayMinMs > state.delayMaxMs {
		return fmt.Errorf("invalid delay range: %d..%d", state.delayMinMs, state.delayMaxMs)
	}

	validTopologies := map[string]struct{}{
		TopologyMesh: {},
		TopologyRing: {},
		TopologyStar: {},
	}
	if _, ok := validTopologies[state.topology]; !ok {
		return fmt.Errorf("invalid topology: %s", state.topology)
	}

	leaders := state.leaders()
	if len(leaders) == 0 {
		return errors.New("at least one master is required")
	}

	validRoles := map[string]struct{}{
		protocol.RoleMaster:   {},
		protocol.RoleFollower: {},
	}
	for _, id := range state.SortedNodeIDs() {
		role := state.nodes[id].Role
		if _, ok := validRoles[role]; !ok {
			return fmt.Errorf("node %s has invalid role %q", id, role)
		}
	}

	if state.topology == TopologyStar {
		if state.starCenter == "" {
			return errors.New("star topology requires star center")
		}
		center, ok := state.nodes[state.starCenter]
		if !ok {
			return fmt.Errorf("unknown star center: %s", state.starCenter)
		}
		if center.Role != protocol.RoleMaster {
			return fmt.Errorf("star center %s must have role=master", state.starCenter)
		}
	}

	return nil
}

func broadcastClusterUpdate(state *State, out io.Writer) {
	request := state.AsClusterUpdate()

	delivered := 0
	total := 0

	for id, node := range state.nodes {
		total++

		var response protocol.ClusterUpdateResponse
		err := sendJSONLine(addrOfNode(node), request, &response)
		if err != nil {
			_, _ = fmt.Fprintf(out, "node %s (%s:%d): %v\n", id, node.Hostname, node.Port, err)
			continue
		}
		if response.Status == protocol.StatusError {
			_, _ = fmt.Fprintf(out, "node %s (%s:%d): rejected update: %s (%s)\n",
				id, node.Hostname, node.Port, response.ErrorMsg, response.ErrorCode)
			continue
		}

		delivered++
	}

	_, _ = fmt.Fprintf(out, "cluster_update delivered: %d/%d\n", delivered, total)
}

func parseTargetClient(state *State, args []string) (target string, client uuid.UUID, rest []string, err error) {
	rest = make([]string, 0, len(args))

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--target":
			if i+1 >= len(args) {
				return "", uuid.Nil, nil, errors.New("missing --target value")
			}
			target = args[i+1]
			i++
		case "--client":
			if i+1 >= len(args) {
				return "", uuid.Nil, nil, errors.New("missing --client value")
			}
			client, err = uuid.Parse(args[i+1])
			if err != nil {
				return "", uuid.Nil, nil, fmt.Errorf("bad --client uuid: %w", err)
			}
			i++
		default:
			rest = append(rest, args[i])
		}
	}

	if client == uuid.Nil {
		client = uuid.New()
	}

	if target == "" {
		ids := state.SortedNodeIDs()
		if len(ids) == 0 {
			return "", uuid.Nil, nil, errors.New("cluster is empty")
		}
		target = ids[rand.Intn(len(ids))]
	}

	return target, client, rest, nil
}

func printClientBase(out io.Writer, base protocol.BaseClientResponse) {
	_, _ = fmt.Fprintf(out, "status=%s node=%s request_id=%s",
		base.Status, base.Node.ID, base.RequestUUID)

	if base.ErrorCode != "" || base.ErrorMsg != "" {
		_, _ = fmt.Fprintf(out, " error_code=%s error_msg=%s", base.ErrorCode, base.ErrorMsg)
	}

	_, _ = fmt.Fprintln(out)
}

func sortedDumpKeys(dump map[string]protocol.Entity) []string {
	keys := make([]string, 0, len(dump))
	for key := range dump {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func withStateMutation(state *State, out io.Writer, mutate func()) error {
	before := *state
	before.nodes = make(map[string]protocol.NodeInfo, len(state.nodes))
	for id, node := range state.nodes {
		before.nodes[id] = node
	}

	mutate()

	if err := validateStateConfig(state); err != nil {
		*state = before
		return err
	}

	broadcastClusterUpdate(state, out)
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdAddNode(state *State, args []string, out io.Writer) error {
	if len(args) != 3 {
		return errors.New("usage: addNode <nodeId> <host> <port>")
	}

	port, err := strconv.Atoi(args[2])
	if err != nil {
		return fmt.Errorf("bad port: %w", err)
	}

	nodeID, host := args[0], args[1]

	return withStateMutation(state, out, func() {
		wasEmpty := len(state.nodes) == 0
		state.AddNode(nodeID, host, port)

		if wasEmpty {
			state.SetRole(nodeID, protocol.RoleMaster)
			if state.topology == TopologyStar && state.starCenter == "" {
				state.starCenter = nodeID
			}
		}
	})
}

func cmdSyncCluster(state *State, args []string, out io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: syncCluster")
	}

	if err := validateStateConfig(state); err != nil {
		return err
	}

	broadcastClusterUpdate(state, out)
	_, _ = fmt.Fprintln(out, "ok")
	return nil
}

func cmdRemoveNode(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: removeNode <nodeId>")
	}

	nodeID := args[0]
	if _, ok := state.nodes[nodeID]; !ok {
		return fmt.Errorf("unknown node: %s", nodeID)
	}

	return withStateMutation(state, out, func() {
		state.RemoveNode(nodeID)

		if state.topology == TopologyStar && state.starCenter == "" {
			for _, id := range state.SortedNodeIDs() {
				if state.nodes[id].Role == protocol.RoleMaster {
					state.starCenter = id
					break
				}
			}
		}
	})
}

func cmdListNodes(state *State, _ []string, out io.Writer) error {
	_, _ = fmt.Fprintf(out, "topology=%s star_center=%s delay=[%d..%d]ms\n",
		state.topology, state.starCenter, state.delayMinMs, state.delayMaxMs)

	for _, id := range state.SortedNodeIDs() {
		node := state.nodes[id]
		_, _ = fmt.Fprintf(out, "- %s %s:%d role=%s\n",
			node.ID, node.Hostname, node.Port, node.Role)
	}

	return nil
}

func cmdSetRole(state *State, args []string, out io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: setRole <nodeId> master|follower")
	}

	nodeID, role := args[0], args[1]

	if _, ok := state.nodes[nodeID]; !ok {
		return fmt.Errorf("unknown node: %s", nodeID)
	}

	validRoles := map[string]struct{}{
		protocol.RoleMaster:   {},
		protocol.RoleFollower: {},
	}
	if _, ok := validRoles[role]; !ok {
		return fmt.Errorf("invalid role: %s", role)
	}

	return withStateMutation(state, out, func() {
		state.SetRole(nodeID, role)

		if state.topology == TopologyStar && state.starCenter == "" && role == protocol.RoleMaster {
			state.starCenter = nodeID
		}
		if state.topology == TopologyStar && state.starCenter == nodeID && role != protocol.RoleMaster {
			state.starCenter = ""
			for _, id := range state.SortedNodeIDs() {
				if state.nodes[id].Role == protocol.RoleMaster {
					state.starCenter = id
					break
				}
			}
		}
	})
}

func cmdSetTopology(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: setTopology mesh|ring|star")
	}

	topology := args[0]

	return withStateMutation(state, out, func() {
		state.SetTopology(topology)

		if topology == TopologyStar && state.starCenter == "" {
			for _, id := range state.SortedNodeIDs() {
				if state.nodes[id].Role == protocol.RoleMaster {
					state.starCenter = id
					break
				}
			}
		}
	})
}

func cmdSetStarCenter(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: setStarCenter <nodeId>")
	}

	nodeID := args[0]
	node, ok := state.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node: %s", nodeID)
	}
	if node.Role != protocol.RoleMaster {
		return fmt.Errorf("star center must be master: %s", nodeID)
	}

	return withStateMutation(state, out, func() {
		state.SetStarCenter(nodeID)
	})
}

func cmdSetDelay(state *State, args []string, out io.Writer) error {
	if len(args) != 2 {
		return errors.New("usage: setReplicationDelayMs <min> <max>")
	}

	minDelay, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("bad min: %w", err)
	}
	maxDelay, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("bad max: %w", err)
	}

	return withStateMutation(state, out, func() {
		state.SetDelayMs(minDelay, maxDelay)
	})
}

func cmdPut(state *State, args []string, out io.Writer) error {
	if len(args) < 2 {
		return errors.New("usage: put <key> <value> [--target <nodeId>] [--client <clientId>]")
	}

	key, value := args[0], args[1]

	targetID, clientID, _, err := parseTargetClient(state, args[2:])
	if err != nil {
		return err
	}

	node, ok := state.nodes[targetID]
	if !ok {
		return fmt.Errorf("unknown target: %s", targetID)
	}

	request := protocol.NewClientPutRequest(uuid.New(), clientID, key, value)

	var response protocol.ClientPutResponse
	if err := sendJSONLine(addrOfNode(node), request, &response); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(out, "target=%s\n", targetID)
	printClientBase(out, response.BaseClientResponse)
	return nil
}

func cmdGet(state *State, args []string, out io.Writer) error {
	if len(args) < 1 {
		return errors.New("usage: get <key> [--target <nodeId>] [--client <clientId>]")
	}

	key := args[0]

	targetID, clientID, _, err := parseTargetClient(state, args[1:])
	if err != nil {
		return err
	}

	node, ok := state.nodes[targetID]
	if !ok {
		return fmt.Errorf("unknown target: %s", targetID)
	}

	request := protocol.NewClientGetRequest(uuid.New(), clientID, key)

	var response protocol.ClientGetResponse
	if err := sendJSONLine(addrOfNode(node), request, &response); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(out, "target=%s\n", targetID)
	printClientBase(out, response.BaseClientResponse)

	if !response.Found {
		_, _ = fmt.Fprintln(out, "found=false")
		return nil
	}

	_, _ = fmt.Fprintf(out, "found=true value=%q version=(%d,%s)\n",
		response.Entity.Value,
		response.Entity.Version.Lamport,
		response.Entity.Version.NodeId,
	)

	return nil
}

func cmdDump(state *State, args []string, out io.Writer) error {
	targetID := ""

	if len(args) >= 2 && args[0] == "--target" {
		targetID = args[1]
		args = args[2:]
	}
	if len(args) != 0 {
		return errors.New("usage: dump [--target <nodeId>]")
	}

	if targetID == "" {
		ids := state.SortedNodeIDs()
		if len(ids) == 0 {
			return errors.New("cluster is empty")
		}
		targetID = ids[rand.Intn(len(ids))]
	}

	node, ok := state.nodes[targetID]
	if !ok {
		return fmt.Errorf("unknown target: %s", targetID)
	}

	request := protocol.NewClientDumpRequest(uuid.New(), uuid.New())

	var response protocol.ClientDumpResponse
	if err := sendJSONLine(addrOfNode(node), request, &response); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(out, "target=%s\n", targetID)
	printClientBase(out, response.BaseClientResponse)

	for _, key := range sortedDumpKeys(response.Dump) {
		entity := response.Dump[key]
		_, _ = fmt.Fprintf(out, "%s => value=%q version=(%d,%s)\n",
			key, entity.Value, entity.Version.Lamport, entity.Version.NodeId)
	}

	return nil
}

func cmdGetAll(state *State, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: getAll <key>")
	}

	key := args[0]
	ids := state.SortedNodeIDs()
	if len(ids) == 0 {
		return errors.New("cluster is empty")
	}

	for _, id := range ids {
		node := state.nodes[id]
		request := protocol.NewClientGetRequest(uuid.New(), uuid.New(), key)

		var response protocol.ClientGetResponse
		if err := sendJSONLine(addrOfNode(node), request, &response); err != nil {
			_, _ = fmt.Fprintf(out, "%s: error=%v\n", id, err)
			continue
		}

		if !response.Found {
			_, _ = fmt.Fprintf(out, "%s: found=false\n", id)
			continue
		}

		_, _ = fmt.Fprintf(out, "%s: value=%q version=(%d,%s)\n",
			id, response.Entity.Value, response.Entity.Version.Lamport, response.Entity.Version.NodeId)
	}

	return nil
}

func cmdClusterDump(state *State, _ []string, out io.Writer) error {
	ids := state.SortedNodeIDs()
	if len(ids) == 0 {
		return errors.New("cluster is empty")
	}

	for _, id := range ids {
		node := state.nodes[id]
		request := protocol.NewClientDumpRequest(uuid.New(), uuid.New())

		var response protocol.ClientDumpResponse
		if err := sendJSONLine(addrOfNode(node), request, &response); err != nil {
			_, _ = fmt.Fprintf(out, "node=%s error=%v\n", id, err)
			continue
		}

		_, _ = fmt.Fprintf(out, "node=%s\n", id)
		for _, key := range sortedDumpKeys(response.Dump) {
			entity := response.Dump[key]
			_, _ = fmt.Fprintf(out, "  %s => value=%q version=(%d,%s)\n",
				key, entity.Value, entity.Version.Lamport, entity.Version.NodeId)
		}
	}

	return nil
}
