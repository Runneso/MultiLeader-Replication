package node

import "HM3/internal/protocol"

type ClusterConfig struct {
	delayMinMs int
	delayMaxMs int
}

func NewClusterConfig(delayMinMs, delayMaxMs int) *ClusterConfig {
	return &ClusterConfig{
		delayMinMs: delayMinMs,
		delayMaxMs: delayMaxMs,
	}
}

func (config *ClusterConfig) Update(request *protocol.ClusterUpdateRequest) {
	config.delayMaxMs = request.MaxDelayMs
	config.delayMinMs = request.MinDelayMs
}
