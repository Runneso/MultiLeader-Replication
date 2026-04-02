package main

import (
	"flag"
	"log/slog"
	"os"

	"HM3/internal/node"
)

func main() {
	id := flag.String("id", "A", "Node ID")
	host := flag.String("host", "127.0.0.1", "Node hostname")
	port := flag.Int("port", 8081, "Node port")

	flag.Parse()

	flag.Usage = func() {
		flag.PrintDefaults()
	}

	peerNode := node.NewNode(*id, *host, *port)
	err := peerNode.Start()

	if err != nil {
		slog.Error("failed to start node", "id", *id, "err", err)
		os.Exit(1)
	}
}