package main

import (
	"bucketdb"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	nodeID := flag.String("id", "node-1", "Node ID")
	httpAddr := flag.String("http", ":8080", "ClusterKit HTTP coordination address")
	apiPort := flag.String("api", ":9080", "BucketDB HTTP API port")
	joinAddr := flag.String("join", "", "Address of a node to join")
	storagePath := flag.String("storage", "./data/chunks", "Path for chunk storage")
	metadataPath := flag.String("metadata", "./data/metadata", "Path for metadata (BadgerDB)")

	flag.Parse()

	// Ensure directories exist
	os.MkdirAll(*storagePath, 0755)
	os.MkdirAll(*metadataPath, 0755)

	// Configure BucketDB
	config := bucketdb.DefaultConfig()
	config.StoragePath = *storagePath
	config.MetadataPath = *metadataPath
	config.Cluster.NodeID = *nodeID
	config.Cluster.HTTPAddr = *httpAddr
	config.Cluster.JoinAddr = *joinAddr
	config.Cluster.DataDir = *metadataPath + "/cluster"
	config.Cluster.HealthCheck.Enabled = true
	config.Cluster.HealthCheck.Interval = 5 * time.Second

	// Create and start BucketDB
	db, err := bucketdb.NewBucketDB(config)
	if err != nil {
		log.Fatalf("Failed to initialize BucketDB: %v", err)
	}
	defer db.Close()

	// Create and start HTTP Server
	server := bucketdb.NewServer(db, *apiPort)
	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	log.Printf("🚀 Node %s started!", *nodeID)
	log.Printf("   Coordination: %s", *httpAddr)
	log.Printf("   Public API:   %s", *apiPort)

	// Wait for termination
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
}
