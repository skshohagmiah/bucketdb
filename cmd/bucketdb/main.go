package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/skshohagmiah/bucketdb/pkg/api"
	"github.com/skshohagmiah/bucketdb/pkg/core"
	"github.com/skshohagmiah/bucketdb/pkg/types"
)

func main() {
	nodeID := flag.String("id", "node-1", "Node ID")
	httpAddr := flag.String("http", ":8080", "ClusterKit HTTP coordination address")
	apiPort := flag.String("api", ":9080", "BucketDB HTTP API port")
	joinAddr := flag.String("join", "", "Address of a node to join")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap as the first node")
	storagePath := flag.String("storage", "./data/chunks", "Path for chunk storage")
	metadataPath := flag.String("metadata", "./data/metadata", "Path for metadata (BadgerDB)")

	tlsEnabled := flag.Bool("tls", false, "Enable TLS")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file")
	tlsKey := flag.String("tls-key", "", "TLS key file")
	tlsCA := flag.String("tls-ca", "", "TLS CA file for internal trust")
	tlsInsecure := flag.Bool("tls-insecure", false, "Skip TLS verification (dev only)")

	flag.Parse()

	// Ensure directories exist
	os.MkdirAll(*storagePath, 0755)
	os.MkdirAll(*metadataPath, 0755)

	// Configure BucketDB
	config := types.DefaultConfig()
	config.StoragePath = *storagePath
	config.MetadataPath = *metadataPath
	config.Cluster.NodeID = *nodeID
	config.Cluster.HTTPAddr = *httpAddr
	config.Cluster.JoinAddr = *joinAddr
	config.Cluster.Bootstrap = *bootstrap
	config.Cluster.DataDir = *metadataPath + "/cluster"
	config.Cluster.HealthCheck.Enabled = true
	config.Cluster.HealthCheck.Interval = 5 * time.Second
	config.Cluster.Services = map[string]string{
		"api": *apiPort,
	}

	config.TLS.Enabled = *tlsEnabled
	config.TLS.CertFile = *tlsCert
	config.TLS.KeyFile = *tlsKey
	config.TLS.CAFile = *tlsCA
	config.TLS.InsecureSkipVerify = *tlsInsecure

	// Create and start BucketDB
	db, err := core.NewBucketDB(config)
	if err != nil {
		log.Fatalf("Failed to initialize BucketDB: %v", err)
	}
	defer db.Close()

	// Create and start HTTP Server
	server := api.NewServer(db, *apiPort)
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
