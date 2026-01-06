package main

import (
	"fmt"
	"log"
	"os"

	"github.com/skshohagmiah/bucketdb"
)

func main() {
	// Create a simple configuration for a single local node
	config := bucketdb.DefaultConfig()
	config.StoragePath = "./example_storage/chunks"
	config.MetadataPath = "./example_storage/metadata"
	config.Cluster.NodeID = "example-node"
	config.Cluster.HTTPAddr = ":8080" // Coordination port

	// Ensure directories exist
	os.MkdirAll(config.StoragePath, 0755)
	os.MkdirAll(config.MetadataPath, 0755)

	// Initialize BucketDB
	db, err := bucketdb.NewBucketDB(config)
	if err != nil {
		log.Fatalf("Failed to initialize BucketDB: %v", err)
	}
	defer db.Close()

	// 1. Create a bucket
	fmt.Println("📦 Creating bucket: 'docs'...")
	if err := db.CreateBucket("docs", "admin"); err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}

	// 2. Put an object
	fmt.Println("📤 Uploading object: 'hello.txt'...")
	data := []byte("Hello, BucketDB! This is a simple example.")
	opts := &bucketdb.PutObjectOptions{
		ContentType: "text/plain",
		Metadata: map[string]string{
			"version": "1.0",
		},
	}
	if err := db.PutObject("docs", "hello.txt", data, opts); err != nil {
		log.Fatalf("Failed to put object: %v", err)
	}

	// 3. Get the object back
	fmt.Println("📥 Retrieving object: 'hello.txt'...")
	retrieved, err := db.GetObject("docs", "hello.txt", nil)
	if err != nil {
		log.Fatalf("Failed to get object: %v", err)
	}
	fmt.Printf("✅ Content: %s\n", string(retrieved))

	// 4. List objects
	fmt.Println("🔍 Listing objects in 'docs'...")
	list, err := db.ListObjects("docs", "", 10)
	if err != nil {
		log.Fatalf("Failed to list objects: %v", err)
	}
	for _, obj := range list.Objects {
		fmt.Printf(" - %s (%d bytes)\n", obj.Key, obj.Size)
	}

	fmt.Println("\n🎉 Example completed successfully!")
}
