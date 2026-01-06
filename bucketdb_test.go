package bucketdb

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// setupTestDB helper to create a temporary DB instance for testing
func setupTestDB(t *testing.T) (*BucketDB, func()) {
	// Create temp directories
	tmpDir, err := os.MkdirTemp("", "bucketdb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	config := DefaultConfig()
	config.StoragePath = filepath.Join(tmpDir, "chunks")
	config.MetadataPath = filepath.Join(tmpDir, "metadata")
	config.Cluster.NodeID = "test-node"
	config.Cluster.HTTPAddr = ":0"  // Random port for test
	config.Cluster.Bootstrap = true // Must bootstrap for single-node test
	// Disable health check to avoid noise/errors in simple tests if possible
	config.Cluster.HealthCheck.Enabled = false

	// Create necessary directories
	os.MkdirAll(config.StoragePath, 0755)
	os.MkdirAll(config.MetadataPath, 0755)

	db, err := NewBucketDB(config)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create BucketDB: %v", err)
	}

	// Wait for cluster to be ready (Raft leader election + partition assignment)
	ready := false
	for i := 0; i < 100; i++ {
		// Try to resolve a partition
		_, err := db.cluster.GetPartition("ping")
		if err == nil {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !ready {
		db.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Cluster failed to become ready (no partitions assigned)")
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

func TestBucketLifecycle(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bucketName := "test-bucket"

	// 1. Create Bucket
	err := db.CreateBucket(bucketName, "tester")
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// 2. Verify bucket exists
	buckets, err := db.ListBuckets()
	if err != nil {
		t.Fatalf("Failed to list buckets: %v", err)
	}

	found := false
	for _, b := range buckets {
		if b.Name == bucketName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Bucket %s not found in list", bucketName)
	}

	// 3. Delete Bucket
	err = db.DeleteBucket(bucketName)
	if err != nil {
		t.Errorf("Failed to delete bucket: %v", err)
	}
}

func TestObjectOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bucketName := "data-bucket"
	if err := db.CreateBucket(bucketName, "tester"); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Test case 1: Small object (single chunk)
	key := "small-file.txt"
	content := []byte("Hello BucketDB")
	opts := &PutObjectOptions{ContentType: "text/plain"}

	if err := db.PutObject(bucketName, key, content, opts); err != nil {
		t.Fatalf("Failed to put small object: %v", err)
	}

	// Read back
	readData, err := db.GetObject(bucketName, key, nil)
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}

	if !bytes.Equal(readData, content) {
		t.Errorf("Content mismatch. Got %s, want %s", readData, content)
	}

	// Test case 2: Larger object (multiple chunks)
	// Force small chunk size for testing without massive files
	db.config.ChunkSize = 1024 // 1KB chunks

	largeContent := make([]byte, 5000) // Should be ~5 chunks
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	largeKey := "large-file.bin"
	if err := db.PutObject(bucketName, largeKey, largeContent, nil); err != nil {
		t.Fatalf("Failed to put large object: %v", err)
	}

	readLarge, err := db.GetObject(bucketName, largeKey, nil)
	if err != nil {
		t.Fatalf("Failed to get large object: %v", err)
	}

	if !bytes.Equal(readLarge, largeContent) {
		t.Errorf("Large content mismatch")
	}

	// Verify chunks created
	objMeta, err := db.GetObjectMetadata(bucketName, largeKey)
	if err != nil {
		t.Fatal(err)
	}

	if len(objMeta.ChunkIDs) < 5 {
		t.Errorf("Expected multiple chunks, got %d", len(objMeta.ChunkIDs))
	}
}

func TestRangeRequest(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	bucketName := "range-bucket"
	db.CreateBucket(bucketName, "tester")

	data := []byte("0123456789")
	if err := db.PutObject(bucketName, "digits", data, nil); err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Range: First 5 bytes
	opts := &GetObjectOptions{RangeStart: 0, RangeEnd: 5}
	part, err := db.GetObject(bucketName, "digits", opts)
	if err != nil {
		t.Fatalf("Range request failed: %v", err)
	}
	if string(part) != "01234" {
		t.Errorf("Expected 01234, got %s", string(part))
	}

	// Range: Middle
	opts = &GetObjectOptions{RangeStart: 3, RangeEnd: 7}
	part, err = db.GetObject(bucketName, "digits", opts)
	if err != nil {
		t.Fatalf("Range request failed: %v", err)
	}
	if string(part) != "3456" {
		t.Errorf("Expected 3456, got %s", string(part))
	}
}
