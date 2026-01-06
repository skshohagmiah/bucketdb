package api

import (
	"bytes"
	"encoding/xml"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/skshohagmiah/bucketdb/pkg/core"
	"github.com/skshohagmiah/bucketdb/pkg/types"
)

func setupTestS3Server(t *testing.T) (*core.BucketDB, *Server, func()) {
	// Setup Temp Dirs
	tmpMetadata, _ := os.MkdirTemp("", "s3_meta_*")
	tmpData, _ := os.MkdirTemp("", "s3_data_*")

	config := types.DefaultConfig()
	config.MetadataPath = tmpMetadata
	config.StoragePath = tmpData
	config.Standalone = true
	config.Cluster.NodeID = "test-node"

	db, err := core.NewBucketDB(config)
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}

	server := NewServer(db, ":0") // random port

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpMetadata)
		os.RemoveAll(tmpData)
	}

	return db, server, cleanup
}

func TestS3ListBuckets(t *testing.T) {
	_, s, cleanup := setupTestS3Server(t)
	defer cleanup()

	// Create a dummy bucket via DB directly to seed state
	s.db.CreateBucket("test-bucket", "user")

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	// Mimic handleRoot for generic cli client
	req.Header.Set("Accept", "*/*")

	s.handleS3Request(w, req)

	resp := w.Result()
	if resp.StatusCode != 200 {
		t.Errorf("Expected 200, got %d", resp.StatusCode)
	}

	if resp.Header.Get("Content-Type") != "application/xml" {
		t.Errorf("Expected application/xml, got %s", resp.Header.Get("Content-Type"))
	}

	var result ListAllMyBucketsResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode XML: %v", err)
	}

	if len(result.Buckets) != 1 || result.Buckets[0].Name != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got %v", result.Buckets)
	}
}

func TestS3BucketLifecycle(t *testing.T) {
	db, s, cleanup := setupTestS3Server(t)
	defer cleanup()

	// 1. Create Bucket via S3 PUT
	req := httptest.NewRequest("PUT", "/new-bucket", nil)
	w := httptest.NewRecorder()
	s.handleS3Request(w, req)

	if w.Code != 200 {
		t.Errorf("PUT /new-bucket failed: %d", w.Code)
	}

	// Verify internal state
	_, err := db.Metadata.GetBucket("new-bucket")
	if err != nil {
		t.Errorf("Bucket was not created internally")
	}

	// 2. Put Object
	body := []byte("s3-data")
	req = httptest.NewRequest("PUT", "/new-bucket/file.txt", bytes.NewReader(body))
	w = httptest.NewRecorder()
	s.handleS3Request(w, req)

	if w.Code != 200 {
		t.Errorf("PUT object failed: %d", w.Code)
	}

	// 3. Get Object
	req = httptest.NewRequest("GET", "/new-bucket/file.txt", nil)
	w = httptest.NewRecorder()
	s.handleS3Request(w, req)

	if w.Code != 200 {
		t.Errorf("GET object failed: %d", w.Code)
	}
	if w.Body.String() != "s3-data" {
		t.Errorf("GET body mismatch")
	}

	// 4. List Objects (XML check)
	req = httptest.NewRequest("GET", "/new-bucket", nil)
	w = httptest.NewRecorder()
	s.handleS3Request(w, req)

	var listRes ListBucketResult
	xml.NewDecoder(w.Body).Decode(&listRes)

	if listRes.Name != "new-bucket" {
		t.Errorf("ListBucket name mismatch: %s", listRes.Name)
	}
	if len(listRes.Contents) != 1 || listRes.Contents[0].Key != "file.txt" {
		t.Errorf("ListBucket contents mismatch")
	}
}
