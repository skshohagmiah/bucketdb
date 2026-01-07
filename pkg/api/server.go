package api

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/skshohagmiah/bucketdb/pkg/core"
	"github.com/skshohagmiah/bucketdb/pkg/metrics"
	"github.com/skshohagmiah/bucketdb/pkg/types"
)

// Server is the HTTP server for BucketDB
type Server struct {
	db     *core.BucketDB
	port   string
	client *http.Client
}

// NewServer creates a new HTTP server
func NewServer(db *core.BucketDB, port string) *Server {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: db.Config.TLS.InsecureSkipVerify,
		},
	}

	if db.Config.TLS.Enabled && db.Config.TLS.CAFile != "" {
		caCert, err := os.ReadFile(db.Config.TLS.CAFile)
		if err == nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tr.TLSClientConfig.RootCAs = caCertPool
		} else {
			slog.Error("Failed to read CA file", "error", err)
		}
	}

	return &Server{
		db:     db,
		port:   port,
		client: &http.Client{Transport: tr},
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Public API & UI
	mux.Handle("/", s.instrumentedHandler(s.handleRoot, "/"))
	mux.Handle("/objects", s.instrumentedHandler(s.handleListObjects, "/objects"))
	mux.Handle("/objects/", s.instrumentedHandler(s.handleObject, "/objects/:bucket/:key"))
	mux.Handle("/buckets", s.instrumentedHandler(s.handleBuckets, "/buckets"))

	// Cluster Information
	mux.Handle("/cluster", s.instrumentedHandler(s.handleCluster, "/cluster"))

	// Internal API (for replication and migration)
	mux.Handle("/internal/chunk/", s.instrumentedHandler(s.handleInternalChunk, "/internal/chunk/:id"))
	mux.Handle("/internal/partition/", s.instrumentedHandler(s.handleInternalPartition, "/internal/partition/:id"))
	mux.Handle("/internal/replicate/", s.instrumentedHandler(s.handleInternalReplicate, "/internal/replicate/:bucket/:key"))

	// Multipart Upload API
	mux.Handle("/multipart/", s.instrumentedHandler(s.handleMultipart, "/multipart/:bucket/:key"))

	// Object Tagging API
	mux.Handle("/objects/", s.instrumentedHandler(s.handleObjectTagging, "/objects/:bucket/:key/tags"))

	// Lifecycle Policy API
	mux.Handle("/lifecycle/", s.instrumentedHandler(s.handleLifecycle, "/lifecycle/:bucket"))

	// Observability
	mux.Handle("/metrics", promhttp.Handler())

	// Health checks
	mux.Handle("/health", s.instrumentedHandler(s.handleHealth, "/health"))
	mux.Handle("/health/ready", s.instrumentedHandler(s.handleHealthReady, "/health/ready"))
	mux.Handle("/health/live", s.instrumentedHandler(s.handleHealthLive, "/health/live"))

	slog.Info("Server starting", "port", s.port, "tls", s.db.Config.TLS.Enabled)

	if s.db.Config.TLS.Enabled {
		return http.ListenAndServeTLS(s.port, s.db.Config.TLS.CertFile, s.db.Config.TLS.KeyFile, mux)
	}
	return http.ListenAndServe(s.port, mux)
}

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request) {
	// Expected path format: /objects/:bucket/:key
	path := r.URL.Path[len("/objects/"):]
	parts := bytes.Split([]byte(path), []byte("/"))
	if len(parts) < 2 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	bucket := string(parts[0])
	key := string(bytes.Join(parts[1:], []byte("/")))

	// Determine partition and primary node
	partition, err := s.db.Cluster.GetPartition(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !s.db.Cluster.IsPrimary(partition) && !s.db.Cluster.IsReplica(partition) {
		// Forward to primary
		primary := s.db.Cluster.GetPrimary(partition)
		slog.Debug("Forwarding request", "method", r.Method, "bucket", bucket, "key", key, "primary", primary.ID)

		resp, err := s.ForwardRequest(primary.IP, r)
		if err != nil {
			http.Error(w, "failed to forward request: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Copy response headers and body
		for k, v := range resp.Header {
			for _, val := range v {
				w.Header().Add(k, val)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Parse Range header if present
		var opts *types.GetObjectOptions
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			opts = parseRangeHeader(rangeHeader)
		}

		// Use streaming for large files
		meta, err := s.db.GetObjectMetadata(bucket, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		// Use streaming if object is large (larger than chunk size)
		if meta.Size > s.db.Config.ChunkSize && (opts == nil || (opts.RangeStart == 0 && opts.RangeEnd == 0)) {
			reader, obj, err := s.db.GetObjectAsStream(bucket, key, opts)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			defer reader.Close()

			w.Header().Set("Content-Type", obj.ContentType)
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", obj.Checksum))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
			io.Copy(w, reader)
		} else {
			// For small files or range requests, use in-memory method
			data, err := s.db.GetObject(bucket, key, opts)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}

			w.Header().Set("Content-Type", meta.ContentType)
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", meta.Checksum))
			if opts != nil && (opts.RangeStart > 0 || opts.RangeEnd > 0) {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", opts.RangeStart, opts.RangeEnd-1, meta.Size))
				w.WriteHeader(http.StatusPartialContent)
			}
			w.Write(data)
		}

	case http.MethodPost:
		opts := &types.PutObjectOptions{
			ContentType: r.Header.Get("Content-Type"),
			Metadata:    make(map[string]string),
		}

		// Use streaming for large files if Content-Length is available
		contentLength := r.ContentLength
		if contentLength > 0 && contentLength > s.db.Config.ChunkSize {
			// Use streaming for files larger than chunk size
			err := s.db.PutObjectFromStream(bucket, key, r.Body, contentLength, opts)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		} else {
			// For small files or unknown size, use in-memory method
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "failed to read body", http.StatusBadRequest)
				return
			}
			if err := s.db.PutObject(bucket, key, data, opts); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// Get the actual checksum from stored object for ETag
		obj, err := s.db.GetObjectMetadata(bucket, key)
		if err == nil && obj != nil {
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", obj.Checksum))
		}
		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		if err := s.db.DeleteObject(bucket, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		http.Error(w, "missing bucket parameter", http.StatusBadRequest)
		return
	}

	// For a truly distributed list, we should ideally coordinate,
	// but since metadata is replicated, any node in the partition group can return it.
	// For simplicity in this demo, the current node will return what it knows.
	res, err := s.db.ListObjects(bucket, "", 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Sort by CreatedAt descending for "latest"
	sort.Slice(res.Objects, func(i, j int) bool {
		return res.Objects[i].CreatedAt.After(res.Objects[j].CreatedAt)
	})

	// Limit to 10
	if len(res.Objects) > 10 {
		res.Objects = res.Objects[:10]
	}

	json.NewEncoder(w).Encode(res.Objects)
}

func (s *Server) handleBuckets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	buckets, err := s.db.ListBuckets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(buckets)
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	// Get cluster info from ClusterKit
	info := s.db.Cluster.GetCluster()
	json.NewEncoder(w).Encode(info)
}

func (s *Server) handleInternalChunk(w http.ResponseWriter, r *http.Request) {
	chunkID := r.URL.Path[len("/internal/chunk/"):]
	data, err := s.db.GetChunkData(chunkID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Write(data)
}

func (s *Server) handleInternalPartition(w http.ResponseWriter, r *http.Request) {
	partitionID := r.URL.Path[len("/internal/partition/"):]
	objects, err := s.db.GetObjectsInPartition(partitionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(objects)
}

// ForwardRequest forwards a request to another node
func (s *Server) ForwardRequest(nodeAddr string, r *http.Request) (*http.Response, error) {
	protocol := "http"
	if s.db.Config.TLS.Enabled {
		protocol = "https"
	}
	url := fmt.Sprintf("%s://%s%s", protocol, nodeAddr, r.URL.Path)

	var bodyReader io.Reader
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		bodyReader = bytes.NewReader(body)
	}

	req, _ := http.NewRequest(r.Method, url, bodyReader)
	req.Header = r.Header

	return s.client.Do(req)
}

// handleInternalReplicate handles incoming replication requests from other nodes
func (s *Server) handleInternalReplicate(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/internal/replicate/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	bucket := parts[0]
	key := strings.Join(parts[1:], "/")

	switch r.Method {
	case http.MethodPost:
		// Replicate PUT operation
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusInternalServerError)
			return
		}

		opts := &types.PutObjectOptions{
			ContentType: r.Header.Get("Content-Type"),
			Metadata:    make(map[string]string),
		}

		// Extract metadata from header
		if metaHeader := r.Header.Get("X-BucketDB-Meta"); metaHeader != "" {
			json.Unmarshal([]byte(metaHeader), &opts.Metadata)
		}

		// Store locally using internal storage logic
		partition, err := s.db.Cluster.GetPartition(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = s.db.StoreObjectLocally(bucket, key, partition.ID, data, opts)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		// Replicate DELETE operation
		err := s.db.DeleteObject(bucket, key)
		if err != nil {
			// If object doesn't exist, consider it already deleted (idempotent)
			if strings.Contains(err.Error(), "not found") {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	// Hybrid Handler: Dashboard for Browser, S3 for Clients

	// 1. serve Dashboard for root HTML requests
	if r.URL.Path == "/" && strings.Contains(r.Header.Get("Accept"), "text/html") {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(uiHTML))
		return
	}

	// 2. Delegate everything else to S3 Gateway
	// This captures /bucket/key requests which match the "/" catch-all pattern
	s.handleS3Request(w, r)
}

// instrumentedHandler wraps a handler with Prometheus metrics and request logging
func (s *Server) instrumentedHandler(handler http.HandlerFunc, path string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Generate request ID
		requestID := generateRequestID()
		r.Header.Set("X-Request-ID", requestID)
		w.Header().Set("X-Request-ID", requestID)

		// Use a response writer that captures the status code
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		// Log request
		slog.Info("Request started",
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"remote_addr", r.RemoteAddr,
		)

		handler.ServeHTTP(wrapped, r)

		duration := time.Since(start).Seconds()
		statusStr := fmt.Sprintf("%d", wrapped.status)

		// Log response
		slog.Info("Request completed",
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"duration_ms", duration*1000,
		)

		metrics.HttpRequestsTotal.WithLabelValues(r.Method, path, statusStr).Inc()
		metrics.HttpRequestDuration.WithLabelValues(r.Method, path).Observe(duration)
	})
}

// generateRequestID generates a unique request ID
func generateRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// parseRangeHeader parses HTTP Range header and returns GetObjectOptions
// Supports formats: "bytes=0-1023", "bytes=1024-", "bytes=-512"
func parseRangeHeader(rangeHeader string) *types.GetObjectOptions {
	if rangeHeader == "" {
		return nil
	}

	// Remove "bytes=" prefix
	rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
	if rangeStr == rangeHeader {
		return nil // Invalid format
	}

	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return nil
	}

	var start, end int64
	var err error

	if parts[0] != "" {
		start, err = parseInt64(parts[0])
		if err != nil {
			return nil
		}
	}

	if parts[1] != "" {
		end, err = parseInt64(parts[1])
		if err != nil {
			return nil
		}
		// Range end is inclusive in HTTP, but our code uses exclusive
		end = end + 1
	} else {
		// Open-ended range: "bytes=1024-"
		end = 0 // Will be set to object size
	}

	return &types.GetObjectOptions{
		RangeStart: start,
		RangeEnd:   end,
	}
}

// parseInt64 is a helper to safely parse int64
func parseInt64(s string) (int64, error) {
	var val int64
	_, err := fmt.Sscanf(s, "%d", &val)
	return val, err
}

// Health check handlers

// handleHealth returns detailed health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"version":   "1.0.0",
	}

	// Check cluster status
	if s.db.Cluster != nil {
		cluster := s.db.Cluster.GetCluster()
		health["cluster"] = map[string]interface{}{
			"nodes": len(cluster.Nodes),
		}
		if cluster.Nodes != nil {
			health["cluster"].(map[string]interface{})["node_count"] = len(cluster.Nodes)
		}
	}

	// Check storage stats
	stats, err := s.db.GetStats()
	if err == nil {
		health["storage"] = map[string]interface{}{
			"total_objects": stats.TotalObjects,
			"total_size":    stats.TotalSize,
			"disk_used_pct": stats.DiskUsedPercent,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// handleHealthReady checks if the service is ready to accept traffic
func (s *Server) handleHealthReady(w http.ResponseWriter, r *http.Request) {
	// Check if cluster is ready
	if s.db.Cluster != nil {
		cluster := s.db.Cluster.GetCluster()
		if len(cluster.Nodes) == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"not ready","reason":"no cluster nodes"}`))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ready"}`))
}

// handleHealthLive checks if the service is alive
func (s *Server) handleHealthLive(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"alive"}`))
}

// ===== MULTIPART UPLOAD HANDLERS =====

// handleMultipart handles multipart upload operations
func (s *Server) handleMultipart(w http.ResponseWriter, r *http.Request) {
	// Parse path: /multipart/:bucket/:key
	path := r.URL.Path[len("/multipart/"):]
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	bucket := parts[0]
	key := strings.Join(parts[1:], "/")

	uploadID := r.URL.Query().Get("uploadId")

	switch r.Method {
	case http.MethodPost:
		// Initiate multipart upload
		if uploadID == "" {
			opts := &types.PutObjectOptions{
				ContentType: r.Header.Get("Content-Type"),
			}
			uploadID, err := s.db.InitiateMultipartUpload(bucket, key, opts)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"uploadId": uploadID})
			return
		}

		// Upload part
		partNumberStr := r.URL.Query().Get("partNumber")
		if partNumberStr == "" {
			http.Error(w, "partNumber required", http.StatusBadRequest)
			return
		}
		var partNumber int
		if _, err := fmt.Sscanf(partNumberStr, "%d", &partNumber); err != nil {
			http.Error(w, "invalid partNumber", http.StatusBadRequest)
			return
		}

		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		etag, err := s.db.UploadPart(bucket, key, uploadID, partNumber, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
		w.WriteHeader(http.StatusOK)

	case http.MethodPut:
		// Complete multipart upload
		if uploadID == "" {
			http.Error(w, "uploadId required", http.StatusBadRequest)
			return
		}

		var parts []types.MultipartPart
		if err := json.NewDecoder(r.Body).Decode(&parts); err != nil {
			http.Error(w, "invalid parts data", http.StatusBadRequest)
			return
		}

		obj, err := s.db.CompleteMultipartUpload(bucket, key, uploadID, parts)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", obj.Checksum))
		json.NewEncoder(w).Encode(map[string]interface{}{
			"bucket":   obj.Bucket,
			"key":      obj.Key,
			"etag":     obj.Checksum,
			"location": fmt.Sprintf("/objects/%s/%s", bucket, key),
		})

	case http.MethodDelete:
		// Abort multipart upload
		if uploadID == "" {
			http.Error(w, "uploadId required", http.StatusBadRequest)
			return
		}

		if err := s.db.AbortMultipartUpload(bucket, key, uploadID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)

	case http.MethodGet:
		// List multipart uploads
		prefix := r.URL.Query().Get("prefix")
		maxUploads := 1000
		if maxStr := r.URL.Query().Get("max-uploads"); maxStr != "" {
			if parsed, err := parseInt64(maxStr); err == nil && parsed > 0 {
				maxUploads = int(parsed)
			}
		}

		uploads, err := s.db.ListMultipartUploads(bucket, prefix, maxUploads)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(uploads)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ===== OBJECT TAGGING HANDLERS =====

// handleObjectTagging handles object tagging operations
func (s *Server) handleObjectTagging(w http.ResponseWriter, r *http.Request) {
	// Parse path: /objects/:bucket/:key/tags
	path := strings.TrimPrefix(r.URL.Path, "/objects/")
	path = strings.TrimSuffix(path, "/tags")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	bucket := parts[0]
	key := strings.Join(parts[1:], "/")

	switch r.Method {
	case http.MethodPut:
		// Put tags
		var tags map[string]string
		if err := json.NewDecoder(r.Body).Decode(&tags); err != nil {
			http.Error(w, "invalid tags data", http.StatusBadRequest)
			return
		}

		if err := s.db.PutObjectTagging(bucket, key, tags); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		// Get tags
		tags, err := s.db.GetObjectTagging(bucket, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tags)

	case http.MethodDelete:
		// Delete tags
		if err := s.db.DeleteObjectTagging(bucket, key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ===== LIFECYCLE POLICY HANDLERS =====

// handleLifecycle handles lifecycle policy operations
func (s *Server) handleLifecycle(w http.ResponseWriter, r *http.Request) {
	// Parse path: /lifecycle/:bucket
	path := strings.TrimPrefix(r.URL.Path, "/lifecycle/")
	bucket := path

	switch r.Method {
	case http.MethodPut:
		// Put lifecycle policy
		var policy types.LifecyclePolicy
		if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
			http.Error(w, "invalid policy data", http.StatusBadRequest)
			return
		}

		policy.Bucket = bucket
		if err := s.db.PutLifecyclePolicy(bucket, &policy); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		// Get lifecycle policy
		policy, err := s.db.GetLifecyclePolicy(bucket)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(policy)

	case http.MethodDelete:
		// Delete lifecycle policy
		if err := s.db.DeleteLifecyclePolicy(bucket); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

const uiHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BucketDB Cluster Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=JetBrains+Mono&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary: #818cf8;
            --primary-hover: #6366f1;
            --accent: #c084fc;
            --bg: #020617;
            --card: rgba(30, 41, 59, 0.7);
            --border: rgba(255, 255, 255, 0.1);
            --text-main: #f8fafc;
            --text-dim: #94a3b8;
            --success: #34d399;
            --error: #f87171;
        }

        * { box-sizing: border-box; transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1); }

        body {
            font-family: 'Outfit', sans-serif;
            background: radial-gradient(circle at top left, #1e1b4b, #020617 50%), 
                        radial-gradient(circle at bottom right, #312e81, #020617 50%);
            background-attachment: fixed;
            color: var(--text-main);
            margin: 0;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 3rem 1rem;
        }

        .glass {
            background: var(--card);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid var(--border);
            border-radius: 1.5rem;
        }

        .container {
            width: 100%;
            max-width: 1000px;
            display: grid;
            grid-template-columns: 1fr 1.5fr;
            gap: 2rem;
        }

        @media (max-width: 850px) {
            .container { grid-template-columns: 1fr; }
        }

        header {
            grid-column: 1 / -1;
            text-align: center;
            margin-bottom: 2rem;
        }

        h1 {
            font-size: 3.5rem;
            margin: 0;
            font-weight: 700;
            background: linear-gradient(135deg, #818cf8, #c084fc, #fb7185);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: -0.05em;
        }

        .tagline {
            color: var(--text-dim);
            font-size: 1.1rem;
            margin-top: 0.5rem;
        }

        .section-title {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 2rem;
            color: var(--primary);
        }

        .card { padding: 2rem; }

        .form-group { margin-bottom: 1.5rem; }
        label { display: block; margin-bottom: 0.5rem; color: var(--text-dim); font-size: 0.9rem; font-weight: 500; }
        
        input {
            width: 100%;
            padding: 1rem;
            background: rgba(15, 23, 42, 0.5);
            border: 1px solid var(--border);
            border-radius: 0.75rem;
            color: white;
            font-size: 1rem;
        }
        
        input:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 4px rgba(99, 102, 241, 0.1);
        }

        .file-upload {
            border: 2px dashed var(--border);
            border-radius: 1rem;
            padding: 2.5rem 1rem;
            text-align: center;
            cursor: pointer;
            position: relative;
        }

        .file-upload:hover { border-color: var(--primary); background: rgba(99, 102, 241, 0.05); }

        .upload-btn {
            width: 100%;
            padding: 1.25rem;
            background: linear-gradient(135deg, var(--primary), var(--accent));
            color: white;
            border: none;
            border-radius: 1rem;
            font-size: 1.1rem;
            font-weight: 700;
            cursor: pointer;
            margin-top: 2rem;
            box-shadow: 0 10px 25px -5px rgba(99, 102, 241, 0.4);
        }

        .upload-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 15px 30px -5px rgba(99, 102, 241, 0.5);
        }

        .upload-btn:active { transform: translateY(0); }

        .status-box {
            margin-top: 1.5rem;
            padding: 1rem;
            border-radius: 0.75rem;
            font-size: 0.95rem;
            display: none;
            animation: slideIn 0.3s ease;
        }

        @keyframes slideIn { from { opacity: 0; transform: translateY(10px); } }

        .status-success { background: rgba(52, 211, 153, 0.1); color: var(--success); border: 1px solid var(--success); display: block; }
        .status-error { background: rgba(248, 113, 113, 0.1); color: var(--error); border: 1px solid var(--error); display: block; }

        .list-header {
            display: grid;
            grid-template-columns: 2fr 1fr 1fr 0.8fr 1.2fr;
            padding: 1rem;
            color: var(--text-dim);
            font-size: 0.85rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .object-item {
            display: grid;
            grid-template-columns: 2fr 1fr 1fr 0.8fr 1.2fr;
            padding: 1.25rem 1rem;
            border-top: 1px solid var(--border);
            align-items: center;
        }

        .object-item:hover { background: rgba(255, 255, 255, 0.03); }

        .obj-name { font-weight: 600; color: var(--text-main); display: flex; align-items: center; gap: 0.75rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .obj-meta { color: var(--text-dim); font-size: 0.9rem; }
        .obj-size { font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: var(--accent); }
        .obj-actions { display: flex; gap: 0.5rem; justify-content: flex-end; }

        .action-btn {
            padding: 0.4rem 0.8rem;
            border-radius: 0.5rem;
            border: 1px solid var(--border);
            background: rgba(255, 255, 255, 0.05);
            color: var(--text-main);
            font-size: 0.8rem;
            cursor: pointer;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 0.3rem;
        }

        .action-btn:hover { background: rgba(255, 255, 255, 0.1); border-color: var(--primary); color: var(--primary); }
        .btn-primary { background: rgba(129, 140, 248, 0.15); border-color: rgba(129, 140, 248, 0.3); color: var(--primary); }
        .btn-primary:hover { background: rgba(129, 140, 248, 0.25); }

        pre {
            font-family: 'JetBrains Mono', monospace;
            background: #000;
            padding: 1.5rem;
            border-radius: 1rem;
            font-size: 0.85rem;
            overflow: auto;
            max-height: 400px;
            border: 1px solid var(--border);
        }

        .badge {
            padding: 0.25rem 0.6rem;
            background: rgba(129, 140, 248, 0.2);
            color: var(--primary);
            border-radius: 2rem;
            font-size: 0.75rem;
            font-weight: 700;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>BucketDB</h1>
            <p class="tagline">Scale your data with confidence across the cluster</p>
        </header>

        <div class="card glass">
            <div class="section-title">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
                Upload Asset
            </div>
            
            <div class="form-group">
                <label>Storage Bucket</label>
                <input type="text" id="bucket" value="my-bucket" placeholder="Enter bucket name" onchange="refresh()">
            </div>

            <div class="form-group">
                <label>Destination Path</label>
                <input type="text" id="key" value="assets/image-01.png" placeholder="e.g. static/hero.jpg">
            </div>

            <div class="file-upload" onclick="document.getElementById('file').click()">
                <div id="file-label" style="color: var(--text-dim)">Click or drag file to upload</div>
                <input type="file" id="file" hidden onchange="updateFileLabel()">
            </div>

            <button class="upload-btn" onclick="upload()">Push to Cluster</button>
            <div id="status" class="status-box"></div>
        </div>

        <div class="card glass">
            <div class="section-title">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="18" rx="2" ry="2"/><line x1="6" y1="9" x2="18" y2="9"/><line x1="6" y1="13" x2="18" y2="13"/><line x1="6" y1="17" x2="18" y2="17"/></svg>
                Cluster Status 
            </div>
            
            <pre id="cluster-info">Initializing coordination matrix...</pre>

            <div style="margin-top: 2rem">
                <div class="section-title" style="font-size: 1rem; justify-content: space-between;">
                    <span>Object Explorer</span>
                    <button class="action-btn" onclick="refresh()">
                         <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 4v6h-6"/><path d="M1 20v-6h6"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
                         Refresh
                    </button>
                </div>
                <div class="list-header">
                    <span>Key</span>
                    <span>Type</span>
                    <span>Part</span>
                    <span>Size</span>
                    <span style="text-align: right">Actions</span>
                </div>
                <div id="object-list">
                    <!-- Objects will be injected here -->
                </div>
            </div>
        </div>
    </div>

    <script>
        function updateFileLabel() {
            const input = document.getElementById('file');
            const label = document.getElementById('file-label');
            if (input.files.length > 0) {
                label.innerText = input.files[0].name;
                label.style.color = 'var(--primary)';
            }
        }

        async function upload() {
            const bucket = document.getElementById('bucket').value;
            const key = document.getElementById('key').value;
            const fileInput = document.getElementById('file');
            const status = document.getElementById('status');

            if (!fileInput.files[0]) {
                showStatus('Please select a file first', 'error');
                return;
            }

            const file = fileInput.files[0];
            showStatus('🚀 Syncing data chunks...', 'success');

            try {
                const response = await fetch('/objects/' + bucket + '/' + key, {
                    method: 'POST',
                    body: await file.arrayBuffer(),
                    headers: { 'Content-Type': file.type || 'application/octet-stream' }
                });

                if (response.ok) {
                    showStatus('✅ Asset distributed successfully across replicas!', 'success');
                    setTimeout(() => refresh(), 1500);
                } else {
                    const errorMsg = await response.text();
                    showStatus('❌ Cluster error: ' + errorMsg, 'error');
                }
            } catch (err) {
                showStatus('❌ Network error: ' + err.message, 'error');
            }
        }

        function showStatus(text, type) {
            const status = document.getElementById('status');
            status.innerText = text;
            status.className = 'status-box status-' + type;
        }

        async function refresh() {
            try {
                // Fetch Cluster Info
                const clusterResp = await fetch('/cluster');
                const clusterData = await clusterResp.json();
                document.getElementById('cluster-info').innerText = JSON.stringify(clusterData, null, 2);

                // Fetch Latest Objects
                const bucket = document.getElementById('bucket').value;
                const objectsResp = await fetch('/objects?bucket=' + bucket);
                const objects = await objectsResp.json();
                
                const objList = document.getElementById('object-list');
                if (!objects || objects.length === 0) {
                    objList.innerHTML = '<div style="padding: 2rem; text-align: center; color: var(--text-dim);">No objects found in ' + bucket + '</div>';
                    return;
                }

                objList.innerHTML = objects.map(obj => {
                    const url = '/objects/' + obj.bucket + '/' + obj.key;
                    // Truncate content type
                    let type = obj.content_type || 'application/octet-stream';
                    if (type.length > 20) type = type.substring(0, 17) + '...';

                    return '<div class="object-item">' +
                        '<div class="obj-name" title="' + obj.key + '">' +
                            '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color: var(--primary); min-width: 18px;"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/></svg>' +
                            obj.key +
                        '</div>' +
                        '<div class="obj-meta">' + type + '</div>' +
                        '<div class="obj-meta">P-' + (obj.partition_id || '?') + '</div>' +
                        '<div class="obj-size">' + formatBytes(obj.size) + '</div>' +
                        '<div class="obj-actions">' +
                            '<a href="' + url + '" target="_blank" class="action-btn btn-primary" title="Open / Download">' +
                                '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>' +
                                'Open' +
                            '</a>' +
                            '<button onclick="deleteObject(\'' + obj.bucket + '\', \'' + obj.key + '\')" class="action-btn" title="Delete">' +
                                '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>' +
                            '</button>' +
                        '</div>' +
                    '</div>';
                }).join('');
            } catch (err) {
                console.error('Refresh failed:', err);
            }
        }

        async function deleteObject(bucket, key) {
            if (!confirm('Are you sure you want to delete ' + key + '?')) return;
            try {
                const response = await fetch('/objects/' + bucket + '/' + key, { method: 'DELETE' });
                if (response.ok) {
                    showStatus('Deleted ' + key, 'success');
                    refresh();
                } else {
                    showStatus('Failed to delete: ' + await response.text(), 'error');
                }
            } catch (err) {
                showStatus('Network error: ' + err.message, 'error');
            }
        }

        function formatBytes(bytes) {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        refresh();
        setInterval(refresh, 5000);
    </script>
</body>
</html>
`
