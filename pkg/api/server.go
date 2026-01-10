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
	mux.Handle("/", s.AuthMiddleware(s.instrumentedHandler(s.handleRoot, "/")))
	mux.Handle("/objects", s.AuthMiddleware(s.instrumentedHandler(s.handleListObjects, "/objects")))
	mux.Handle("/objects/", s.AuthMiddleware(s.instrumentedHandler(s.handleObject, "/objects/:bucket/:key")))
	mux.Handle("/buckets", s.AuthMiddleware(s.instrumentedHandler(s.handleBuckets, "/buckets")))

	// Cluster Information
	mux.Handle("/cluster", s.AuthMiddleware(s.instrumentedHandler(s.handleCluster, "/cluster")))

	// Internal API (for replication and migration)
	mux.Handle("/internal/chunk/", s.AuthMiddleware(s.instrumentedHandler(s.handleInternalChunk, "/internal/chunk/:id")))
	mux.Handle("/internal/partition/", s.AuthMiddleware(s.instrumentedHandler(s.handleInternalPartition, "/internal/partition/:id")))
	mux.Handle("/internal/replicate/", s.AuthMiddleware(s.instrumentedHandler(s.handleInternalReplicate, "/internal/replicate/:bucket/:key")))

	// Multipart Upload API
	mux.Handle("/multipart/", s.AuthMiddleware(s.instrumentedHandler(s.handleMultipart, "/multipart/:bucket/:key")))

	// Object Tagging API
	// Object Tagging API (Handled within handleObject)
	// mux.Handle("/objects/", s.instrumentedHandler(s.handleObjectTagging, "/objects/:bucket/:key/tags"))

	// Lifecycle Policy API
	mux.Handle("/lifecycle/", s.AuthMiddleware(s.instrumentedHandler(s.handleLifecycle, "/lifecycle/:bucket")))

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
	// Check for object tagging requests
	if strings.HasSuffix(r.URL.Path, "/tags") {
		s.handleObjectTagging(w, r)
		return
	}

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
	// Delegate everything to S3 Gateway
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
