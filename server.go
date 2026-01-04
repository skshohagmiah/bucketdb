package bucketdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

// Server is the HTTP server for BucketDB
type Server struct {
	db   *BucketDB
	port string
}

// NewServer creates a new HTTP server
func NewServer(db *BucketDB, port string) *Server {
	return &Server{
		db:   db,
		port: port,
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// Public API
	mux.HandleFunc("/objects/", s.handleObject)
	mux.HandleFunc("/buckets", s.handleBuckets)

	// Cluster Information
	mux.HandleFunc("/cluster", s.handleCluster)

	// Internal API (for replication and migration)
	mux.HandleFunc("/internal/chunk/", s.handleInternalChunk)
	mux.HandleFunc("/internal/partition/", s.handleInternalPartition)

	log.Printf("[Server] HTTP server starting on %s", s.port)
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
	partition, err := s.db.cluster.GetPartition(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !s.db.cluster.IsPrimary(partition) && !s.db.cluster.IsReplica(partition) {
		// Forward to primary
		primary := s.db.cluster.GetPrimary(partition)
		log.Printf("[Server] Forwarding %s request for %s/%s to node %s (%s)", r.Method, bucket, key, primary.ID, primary.IP)

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
		data, err := s.db.GetObject(bucket, key, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		// Get metadata for Content-Type
		meta, _ := s.db.GetObjectMetadata(bucket, key)
		if meta != nil {
			w.Header().Set("Content-Type", meta.ContentType)
		}
		w.Write(data)

	case http.MethodPost:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		opts := &PutObjectOptions{
			ContentType: r.Header.Get("Content-Type"),
			Metadata:    make(map[string]string),
		}

		if err := s.db.PutObject(bucket, key, data, opts); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
	info := s.db.cluster.GetCluster()
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
	url := fmt.Sprintf("http://%s%s", nodeAddr, r.URL.Path)

	var bodyReader io.Reader
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		bodyReader = bytes.NewReader(body)
	}

	req, _ := http.NewRequest(r.Method, url, bodyReader)
	req.Header = r.Header

	client := &http.Client{}
	return client.Do(req)
}
