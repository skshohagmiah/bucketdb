package api

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/skshohagmiah/bucketdb/pkg/types"
)

// handleS3Request routes S3-style requests to BucketDB operations
// It intelligently dispatches based on path depth to simulate S3 hierarchy
func (s *Server) handleS3Request(w http.ResponseWriter, r *http.Request) {
	// 1. Parse Path
	// S3 Path: /bucket/key or /bucket
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	// Handle Root (List Buckets)
	if path == "" {
		s.handleS3ListBuckets(w, r)
		return
	}

	// Handle Bucket Operations (List Objects, Create Bucket)
	if len(parts) == 1 {
		s.handleS3BucketOps(w, r, parts[0])
		return
	}

	// Handle Object Operations (Get, Put, Delete)
	bucket := parts[0]
	key := strings.Join(parts[1:], "/")
	s.handleS3Object(w, r, bucket, key)
}

// GET /
func (s *Server) handleS3ListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.db.ListBuckets()
	if err != nil {
		s3Error(w, "InternalError", err.Error(), "", http.StatusInternalServerError)
		return
	}

	resp := ListAllMyBucketsResult{
		Owner: Owner{ID: "bucketdb", DisplayName: "bucketdb"},
	}

	for _, b := range buckets {
		resp.Buckets = append(resp.Buckets, BucketEntry{
			Name:         b.Name,
			CreationDate: formatS3Time(time.Now()), // TODO: Store creation time in metadata
		})
	}

	w.Header().Set("Content-Type", "application/xml")
	writeXML(w, resp)
}

// GET /bucket (List), PUT /bucket (Create)
func (s *Server) handleS3BucketOps(w http.ResponseWriter, r *http.Request, bucket string) {
	if r.Method == http.MethodPut {
		// Create Bucket
		if err := s.db.CreateBucket(bucket, "s3-user"); err != nil {
			if strings.Contains(err.Error(), "exists") {
				s3Error(w, "BucketAlreadyExists", "Bucket already exists", bucket, http.StatusConflict)
			} else {
				s3Error(w, "InternalError", err.Error(), bucket, http.StatusInternalServerError)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == http.MethodGet {
		// List Objects
		// TODO: Handle prefix, delimiter, marker from URL query
		objects, err := s.db.ListObjects(bucket, "", 1000)
		if err != nil {
			s3Error(w, "NoSuchBucket", "The specified bucket does not exist", bucket, http.StatusNotFound)
			return
		}

		resp := ListBucketResult{
			Name:        bucket,
			MaxKeys:     1000,
			IsTruncated: false,
		}

		// Fix: Iterate over Objects field
		for _, o := range objects.Objects {
			resp.Contents = append(resp.Contents, ObjectEntry{
				Key:          o.Key,
				LastModified: formatS3Time(time.Now()),
				ETag:         fmt.Sprintf("\"%s\"", o.Checksum),
				Size:         o.Size,
				Owner:        Owner{ID: "bucketdb", DisplayName: "bucketdb"},
				StorageClass: "STANDARD",
			})
		}

		w.Header().Set("Content-Type", "application/xml")
		writeXML(w, resp)
		return
	}

	s3Error(w, "MethodNotAllowed", "Method not allowed", bucket, http.StatusMethodNotAllowed)
}

// GET/PUT/DELETE /bucket/key
func (s *Server) handleS3Object(w http.ResponseWriter, r *http.Request, bucket, key string) {
	// 1. Cluster Coordination (Forwarding)
	// We need to route based on Object Key
	if !s.db.Config.Standalone {
		partition, err := s.db.Cluster.GetPartition(key)
		if err != nil {
			s3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
			return
		}

		if !s.db.Cluster.IsPrimary(partition) && !s.db.Cluster.IsReplica(partition) {
			primary := s.db.Cluster.GetPrimary(partition)
			// Forwarding
			resp, err := s.ForwardRequest(primary.IP, r)
			if err != nil {
				s3Error(w, "ServiceUnavailable", "Forwarding failed: "+err.Error(), key, http.StatusServiceUnavailable)
				return
			}
			defer resp.Body.Close()

			// Propagate Headers & Body
			for k, v := range resp.Header {
				for _, val := range v {
					w.Header().Add(k, val)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		}
	}

	// 2. Local Processing
	switch r.Method {
	case http.MethodPut:
		// Map to PutObject
		data, err := io.ReadAll(r.Body)
		if err != nil {
			s3Error(w, "InternalError", "Read failed", key, http.StatusInternalServerError)
			return
		}

		opts := &types.PutObjectOptions{
			ContentType: r.Header.Get("Content-Type"),
			// S3 Metadata headers could be parsed here
		}

		if err := s.db.PutObject(bucket, key, data, opts); err != nil {
			s3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
			return
		}

		w.Header().Set("ETag", "\"checksum-placeholder\"")
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		// Map to GetObject
		// Parse Range header if present
		var opts *types.GetObjectOptions
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			// Basic parsing: bytes=0-1024
			var start, end int64
			fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
			if end > 0 {
				opts = &types.GetObjectOptions{RangeStart: start, RangeEnd: end + 1}
			}
		}

		res, err := s.db.GetObject(bucket, key, opts)
		if err != nil {
			s3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", http.DetectContentType(res)) // Improved detection needed
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(res)))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Write(res)

	case http.MethodDelete:
		if err := s.db.DeleteObject(bucket, key); err != nil {
			s3Error(w, "InternalError", err.Error(), key, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		s3Error(w, "MethodNotAllowed", "Method not allowed", key, http.StatusMethodNotAllowed)
	}
}

func s3Error(w http.ResponseWriter, code, message, resource string, status int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	writeXML(w, Error{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: "req-id",
	})
}
