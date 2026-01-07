package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"

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
			CreationDate: formatS3Time(b.CreatedAt),
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
		// List Objects with S3 query parameters
		prefix := r.URL.Query().Get("prefix")
		delimiter := r.URL.Query().Get("delimiter")
		marker := r.URL.Query().Get("marker")
		maxKeysStr := r.URL.Query().Get("max-keys")

		maxKeys := 1000
		if maxKeysStr != "" {
			var parsed int64
			if _, err := fmt.Sscanf(maxKeysStr, "%d", &parsed); err == nil && parsed > 0 {
				maxKeys = int(parsed)
				if maxKeys > 1000 {
					maxKeys = 1000 // Cap at 1000
				}
			}
		}

		objects, err := s.db.ListObjects(bucket, prefix, maxKeys)
		if err != nil {
			s3Error(w, "NoSuchBucket", "The specified bucket does not exist", bucket, http.StatusNotFound)
			return
		}

		resp := ListBucketResult{
			Name:        bucket,
			Prefix:      prefix,
			Marker:      marker,
			MaxKeys:     maxKeys,
			Delimiter:   delimiter,
			IsTruncated: objects.IsTruncated,
		}

		// Filter by marker (start after marker)
		startIndex := 0
		if marker != "" {
			for i, o := range objects.Objects {
				if o.Key > marker {
					startIndex = i
					break
				}
			}
		}

		// Process objects with delimiter support
		seenPrefixes := make(map[string]bool)
		for i := startIndex; i < len(objects.Objects) && len(resp.Contents)+len(resp.CommonPrefixes) < maxKeys; i++ {
			o := objects.Objects[i]

			// Handle delimiter (folder-like structure)
			if delimiter != "" {
				keyAfterPrefix := strings.TrimPrefix(o.Key, prefix)
				delimiterIndex := strings.Index(keyAfterPrefix, delimiter)
				if delimiterIndex >= 0 {
					// This is a "folder"
					commonPrefix := prefix + keyAfterPrefix[:delimiterIndex+len(delimiter)]
					if !seenPrefixes[commonPrefix] {
						resp.CommonPrefixes = append(resp.CommonPrefixes, CommonPrefix{
							Prefix: commonPrefix,
						})
						seenPrefixes[commonPrefix] = true
					}
					continue
				}
			}

			// Regular object
			resp.Contents = append(resp.Contents, ObjectEntry{
				Key:          o.Key,
				LastModified: formatS3Time(o.CreatedAt),
				ETag:         fmt.Sprintf("\"%s\"", o.Checksum),
				Size:         o.Size,
				Owner:        Owner{ID: "bucketdb", DisplayName: "bucketdb"},
				StorageClass: "STANDARD",
			})
		}

		// Set truncation marker
		if len(resp.Contents) > 0 {
			resp.NextMarker = resp.Contents[len(resp.Contents)-1].Key
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

		// Get the actual checksum from stored object
		obj, err := s.db.GetObjectMetadata(bucket, key)
		if err == nil && obj != nil {
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", obj.Checksum))
		} else {
			// Fallback: calculate checksum from data
			hash := sha256.Sum256(data)
			checksum := hex.EncodeToString(hash[:])
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", checksum))
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		// Map to GetObject
		// Parse Range header if present
		var opts *types.GetObjectOptions
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			opts = parseS3RangeHeader(rangeHeader)
		}

		// Get metadata first
		meta, err := s.db.GetObjectMetadata(bucket, key)
		if err != nil {
			s3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
			return
		}

		// Use streaming if object is large (larger than chunk size) and not a range request
		if meta.Size > s.db.Config.ChunkSize && (opts == nil || (opts.RangeStart == 0 && opts.RangeEnd == 0)) {
			reader, obj, err := s.db.GetObjectAsStream(bucket, key, opts)
			if err != nil {
				s3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
				return
			}
			defer reader.Close()

			w.Header().Set("Content-Type", obj.ContentType)
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", obj.Checksum))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
			w.Header().Set("Accept-Ranges", "bytes")
			io.Copy(w, reader)
		} else {
			// For small files or range requests, use in-memory method
			res, err := s.db.GetObject(bucket, key, opts)
			if err != nil {
				s3Error(w, "NoSuchKey", "The specified key does not exist.", key, http.StatusNotFound)
				return
			}

			if meta != nil {
				w.Header().Set("Content-Type", meta.ContentType)
				w.Header().Set("ETag", fmt.Sprintf("\"%s\"", meta.Checksum))
			} else {
				w.Header().Set("Content-Type", http.DetectContentType(res))
			}

			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(res)))
			w.Header().Set("Accept-Ranges", "bytes")

			if opts != nil && (opts.RangeStart > 0 || opts.RangeEnd > 0) {
				if meta != nil {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", opts.RangeStart, opts.RangeEnd-1, meta.Size))
				}
				w.WriteHeader(http.StatusPartialContent)
			}

			w.Write(res)
		}

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

// parseS3RangeHeader parses HTTP Range header for S3 requests
// Supports formats: "bytes=0-1023", "bytes=1024-", "bytes=-512"
func parseS3RangeHeader(rangeHeader string) *types.GetObjectOptions {
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
		_, err = fmt.Sscanf(parts[0], "%d", &start)
		if err != nil {
			return nil
		}
	}

	if parts[1] != "" {
		_, err = fmt.Sscanf(parts[1], "%d", &end)
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
