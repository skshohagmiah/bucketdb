package api

import (
	"encoding/xml"
	"io"
	"time"
)

// S3 Error Response
type Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// ListAllMyBucketsResult (GET Service)
type ListAllMyBucketsResult struct {
	XMLName xml.Name      `xml:"ListAllMyBucketsResult"`
	Owner   Owner         `xml:"Owner"`
	Buckets []BucketEntry `xml:"Buckets>Bucket"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type BucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ListBucketResult (GET Bucket)
type ListBucketResult struct {
	XMLName     xml.Name      `xml:"ListBucketResult"`
	Name        string        `xml:"Name"`
	Prefix      string        `xml:"Prefix"`
	Marker      string        `xml:"Marker"`
	MaxKeys     int           `xml:"MaxKeys"`
	IsTruncated bool          `xml:"IsTruncated"`
	Contents    []ObjectEntry `xml:"Contents"`
}

type ObjectEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	Owner        Owner  `xml:"Owner"`
	StorageClass string `xml:"StorageClass"`
}

// Helper to write XML response
func writeXML(w io.Writer, data interface{}) error {
	w.Write([]byte(xml.Header))
	return xml.NewEncoder(w).Encode(data)
}

// Helper to format S3 time
func formatS3Time(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000Z")
}
