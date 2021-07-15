package backup

import (
	"context"
	"fmt"
	"io"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob" // nolint:nolintlint,golint,gci
	_ "gocloud.dev/blob/gcsblob"   // nolint:nolintlint,golint,gci
	_ "gocloud.dev/blob/s3blob"    // nolint:nolintlint,golint,gci
	"gocloud.dev/gcerrors"
)

// StorageServiceSink uses a storage engine that can be defined by the construction url on creation.
type StorageServiceSink struct {
	bucket *blob.Bucket
}

// NewStorageServiceSink returns initialized instance of StorageServiceSink instance.
// The storage engine is chosen based on the provided url value and a set of pre-registered
// blank imports in that file. It is the caller's responsibility to provide all required environment
// variables in order to get properly initialized storage engine driver.
func NewStorageServiceSink(ctx context.Context, url string) (*StorageServiceSink, error) {
	bucket, err := blob.OpenBucket(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("storage service sink: open bucket: %w", err)
	}

	return &StorageServiceSink{bucket: bucket}, nil
}

// Close releases resources associated with the bucket communication.
func (s *StorageServiceSink) Close() error {
	if s.bucket != nil {
		bucket := s.bucket
		s.bucket = nil
		if err := bucket.Close(); err != nil {
			return fmt.Errorf("storage service sink: close bucket: %w", err)
		}
		return nil
	}
	return nil
}

// Write stores data from the r into a relativePath path on the configured bucket.
func (s *StorageServiceSink) Write(ctx context.Context, relativePath string, r io.Reader) error {
	writer, err := s.bucket.NewWriter(ctx, relativePath, &blob.WriterOptions{
		// 'no-store' - we don't want the backup to be cached as the content could be changed,
		// so we always want a fresh and up to date data
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#cacheability
		// 'no-transform' - disallows intermediates to modify data
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#other
		CacheControl: "no-store, no-transform",
		ContentType:  "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("storage service sink: new writer for %q: %w", relativePath, err)
	}
	defer func() { _ = writer.Close() }()

	if _, err := io.Copy(writer, r); err != nil {
		return fmt.Errorf("storage service sink: coping data for %q: %w", relativePath, err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("storage service sink: finalise creation for %q: %w", relativePath, err)
	}

	return nil
}

// GetReader returns a reader to consume the data from the configured bucket.
// It is the caller's responsibility to Close the reader after usage.
func (s *StorageServiceSink) GetReader(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	reader, err := s.bucket.NewReader(ctx, relativePath, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			err = ErrDoesntExist
		}
		return nil, fmt.Errorf("storage service sink: new reader for %q: %w", relativePath, err)
	}
	return reader, nil
}
