/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/bubustack/core/contracts"
)

// S3Store implements the Store interface for an S3-compatible object store.
// It supports standard AWS S3 and S3-compatible services like MinIO.
// Uses manager.Uploader for automatic multipart uploads on large files.
type S3Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func parseBoolEnv(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "t", "yes", "y", "on":
		return true, nil
	case "0", "false", "f", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value %q", value)
	}
}

func getS3TLSSetting() (*bool, string, error) {
	if raw, ok := os.LookupEnv(contracts.StorageS3TLSEnv); ok {
		if strings.TrimSpace(raw) == "" {
			return nil, "", fmt.Errorf("%s is set but empty", contracts.StorageS3TLSEnv)
		}
		val, err := parseBoolEnv(raw)
		if err != nil {
			return nil, "", fmt.Errorf("invalid %s value: %w", contracts.StorageS3TLSEnv, err)
		}
		v := val
		if val {
			return &v, "true", nil
		}
		return &v, "false", nil
	}
	return nil, "", nil
}

func resolveS3Endpoint(raw string, tlsEnabled *bool) (string, error) {
	endpoint := strings.TrimSpace(raw)
	if endpoint == "" {
		return "", nil
	}
	if !strings.Contains(endpoint, "://") {
		scheme := "https"
		if tlsEnabled != nil && !*tlsEnabled {
			scheme = "http"
		}
		endpoint = fmt.Sprintf("%s://%s", scheme, endpoint)
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid S3 endpoint %q: %w", raw, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid S3 endpoint %q: expected scheme and host", raw)
	}
	if tlsEnabled != nil {
		expected := "https"
		if !*tlsEnabled {
			expected = "http"
		}
		if parsed.Scheme != expected {
			return "", fmt.Errorf(
				"%s=%t requires %s endpoint scheme, but got %q",
				contracts.StorageS3TLSEnv,
				*tlsEnabled,
				expected,
				parsed.Scheme,
			)
		}
	}
	parsed.Fragment = ""
	return parsed.String(), nil
}

// getS3RetryConfig returns retry configuration from env or defaults
func getS3RetryConfig() (maxAttempts int, maxBackoff time.Duration) {
	maxAttempts = 3 // Default
	if v := getEnvWithFallback(contracts.StorageS3MaxRetriesEnv, ""); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			maxAttempts = i
		}
	}

	maxBackoff = 10 * time.Second // Default
	if v := getEnvWithFallback(contracts.StorageS3MaxBackoffEnv, ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			maxBackoff = d
		}
	}

	return maxAttempts, maxBackoff
}

// getS3UsePathStyle determines whether to use path-style addressing for S3.
// - Path-style: https://s3.amazonaws.com/bucket/key (required for MinIO, Ceph)
// - Virtual-hosted-style: https://bucket.s3.amazonaws.com/key (AWS S3 standard, required for TLS)
//
// Logic:
// 1. If BUBU_S3_FORCE_PATH_STYLE is set, use that value
// 2. If BUBU_S3_ENDPOINT is set (custom endpoint → MinIO/Ceph), default to true
// 3. Otherwise (AWS S3), default to false (virtual-hosted)
func getS3UsePathStyle() (bool, error) {
	if v := getEnvWithFallback(contracts.StorageS3ForcePathStyleEnv, ""); v != "" {
		parsed, err := parseBoolEnv(v)
		if err != nil {
			return false, fmt.Errorf("invalid %s value: %w", contracts.StorageS3ForcePathStyleEnv, err)
		}
		return parsed, nil
	}

	// Auto-detect: custom endpoints (MinIO, Ceph) typically require path-style
	endpoint := getEnvWithFallback(contracts.StorageS3EndpointEnv, "")
	if endpoint != "" {
		return true, nil
	}

	// Default to virtual-hosted-style for AWS S3 (TLS compatibility)
	return false, nil
}

func resolveS3Credentials() (aws.CredentialsProvider, error) {
	accessKey := getEnvWithFallback(contracts.StorageS3AccessKeyIDEnv, "")
	secretKey := getEnvWithFallback(contracts.StorageS3SecretAccessKeyEnv, "")
	sessionToken := getEnvWithFallback(contracts.StorageS3SessionTokenEnv, "")

	if accessKey == "" && secretKey == "" && sessionToken == "" {
		return nil, nil
	}
	if accessKey == "" {
		return nil, fmt.Errorf("%s must be set when providing explicit S3 credentials", contracts.StorageS3AccessKeyIDEnv)
	}
	if secretKey == "" {
		return nil, fmt.Errorf("%s must be set when providing explicit S3 credentials", contracts.StorageS3SecretAccessKeyEnv)
	}

	return credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken), nil
}

// NewS3Store creates a new S3Store.
// It automatically configures the S3 client from environment variables.
func NewS3Store(ctx context.Context) (*S3Store, error) {
	bucket, err := resolveS3Bucket()
	if err != nil {
		return nil, err
	}
	tlsEnabled, _, err := getS3TLSSetting()
	if err != nil {
		return nil, err
	}

	credProvider, err := resolveS3Credentials()
	if err != nil {
		return nil, err
	}

	cfg, err := loadAWSConfig(ctx, credProvider)
	if err != nil {
		return nil, err
	}

	httpClient := buildS3HTTPClient()
	if err := applyS3EndpointOverride(&cfg, tlsEnabled); err != nil {
		return nil, err
	}

	usePathStyle, err := getS3UsePathStyle()
	if err != nil {
		return nil, err
	}

	client := newS3Client(cfg, httpClient, usePathStyle)
	uploader := newS3Uploader(client)

	return &S3Store{
		client:   client,
		uploader: uploader,
		bucket:   bucket,
	}, nil
}

func resolveS3Bucket() (string, error) {
	bucket := getEnvWithFallback(contracts.StorageS3BucketEnv, "")
	if bucket == "" {
		return "", fmt.Errorf("s3 storage is enabled but %s is not set", contracts.StorageS3BucketEnv)
	}
	return bucket, nil
}

func loadAWSConfig(ctx context.Context, credProvider aws.CredentialsProvider) (aws.Config, error) {
	loadOpts := []func(*config.LoadOptions) error{}
	if credProvider != nil {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(aws.NewCredentialsCache(credProvider)))
	} else if endpoint := getEnvWithFallback(contracts.StorageS3EndpointEnv, ""); endpoint != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load AWS config: %w", err)
	}

	maxAttempts, maxBackoff := getS3RetryConfig()
	cfg.Retryer = func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = maxAttempts
			o.MaxBackoff = maxBackoff
		})
	}

	if region := getEnvWithFallback(contracts.StorageS3RegionEnv, ""); region != "" {
		cfg.Region = region
	} else if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	return cfg, nil
}

func buildS3HTTPClient() *http.Client {
	timeout := 5 * time.Minute
	if v := getEnvWithFallback(contracts.StorageS3TimeoutEnv, contracts.StorageTimeoutEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}
	return &http.Client{Timeout: timeout}
}

func applyS3EndpointOverride(cfg *aws.Config, tlsEnabled *bool) error {
	endpoint := getEnvWithFallback(contracts.StorageS3EndpointEnv, "")
	if endpoint == "" {
		return nil
	}
	resolvedEndpoint, err := resolveS3Endpoint(endpoint, tlsEnabled)
	if err != nil {
		return err
	}
	cfg.BaseEndpoint = aws.String(resolvedEndpoint)
	return nil
}

func newS3Client(cfg aws.Config, httpClient *http.Client, usePathStyle bool) *s3.Client {
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
		o.HTTPClient = httpClient
	})
}

func newS3Uploader(client *s3.Client) *manager.Uploader {
	return manager.NewUploader(client, func(u *manager.Uploader) {
		if partSize := getEnvWithFallback(contracts.StorageS3MaxPartSizeEnv, ""); partSize != "" {
			if size, err := strconv.ParseInt(partSize, 10, 64); err == nil && size > 0 {
				u.PartSize = size
			}
		}
		if concurrency := getEnvWithFallback(contracts.StorageS3ConcurrencyEnv, ""); concurrency != "" {
			if n, err := strconv.Atoi(concurrency); err == nil && n > 0 {
				u.Concurrency = n
			}
		}
	})
}

// Write uploads data to S3 from a reader.
// Uses manager.Uploader which automatically handles multipart uploads for large files.
// For files >5MB, it will automatically split into parts and upload in parallel.
func (s *S3Store) Write(ctx context.Context, path string, reader io.Reader) error {
	cleanPath := filepath.Clean(path)
	if strings.HasPrefix(cleanPath, "..") {
		return fmt.Errorf("invalid s3 path: %q contains path traversal", path)
	}
	input := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &cleanPath,
		Body:   reader,
	}
	applyWriteMetadataToPutObject(ctx, input)

	// Server-side encryption controls (opt-in via env/policy)
	if sse := strings.ToLower(getEnvWithFallback(contracts.StorageS3SSEEnv, "")); sse != "" {
		switch sse {
		case "s3", "sse-s3", "aes256":
			input.ServerSideEncryption = s3types.ServerSideEncryptionAes256
		case "kms", "aws:kms":
			input.ServerSideEncryption = s3types.ServerSideEncryptionAwsKms
			if kmsKey := getEnvWithFallback(contracts.StorageS3SSEKMSKeyEnv, ""); kmsKey != "" {
				input.SSEKMSKeyId = &kmsKey
			}
		default:
			return fmt.Errorf("unsupported %s value %q (expected 's3' or 'kms')", contracts.StorageS3SSEEnv, sse)
		}
	}

	// Use uploader which automatically handles:
	// - Small files: single PutObject
	// - Large files: automatic multipart upload with parallel parts
	// - No need to know file size upfront!
	_, err := s.uploader.Upload(ctx, input)
	if err != nil {
		// If the upload fails, attempt to abort the multipart upload to prevent orphaned parts.
		var multiErr manager.MultiUploadFailure
		if errors.As(err, &multiErr) {
			// This is a best-effort cleanup. If the parent context is already
			// canceled, this operation may also fail, which can lead to orphaned S3 parts.
			// We prioritize honoring the context cancellation invariant over a detached cleanup.
			abortCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			_, abortErr := s.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
				Bucket:   &s.bucket,
				Key:      &cleanPath,
				UploadId: aws.String(multiErr.UploadID()),
			})
			if abortErr != nil {
				// Return a wrapped error that includes both the original upload failure
				// and the subsequent failure to abort.
				return fmt.Errorf(
					"failed to upload object '%s' to s3 bucket '%s': %w (also failed to abort multipart upload: %v)",
					path,
					s.bucket,
					err,
					abortErr,
				)
			}
		}
		return fmt.Errorf("failed to upload object '%s' to s3 bucket '%s': %w", path, s.bucket, err)
	}
	return nil
}

// Read downloads data from S3 and writes it to a writer.
func (s *S3Store) Read(ctx context.Context, path string, writer io.Writer) error {
	cleanPath := filepath.Clean(path)
	if strings.HasPrefix(cleanPath, "..") {
		return fmt.Errorf("invalid s3 path: %q contains path traversal", path)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &cleanPath,
	})
	if err != nil {
		return fmt.Errorf("failed to get object '%s' from s3 bucket '%s': %w", path, s.bucket, err)
	}
	defer func() { _ = out.Body.Close() }()

	_, err = io.Copy(writer, out.Body)
	if err != nil {
		return fmt.Errorf("failed to copy object '%s' from s3 bucket '%s': %w", path, s.bucket, err)
	}
	return nil
}

func applyWriteMetadataToPutObject(ctx context.Context, input *s3.PutObjectInput) {
	meta := writeMetadataFromContext(ctx)
	if meta == nil {
		return
	}
	if meta.ContentType != "" {
		input.ContentType = aws.String(meta.ContentType)
	}
	if meta.ContentMD5 != "" {
		input.ContentMD5 = aws.String(meta.ContentMD5)
	}
	if meta.ChecksumSHA256 != "" {
		input.ChecksumAlgorithm = s3types.ChecksumAlgorithmSha256
		input.ChecksumSHA256 = aws.String(meta.ChecksumSHA256)
	}
	if meta.ContentLength > 0 {
		input.ContentLength = aws.Int64(meta.ContentLength)
	}
}
