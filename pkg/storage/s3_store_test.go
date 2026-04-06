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
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bubustack/core/contracts"
)

const (
	fingerprintFalse = "false"
	fingerprintTrue  = "true"
)

func TestGetS3TLSSetting(t *testing.T) {
	t.Setenv(contracts.StorageS3TLSEnv, "false")
	val, fingerprint, err := getS3TLSSetting()
	if err != nil {
		t.Fatalf("getS3TLSSetting() error = %v", err)
	}
	if val == nil || *val {
		t.Fatalf("getS3TLSSetting() value = %#v, want false", val)
	}
	if fingerprint != fingerprintFalse {
		t.Fatalf("getS3TLSSetting() fingerprint = %q, want %q", fingerprint, fingerprintFalse)
	}

	t.Setenv(contracts.StorageS3TLSEnv, "true")
	val, fingerprint, err = getS3TLSSetting()
	if err != nil {
		t.Fatalf("getS3TLSSetting() error = %v", err)
	}
	if val == nil || !*val {
		t.Fatalf("getS3TLSSetting() value = %#v, want true", val)
	}
	if fingerprint != fingerprintTrue {
		t.Fatalf("getS3TLSSetting() fingerprint = %q, want %q", fingerprint, fingerprintTrue)
	}
}

func TestGetS3TLSSettingInvalid(t *testing.T) {
	t.Setenv(contracts.StorageS3TLSEnv, "definitely-not-bool")
	if _, _, err := getS3TLSSetting(); err == nil {
		t.Fatalf("getS3TLSSetting() expected error for invalid value")
	}
}

func TestGetS3UsePathStyleParsesVariants(t *testing.T) {
	t.Setenv(contracts.StorageS3ForcePathStyleEnv, "TRUE")
	val, err := getS3UsePathStyle()
	if err != nil {
		t.Fatalf("getS3UsePathStyle() error = %v", err)
	}
	if !val {
		t.Fatalf("getS3UsePathStyle() = %v, want true", val)
	}

	t.Setenv(contracts.StorageS3ForcePathStyleEnv, "0")
	val, err = getS3UsePathStyle()
	if err != nil {
		t.Fatalf("getS3UsePathStyle() error = %v", err)
	}
	if val {
		t.Fatalf("getS3UsePathStyle() = %v, want false", val)
	}
}

func TestGetS3UsePathStyleInvalid(t *testing.T) {
	t.Setenv(contracts.StorageS3ForcePathStyleEnv, "sometimes")
	if _, err := getS3UsePathStyle(); err == nil {
		t.Fatalf("getS3UsePathStyle() expected error for invalid value")
	}
}

func TestResolveS3Endpoint(t *testing.T) {
	val := false
	endpoint, err := resolveS3Endpoint("minio.default.svc:9000", &val)
	if err != nil {
		t.Fatalf("resolveS3Endpoint() error = %v", err)
	}
	if endpoint != "http://minio.default.svc:9000" {
		t.Fatalf("resolveS3Endpoint() = %q, want %q", endpoint, "http://minio.default.svc:9000")
	}

	endpoint, err = resolveS3Endpoint("https://s3.amazonaws.com", nil)
	if err != nil {
		t.Fatalf("resolveS3Endpoint() error = %v", err)
	}
	if endpoint != "https://s3.amazonaws.com" {
		t.Fatalf("resolveS3Endpoint() = %q, want %q", endpoint, "https://s3.amazonaws.com")
	}
}

func TestResolveS3EndpointMismatch(t *testing.T) {
	val := true
	if _, err := resolveS3Endpoint("http://minio", &val); err == nil {
		t.Fatalf("resolveS3Endpoint() expected error for TLS mismatch")
	}
}

// s3API interface defines the methods we need from the S3 client
type s3API interface {
	PutObject(
		ctx context.Context,
		params *s3.PutObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.PutObjectOutput, error)
	GetObject(
		ctx context.Context,
		params *s3.GetObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

// mockS3Client is a mock implementation of the S3 API for testing
type mockS3Client struct {
	putObjectFunc func(
		ctx context.Context,
		params *s3.PutObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.PutObjectOutput, error)
	getObjectFunc func(
		ctx context.Context,
		params *s3.GetObjectInput,
		optFns ...func(*s3.Options),
	) (*s3.GetObjectOutput, error)
}

func (m *mockS3Client) PutObject(
	ctx context.Context,
	params *s3.PutObjectInput,
	optFns ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	return m.putObjectFunc(ctx, params, optFns...)
}

func (m *mockS3Client) GetObject(
	ctx context.Context,
	params *s3.GetObjectInput,
	optFns ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	return m.getObjectFunc(ctx, params, optFns...)
}

// mockS3Store wraps the mock client for testing
type mockS3Store struct {
	client s3API
	bucket string
}

func (s *mockS3Store) Write(ctx context.Context, path string, reader io.Reader) error {
	input := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
		Body:   reader,
	}
	applyWriteMetadataToPutObject(ctx, input)
	_, err := s.client.PutObject(ctx, input)
	return err
}

func (s *mockS3Store) Read(ctx context.Context, path string, writer io.Writer) error {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &path,
	})
	if err != nil {
		return err
	}
	defer func() { _ = out.Body.Close() }()
	_, err = io.Copy(writer, out.Body)
	return err
}

func TestNewS3Store_MissingBucket(t *testing.T) {
	t.Setenv(contracts.StorageS3BucketEnv, "")
	_, err := NewS3Store(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), contracts.StorageS3BucketEnv+" is not set")
}

func TestS3Store_Write(t *testing.T) {
	testData := []byte("test s3 data")
	testBucket := "test-bucket"
	testKey := "test/path.txt"
	meta := &writeMetadata{
		ContentType:    "application/json",
		ContentMD5:     "md5==",
		ChecksumSHA256: "sha==",
		ContentLength:  int64(len(testData)),
	}

	mock := &mockS3Client{
		putObjectFunc: func(
			ctx context.Context,
			params *s3.PutObjectInput,
			optFns ...func(*s3.Options),
		) (*s3.PutObjectOutput, error) {
			// Verify parameters
			if *params.Bucket != testBucket {
				t.Errorf("PutObject bucket = %v, want %v", *params.Bucket, testBucket)
			}
			if *params.Key != testKey {
				t.Errorf("PutObject key = %v, want %v", *params.Key, testKey)
			}
			if params.ContentType == nil || *params.ContentType != meta.ContentType {
				t.Fatalf("ContentType = %v, want %v", params.ContentType, meta.ContentType)
			}
			if params.ContentMD5 == nil || *params.ContentMD5 != meta.ContentMD5 {
				t.Fatalf("ContentMD5 = %v, want %v", params.ContentMD5, meta.ContentMD5)
			}
			if params.ChecksumSHA256 == nil || *params.ChecksumSHA256 != meta.ChecksumSHA256 {
				t.Fatalf("ChecksumSHA256 = %v, want %v", params.ChecksumSHA256, meta.ChecksumSHA256)
			}
			if params.ContentLength == nil || *params.ContentLength != meta.ContentLength {
				t.Fatalf("ContentLength = %v, want %d", params.ContentLength, meta.ContentLength)
			}

			// Read and verify body
			data, err := io.ReadAll(params.Body)
			if err != nil {
				t.Fatalf("Failed to read body: %v", err)
			}
			if !bytes.Equal(data, testData) {
				t.Errorf("PutObject body = %v, want %v", data, testData)
			}

			return &s3.PutObjectOutput{}, nil
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: testBucket,
	}

	ctx := withWriteMetadata(context.Background(), meta)
	err := store.Write(ctx, testKey, bytes.NewReader(testData))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
}

func TestS3Store_Read(t *testing.T) {
	testData := []byte("s3 read data")
	testBucket := "test-bucket"
	testKey := "read/path.txt"

	mock := &mockS3Client{
		getObjectFunc: func(
			ctx context.Context,
			params *s3.GetObjectInput,
			optFns ...func(*s3.Options),
		) (*s3.GetObjectOutput, error) {
			// Verify parameters
			if *params.Bucket != testBucket {
				t.Errorf("GetObject bucket = %v, want %v", *params.Bucket, testBucket)
			}
			if *params.Key != testKey {
				t.Errorf("GetObject key = %v, want %v", *params.Key, testKey)
			}

			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader(testData)),
			}, nil
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: testBucket,
	}

	ctx := context.Background()
	var buf bytes.Buffer
	err := store.Read(ctx, testKey, &buf)
	if err != nil {
		t.Errorf("Read() error = %v", err)
	}

	if !bytes.Equal(buf.Bytes(), testData) {
		t.Errorf("Read() got %v, want %v", buf.Bytes(), testData)
	}
}

func TestS3Store_Write_Error(t *testing.T) {
	expectedErr := io.EOF

	mock := &mockS3Client{
		putObjectFunc: func(
			ctx context.Context,
			params *s3.PutObjectInput,
			optFns ...func(*s3.Options),
		) (*s3.PutObjectOutput, error) {
			return nil, expectedErr
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: "test-bucket",
	}

	ctx := context.Background()
	err := store.Write(ctx, "error/path", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Error("Write() should return error when PutObject fails")
	}
}

func TestS3Store_Read_Error(t *testing.T) {
	expectedErr := io.EOF

	mock := &mockS3Client{
		getObjectFunc: func(
			ctx context.Context,
			params *s3.GetObjectInput,
			optFns ...func(*s3.Options),
		) (*s3.GetObjectOutput, error) {
			return nil, expectedErr
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: "test-bucket",
	}

	ctx := context.Background()
	var buf bytes.Buffer
	err := store.Read(ctx, "error/path", &buf)
	if err == nil {
		t.Error("Read() should return error when GetObject fails")
	}
}

func TestS3Store_RoundTrip(t *testing.T) {
	testData := []byte("round trip data")
	storage := make(map[string][]byte)

	mock := &mockS3Client{
		putObjectFunc: func(
			ctx context.Context,
			params *s3.PutObjectInput,
			optFns ...func(*s3.Options),
		) (*s3.PutObjectOutput, error) {
			data, _ := io.ReadAll(params.Body)
			storage[*params.Key] = data
			return &s3.PutObjectOutput{}, nil
		},
		getObjectFunc: func(
			ctx context.Context,
			params *s3.GetObjectInput,
			optFns ...func(*s3.Options),
		) (*s3.GetObjectOutput, error) {
			data := storage[*params.Key]
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader(data)),
			}, nil
		},
	}

	store := &mockS3Store{
		client: mock,
		bucket: "test-bucket",
	}

	ctx := context.Background()
	path := "roundtrip/test.txt"

	// Write
	if err := store.Write(ctx, path, bytes.NewReader(testData)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Read
	var buf bytes.Buffer
	if err := store.Read(ctx, path, &buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Verify
	if !bytes.Equal(buf.Bytes(), testData) {
		t.Errorf("Round trip failed: got %v, want %v", buf.Bytes(), testData)
	}
}

func TestNewManager_S3Store(t *testing.T) {
	// This test confirms that NewManager correctly initializes the S3 store
	// when the provider is set to "s3".
	t.Setenv(contracts.StorageProviderEnv, "s3")
	t.Setenv(contracts.StorageS3BucketEnv, "test-bucket")
	t.Setenv(contracts.StorageS3RegionEnv, "us-east-1")

	manager, err := NewManager(context.Background())
	require.NoError(t, err, "NewManager() should initialize S3 store when relying on ambient credentials")
	if _, ok := manager.store.(*S3Store); !ok {
		t.Fatalf("expected S3Store, got %T", manager.store)
	}
}
