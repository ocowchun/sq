package physical

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type s3ObjectScan struct {
	allocator             memory.Allocator
	BucketName            string
	KeyPrefix             *string
	output                catalog.Schema
	s3Client              *s3.Client
	nextContinuationToken *string
	hasNext               bool
}

func newS3ObjectScan(node *logical.S3ObjectScan, allocator memory.Allocator) *s3ObjectScan {
	// TODO: inject aws config later
	loadOptions := make([]func(*config.LoadOptions) error, 0, 2)
	ctx := context.Background()
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		panic(err)
	}

	s3Client := s3.NewFromConfig(awsCfg)
	return &s3ObjectScan{
		allocator:  allocator,
		BucketName: node.BucketName,
		KeyPrefix:  node.KeyPrefix,
		output:     node.Schema(),
		s3Client:   s3Client,
		hasNext:    true,
	}
}

func (s *s3ObjectScan) Open() error {
	return nil
}

func (s *s3ObjectScan) Close() error {
	return nil
}

func (s *s3ObjectScan) Next(ctx context.Context) NextResponse {
	request := &s3.ListObjectsV2Input{
		Bucket: &s.BucketName,
		Prefix: s.KeyPrefix,

		OptionalObjectAttributes: []types.OptionalObjectAttributes{types.OptionalObjectAttributesRestoreStatus},
	}
	page, err := s.s3Client.ListObjectsV2(ctx, request)
	if err != nil {
		return NextResponse{Err: err}
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "key", Type: arrow.BinaryTypes.String},
		{Name: "bucket_name", Type: arrow.BinaryTypes.String},
		{Name: "size", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	keyBuilder := array.NewStringBuilder(s.allocator)
	defer keyBuilder.Release()
	bucketNameBuilder := array.NewStringBuilder(s.allocator)
	defer bucketNameBuilder.Release()
	sizeBuilder := array.NewInt64Builder(s.allocator)
	defer sizeBuilder.Release()

	for _, object := range page.Contents {
		keyBuilder.Append(*object.Key)
		bucketNameBuilder.Append(s.BucketName)
		sizeBuilder.Append(*object.Size)
	}
	s.nextContinuationToken = page.NextContinuationToken
	s.hasNext = page.NextContinuationToken != nil

	keys := keyBuilder.NewArray()
	defer keys.Release()
	buckets := bucketNameBuilder.NewArray()
	defer bucketNameBuilder.Release()
	sizes := sizeBuilder.NewArray()
	defer sizes.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{keys, buckets, sizes}, int64(keys.Len()))

	return NextResponse{
		Batch:   batch,
		Err:     nil,
		HasNext: s.hasNext,
	}

}

func (s *s3ObjectScan) Schema() *catalog.Schema {
	return &s.output
}
