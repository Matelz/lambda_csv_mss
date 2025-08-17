package db

import (
	"context"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3BucketBasics struct {
	S3Client *s3.Client
}

func NewS3BucketBasics(client *s3.Client) S3BucketBasics {
	return S3BucketBasics{
		S3Client: client,
	}
}

func (basics S3BucketBasics) StreamFile(ctx context.Context, object *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	result, err := basics.S3Client.GetObject(ctx, object)
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Can't get object %s from bucket %s. No such key exists.\n", *object.Key, *object.Bucket)
			err = noKey
		} else {
			log.Printf("Couldn't get object %v:%v. Here's why: %v\n", *object.Bucket, *object.Key, err)
		}
		return nil, err
	}

	return result, err
}
