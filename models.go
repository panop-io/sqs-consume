package consumer

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	DefaultMaxNumberOfMessages = int32(10)
	DefaultWaitTimeSeconds     = int32(5)
	DefaultConcurrency         = 1
)

type SQSConf struct {
	Queue               string
	Concurrency         int
	MaxNumberOfMessages int32
	VisibilityTimeout   int32
	WaitTimeSeconds     int32
}

type SQSClient interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type Consumer struct {
	config    *SQSConf
	client    SQSClient
	consumeFn ConsumerFn
}

type ConsumerFn func(data []byte) error
