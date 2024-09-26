package consumer

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	DefaultMaxNumberOfMessages = int32(10)
	DefaultWaitTimeSeconds     = int32(5)
	DefaultConcurrency         = 1
)

var (
	SentinelErrorQueueNotSet = errors.New("queue not set")
	SentinelErrorConfigIsNil = errors.New("configuration is nil")
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
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

type SQS struct {
	config    *SQSConf
	sqs       SQSClient
	consumeFn ConsumerFn
}

type ConsumerFn func(data []byte) error
