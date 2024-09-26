package consumer

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	_ "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type SqsMock struct {
	mock.Mock
	inputs       []*sqs.ReceiveMessageInput
	receiveError error
	deleteInputs []*sqs.DeleteMessageBatchInput
	deleteError  error
}

func (m *SqsMock) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	args := m.Called(ctx, params, optFns)
	m.inputs = append(m.inputs, params)
	return getQueueContent(), args.Error(1)
}

func (m *SqsMock) DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	args := m.Called(ctx, params, optFns)
	m.deleteInputs = append(m.deleteInputs, params)
	return nil, args.Error(1)
}

func TestNewSQSWorker(t *testing.T) {
	sqsConf := &SQSConf{
		Queue:               "queue",
		Concurrency:         2,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     20,
	}

	cfg, _ := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("foo", "bar", "")),
		config.WithRegion("eu-central-1"),
	)

	svc := sqs.NewFromConfig(cfg)

	type args struct {
		conf      *SQSConf
		svc       *sqs.Client
		consumeFn ConsumerFn
	}
	tests := []struct {
		name    string
		args    args
		want    *SQS
		wantErr error
	}{
		{
			name: "shouldCreateNewSQSConsumer",
			args: args{
				conf:      sqsConf,
				svc:       svc,
				consumeFn: consumeTestFunc,
			},
			want: &SQS{
				config:    sqsConf,
				sqs:       svc,
				consumeFn: consumeTestFunc,
			},

			wantErr: nil,
		},
		{
			name: "shouldCreateNewSQSConsumerWithDefaultValues",
			args: args{
				conf: &SQSConf{
					Queue: "queue",
				},
				svc:       svc,
				consumeFn: consumeTestFunc,
			},
			want: &SQS{
				config: &SQSConf{
					Queue:               "queue",
					Concurrency:         DefaultConcurrency,
					MaxNumberOfMessages: DefaultMaxNumberOfMessages,
					WaitTimeSeconds:     DefaultWaitTimeSeconds,
				},
				sqs:       svc,
				consumeFn: consumeTestFunc,
			},

			wantErr: nil,
		},
		{
			name: "shouldErrorQueueEmptyNewSQSConsumer",
			args: args{
				conf: &SQSConf{
					Queue: "",
				},
				svc:       svc,
				consumeFn: consumeTestFunc,
			},
			want: &SQS{
				config: &SQSConf{
					Queue:               "queue",
					Concurrency:         DefaultConcurrency,
					MaxNumberOfMessages: DefaultMaxNumberOfMessages,
					WaitTimeSeconds:     DefaultWaitTimeSeconds,
				},
				sqs:       svc,
				consumeFn: consumeTestFunc,
			},

			wantErr: SentinelErrorQueueNotSet,
		},
		{
			name: "shouldErrorConfigNilNewSQSConsumer",
			args: args{
				conf:      nil,
				svc:       svc,
				consumeFn: consumeTestFunc,
			},
			want: &SQS{
				config: &SQSConf{
					Queue:               "queue",
					Concurrency:         DefaultConcurrency,
					MaxNumberOfMessages: DefaultMaxNumberOfMessages,
					WaitTimeSeconds:     DefaultWaitTimeSeconds,
				},
				sqs:       svc,
				consumeFn: consumeTestFunc,
			},

			wantErr: SentinelErrorConfigIsNil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSQSConsumer(tt.args.conf, tt.args.svc, tt.args.consumeFn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

var consumeTestFunc ConsumerFn

func TestSQS_Start(t *testing.T) {
	queueUrl := "queue"
	var actual []string
	consumeTestFunc = func(data []byte) error {
		return nil
	}

	type fields struct {
		config *SQSConf
		svc    SQSClient
	}

	type args struct {
		consumeFn ConsumerFn
	}
	var tests = []struct {
		name           string
		fields         fields
		args           args
		wantReceiveErr error
		wantDeleteErr  error
	}{
		{
			name: "shouldHandleMessage",
			fields: fields{
				config: &SQSConf{
					Queue: queueUrl,
				},
				svc: new(SqsMock),
			},
			args: args{
				consumeFn: consumeTestFunc,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  nil,
		},
		///*
		{
			name: "should error when receive",
			fields: fields{
				config: &SQSConf{
					Queue: queueUrl,
				},
				svc: new(SqsMock),
			},
			args: args{
				consumeFn: consumeTestFunc,
			},
			wantReceiveErr: errors.New("fake receive error"),
			wantDeleteErr:  nil,
		},
		{
			name: "should error when delete",
			fields: fields{
				config: &SQSConf{
					Queue: queueUrl,
				},
				svc: new(SqsMock),
			},
			args: args{
				consumeFn: consumeTestFunc,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  errors.New("fake delete error"),
		},
		{
			name: "should context timeout",
			fields: fields{
				config: &SQSConf{
					Queue: queueUrl,
				},
				svc: new(SqsMock),
			},
			args: args{
				consumeFn: consumeTestFunc,
			},
			wantReceiveErr: nil,
			wantDeleteErr:  nil,
		},

		//*/
	}
	for _, tt := range tests {
		actual = make([]string, 0)
		t.Run(tt.name, func(t *testing.T) {
			sqsMock := tt.fields.svc.(*SqsMock)
			sqsMock.On("ReceiveMessage", mock.Anything, mock.AnythingOfType("*sqs.ReceiveMessageInput"),
				mock.AnythingOfType("[]func(*sqs.Options)")).Return(getQueueContent(), tt.wantReceiveErr)
			sqsMock.On("DeleteMessageBatch", mock.Anything, mock.AnythingOfType("*sqs.DeleteMessageBatchInput"),
				mock.AnythingOfType("[]func(*sqs.Options)")).Return(nil, tt.wantDeleteErr)

			ctx, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)

			s, _ := NewSQSConsumer(tt.fields.config, tt.fields.svc, tt.args.consumeFn)
			err := s.Start(ctx)

			t.Log(len(actual))
			mocked := tt.fields.svc.(*SqsMock)
			if tt.wantReceiveErr == nil && tt.wantDeleteErr == nil {
				assert.NotNil(t, mocked.inputs)
				assert.NotNil(t, mocked.deleteInputs)
				for _, msg := range actual {
					assert.Contains(t, []string{
						"msg1",
						"msg2",
						"msg3",
					}, msg)
				}
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func getQueueContent() *sqs.ReceiveMessageOutput {
	return &sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				MessageId: aws.String("msg1"),
				Body:      aws.String("msg1"),
			},
			{
				MessageId: aws.String("msg2"),
				Body:      aws.String("msg2"),
			},
			{
				MessageId: aws.String("msg3"),
				Body:      aws.String("msg3"),
			},
		},
	}
}
