package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func NewSQSConsumer(conf *SQSConf, svc SQSClient) (*Consumer, error) {

	if conf == nil {
		return nil, errors.New("configuration is nil")
	}

	if conf.Queue == "" {
		return nil, errors.New("queue not set")
	}

	if conf.Concurrency == 0 {
		conf.Concurrency = DefaultConcurrency
	}

	if conf.WaitTimeSeconds == 0 {
		conf.WaitTimeSeconds = DefaultWaitTimeSeconds
	}

	if conf.MaxNumberOfMessages == 0 {
		conf.MaxNumberOfMessages = DefaultMaxNumberOfMessages
	}

	return &Consumer{config: conf, client: svc}, nil
}

func (consumer *Consumer) Start(ctx context.Context, consumeFn ConsumerFn) {
	params := &sqs.ReceiveMessageInput{
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		MaxNumberOfMessages:         consumer.config.MaxNumberOfMessages, // max it can receive
		MessageAttributeNames:       []string{string(types.QueueAttributeNameAll)},
		QueueUrl:                    aws.String(consumer.config.Queue),
		WaitTimeSeconds:             consumer.config.WaitTimeSeconds, // wait for 20 seconds at max for at least 1 message to be received
	}

	msgCh := make(chan types.Message)

	var wg sync.WaitGroup

	consumer.consumeFn = consumeFn

	startPool(ctx, msgCh, &wg, consumer)

	for {
		select {
		case <-ctx.Done():
			close(msgCh)

			return
		default:
			resp, err := consumer.client.ReceiveMessage(ctx, params)
			if err != nil {
				slog.Error("cannot receive messages", slog.Any("error", err.Error()))
				continue
			}

			// add number of messages received from the queue
			wg.Add(len(resp.Messages))

			// send received messages to sqs, so they can be processed
			for _, message := range resp.Messages {
				msgCh <- message
			}

			// wait for workers in the pool to be finished.
			wg.Wait()
		}
	}
}

// startPool starts 10 goroutines which listens to the msgCh which receives the 
// messages from the SQS.
func startPool(ctx context.Context, msgCh chan types.Message, wg *sync.WaitGroup, consumer *Consumer) {
	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg, channelClosed := <-msgCh:
					// If the channel is closed
					if !channelClosed {
						return
					}

					err := consumer.consumeFn([]byte(*msg.Body))
					if err != nil {
						slog.Error("Error calling consumer function", slog.Any("error", err))
					}

					err = consumer.deleteMessage(ctx, msg)
					if err != nil {
						slog.Error("Failed to delete message", slog.Any("error", err))
					} else {
						slog.Debug("Message deleted successfully:", *msg.MessageId)
					}

					// release the waitgroup to inform that the message has been processed.
					wg.Done()
				}
			}
		}()
	}
}

// deleteMessage deletes the given message from the queue
func (consumer *Consumer) deleteMessage(ctx context.Context, msg types.Message) error {
	_, err := consumer.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(consumer.config.Queue),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

func test(out []byte) error {
	fmt.Println(string(out))
	return nil
}
