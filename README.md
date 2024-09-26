# Sqs Consume

### Simple and concurrently consuming utility for AWS SQS
Sqs-consumer allows developers to consume messages from a sqs queue leveraging go's competition management. Use Sqs consumer is very simple, fast and clean.

### Example
```go
package main

import (
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)
func main() {
	cred := credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(cred),
	)

	sqsClient := sqs.NewFromConfig(cfg)

	if err != nil {
		os.Exit(1)
	}

	sqsConf := SQSConf{}
	sqsConf.Queue = "https://sqs.eu-central-1.amazonaws.com/357464669851/panop-records-queue-cron-sandbox"
	c, err := NewSQSConsumer(&sqsConf, sqsClient)
	if err != nil {
		os.Exit(1)
	}
	c.Start(context.Background(), test)
}
```