# Sqs Consume

Based on the work  of github.com/The-Data-Appeal-Company
this consumer use now aws-sdk-go-v2 and AWS credentials are set with environment variables

### Simple and concurrently consuming utility for AWS SQS
Sqs-consumer allows developers to consume messages from a sqs queue leveraging go's competition management. Use Sqs consumer is very simple, fast and clean.

### Envvar
Asume these variables are set
- AWS_REGION
- AWS_SECRET_ACCESS_KEY
- AWS_ACCESS_KEY_ID

### Example
```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/panop-io/sqs-consume/consumer"
)

func test(b []byte) error {
	fmt.Println(string(b))
	time.Sleep(time.Second * 30)
	return nil
}

func main() {
	sqsConf := consumer.SQSConf{
		Queue: "https://sqs.eu-central-1.amazonaws.com/357464669851/panop-records-queue-cron-sandbox",
	}
	c, err := consumer.NewSQSConsumer(&sqsConf)
	if err != nil {
		os.Exit(1)
	}
	c.Start(context.Background(), test)
}
```