package kafka

import (
	"context"
	"github.com/c0olix/goChan"
	"github.com/c0olix/goChan/kafka/middleware"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_e2e(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	config := ChannelConfig{
		TopicGroup:        "test",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	manager := NewManager("localhost")
	errorCallback := func(ctx context.Context, err error) {
		assert.NoError(t, err)
	}

	channel, err := manager.CreateChannel("TEST", errorCallback, config)
	assert.NoError(t, err)
	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.DebugLevel)
	channel.SetReaderMiddleWares(middleware.Logger(logger))
	handler := func(ctx context.Context, message goChan.MessageInterface) error {
		msg, ok := message.(kafka.Message)
		assert.True(t, ok)
		assert.Equal(t, "hallo", string(msg.Value))
		return nil
	}
	channel.Consume(handler)
	msg := kafka.Message{
		Value: []byte("hallo"),
	}
	err = channel.Produce(ctx, msg)
	assert.NoError(t, err)
}
