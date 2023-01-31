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
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	config := ChannelConfig{
		TopicGroup:        "test",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	manager := NewManager("localhost")

	channel, err := manager.CreateChannel("TEST", config)
	assert.NoError(t, err)
	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.DebugLevel)
	channel.SetReaderMiddleWares(middleware.Logger(logger))
	called := false
	handler := func(ctx context.Context, message goChan.MessageInterface) error {
		called = true
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
	for {
		if called {
			break
		}
	}
	assert.True(t, called)
}
