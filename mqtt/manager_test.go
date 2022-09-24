package mqtt

import (
	"context"
	"github.com/c0olix/goChan"
	"github.com/c0olix/goChan/mqtt/middleware"
	mqtt "github.com/eclipse/paho.mqtt.golang"
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
		Host: "localhost",
		Port: 8883,
		Qos:  2,
	}

	manager, err := NewManager(config)
	assert.Nil(t, err)
	errorCallback := func(ctx context.Context, err error) {
		assert.NoError(t, err)
	}

	channel, err := manager.CreateChannel("TEST", errorCallback, config)
	assert.NoError(t, err)
	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.DebugLevel)
	channel.SetReaderMiddleWares(middleware.Logger(logger))
	handler := func(ctx context.Context, message goChan.MessageInterface) error {
		msg, ok := message.(mqtt.Message)
		assert.True(t, ok)
		assert.Equal(t, "hallo", string(msg.Payload()))
		return nil
	}
	channel.Consume(handler)

	err = channel.Produce(ctx, "hallo")
	assert.NoError(t, err)
}
