package mqtt

import (
	"context"
	"github.com/c0olix/goChan"
	"github.com/c0olix/goChan/mqtt/middleware"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
	"time"
)

func Test_e2e(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()
	req := testcontainers.ContainerRequest{
		Image:        "eclipse-mosquitto:latest",
		ExposedPorts: []string{"1883/tcp", "9001/tcp"},
		WaitingFor:   wait.ForLog("mosquitto version 2.0.14 running"),
	}

	mqttC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	assert.NoError(t, err)
	defer func(mqttC testcontainers.Container, ctx context.Context) {
		err := mqttC.Terminate(ctx)
		assert.NoError(t, err)
	}(mqttC, ctx)

	host, err := mqttC.Host(ctx)
	assert.NoError(t, err)

	config := ChannelConfig{
		host: host,
		port: 1883,
		qos:  2,
	}

	manager, err := NewManager(config)
	assert.NoError(t, err)
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
