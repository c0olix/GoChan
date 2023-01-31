package kafka

import (
	"encoding/json"
	"github.com/c0olix/goChan"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

type ChannelConfig struct {
	TopicGroup        string
	NumPartitions     int
	ReplicationFactor int
}

type Manager struct {
	Host        string
	ConnFactory func(host string) (*kafka.Conn, error)
}

func NewManager(host string) Manager {
	return Manager{
		Host: host,
		ConnFactory: func(host string) (*kafka.Conn, error) {
			conn, err := kafka.Dial("tcp", host)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
	}
}

func (manager Manager) Ready() error {
	conn, err := manager.ConnFactory(manager.Host)
	if err != nil {
		return errors.Wrap(err, "failed to create connection to Kafka")
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return errors.Wrap(err, "unable to get controller for "+manager.Host)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return errors.Wrap(err, "unable to create connection to kafka controller")
	}
	defer controllerConn.Close()

	return nil
}

func (manager Manager) CreateChannel(name string, config goChan.ConfigInterface) (goChan.ChannelInterface, error) {
	bytes, err := json.Marshal(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to convert to kafka channel config")
	}
	conf := ChannelConfig{}
	err = json.Unmarshal(bytes, &conf)
	if err != nil {
		return nil, errors.Wrap(err, "unable to convert to kafka channel config")
	}

	conn, err := manager.ConnFactory(manager.Host)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new Kafka connection to "+manager.Host)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get controller for "+manager.Host)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create connection to kafka controller")
	}

	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             name,
		NumPartitions:     conf.NumPartitions,
		ReplicationFactor: conf.ReplicationFactor,
	}

	err = controllerConn.CreateTopics(topicConfig)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create topics")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{manager.Host},
		GroupID:  conf.TopicGroup,
		Topic:    name,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	writer := kafka.Writer{
		Addr:     kafka.TCP(manager.Host),
		Topic:    name,
		Balancer: kafka.Murmur2Balancer{},
	}

	return &Channel{
		reader: reader,
		writer: &writer}, nil
}
