package middleware

import (
	"context"
	"github.com/c0olix/goChan"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
)

type LoggerInterface interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
}

func Logger(log LoggerInterface) goChan.Middleware {
	logged := func(next goChan.Handler) goChan.Handler {
		handler := func(ctx context.Context, msg goChan.MessageInterface) error {
			message, ok := msg.(kafka.Message)
			if !ok {
				return errors.New("wrong type")
			}
			log.Debugf("Got event on channel %s", message.Topic)
			err := next(ctx, msg)
			if err != nil {
				return errors.Wrap(err, "unable to handle next handler")
			}
			log.Debug("Event processed")
			return nil
		}
		return handler
	}
	return logged
}
