package goChan

import "context"

type Handler func(ctx context.Context, message MessageInterface) error
type Middleware func(Handler) Handler

type MessageInterface = interface {
}

type ConfigInterface = interface {
}

type ChannelInterface interface {
	Consume(Handler)
	Produce(context.Context, MessageInterface) error
	SetReaderMiddleWares(mw ...Middleware)
	SetWriterMiddleWares(mw ...Middleware)
}

func WrapMiddleware(mw []Middleware, handler Handler) Handler {
	// Loop backwards through the middleware invoking each one. Replace the
	// handler with the new wrapped handler. Looping backwards ensures that the
	// first middleware of the slice is the first to be executed by requests.
	for i := len(mw) - 1; i >= 0; i-- {
		h := mw[i]
		if h != nil {
			handler = h(handler)
		}
	}

	return handler
}

type ManagerInterface interface {
	CreateChannel(name string, errorCallback func(ctx context.Context, err error), config ConfigInterface) (ChannelInterface, error)
	Ready() error
}
