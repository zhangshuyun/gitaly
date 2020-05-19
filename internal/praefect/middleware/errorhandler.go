package middleware

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"google.golang.org/grpc"
)

func StreamErrorHandler(registry *protoregistry.Registry, errorTracker *Errors, node string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)

		mi, lookupErr := registry.LookupMethod(method)
		if err != nil {
			return nil, fmt.Errorf("error when looking up method: %w %v", err, lookupErr)
		}

		return newCatchErrorStreamer(stream, errorTracker, mi.Operation, node), err
	}
}

type CatchErrorStreamer struct {
	grpc.ClientStream
	errors    *Errors
	operation protoregistry.OpType
	node      string
}

func (c *CatchErrorStreamer) SendMsg(m interface{}) error {
	err := c.ClientStream.SendMsg(m)
	if err != nil {
		switch c.operation {
		case protoregistry.OpAccessor:
			c.errors.IncrReadErr(c.node)
		case protoregistry.OpMutator:
			c.errors.IncrWriteErr(c.node)
		}
	}

	return err
}

func (c *CatchErrorStreamer) RecvMsg(m interface{}) error {
	err := c.ClientStream.RecvMsg(m)
	if err != nil && err != io.EOF {
		switch c.operation {
		case protoregistry.OpAccessor:
			c.errors.IncrReadErr(c.node)
		case protoregistry.OpMutator:
			c.errors.IncrWriteErr(c.node)
		}
	}

	return err
}

func newCatchErrorStreamer(streamer grpc.ClientStream, errors *Errors, operation protoregistry.OpType, node string) *CatchErrorStreamer {
	return &CatchErrorStreamer{
		ClientStream: streamer,
		errors:       errors,
		operation:    operation,
		node:         node,
	}
}

type Errors struct {
	duration                      time.Duration
	m                             sync.RWMutex
	writeThreshold, readThreshold int
	readErrors, writeErrors       map[string][]int64
}

const (
	defaultWindowSeconds = 10
)

func NewErrors(windowSeconds, readThreshold, writeThreshold int) *Errors {
	if windowSeconds <= 0 {
		windowSeconds = 10
	}

	window := time.Duration(windowSeconds) * time.Second

	if readThreshold == 0 {
		readThreshold = 100 * defaultWindowSeconds
	}

	if writeThreshold == 0 {
		writeThreshold = 100 * defaultWindowSeconds
	}

	e := &Errors{
		duration:       window,
		readErrors:     make(map[string][]int64),
		writeErrors:    make(map[string][]int64),
		readThreshold:  readThreshold,
		writeThreshold: writeThreshold,
	}
	go e.PeriodicallyClear()

	return e
}

func (e *Errors) IncrReadErr(node string) {
	e.m.Lock()
	defer e.m.Unlock()

	if len(e.readErrors[node]) < e.readThreshold {
		e.readErrors[node] = append(e.readErrors[node], time.Now().UnixNano())
	}
}

func (e *Errors) IncrWriteErr(node string) {
	e.m.Lock()
	defer e.m.Unlock()

	if len(e.writeErrors[node]) < e.writeThreshold {
		e.writeErrors[node] = append(e.writeErrors[node], time.Now().UnixNano())
	}
}

func (e *Errors) ReadThresholdReached(node string) bool {
	e.m.RLock()
	defer e.m.RUnlock()

	return len(e.readErrors[node]) >= e.readThreshold
}

func (e *Errors) WriteThresholdReached(node string) bool {
	e.m.RLock()
	defer e.m.RUnlock()

	return len(e.writeErrors[node]) >= e.writeThreshold
}

func (e *Errors) PeriodicallyClear() {
	ticker := time.NewTicker(e.duration)
	for {
		start := time.Now()
		<-ticker.C
		e.clear(start)
	}
}

func (e *Errors) clear(olderThan time.Time) {
	e.m.Lock()
	defer e.m.Unlock()

	for node, nodeWriteErrors := range e.writeErrors {
		for i, writeErrorTime := range nodeWriteErrors {
			if time.Unix(0, writeErrorTime).Before(olderThan) {
				if i+1 == len(nodeWriteErrors) {
					e.writeErrors[node] = nil
				} else {
					e.writeErrors[node] = nodeWriteErrors[i+1:]
				}
				break
			}
		}
	}

	for node, nodeReadErrors := range e.readErrors {
		for i, readErrorTime := range nodeReadErrors {
			if time.Unix(0, readErrorTime).Before(olderThan) {
				if i+1 == len(nodeReadErrors) {
					e.readErrors[node] = nil
				} else {
					e.readErrors[node] = nodeReadErrors[i+1:]
				}
				break
			}
		}
	}
}
