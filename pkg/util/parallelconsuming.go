package util

import (
	"context"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
)

// TODO: use template
type Producer func() interface{}
type Consumer func(obj interface{})

type ParallelConsumingProcessor struct {
	producer Producer
	consumer Consumer
	// If we don't mind relying on k8s library, then use k8s.io/apimachinery/pkg/util.Group
	wait wait.Group
}

func NewParallelConsumingProcessor(producer Producer, consumer Consumer) *ParallelConsumingProcessor {
	return &ParallelConsumingProcessor{
		producer: producer,
		consumer: consumer,
		wait:     wait.Group{},
	}
}

func (c *ParallelConsumingProcessor) Start(consumerNum int, ctx context.Context) {
	for i := 0; i < consumerNum; i++ {
		c.wait.StartWithContext(ctx, c.worker)
	}
	c.wait.Wait()
}

func (c *ParallelConsumingProcessor) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			c.process(ctx)
		}
	}
}

func (c *ParallelConsumingProcessor) process(ctx context.Context) {
	defer func() {
		recover()
	}()

	defer runtime.HandleCrash()

	product := c.producer()
	select {
	case <-ctx.Done():
		return
	default:
		c.consumer(product)
	}
}
