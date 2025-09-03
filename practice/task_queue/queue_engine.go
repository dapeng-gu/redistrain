package task_queue

import (
	"github.com/redis/go-redis/v9"
)

type QueueEngine struct {
	client      redis.UniversalClient
	engine_name string
}

func NewQueueEngine(client redis.UniversalClient, engine_name string) *QueueEngine {
	return &QueueEngine{client: client, engine_name: engine_name}
}

func (engine *QueueEngine) GetName() string {
	return engine.engine_name
}
