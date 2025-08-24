package main

import (
	"context"
	"fmt"
	"practice/task_queue"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	queue_name := "default"

	task := &task_queue.Task{
		ID:       "task_001",
		Type:     "test",
		Payload:  map[string]interface{}{"test01": "test01", "test02": "test02"},
		MaxRetry: 3,
		Queue:    queue_name,
		Created:  time.Now(),
	}

	storage := task_queue.NewTaskStorage(redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "123456",
	}))
	ctx := context.Background()

	storage.EnqueueTask(ctx, task)

	fmt.Scanln()

	task, err := storage.DequeueTask(ctx, queue_name)
	if err != nil {
		fmt.Println("出队失败:", err)
		return
	}
	fmt.Println("出队任务:", task)
}
