package main

import (
	"context"
	"fmt"
	"practice/task_queue"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {

	engine := task_queue.NewQueueEngine(redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}), "default")

	task1 := &task_queue.Task{
		ID:       "task_001",
		Type:     "test",
		Payload:  map[string]interface{}{"test01": "test01", "test02": "test02"},
		MaxRetry: 3,
		Created:  time.Now(),
	}

	task2 := &task_queue.Task{
		ID:       "task_delay_001",
		Type:     "test",
		Payload:  map[string]interface{}{"test01": "test01", "test02": "test02"},
		MaxRetry: 3,
		Created:  time.Now(),
	}

	test_queue(engine, task1)
	test_deplay_queue(engine, task2)
}

func test_queue(engine *task_queue.QueueEngine, task *task_queue.Task) {
	queue_name := "queue_test"

	queue := task_queue.NewQueue(queue_name, engine)
	ctx := context.Background()

	err := queue.EnqueueTask(ctx, task)
	if err != nil {
		fmt.Println("入队失败:", err)
		return
	}
	fmt.Println("任务已入队")
	fmt.Scanln()

	task, err = queue.DequeueTask(ctx)
	if err != nil {
		fmt.Println("出队失败:", err)
		return
	}
	fmt.Println("出队任务:", task)
}

func test_deplay_queue(engine *task_queue.QueueEngine, task *task_queue.Task) {
	queue_name := "delay_queue_test"

	queue := task_queue.NewDelayQueue(queue_name, engine, time.Second*10)
	ctx := context.Background()

	err := queue.EnqueueTask(ctx, task)
	if err != nil {
		fmt.Println("入队失败:", err)
		return
	}
	fmt.Println("任务已入队")
	fmt.Scanln()

	task, err = queue.DequeueTask(ctx)
	if err != nil {
		fmt.Println("出队失败:", err)
		return
	}
	fmt.Println("出队任务:", task)
}
