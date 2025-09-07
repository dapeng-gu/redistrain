package main

import (
	"context"
	"fmt"
	queue "practice/queue"
	"practice/scheduler"
	"time"

	"practice/redisengine"

	"github.com/redis/go-redis/v9"
)

func main() {

	redisEngine := redisengine.NewRedisEngine(redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}), "default")

	// task1 := &queue.Task{
	// 	ID:       "task_001",
	// 	Type:     "test",
	// 	Payload:  map[string]interface{}{"test01": "test01", "test02": "test02"},
	// 	MaxRetry: 3,
	// 	Created:  time.Now(),
	// }

	// task2 := &queue.Task{
	// 	ID:       "task_delay_001",
	// 	Type:     "test",
	// 	Payload:  map[string]interface{}{"test01": "test01", "test02": "test02"},
	// 	MaxRetry: 3,
	// 	Created:  time.Now(),
	// }

	// test_queue(redisEngine, task1)
	// test_deplay_queue(redisEngine, task2)
	test_scheduler(redisEngine)

}

func test_queue(engine *redisengine.RedisEngine, task *queue.Task) {
	queue_name := "queue_test"

	queue := queue.NewQueue(queue_name, engine)
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

func test_deplay_queue(engine *redisengine.RedisEngine, task *queue.Task) {
	queue_name := "delay_queue_test"

	queue := queue.NewDelayQueue(queue_name, engine, time.Second*10)
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

func test_scheduler(engine *redisengine.RedisEngine) {
	ctx := context.Background()

	scheduler := scheduler.NewPriorityScheduler(scheduler.SchedulerPriority, "test_scheduler", engine)
	queue_1 := queue.NewQueue("queue_1", engine)
	queue_2 := queue.NewQueue("queue_2", engine)

	tasks_1 := queue.CreateTask("priority=1", 3)
	tasks_2 := queue.CreateTask("priority=2", 3)

	for _, test_task := range tasks_1 {
		err := queue_1.EnqueueTask(ctx, &test_task)
		if err != nil {
			fmt.Println("添加队列失败:", err)
			return
		}
	}

	for _, test_task := range tasks_2 {
		err := queue_2.EnqueueTask(ctx, &test_task)
		if err != nil {
			fmt.Println("添加队列失败:", err)
			return
		}
	}

	err := scheduler.AddQueue(ctx, queue_1, 99)
	if err != nil {
		fmt.Println("添加队列失败:", err)
		return
	}
	err = scheduler.AddQueue(ctx, queue_2, 1)
	if err != nil {
		fmt.Println("添加队列失败:", err)
		return
	}

	for i := 0; i < 6; i++ {
		task, err := scheduler.GetTask(ctx)
		if err != nil {
			fmt.Println("获取任务失败:", err)
			return
		}
		queue.ProgressTask(task)
	}

}
