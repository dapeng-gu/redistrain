package task_queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type DelayQueue struct {
	Queue
	DelayDuration time.Duration
}

func NewDelayQueue(name string, engine *QueueEngine, delayDuration time.Duration) *DelayQueue {
	queue := Queue{
		name:          name,
		engine:        engine,
		queue_type:    "delay_queue",
		enqueueScript: delayEnqueueScript,
		dequeueScript: dequeueScript,
	}

	return &DelayQueue{
		Queue:         queue,
		DelayDuration: delayDuration,
	}
}

func (q *DelayQueue) getQueueKey() string {
	return fmt.Sprintf("%s:%s", q.queue_type, q.name)
}

func (q *DelayQueue) EnqueueTask(ctx context.Context, task *Task) error {
	taskKey := task.getTaskKey()
	queueKey := q.getQueueKey()

	taskData, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("序列化任务失败: %w", err)
	}

	result, err := q.enqueueScript.Run(ctx, q.engine.client, []string{q.engine.GetName(), queueKey}, taskKey, taskData, task.Created.Add(q.DelayDuration).UnixMilli(), task.ID).Result()
	if err != nil {
		return fmt.Errorf("任务入队失败: %w", err)
	}

	if result.(int64) == 0 {
		fmt.Println("任务入队失败，任务已存在: ", task.ID)
		return nil
	}

	return nil
}

func (q *DelayQueue) DequeueTask(ctx context.Context) (*Task, error) {
	currentTime := time.Now().UnixMilli()
	taskID, err := q.engine.client.ZRangeByScore(ctx, q.getQueueKey(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", currentTime),
		Offset: 0,
		Count:  1,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			fmt.Printf("%s 没有就绪任务\n", q.getQueueKey())
			return nil, nil
		}
		return nil, fmt.Errorf("延迟队列出队失败: %w", err)
	}
	if len(taskID) == 0 {
		fmt.Printf("%s 没有就绪任务\n", q.getQueueKey())
		return nil, nil
	}

	task := Task{ID: taskID[0]}

	taskKey := task.getTaskKey()

	taskData, err := q.dequeueScript.Run(ctx, q.engine.client, []string{q.engine.GetName()}, taskKey).Result()
	if err != nil {
		if err == redis.Nil {
			fmt.Printf("%s 没有找到任务\n", taskKey)
			return nil, nil
		}
		return nil, err
	}

	if err := json.Unmarshal([]byte(taskData.(string)), &task); err != nil {
		return nil, err
	}
	return &task, nil
}
