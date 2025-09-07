package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"practice/redisengine"
	"time"

	"github.com/redis/go-redis/v9"
)

type DelayQueue struct {
	Queue
	DelayDuration time.Duration
}

func NewDelayQueue(name string, redisEngine *redisengine.RedisEngine, delayDuration time.Duration) *DelayQueue {
	queue := Queue{
		name:          name,
		redisEngine:   redisEngine,
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
	result, err := q.redisEngine.RunScript(ctx, q.enqueueScript, []string{q.redisEngine.GetName(), queueKey}, taskKey, taskData, task.Created.Add(q.DelayDuration).UnixMilli(), task.ID)
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
	taskID, err := q.redisEngine.ZRangeByScore(ctx, q.getQueueKey(), "-inf", fmt.Sprintf("%d", currentTime), 0, 1)
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

	taskData, err := q.redisEngine.RunScript(ctx, q.dequeueScript, []string{q.redisEngine.GetName()}, taskKey)
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
