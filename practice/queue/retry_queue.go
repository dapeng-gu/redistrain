package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"practice/redisengine"
	"practice/taskstruct"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RetryQueue struct {
	Queue
	baseDelay time.Duration
	maxDelay  time.Duration
	maxRetry  int
	deadQueue *DeadQueue
	mutex     sync.Mutex
}

func NewRetryQueue(name string, redisEngine *redisengine.RedisEngine, delayDuration time.Duration, maxDelay time.Duration, maxRetry int) *RetryQueue {
	queue := Queue{
		name:          name,
		redisEngine:   redisEngine,
		queue_type:    "retry_queue",
		enqueueScript: delayEnqueueScript,
		dequeueScript: dequeueScript,
	}

	return &RetryQueue{
		Queue:     queue,
		baseDelay: delayDuration,
		maxDelay:  maxDelay,
		maxRetry:  maxRetry,
		deadQueue: NewDeadQueue("dead_queue", redisEngine),
	}
}

func (q *RetryQueue) getQueueKey() string {
	return fmt.Sprintf("%s:%s", q.queue_type, q.name)
}

func (q *RetryQueue) EnqueueTask(ctx context.Context, task *taskstruct.Task) error {
	task.Retry++
	if task.Retry > q.maxRetry {
		task.Status = taskstruct.TaskStatusDeadLetter
		return q.deadQueue.EnqueueTask(ctx, task)
	}
	task.Status = taskstruct.TaskStatusRetrying

	// 修正版本
	exponentialDelay := q.baseDelay * time.Duration(1<<uint(task.Retry))
	jitter := time.Duration(rand.Intn(int(exponentialDelay / 4)))
	delayDuration := exponentialDelay + jitter
	if delayDuration > q.maxDelay {
		delayDuration = q.maxDelay
	}

	taskKey := task.GetTaskKey()
	queueKey := q.getQueueKey()

	taskData, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("序列化任务失败: %w", err)
	}
	result, err := q.redisEngine.RunScript(ctx, q.enqueueScript, []string{q.redisEngine.GetName(), queueKey}, taskKey, taskData, task.Created.Add(delayDuration).UnixMilli(), task.ID)
	if err != nil {
		return fmt.Errorf("任务入队失败: %w", err)
	}

	if result.(int64) == 0 {
		fmt.Println("任务入队失败，任务已存在: ", task.ID)
		return nil
	}

	return nil
}

func (q *RetryQueue) DequeueTask(ctx context.Context) (*taskstruct.Task, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

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

	task := taskstruct.Task{ID: taskID[0]}

	taskKey := task.GetTaskKey()

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
