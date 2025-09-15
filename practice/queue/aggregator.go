package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"practice/redisengine"
	"practice/taskstruct"

	"github.com/redis/go-redis/v9"
)

type Aggregator struct {
	redisEngine        *redisengine.RedisEngine
	queueName          string
	addToGroupScript   *redis.Script
	triggerBatchScript *redis.Script
}

func NewAggregator(redisEngine *redisengine.RedisEngine, queueName string) *Aggregator {
	return &Aggregator{
		redisEngine:        redisEngine,
		queueName:          queueName,
		addToGroupScript:   addToGroupScript,
		triggerBatchScript: triggerBatchScript,
	}
}

// GetGroupKey 获取分组键
func (a *Aggregator) GetGroupKey(groupKey string) string {
	return fmt.Sprintf("aggregator:groups:%s:%s", a.queueName, groupKey)
}

// GetTimerKey 获取定时器键
func (a *Aggregator) GetTimerKey(groupKey string) string {
	return fmt.Sprintf("aggregator:timer:%s:%s", a.queueName, groupKey)
}

// AddTaskToGroup 将任务添加到分组
func (a *Aggregator) AddTaskToGroup(ctx context.Context, task *taskstruct.Task) (*taskstruct.Task, error) {
	if task.GroupKey == "" {
		return nil, fmt.Errorf("任务缺少GroupKey")
	}

	groupKey := a.GetGroupKey(task.GroupKey)
	timerKey := a.GetTimerKey(task.GroupKey)

	taskData, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("序列化任务失败: %w", err)
	}

	// 使用Lua脚本原子性添加任务到分组并检查触发条件
	result, err := a.redisEngine.RunScript(ctx, a.addToGroupScript,
		[]string{groupKey, timerKey},
		task.ID, taskData, task.MaxBatchSize, task.MaxWaitTime)
	if err != nil {
		return nil, fmt.Errorf("添加任务到分组失败: %w", err)
	}

	switch result.(int64) {
	case 1:
		fmt.Printf("任务 %s 已添加到分组 %s\n", task.ID, task.GroupKey)
		return nil, nil
	case 2:
		fmt.Printf("分组 %s 达到批处理条件，开始聚合\n", task.GroupKey)
		return a.TriggerBatch(ctx, task.GroupKey)
	default:
		return nil, fmt.Errorf("未知的脚本返回值: %v", result)
	}
}

// TriggerBatch 触发批处理
func (a *Aggregator) TriggerBatch(ctx context.Context, groupKey string) (*taskstruct.Task, error) {
	groupKeyFull := a.GetGroupKey(groupKey)
	timerKey := a.GetTimerKey(groupKey)

	result, err := a.redisEngine.RunScript(ctx, a.triggerBatchScript,
		[]string{groupKeyFull, timerKey})
	if err != nil {
		return nil, fmt.Errorf("触发批处理失败: %w", err)
	}

	taskIDs, ok := result.([]interface{})
	if !ok || len(taskIDs) == 0 {
		return nil, fmt.Errorf("分组中没有任务")
	}

	aggregatedTask := &taskstruct.Task{
		ID:            fmt.Sprintf("batch_%s_%d", groupKey, time.Now().Unix()),
		Type:          "aggregated",
		Payload:       map[string]interface{}{"group_key": groupKey, "task_count": len(taskIDs)},
		MaxRetry:      3,
		Created:       time.Now(),
		Status:        taskstruct.TaskStatusPending,
		GroupKey:      groupKey,
		IsAggregated:  true,
		OriginalTasks: make([]string, len(taskIDs)),
	}

	for i, taskID := range taskIDs {
		aggregatedTask.OriginalTasks[i] = taskID.(string)
	}

	fmt.Printf("创建聚合任务 %s，包含 %d 个原始任务\n", aggregatedTask.ID, len(taskIDs))
	return aggregatedTask, nil
}
