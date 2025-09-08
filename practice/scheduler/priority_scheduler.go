// 普通优先级调度器

// 根据优先级调度任务

package scheduler

import (
	"context"
	"fmt"
	"practice/queue"
	"practice/redisengine"
	"practice/taskstruct"
	"strconv"
)

type queueConfig struct {
	queue *queue.Queue

	priority      int
	weightCurrent int
	totalTask     int
}

type SchedulerMode string

const (
	SchedulerPriority SchedulerMode = "priority"
	SchedulerWeight   SchedulerMode = "weight"
)

type PriorityScheduler struct {
	name string
	mode SchedulerMode

	redisEngine    *redisengine.RedisEngine
	queueConfigMap map[string]*queueConfig

	totalWeight int
}

func NewPriorityScheduler(mode SchedulerMode, name string, redisEngine *redisengine.RedisEngine) *PriorityScheduler {
	return &PriorityScheduler{
		name:           name,
		mode:           mode,
		queueConfigMap: make(map[string]*queueConfig),
		redisEngine:    redisEngine,
		totalWeight:    0,
	}
}

func (ps *PriorityScheduler) GetTask(ctx context.Context) (*taskstruct.Task, error) {
	switch ps.mode {
	case SchedulerWeight:
		return ps.getTaskByWeight(ctx)
	case SchedulerPriority:
		return ps.getTaskByPriority(ctx)
	}
	return nil, fmt.Errorf("无效模式 %s", ps.mode)
}

func (ps *PriorityScheduler) getTaskByPriority(ctx context.Context) (*taskstruct.Task, error) {
	queueKeys, err := ps.redisEngine.ZRevRangeByScore(ctx, ps.getSchedulerKey(), "+inf", "-inf", 0, -1)
	if err != nil {
		return nil, err
	}
	for _, queueKey := range queueKeys {
		queueConfig, ok := ps.queueConfigMap[queueKey]
		if !ok {
			return nil, fmt.Errorf("%s not found", queueKey)
		}
		task, err := queueConfig.queue.DequeueTask(ctx)
		if err != nil {
			return nil, err
		}
		if task != nil {
			return task, nil
		}
	}

	return nil, fmt.Errorf("没有任务")
}

func (ps *PriorityScheduler) AddQueue(ctx context.Context, queue *queue.Queue, priority int) error {
	schedulerKey := ps.getSchedulerKey()
	queueKey := ps.getQueueKey(queue)

	queueConfig := &queueConfig{
		queue:         queue,
		priority:      priority,
		weightCurrent: priority,
		totalTask:     0,
	}
	ps.queueConfigMap[queueKey] = queueConfig

	switch ps.mode {
	case SchedulerPriority:
		score := float64(queueConfig.priority)
		err := ps.redisEngine.ZAdd(ctx, schedulerKey, score, queueKey)
		if err != nil {
			return err
		}
		return nil
	case SchedulerWeight:
		ps.totalWeight += priority
		return nil
	}
	return fmt.Errorf("无效模式 %s", ps.mode)
}

func (ps *PriorityScheduler) getQueueKey(queue *queue.Queue) string {
	return fmt.Sprintf("%s:%s", ps.name, queue.GetQueueKey())
}

func (ps *PriorityScheduler) getSchedulerKey() string {
	return fmt.Sprintf("scheduler:%s", ps.name)
}

func (ps *PriorityScheduler) getWeightKey() string {
	return fmt.Sprintf("scheduler:weight:%s", ps.name)
}

// 加权轮询算法获取任务
func (ps *PriorityScheduler) getTaskByWeight(ctx context.Context) (*taskstruct.Task, error) {
	weightKey := ps.getWeightKey()

	fieldIncrements := make(map[string]int64)
	for queueKey, queueConfig := range ps.queueConfigMap {
		fieldIncrements[queueKey] = int64(queueConfig.priority)
	}

	_, err := ps.redisEngine.HIncrByBatch(ctx, weightKey, fieldIncrements)
	if err != nil {
		return nil, fmt.Errorf("批量更新权重失败: %v", err)
	}

	currentWeights, err := ps.redisEngine.HGetAll(ctx, weightKey)
	if err != nil {
		return nil, err
	}

	selectedQueue, err := ps.selectMaxWeightQueue(currentWeights)
	if err != nil {
		return nil, err
	}

	queueConfig, ok := ps.queueConfigMap[selectedQueue]
	if !ok {
		return nil, fmt.Errorf("队列配置不存在: %s", selectedQueue)
	}

	task, err := queueConfig.queue.DequeueTask(ctx)
	if err != nil {
		return nil, err
	}

	if task != nil {
		_, err = ps.redisEngine.HIncrBy(ctx, weightKey, selectedQueue, -int64(ps.totalWeight))
		if err != nil {
			return nil, fmt.Errorf("减少权重失败: %v", err)
		}
		return task, nil
	}

	return ps.fallbackToOtherQueues(ctx, selectedQueue)
}

// 选择权重最高的队列
func (ps *PriorityScheduler) selectMaxWeightQueue(currentWeights map[string]string) (string, error) {
	var selectedQueue string
	var maxWeight int64 = -1

	for queueKey, weightStr := range currentWeights {
		// 检查队列是否还在配置中
		if _, exists := ps.queueConfigMap[queueKey]; !exists {
			continue
		}

		weight, err := strconv.ParseInt(weightStr, 10, 64)
		if err != nil {
			return "", fmt.Errorf("解析权重失败: %v", err)
		}

		if weight > maxWeight {
			maxWeight = weight
			selectedQueue = queueKey
		}
	}

	if selectedQueue == "" {
		return "", fmt.Errorf("没有可用队列")
	}

	return selectedQueue, nil
}

func (ps *PriorityScheduler) fallbackToOtherQueues(ctx context.Context, excludeQueue string) (*taskstruct.Task, error) {
	weightKey := ps.getWeightKey()
	currentWeights, err := ps.redisEngine.HGetAll(ctx, weightKey)
	if err != nil {
		return nil, err
	}

	for queueKey := range currentWeights {
		if queueKey == excludeQueue {
			continue
		}

		queueConfig, ok := ps.queueConfigMap[queueKey]
		if !ok {
			continue
		}

		task, err := queueConfig.queue.DequeueTask(ctx)
		if err != nil {
			continue
		}

		if task != nil {
			_, err = ps.redisEngine.HIncrBy(ctx, weightKey, queueKey, -int64(ps.totalWeight))
			if err != nil {
				return nil, fmt.Errorf("减少权重失败: %v", err)
			}

			queueConfig.totalTask++
			return task, nil
		}
	}

	return nil, fmt.Errorf("所有队列都没有任务")
}
