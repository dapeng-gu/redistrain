package taskstruct

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"time"
)

type Task struct {
	ID            string                 `json:"id"`             // 任务ID
	Type          string                 `json:"type"`           // 任务类型
	Payload       map[string]interface{} `json:"payload"`        // 任务负载
	MaxRetry      int                    `json:"max_retry"`      // 最大重试次数
	Created       time.Time              `json:"created"`        // 创建时间
	Retry         int                    `json:"retry"`          // 重试次数
	Status        TaskStatus             `json:"status"`         // 任务状态
	GroupKey      string                 `json:"group_key"`      // 分组键
	MaxBatchSize  int                    `json:"max_batch_size"` // 最大批处理大小
	MaxWaitTime   int                    `json:"max_wait_time"`  // 最大等待时间(秒)
	IsAggregated  bool                   `json:"is_aggregated"`  // 是否为聚合任务
	OriginalTasks []string               `json:"original_tasks"` // 原始任务ID列表
}

type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"    // 待处理
	TaskStatusProcessing TaskStatus = "processing" // 处理中
	TaskStatusCompleted  TaskStatus = "completed"  // 已完成
	TaskStatusFailed     TaskStatus = "failed"     // 失败
	TaskStatusRetrying   TaskStatus = "retrying"   // 重试中
	TaskStatusDeadLetter TaskStatus = "dead"       // 死信
)

// BatchStatus 批处理状态
type BatchStatus string

const (
	BatchStatusAllSuccess    BatchStatus = "all_success"    // 全部成功
	BatchStatusAllFailed     BatchStatus = "all_failed"     // 全部失败
	BatchStatusPartialFailed BatchStatus = "partial_failed" // 部分失败
)

// BatchResult 批处理结果
type BatchResult struct {
	AggregatedTaskID string      `json:"aggregated_task_id"` // 聚合任务ID
	TotalTasks       int         `json:"total_tasks"`        // 总任务数
	SuccessfulTasks  []string    `json:"successful_tasks"`   // 成功的任务ID列表
	FailedTasks      []string    `json:"failed_tasks"`       // 失败的任务ID列表
	Status           BatchStatus `json:"status"`             // 批处理状态
	ProcessedAt      time.Time   `json:"processed_at"`       // 处理时间
}

func (t *Task) GetTaskKey() string {
	return fmt.Sprintf("task:%s", t.ID)
}

// GetDeduplicationKey 生成任务去重键
func (t *Task) GetDeduplicationKey() string {
	payloadBytes, err := json.Marshal(t.Payload)
	if err != nil {
		return fmt.Sprintf("dedup:%s", t.ID)
	}

	hash := md5.Sum(payloadBytes)
	return fmt.Sprintf("dedup:%x", hash)
}

func (t *Task) ProgressTask() {
	fmt.Printf("任务%s 处理中 \n", t.ID)
	t.Status = TaskStatusProcessing
	time.Sleep(time.Second * 1)
	t.Status = TaskStatusCompleted
}

func (t *Task) ProgressFailedTask() {
	fmt.Printf("任务%s 处理中，假设执行失败 \n", t.ID)
	t.Status = TaskStatusFailed
}

// ProcessAggregatedTask 处理聚合任务
func (t *Task) ProcessAggregatedTask() *BatchResult {
	if !t.IsAggregated {
		fmt.Printf("任务%s 不是聚合任务\n", t.ID)
		return nil
	}

	t.Status = TaskStatusProcessing

	result := &BatchResult{
		AggregatedTaskID: t.ID,
		TotalTasks:       len(t.OriginalTasks),
		SuccessfulTasks:  []string{},
		FailedTasks:      []string{},
		ProcessedAt:      time.Now(),
	}

	// 模拟批处理执行
	for i, originalTaskID := range t.OriginalTasks {
		fmt.Printf("  处理原始任务 %s (%d/%d)\n", originalTaskID, i+1, len(t.OriginalTasks))

		// 模拟不同的执行结果
		success := t.simulateTaskExecution(i)

		if success {
			result.SuccessfulTasks = append(result.SuccessfulTasks, originalTaskID)
		} else {
			result.FailedTasks = append(result.FailedTasks, originalTaskID)
		}

		// 模拟处理时间
		time.Sleep(100 * time.Millisecond)
	}

	if len(result.FailedTasks) == 0 {
		t.Status = TaskStatusCompleted
		result.Status = BatchStatusAllSuccess
		fmt.Printf("聚合任务%s 全部成功，成功%d个\n", t.ID, len(result.SuccessfulTasks))
	} else if len(result.SuccessfulTasks) == 0 {
		t.Status = TaskStatusFailed
		result.Status = BatchStatusAllFailed
		fmt.Printf("聚合任务%s 全部失败，失败%d个\n", t.ID, len(result.FailedTasks))
	} else {
		t.Status = TaskStatusFailed
		result.Status = BatchStatusPartialFailed
		fmt.Printf("聚合任务%s 部分失败，成功%d个，失败%d个\n", t.ID, len(result.SuccessfulTasks), len(result.FailedTasks))
	}

	return result
}

// simulateTaskExecution 模拟单个任务的执行结果
func (t *Task) simulateTaskExecution(index int) bool {
	switch t.GroupKey {
	case "email_batch":
		// 邮件批处理：90%成功率
		return (index+1)%10 != 0
	case "data_sync":
		// 数据同步：70%成功率
		return (index+1)%10 <= 7
	case "report_gen":
		// 报表生成：50%成功率
		return (index+1)%2 == 0
	case "test_fail_all":
		// 测试全部失败场景
		return false
	case "test_success_all":
		// 测试全部成功场景
		return true
	default:
		// 默认80%成功率
		return (index+1)%5 != 0
	}
}

func CreateTask(mark string, count int) []Task {
	tasks := []Task{}
	for i := 0; i < count; i++ {
		tasks = append(tasks, Task{
			ID:       fmt.Sprintf("task_%s_%d", mark, i),
			Type:     "test",
			Payload:  map[string]interface{}{"test01": "test01", "test02": "test02"},
			MaxRetry: 3,
			Created:  time.Now(),
			Status:   TaskStatusPending,
		})
	}
	return tasks
}
