package taskstruct

import (
	"fmt"
	"time"
)

type Task struct {
	ID       string                 `json:"id"`        // 任务ID
	Type     string                 `json:"type"`      // 任务类型
	Payload  map[string]interface{} `json:"payload"`   // 任务负载
	MaxRetry int                    `json:"max_retry"` // 最大重试次数
	Created  time.Time              `json:"created"`   // 创建时间
	Retry    int                    `json:"retry"`     // 重试次数
	Status   TaskStatus             `json:"status"`    // 任务状态
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

func (t *Task) GetTaskKey() string {
	return fmt.Sprintf("task:%s", t.ID)
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
