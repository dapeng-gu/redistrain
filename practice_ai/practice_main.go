package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Task 表示要执行的工作单元 - 参考 asynq/asynq.go
type Task struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Payload  map[string]interface{} `json:"payload"`
	MaxRetry int                    `json:"max_retry"`
	Queue    string                 `json:"queue"`
	Created  time.Time              `json:"created"`
}

// TaskStorage Redis任务存储器
type TaskStorage struct {
	client redis.UniversalClient
}

// NewTaskStorage 创建新的任务存储器
func NewTaskStorage(client redis.UniversalClient) *TaskStorage {
	return &TaskStorage{client: client}
}

// ==================== 🎯 你需要编码的核心功能区域 ====================

// EnqueueTask 将任务入队 - 参考 asynq/internal/rdb/rdb.go enqueueCmd
func (ts *TaskStorage) EnqueueTask(ctx context.Context, task *Task) error {
	log.Printf("🔄 开始入队任务: ID=%s, Type=%s, Queue=%s", task.ID, task.Type, task.Queue)
	
	// TODO 1: 将任务序列化为JSON
	// 提示：使用 json.Marshal(task) 将任务结构体转换为JSON字节数组
	// 变量名建议：taskData
	// ========== 在此编写代码 ==========
	
	
	// ================================
	
	log.Printf("📝 任务序列化成功，数据长度: %d bytes", len(taskData))

	// TODO 2: 存储任务数据到Redis Hash
	// 提示：使用 ts.client.HSet(ctx, key, field, value).Err()
	// key格式：fmt.Sprintf("task:%s", task.ID)
	// field：固定为 "data"
	// value：上面序列化的taskData
	// ========== 在此编写代码 ==========
	
	
	// ================================
	
	log.Printf("💾 任务数据已存储到Redis Hash: %s", taskKey)

	// TODO 3: 将任务ID推入队列
	// 提示：使用 ts.client.LPush(ctx, queueKey, taskID).Err()
	// queueKey格式：fmt.Sprintf("queue:%s", task.Queue)
	// ========== 在此编写代码 ==========
	
	
	// ================================
	
	log.Printf("📥 任务ID已推入队列: %s -> %s", task.ID, queueKey)
	log.Printf("✅ 任务入队完成: ID=%s, Queue=%s", task.ID, task.Queue)
	return nil
}

// DequeueTask 从队列中取出任务 - 参考 asynq processor 逻辑
func (ts *TaskStorage) DequeueTask(ctx context.Context, queueName string) (*Task, error) {
	log.Printf("🔄 开始从队列出队: %s", queueName)
	
	queueKey := fmt.Sprintf("queue:%s", queueName)
	
	// TODO 4: 从队列右端弹出任务ID
	// 提示：使用 ts.client.RPop(ctx, queueKey).Result()
	// 需要处理 redis.Nil 错误（表示队列为空）
	// ========== 在此编写代码 ==========
	
	
	// ================================
	
	log.Printf("📤 从队列取出任务ID: %s", taskID)

	// TODO 5: 获取任务数据
	// 提示：使用 ts.client.HGet(ctx, taskKey, "data").Result()
	// taskKey格式：fmt.Sprintf("task:%s", taskID)
	// ========== 在此编写代码 ==========
	
	
	// ================================
	
	log.Printf("💾 从Redis Hash获取任务数据: %s (长度: %d bytes)", taskKey, len(taskData))

	// TODO 6: 反序列化任务数据
	// 提示：使用 json.Unmarshal([]byte(taskData), &task)
	// 需要先声明 var task Task
	// ========== 在此编写代码 ==========
	
	
	// ================================
	
	log.Printf("📝 任务反序列化成功: Type=%s", task.Type)
	log.Printf("✅ 任务出队完成: ID=%s, Type=%s", task.ID, task.Type)
	return &task, nil
}

// GetQueueLength 获取队列长度
func (ts *TaskStorage) GetQueueLength(ctx context.Context, queueName string) (int64, error) {
	// TODO 7: 获取队列长度
	// 提示：使用 ts.client.LLen(ctx, queueKey).Result()
	// queueKey格式：fmt.Sprintf("queue:%s", queueName)
	// ========== 在此编写代码 ==========
	
	
	// ================================
}

// ListQueues 列出所有队列
func (ts *TaskStorage) ListQueues(ctx context.Context) ([]string, error) {
	// TODO 8: 获取所有队列键
	// 提示：使用 ts.client.Keys(ctx, "queue:*").Result()
	// ========== 在此编写代码 ==========
	
	
	// ================================

	queues := make([]string, len(keys))
	for i, key := range keys {
		// TODO 9: 移除 "queue:" 前缀
		// 提示：队列名 = key[6:]（因为"queue:"长度为6）
		// ========== 在此编写代码 ==========
		
		
		// ================================
	}
	return queues, nil
}

// ==================== 🛠️ 辅助功能（已实现，供参考） ====================

// VerifyRedisData 验证Redis中存储的数据
func (ts *TaskStorage) VerifyRedisData(ctx context.Context, taskIDs []string) {
	fmt.Println("🔍 验证Redis中的任务数据:")
	
	for _, taskID := range taskIDs {
		taskKey := fmt.Sprintf("task:%s", taskID)
		
		// 检查Hash是否存在
		exists, err := ts.client.Exists(ctx, taskKey).Result()
		if err != nil {
			fmt.Printf("❌ 检查任务 %s 失败: %v\n", taskID, err)
			continue
		}
		
		if exists == 0 {
			fmt.Printf("❌ 任务 %s 不存在于Redis\n", taskID)
			continue
		}
		
		// 获取任务数据
		taskData, err := ts.client.HGet(ctx, taskKey, "data").Result()
		if err != nil {
			fmt.Printf("❌ 获取任务 %s 数据失败: %v\n", taskID, err)
			continue
		}
		
		// 验证JSON格式
		var task Task
		if err := json.Unmarshal([]byte(taskData), &task); err != nil {
			fmt.Printf("❌ 任务 %s JSON格式无效: %v\n", taskID, err)
			continue
		}
		
		fmt.Printf("✅ 任务 %s 验证成功 - Type: %s, Queue: %s\n", taskID, task.Type, task.Queue)
		
		// 验证队列中是否包含该任务ID
		queueKey := fmt.Sprintf("queue:%s", task.Queue)
		members, err := ts.client.LRange(ctx, queueKey, 0, -1).Result()
		if err != nil {
			fmt.Printf("⚠️  检查队列 %s 失败: %v\n", task.Queue, err)
			continue
		}
		
		found := false
		for _, member := range members {
			if member == taskID {
				found = true
				break
			}
		}
		
		if found {
			fmt.Printf("✅ 任务 %s 在队列 %s 中找到\n", taskID, task.Queue)
		} else {
			fmt.Printf("⚠️  任务 %s 不在队列 %s 中\n", taskID, task.Queue)
		}
	}
}

// CleanupDemo 清理演示数据
func (ts *TaskStorage) CleanupDemo(ctx context.Context) {
	fmt.Println("🧹 开始清理演示数据...")
	
	// 清理所有任务Hash
	taskKeys, err := ts.client.Keys(ctx, "task:*").Result()
	if err != nil {
		fmt.Printf("❌ 获取任务键失败: %v\n", err)
	} else {
		for _, key := range taskKeys {
			if err := ts.client.Del(ctx, key).Err(); err != nil {
				fmt.Printf("❌ 删除 %s 失败: %v\n", key, err)
			} else {
				fmt.Printf("🗑️  已删除任务数据: %s\n", key)
			}
		}
	}
	
	// 清理所有队列
	queueKeys, err := ts.client.Keys(ctx, "queue:*").Result()
	if err != nil {
		fmt.Printf("❌ 获取队列键失败: %v\n", err)
	} else {
		for _, key := range queueKeys {
			if err := ts.client.Del(ctx, key).Err(); err != nil {
				fmt.Printf("❌ 删除 %s 失败: %v\n", key, err)
			} else {
				fmt.Printf("🗑️  已删除队列: %s\n", key)
			}
		}
	}
	
	fmt.Println("✅ 演示数据清理完成")
}

// ==================== 🚀 主程序（已实现） ====================

func main() {
	// 获取Redis配置
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	
	redisPassword := os.Getenv("REDIS_PASSWORD")
	
	// 连接Redis
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       0,
	})
	defer client.Close()

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Printf("❌ Redis连接失败: %v\n", err)
		fmt.Println("💡 解决方案:")
		fmt.Println("   1. 确保Redis服务器正在运行")
		fmt.Println("   2. 如果需要密码认证，请设置环境变量:")
		fmt.Println("      set REDIS_PASSWORD=your_password")
		fmt.Println("   3. 如果Redis运行在其他地址，请设置:")
		fmt.Println("      set REDIS_ADDR=your_redis_address:port")
		os.Exit(1)
	}
	
	fmt.Println("✅ Redis连接成功!")

	// 创建任务存储器
	storage := NewTaskStorage(client)

	// 创建测试任务
	task1 := &Task{
		ID:       "task_001",
		Type:     "email:send",
		Payload:  map[string]interface{}{"to": "user@example.com", "subject": "Welcome"},
		MaxRetry: 3,
		Queue:    "default",
		Created:  time.Now(),
	}

	task2 := &Task{
		ID:       "task_002",
		Type:     "image:resize",
		Payload:  map[string]interface{}{"url": "https://example.com/image.jpg", "width": 800},
		MaxRetry: 5,
		Queue:    "high",
		Created:  time.Now(),
	}

	// 演示基础操作
	fmt.Println("🚀 开始Redis任务存储演示...")
	fmt.Println(strings.Repeat("=", 50))

	// 1. 入队任务
	fmt.Println("\n📥 第一步：任务入队")
	fmt.Println(strings.Repeat("-", 30))
	if err := storage.EnqueueTask(ctx, task1); err != nil {
		log.Fatal("入队失败:", err)
	}
	fmt.Println()
	if err := storage.EnqueueTask(ctx, task2); err != nil {
		log.Fatal("入队失败:", err)
	}

	// 2. 验证Redis中的数据
	fmt.Println("\n🔍 第二步：验证Redis数据")
	fmt.Println(strings.Repeat("-", 30))
	storage.VerifyRedisData(ctx, []string{task1.ID, task2.ID})

	// 3. 查看队列状态
	fmt.Println("\n📊 第三步：队列状态检查")
	fmt.Println(strings.Repeat("-", 30))
	queues, _ := storage.ListQueues(ctx)
	for _, queue := range queues {
		length, _ := storage.GetQueueLength(ctx, queue)
		fmt.Printf("📋 队列 '%s': %d 个任务\n", queue, length)
	}

	// 4. 出队任务
	fmt.Println("\n📤 第四步：任务出队")
	fmt.Println(strings.Repeat("-", 30))
	if task, err := storage.DequeueTask(ctx, "default"); err != nil {
		log.Fatal("出队失败:", err)
	} else if task != nil {
		fmt.Printf("🎯 处理任务: %s\n", task.Type)
		fmt.Printf("   载荷: %+v\n", task.Payload)
		fmt.Printf("   创建时间: %s\n", task.Created.Format("2006-01-02 15:04:05"))
	}

	fmt.Println()
	if task, err := storage.DequeueTask(ctx, "high"); err != nil {
		log.Fatal("出队失败:", err)
	} else if task != nil {
		fmt.Printf("🎯 处理任务: %s\n", task.Type)
		fmt.Printf("   载荷: %+v\n", task.Payload)
		fmt.Printf("   创建时间: %s\n", task.Created.Format("2006-01-02 15:04:05"))
	}

	// 5. 最终队列状态
	fmt.Println("\n📊 第五步：最终队列状态")
	fmt.Println(strings.Repeat("-", 30))
	queues, _ = storage.ListQueues(ctx)
	for _, queue := range queues {
		length, _ := storage.GetQueueLength(ctx, queue)
		fmt.Printf("📋 队列 '%s': %d 个任务\n", queue, length)
	}

	// 6. 清理演示数据
	fmt.Println("\n🧹 第六步：清理演示数据")
	fmt.Println(strings.Repeat("-", 30))
	storage.CleanupDemo(ctx)

	fmt.Println("\n✨ 演示完成！功能验证成功 ✅")
	fmt.Println(strings.Repeat("=", 50))
}
