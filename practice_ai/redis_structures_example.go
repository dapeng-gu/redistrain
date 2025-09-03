package task_queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStructuresDemo Redis 数据结构演示
type RedisStructuresDemo struct {
	client redis.UniversalClient
}

func NewRedisStructuresDemo(client redis.UniversalClient) *RedisStructuresDemo {
	return &RedisStructuresDemo{client: client}
}

// StringExample String 类型示例
func (r *RedisStructuresDemo) StringExample(ctx context.Context) {
	fmt.Println("=== String 类型示例 ===")
	
	// 设置字符串
	r.client.Set(ctx, "user:1001:name", "张三", 0)
	r.client.Set(ctx, "counter", 100, 0)
	
	// 获取字符串
	name, _ := r.client.Get(ctx, "user:1001:name").Result()
	fmt.Printf("用户名: %s\n", name)
	
	// 数字操作
	r.client.Incr(ctx, "counter")
	counter, _ := r.client.Get(ctx, "counter").Result()
	fmt.Printf("计数器: %s\n", counter)
}

// HashExample Hash 类型示例
func (r *RedisStructuresDemo) HashExample(ctx context.Context) {
	fmt.Println("\n=== Hash 类型示例 ===")
	
	// 设置用户信息
	r.client.HMSet(ctx, "user:1001", map[string]interface{}{
		"name":  "张三",
		"age":   25,
		"email": "zhangsan@example.com",
		"city":  "北京",
	})
	
	// 获取单个字段
	name, _ := r.client.HGet(ctx, "user:1001", "name").Result()
	fmt.Printf("姓名: %s\n", name)
	
	// 获取所有字段
	userInfo, _ := r.client.HGetAll(ctx, "user:1001").Result()
	fmt.Printf("用户信息: %+v\n", userInfo)
}

// ListExample List 类型示例
func (r *RedisStructuresDemo) ListExample(ctx context.Context) {
	fmt.Println("\n=== List 类型示例 ===")
	
	// 任务队列
	r.client.LPush(ctx, "task_queue", "task1", "task2", "task3")
	
	// 获取队列长度
	length, _ := r.client.LLen(ctx, "task_queue").Result()
	fmt.Printf("队列长度: %d\n", length)
	
	// 处理任务（FIFO）
	for i := 0; i < 3; i++ {
		task, err := r.client.RPop(ctx, "task_queue").Result()
		if err == nil {
			fmt.Printf("处理任务: %s\n", task)
		}
	}
}

// SetExample Set 类型示例
func (r *RedisStructuresDemo) SetExample(ctx context.Context) {
	fmt.Println("\n=== Set 类型示例 ===")
	
	// 用户标签
	r.client.SAdd(ctx, "user:1001:tags", "程序员", "北京", "Go语言", "Redis")
	r.client.SAdd(ctx, "user:1002:tags", "设计师", "上海", "UI", "Redis")
	
	// 获取用户标签
	tags, _ := r.client.SMembers(ctx, "user:1001:tags").Result()
	fmt.Printf("用户1001标签: %v\n", tags)
	
	// 求交集（共同标签）
	common, _ := r.client.SInter(ctx, "user:1001:tags", "user:1002:tags").Result()
	fmt.Printf("共同标签: %v\n", common)
}

// SortedSetExample Sorted Set 类型示例
func (r *RedisStructuresDemo) SortedSetExample(ctx context.Context) {
	fmt.Println("\n=== Sorted Set 类型示例 ===")
	
	// 排行榜
	r.client.ZAdd(ctx, "leaderboard", 
		redis.Z{Score: 1000, Member: "张三"},
		redis.Z{Score: 950, Member: "李四"},
		redis.Z{Score: 1200, Member: "王五"},
		redis.Z{Score: 800, Member: "赵六"},
	)
	
	// 获取排行榜（按分数降序）
	top3, _ := r.client.ZRevRange(ctx, "leaderboard", 0, 2).Result()
	fmt.Printf("前三名: %v\n", top3)
	
	// 获取指定用户排名
	rank, _ := r.client.ZRevRank(ctx, "leaderboard", "张三").Result()
	fmt.Printf("张三排名: %d\n", rank+1) // Redis 排名从0开始
}

// DelayQueueExample 延迟队列示例（使用 Sorted Set）
func (r *RedisStructuresDemo) DelayQueueExample(ctx context.Context) {
	fmt.Println("\n=== 延迟队列示例 ===")
	
	now := time.Now()
	
	// 添加延迟任务
	r.client.ZAdd(ctx, "delay_queue",
		redis.Z{Score: float64(now.Add(5*time.Second).Unix()), Member: "task1"},
		redis.Z{Score: float64(now.Add(10*time.Second).Unix()), Member: "task2"},
		redis.Z{Score: float64(now.Add(15*time.Second).Unix()), Member: "task3"},
	)
	
	fmt.Println("已添加延迟任务，等待处理...")
	
	// 模拟处理延迟任务
	for i := 0; i < 20; i++ {
		currentTime := time.Now().Unix()
		
		// 获取已到期的任务
		readyTasks, _ := r.client.ZRangeByScore(ctx, "delay_queue", &redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf("%d", currentTime),
		}).Result()
		
		if len(readyTasks) > 0 {
			fmt.Printf("处理就绪任务: %v\n", readyTasks)
			// 移除已处理的任务
			for _, task := range readyTasks {
				r.client.ZRem(ctx, "delay_queue", task)
			}
		}
		
		time.Sleep(1 * time.Second)
	}
}

// RunAllExamples 运行所有示例
func (r *RedisStructuresDemo) RunAllExamples(ctx context.Context) {
	r.StringExample(ctx)
	r.HashExample(ctx)
	r.ListExample(ctx)
	r.SetExample(ctx)
	r.SortedSetExample(ctx)
	r.DelayQueueExample(ctx)
}
