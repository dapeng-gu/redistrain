# 第1步：Redis基础数据结构与任务存储

## 🎯 学习目标
通过分析asynq项目，理解如何使用Redis基础数据结构存储和管理任务队列。

## 📚 核心概念
- **Hash结构**：存储任务的详细信息和元数据
- **List结构**：实现FIFO队列，管理任务执行顺序
- **任务序列化**：JSON格式存储复杂数据结构
- **键命名规范**：`task:{id}` 和 `queue:{name}` 的命名策略

## 🔍 asynq参考代码分析

### 1. 任务数据结构 (`asynq/asynq.go:21-50`)
```go
type Task struct {
    typename string    // 任务类型
    payload  []byte   // 任务载荷
    opts     []Option // 任务选项
}
```

### 2. Redis存储脚本 (`asynq/internal/rdb/rdb.go:98-120`)
```lua
-- enqueueCmd: 任务入队的Lua脚本
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0  -- 任务已存在
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "pending")
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
```

## 🚀 运行演示

### 前置条件
1. 确保Redis服务器运行在 `localhost:6379`
2. 安装Go依赖：`go mod tidy`

### 执行步骤
```bash
cd 02_advanced_redis/step1_task_storage
go run main.go
```

## 📋 验收标准检查清单

### ✅ 基础功能
- [ ] 任务能够序列化为JSON格式
- [ ] 使用Hash存储任务完整信息
- [ ] 使用List实现队列的FIFO特性
- [ ] 正确处理空队列的情况

### ✅ 数据完整性
- [ ] 入队和出队的任务数据一致
- [ ] 队列长度统计准确
- [ ] 支持多个命名队列

### ✅ 错误处理
- [ ] Redis连接失败时的错误处理
- [ ] 任务序列化失败的处理
- [ ] 队列为空时的优雅处理

## 🤔 思考题（苏格拉底式引导）

1. **数据一致性**：如果在HSET成功但LPUSH失败的情况下，会发生什么？如何保证原子性？

2. **内存管理**：任务处理完成后，Hash中的任务数据何时清理？会不会造成内存泄漏？

3. **并发安全**：多个worker同时从同一队列取任务时，会不会出现竞态条件？

4. **扩展性**：当前的键命名策略是否支持多租户场景？如何设计命名空间？

## 🎯 下一步预告
完成本步骤后，你将学习**第2步：有序集合实现延迟队列**，探索如何使用Sorted Set实现任务的延迟调度功能。

## 💡 实践建议
- 使用Redis CLI (`redis-cli`) 观察键值的变化
- 尝试修改任务结构，添加更多字段
- 实验不同的序列化格式（如MessagePack）
- 思考如何优化Redis内存使用
