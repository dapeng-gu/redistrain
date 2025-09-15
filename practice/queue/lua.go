package queue

import "github.com/redis/go-redis/v9"

// enqueueScript 原子性入队Lua脚本
var enqueueScript = redis.NewScript(`
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
    return 0
end
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("LPUSH", KEYS[2], ARGV[3])

return 1
`)

var dequeueScript = redis.NewScript(`
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
    return nil
end

local taskData = redis.call("HGET", KEYS[1], ARGV[1])

redis.call("HDEL", KEYS[1], ARGV[1])

return taskData
`)

var delayEnqueueScript = redis.NewScript(`
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
    return 0
end

redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])

return 1
`)

// enqueueWithDeduplicationScript 支持去重的入队Lua脚本
// KEYS[1]: 任务键
// KEYS[2]: 队列键
// KEYS[3]: 去重键
// ARGV[1]: 任务ID
// ARGV[2]: 任务数据
// ARGV[3]: 任务过期时间
// ARGV[4]: 去重键过期时间
var enqueueWithDeduplicationScript = redis.NewScript(`
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
    return 0
end

if redis.call("EXISTS", KEYS[3]) == 1 then
    return -1
end
redis.call("SETEX", KEYS[3], ARGV[4], ARGV[3])

redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])

redis.call("LPUSH", KEYS[2], ARGV[3])

return 1
`)

// addToGroupScript 添加任务到分组的Lua脚本
// KEYS[1]: 分组键
// KEYS[2]: 定时器键
// ARGV[1]: 任务ID
// ARGV[2]: 任务数据
// ARGV[3]: 最大批处理大小
// ARGV[4]: 最大等待时间
var addToGroupScript = redis.NewScript(`
redis.call("LPUSH", KEYS[1], ARGV[1])

local groupSize = redis.call("LLEN", KEYS[1])
if groupSize == 1 then
    redis.call("SETEX", KEYS[2], ARGV[4], "timer")
end

local maxBatchSize = tonumber(ARGV[3])
if groupSize >= maxBatchSize then
    return 2
end

return 1
`)

// triggerBatchScript 触发批处理的Lua脚本
// KEYS[1]: 分组键
// KEYS[2]: 定时器键
var triggerBatchScript = redis.NewScript(`
local taskIDs = redis.call("LRANGE", KEYS[1], 0, -1)
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])

return taskIDs
`)
