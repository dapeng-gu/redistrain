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
