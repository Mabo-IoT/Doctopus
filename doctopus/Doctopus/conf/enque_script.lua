-- 该Lua脚本用于对入redis的数据进行去重等处理，最少满足如下条件之一的数据允许存储到redis：
-- 1. 'new_fields'和'old_fields'（指redis的键'threshold*'里的'old_fields'的值）之间是不同的
-- 2. ‘new_timestamp’和‘old_timestamp'之间差了'time_range'秒以上
-- 并且会每隔mark_range发送两条心跳信息

local new_table_name = KEYS[1]
local new_fields = ARGV[1]
local new_timestamp = ARGV[2]

-- Stream的最大尺寸
local MAXLEN = 100000

-- 'new_timestamp'和'old_timestamp'之间差的时间（单位秒）
local time_range = 10
-- mark_range必须是time_range的整数倍
local mark_range = 300
-- 'new_fields'中'unit'的值是'u'（微秒）时，x1000000才能正确计算'new_timetamp'和'old_timestamp'的差值
if cmsgpack.unpack(new_fields)['unit'] == 'u' then
    time_range = time_range * 1000000
    mark_range = mark_range * 1000000
end


-------------------- FUNCTION PART --------------------
local function set_threshold(threshold_name, fields, timestamp)
    -- 刷新'threshold*'键（Hash类型），存储'fields'和'timestamp'用以校验数据
    -- parameters:
    --   threshold_name      'threshold*'键的具体名字
    --   fields              写入redis的数据
    --   timestamp           时间戳，string类型

    redis.call('HSET', threshold_name, 'fields', fields)
    redis.call('HSET', threshold_name, 'timestamp', timestamp)
end

local function get_threshold(fields, timestamp, t_range, m_range)
    -- 获取'threshold*'键中的数据
    -- parameters:
    --   fields         要存储到redis的数据
    --   timestamp      要存储到redis的时间戳，string类型
    --   t_range        'new_timestamp'和'old_timestamp'之间差的时间，用于heartbeat
    --   m_range        'new_timestamp'和'old_timestamp'之间差的时间，用于connbeat和databeat
    --   return field_flag, time_flag, mark_flag      布尔值

    -- 初始化field_flag、time_flag和mark_flag的值为false
    local field_flag = false
    local time_flag = false
    local mark_flag = false

    -- 解包得到eqpt_no和table_name用以组成'threshold_name'
    local eqpt_no = cmsgpack.unpack(fields)['tags']['eqpt_no']
    local tab_name = cmsgpack.unpack(new_table_name)
    local threshold_name = string.format("threshold_%s_%s", eqpt_no, tab_name)

    -- 从'threshold_name'中获取已存入redis的'fields'、'timestamp'和'timemark'的值
    local old_fields = redis.call("HGET", threshold_name , "fields")
    local old_timestamp = redis.call("HGET", threshold_name, "timestamp")
    -- timemark用于每mark_range更新一次心跳信息
    local old_timemark = redis.call("HGET", threshold_name, "timemark")

    -- 需要刷新'threshold*'键的3种情况
    -- 1. 'threshold*'中的'fields'、'timestamp'和'timemark'其中某个不存在
    if old_fields == false or old_timestamp == false or old_timemark == false then
        set_threshold(threshold_name, fields, timestamp)
        redis.call('HSET', threshold_name, 'timemark', timestamp)
        return true, true, true
    end
    -- 2. 'fields'和'old_fields'不一样
    -- 并且将field_flag的值设置为true
    if fields ~= old_fields then
        set_threshold(threshold_name, fields, timestamp)
        field_flag = true
    end
    -- 3. ‘new_timestamp’和‘old_timestamp'之间差了'time_range'秒以上
    -- 并且将time_flag的值设置为true
    if tonumber(timestamp) - tonumber(old_timestamp) >= t_range then
        set_threshold(threshold_name, fields, timestamp)
        time_flag = true
    end

    -- 更新timemark
    if tonumber(timestamp) - tonumber(old_timemark) >= m_range then
        redis.call('HSET', threshold_name, 'timemark', timestamp)
        mark_flag = true
    end

    return field_flag, time_flag, mark_flag
end
-------------------- FUNCTION PART --------------------


-- 两个标志位：
-- field_flag = true时代表'new_fields'和'old_fields'是不同的，可以写入数据
-- time_flag = true时代表'new_timestamp'和'old_timestamp'之间差了'time_range'秒以上，可以写入数据
-- mark_flag = true时代表'new_timestamp'和'old_timestamp'之间差了'mark_range'秒以上，给出心跳信息
local field_flag = nil
local time_flag = nil
local mark_flag = nil

-- CHANGED: 这个操作貌似没用了，因为没有'data_queue'这个键了
if redis.call("llen", "data_queue") > 2 * 24 * 60 * 60 / 5 then
    redis.call("lpop", "data_queue")
end

-- 调用get_threshold，由get_threshold调用set_threshold并返回field_flag、time_flag、mark_flag的值
local time_stamp = cmsgpack.unpack(new_timestamp)
field_flag, time_flag, mark_flag = get_threshold(new_fields, time_stamp, time_range, mark_range)

local heartbeat_str = {name = "heartbeat", title = "存活心跳", value = 1, type = "int", unit = nil} -- 固定表示存活
local connbeat_str = {name = "connbeat", title = "网络心跳", value = 1, type = "int", unit = nil}   -- 表示网络连接（有瑕疵）
local databeat_str = {name = "databeat", title = "数据心跳", value = 1, type = "int", unit = nil}   -- 表示有新数据
local all_fields = cmsgpack.unpack(new_fields)
-- 根据field_flag、time_flag和mark_flag的值将对应格式的数据写入redis
if field_flag == true and mark_flag == true then
    -- 有新数据且过了mark_range时间
    all_fields["heartbeat"] = heartbeat_str
    all_fields["connbeat"] = connbeat_str
    all_fields["databeat"] = databeat_str
    local data = {
        table_name = new_table_name,
        time = new_timestamp,
        fields = cmsgpack.pack(all_fields),
    }

    local msg = cmsgpack.pack(data)
    redis.call("XADD", "data_stream", "*", "MAXLEN", MAXLEN, "data", msg)

    return 'Field enque completed.'
elseif field_flag == true and mark_flag == false then
    -- 有新数据且没过mark_range时间
    all_fields["heartbeat"] = heartbeat_str
    all_fields["databeat"] = databeat_str
    local data = {
        table_name = new_table_name,
        time = new_timestamp,
        fields = cmsgpack.pack(all_fields),
    }

    local msg = cmsgpack.pack(data)
    redis.call("XADD", "data_stream", "*", "MAXLEN", MAXLEN, "data", msg)

    return 'Field enque completed.'
elseif time_flag == true and mark_flag == true then
    -- 新旧时间戳差值超过time_range且过了mark_range时间
    all_fields["heartbeat"] = heartbeat_str
    all_fields["connbeat"] = connbeat_str
    local data = {
        table_name = new_table_name,
        time = new_timestamp,
        fields = cmsgpack.pack(all_fields),
    }

    local msg = cmsgpack.pack(data)
    redis.call("XADD", "data_stream", "*", "MAXLEN", MAXLEN, "data", msg)

    return 'Time enque completed.'
elseif time_flag == true and mark_flag == false then
    -- 新旧时间戳差值超过time_range且没过mark_range时间
    all_fields["heartbeat"] = heartbeat_str
    local data = {
        table_name = new_table_name,
        time = new_timestamp,
        fields = cmsgpack.pack(all_fields),
    }

    local msg = cmsgpack.pack(data)
    redis.call("XADD", "data_stream", "*", "MAXLEN", MAXLEN, "data", msg)

    return 'Time enque completed.'
else
    return 'Waiting for new data ...'
end
