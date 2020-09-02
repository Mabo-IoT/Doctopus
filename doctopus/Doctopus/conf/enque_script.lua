-- Creator: Marshall Fate
-- editor: driftluo
-- CreateTime: 2016/11/26 16:56
-- EditTime: 2017/06/07

--this lua script is used for redis enque action with two conditions:
--1. fields is diffrent with former fields which refers to old_fields in key threshold of redis
--2. the difference between timestamp and old timestamp(in the key threshold of redis)
--   is longer than the time range (user specified), as a heart beat.
--The data can be enque at least one of above condition met, or just drop it.^_^

--  keys[1]  table_name
--  args[1]  fields
--  args[2]  timestamp
--set parameters:
--how much seconds you want your time range is ???
local time_range = 10
local table_name = KEYS[1]
local fields = ARGV[1]
local timestamp = ARGV[2]
local MAXLEN = 100000

-- FUNCTION PART----------------------------------------------------------------------
local function set_threshold(threshold_name, timestamp)
    redis.call('HSET', threshold_name, 'fields', fields)
    redis.call('HSET', threshold_name, 'timestamp', timestamp)
end

local function threshold(fields, timestamp, time_range)
    --    parameters:
    --      fields      table influx json fields.
    --      timestamp   string timestamp
    --      return f_flag,t_flag  boolean
    --    f_flag get True when fields is diffrent with old fields.
    --    t_flag get True when time is longer than threshold time range.

    local eqpt_no = cmsgpack.unpack(fields)['tags']['eqpt_no']
    local threshold_name = string.format("threshold_%s_%s", eqpt_no, cmsgpack.unpack(table_name)) 
    local old_fields = redis.call("HGET", threshold_name , "fields")
    local old_timestamp = redis.call("HGET", threshold_name, "timestamp")

    if old_fields == false or old_timestamp == false then
        set_threshold(threshold_name, timestamp)
        return true, true
    end

    local f_flag = false
    local t_flag = false
    -- fields changed then set fields
    if fields ~= old_fields then
        f_flag = true
        set_threshold(threshold_name, timestamp)
    end
    -- time bigger than last time set fields

    if tonumber(timestamp) - tonumber(old_timestamp) > time_range then
        t_flag = true
        set_threshold(threshold_name, timestamp)
    end
    return f_flag, t_flag
end


-- FUNCTION PART----------------------------------------------------------------------



--for using two user variables f_flag and t_flag.
local f_flag = nil; local t_flag = nil

-- use unit to determine time_range.
if cmsgpack.unpack(fields)['unit'] == 'u' then
    time_range = time_range * 1000000
end

--Save the amount of data for 2 days based on a data of 5 seconds
if redis.call("llen", "data_queue") > 2 * 24 * 60 * 60 / 5 then
    redis.call("lpop", "data_queue")
end

--set threshold according to time_range, fields['eqpt_no'], table_name
f_flag, t_flag = threshold(fields, cmsgpack.unpack(timestamp), time_range)


if f_flag == true then

    local data = {
        table_name = table_name,
        time = timestamp,
        fields = fields,
    }

    local msg = cmsgpack.pack(data)
    -- redis.call("RPUSH", "data_queue", msg) -- msg queue
    redis.call("XADD", "data_stream", "MAXLEN", MAXLEN, "*", "data", msg)
    return 'field enque worked~'

elseif t_flag == true then

    local data = {
        heartbeat = cmsgpack.pack(true),
        table_name = table_name,
        time = timestamp,
        fields = fields,
    }
    local msg = cmsgpack.pack(data)
    -- redis.call("RPUSH", "data_queue", msg) -- msg queue
    redis.call("XADD", "data_stream", "*", "MAXLEN", MAXLEN, "data", msg)
    return 'heart beat enque worked~'
else
    return 'ignoring schema worked!'
end
