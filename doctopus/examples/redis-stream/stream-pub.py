import time
import random

import msgpack
import redis

def init():
    lua_script_file = "./enque.lua"
    pool = redis.ConnectionPool(
            host= "localhost",
            port= 6379,
            db= 4)
    db: redis.Redis = redis.StrictRedis(connection_pool=pool, socket_timeout=1)
    # load lua script
    with open(lua_script_file, 'r') as fn:
        script = fn.read()
        sha = db.script_load(script)
    
    return db, sha

def generateMessage() -> dict:
    timestamp: int = int(time.time())
    fields: dict = {
        "status": random.randint(0,1),
        "temp": round(random.uniform(21.0,30.0), 2),
        "msg": "this is a msg",
        "tags": {
            "eqpt_no": "1900",
        },
        "unit": "s",
    }
    table_name: str = "temprature"

    return {
        "timestamp": timestamp,
        "fields": fields,
        "table_name": table_name,
    }

def main():
    db, sha = init()
    while True:
        msg: dict = generateMessage()
        
        table_name = msgpack.packb(msg["table_name"])
        fields = msgpack.packb(msg["fields"])
        timestamp = msgpack.packb(msg["timestamp"])

        msg = db.evalsha(sha, 1, table_name, fields, timestamp)
        print(msg)

        time.sleep(1)


if __name__ == '__main__':
    main()