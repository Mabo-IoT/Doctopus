import unittest
import time
import random
import kafka
import time

from Doctopus.lib.kafka_wrapper import KafkaWrapper


class TestKafka(unittest.TestCase):
    def setUp(self):
        conf: dict = {
            "bootstrap_servers": "192.168.1.21:9091",
            "topic": "custom_collector_json",
            "ip": "192.168.1.21",
            "org":3101,
        }
        self.client = KafkaWrapper(conf)
        
    
    def testSendMessage(self):
        
        for i in range(10):
            msg = self.generateMessage()
            # print(msg)
            self.client.sendMessage(msg)
            time.sleep(1)

    def testPackMessage(self):
        data = {
            "table_name": "performance",
            "time": int(time.time()),
            "fields":{
                "status": random.randint(0,1),
                "temp": round(random.uniform(20.0, 29.0),2),
                "unit": "s", 
                "tags": {
                    "eapt_no":"1900",
                    "source": "mabo01",
                    },
            }
        }

        msg = self.client.pack(data)
        print(msg)
        self.assertIsInstance(msg, dict)

    def generateMessage(self) -> dict:
        timestamp: int = int(time.time())
        ip: str = "192.168.1.21"
        dataid: int = 3605
        org: int = 3101
        msg: dict = {
            "ts": timestamp,
            "ip": "192.168.1.21",
            "dataid": dataid,
            "org": org,
            "dims": {
                "measurement":"Performance",
                "eqpt_no": "1900",
            },
            "vals": {
                "status": random.randint(0,1),
                "temp": round(random.uniform(20.1,30.0), 2),
            },
        }

        return msg


if __name__ == '__main__':
    unittest.main()