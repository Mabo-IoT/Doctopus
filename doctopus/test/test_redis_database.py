import unittest

from Doctopus.lib.database_wrapper import RedisWrapper


class TestRedis(unittest.TestCase):
    def setUp(self):
        conf: dict = {
            "host": "127.0.0.1",
            "port": 6379,
            "db": 4,
        }
        self.client = RedisWrapper(conf)

    def testAddGroup(self):
        group_name = "test_group"
        self.client.addGroup(group_name)

    def testReaddGroup(self):
        group_name = "test_group"
        consumer = "chitu"
        result = self.client.readGroup(group_name, consumer)
        print("==========readgroup===========")
        print(result)
        print("===============================")

    def testPending(self):
        group_name = "test_group"
        result = self.client.xPending(group_name)
        print(result)

    def testReadPending(self):
        group_name = "test_group"
        consumer = "chitu"
        id = "1571038514316-0"
        result = self.client.readPending(group_name, consumer, id)
        print(result)

    def testACK(self):
        group_name = "test_group"
        id = "1571041740221-0"
        self.client.ack(group_name, id)


if __name__ == "__main__":
    unittest.main()
