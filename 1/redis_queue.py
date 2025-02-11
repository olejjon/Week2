import redis
import json


class RedisQueue:
    def __init__(self, host="localhost", port=6379, db=0, queue_name="default"):
        self.r = redis.Redis(host=host, port=port, db=db)
        self.queue_name = queue_name

    def publish(self, msg: dict):
        serialized_msg = json.dumps(msg)
        self.r.lpush(self.queue_name, serialized_msg)

    def consume(self) -> dict:
        _, serialized_msg = self.r.brpop(self.queue_name)
        return json.loads(serialized_msg)


if __name__ == "__main__":
    q = RedisQueue()
    q.publish({"a": 1})
    q.publish({"b": 2})
    q.publish({"c": 3})

    assert q.consume() == {"a": 1}
    assert q.consume() == {"b": 2}
    assert q.consume() == {"c": 3}
