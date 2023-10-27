import os
import json
import time

from kafka import KafkaProducer
from locust import task, User, events
from locust.user.wait_time import constant
"""
   Since this is optional i do not add locust to requirements 
   but i strongly recommend you to use this within your setup
   to find optimal partition-consumer and worker count
"""

TASK_DELAY = os.getenv("TASK_DELAY", 0.01)

body = {
    "task_name": "hello_world",
    "task_parameters": {}
}


class KafkaClient:

    def __init__(self, kafka_brokers=None):
        if kafka_brokers is None:
            kafka_brokers = ['localhost:9092']
        self.producer = KafkaProducer(bootstrap_servers=kafka_brokers)

    def __handle_success(self, *args, **kwargs):
        end_time = time.time()
        elapsed_time = int((end_time - kwargs["start_time"]) * 1000)
        try:
            record_metadata = kwargs["future"].get(timeout=1)

            request_data = dict(request_type="ENQUEUE",
                                name=record_metadata.topic,
                                response_time=elapsed_time,
                                response_length=record_metadata.serialized_value_size)

            self.fire_success(**request_data)
        except Exception:
            raise Exception("?")

    def __handle_failure(self, *arguments, **kwargs):
        end_time = time.time()
        elapsed_time = int((end_time - kwargs["start_time"]) * 1000)

        request_data = dict(request_type="ENQUEUE", name=kwargs["topic"], response_time=elapsed_time,
                            exception=arguments[0])

        self.fire_failure(**request_data)

    @staticmethod
    def fire_failure(**kwargs):
        events.request_failure.fire(**kwargs)

    @staticmethod
    def fire_success(**kwargs):
        events.request_success.fire(**kwargs)

    def send(self, topic, key=None, message=None):
        start_time = time.time()
        future = self.producer.send(topic, key=key.encode() if key else None,
                                    value=message.encode() if message else None)
        future.add_callback(self.__handle_success, start_time=start_time, future=future)
        future.add_errback(self.__handle_failure, start_time=start_time, topic=topic)

    def finalize(self):
        self.producer.flush(timeout=5)


class KafkaUser(User, KafkaClient):
    wait_time = constant(TASK_DELAY)

    @task
    def task1(self):
        self.send("test", message=json.dumps(body))
