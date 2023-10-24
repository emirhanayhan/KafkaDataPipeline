import optparse
import os
import asyncio
import json
import time
import logging
import uuid
import multiprocessing as mp

import uvloop
from aiokafka import AIOKafkaConsumer

from configs.local import local_config
from configs.prod import prod_config
from src.utils.errors import main_thread_exception_handler
from src.utils.workers import WorkerThread

# we do not need crazy amount of worker threads because each thread
# run tasks concurrently note that this approach only take benefit from
# i/o bound tasks
WORKER_COUNT = os.cpu_count() / 2 if all([os.cpu_count(), os.cpu_count() > 8]) else 4


logger = logging.getLogger(__name__)


CONFIG_MAPPING = {
    "local": local_config,
    "prod": prod_config
}


async def main(server_settings):
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(main_thread_exception_handler)
    threads = []
    consumer = AIOKafkaConsumer(
        server_settings["topic_name"],
        bootstrap_servers=server_settings["kafka_host"],
        loop=loop,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        group_id=server_settings["group_id"])

    for thread_index in range(WORKER_COUNT):
        worker_thread = WorkerThread("WorkerThread-{}".format(thread_index), loop, consumer)
        worker_thread.daemon = True
        worker_thread.start()
        while not getattr(worker_thread, "queue"):
            print("waiting for queues to initialize")
            time.sleep(0.1)
        threads.append(worker_thread)

    await consumer.start()
    async for msg in consumer:
        try:
            message = json.loads(msg.value)
            message['id'] = uuid.uuid4().hex

            # least task scheduled worker
            thread = next(iter(sorted(threads, key=lambda t: t.task_count)))
            thread.queue.sync_q.put(message, block=False)
        except Exception as e:
            logger.exception("Failed while putting message to queue <{}>".format(str(e)))


parser = optparse.OptionParser()
parser.add_option("--config", default="local")
options, _ = parser.parse_args()
settings = CONFIG_MAPPING[options.config]

if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    processes = []

    # One process for each partition and also this main process
    partitions_count = settings["partitions_count"]
    for _ in range(partitions_count):
        process = mp.Process(
            target=asyncio.run(main(settings))
        )
        process.daemon = True
        process.start()
        processes.append(process)
    for process in processes:
        process.join()
    for process in processes:
        process.terminate()
    # TODO make useful of this mainprocess