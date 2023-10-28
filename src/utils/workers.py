import threading
import asyncio
import logging

from src.tasks.hello_world import hello_world_task
from src.utils.errors import worker_thread_exception_handler, CustomError
from src.utils.queue import TaskQueue

TASK_MAPPING = {
    "hello_world": hello_world_task
}


logger = logging.getLogger("worker_thread")


class WorkerThread(threading.Thread):
    def __init__(self, name, main_event_loop, consumer):
        super().__init__()
        self.name = name
        self.queue = None
        self.main_event_loop = main_event_loop
        self.consumer = consumer

    def run(self) -> None:
        asyncio.run(self._run())

    @property
    def task_count(self):
        return len(asyncio.all_tasks(loop=self.loop))

    async def _run(self):
        self.loop = asyncio.get_event_loop()
        self.queue = TaskQueue(loop=self.loop)
        self.loop.set_exception_handler(worker_thread_exception_handler)

        async for item in self.queue:
            try:
                task = TASK_MAPPING.get(item["task_name"])
                if not all([task, isinstance(item["task_parameters"], dict)]):
                    raise CustomError("Task not runnable")

                asyncio.ensure_future(task(**item["task_parameters"]))
                asyncio.run_coroutine_threadsafe(self.consumer.commit(), loop=self.main_event_loop)
                self.queue.task_done()
            except CustomError as e:
                logger.info("Task not runnable id: {}".format(item["id"]))
            except Exception as e:
                logger.exception("Task not scheduled unexpectedly id:{} <{}>".format(item["id"], str(e)))

