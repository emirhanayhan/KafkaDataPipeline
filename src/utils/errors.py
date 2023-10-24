import logging
import os

import janus
from aiokafka.errors import CommitFailedError, KafkaConnectionError


logger = logging.getLogger(__name__)


class CustomError(Exception):

    def __init__(self, err_msg):
        super().__init__(err_msg)


def main_thread_exception_handler(loop, context):
    """
    exception handler for futures or tasks
    using os.getpid() to find which partition raised exception
    """
    if isinstance(context["exception"], KafkaConnectionError):
        logger.error("Kafka Connection failed on {}".format(os.getpid()))

    elif isinstance(context["exception"], CommitFailedError):
        logger.error("Committing message has failed on {}".format(os.getpid()))

    elif isinstance(context["exception"], janus.SyncQueueFull):
        # this should only happens if you give TaskQueue(maxsize=x)
        logger.error("Task Queue is full {}".format(os.getpid()))


def worker_thread_exception_handler(loop, context):
    """
    exception handler for futures or tasks
    """

    if isinstance(context["exception"], CustomError):
        logger.info("Task not eligible to run".format(os.getpid()))
    logger.error(context)
