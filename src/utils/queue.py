import janus


class TaskQueue(janus.Queue):
    """
    asynchronously iterable version of janus.Queue
    """

    def __init__(self, maxsize=0):
        super().__init__(maxsize=maxsize)
        self._async_queue = AsyncQueueProxy(self)


class AsyncQueueProxy(janus._AsyncQueueProxy):
    def __init__(self, parent):
        super().__init__(parent)

    def __aiter__(self):
        if self.closed:
            raise RuntimeError("Operation on the closed queue is forbidden")
        return self

    async def __anext__(self):
        return await self.get()
