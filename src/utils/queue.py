from asyncio import Queue


class TaskQueue(Queue):
    def __init__(self, loop):
        super().__init__(loop=loop)

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.get()
