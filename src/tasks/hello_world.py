import asyncio


async def hello_world_task(*args, **kwargs):
    # This is just an example task for data processing which it symbolizes
    # your data sending to various data lakes or databases
    print("Hello world!")
    await asyncio.sleep(5)
