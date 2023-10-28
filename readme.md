## Data pipeline for kafka

# About The Project
This is a example for consuming and processing data from kafka also know as data pipelining.
* The implementation take advantages from multiprocessing, multithreading, and asynchronous programming
* The project designed for single topic with single or multiple partitions
* Each partition have it own isolated process that have multiple threads consuming and processing messages
* Mainprocess stays idle for now but i keep it anyway maybe some monitoring features for future

## Multiprocessing
* An isolated process for each partition topic have

## Multithreading
* Each process have one "MainThread" which consumes messages add this messages to queue's for least busy worker thread

## Asynchronous Programming
* Each thread has its own event loop running and scheduling tasks
* This approach achieves efficient concurrency with io bound callbacks


## Packages
* uvloop ~ fast asyncio event loop
* aiokafka ~ asynchronous kafka client

# task payload example
```json
{
  "task_name": "hello_world",
  "task_parameters": {}
}
```
