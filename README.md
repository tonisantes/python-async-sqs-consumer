# python-async-sqs-consumer

Python client for consuming messages asynchronously from AWS Simple Queue Service (SQS).

This provides a `SQSConsumer class` witch is able to consume multiple fifo queues from SQS.
It uses `Asyncio` library in order to process the messages asynchronously with a predefined concurrency.
It also is able to perform retries and give up messages, sending them to a failure queue.

Example:

```python
from sqs_consumer import SQSConsumer


consumer = SQSConsumer(concurrency=10)

@consumer.consume(
    queue_name="MyQueue.fifo",
    failure_queue="MyFailureQueue.fifo",
    attempts=10
)
async def handle_message(message):
    print(message)

# This is a blocking call.
# To stop the consumer call `consumer.stop()` in some point of the program.
consumer.start()
```