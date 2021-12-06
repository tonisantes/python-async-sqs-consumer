# python-sqs-consumer

Python client for consuming messages asynchronously from AWS Simple Queue Service (SQS).

This provides a `SQSConcumer class` witch is able to consume multiple fifo queues from SQS.
It depends on a library (`eventlet`) to process the messages asynchronously with a predefined concurrency.
It also is able to perform retries and give up messages sending them to a failure queue.

Beacuse it depends on `eventlet` it is necessary call `eventlet.monkey_patch()` function before all for asynchronicity to work.

Example:

```python
import eventlet
eventlet.monkey_patch()

from sqs_consumer import SQSConsumer


consumer = SQSConsumer(concurrency=10)

@consumer.consume(
    queue_name="MyQueue.fifo",
    failure_queue="MyFailureQueue.fifo",
    attempts=10
)
def handle_message(message):
    print(message)

# This is a blocking call.
# To stop the consumer call `consumer.stop()` in some point of the program.
consumer.start()
```