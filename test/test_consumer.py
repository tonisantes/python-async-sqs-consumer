import eventlet
eventlet.monkey_patch()

import json
import string
import random
import time
import boto3
import uuid
from sqs_consumer import SQSConsumer


sqs = boto3.resource('sqs')


def test_message_consumption():
    queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue = sqs.create_queue(QueueName=queue_name, Attributes={'FifoQueue': 'true'})

    # Sqs documentation says:
    # - "After you create a queue, you must wait at least one second after the queue is created to be able to use the queue."
    time.sleep(1)

    message_sent = {"name": "test"}
    queue.send_message(
        MessageBody=json.dumps(message_sent),
        MessageGroupId="test",
        MessageDeduplicationId=uuid.uuid4().hex
    )

    consumer = SQSConsumer()

    total_messages = 0
    message_received= {}

    def handle_message(message):
        nonlocal total_messages
        total_messages += 1
        message_received.update(message)
        consumer.stop()

    consumer.consume(
        queue_name=queue_name,
        attempts=10
    )(handle_message)

    consumer.start()

    queue.delete()
    assert total_messages == 1
    assert message_received == message_sent


def test_message_concurrency():
    queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue = sqs.create_queue(QueueName=queue_name, Attributes={'FifoQueue': 'true'})

    # Sqs documentation says:
    # - "After you create a queue, you must wait at least one second after the queue is created to be able to use the queue."
    time.sleep(1)
    
    total_messages = 40

    for _ in range(total_messages):
        queue.send_message(
            MessageBody=json.dumps({"name": "test"}),
            MessageGroupId="test",
            MessageDeduplicationId=uuid.uuid4().hex
        )

    consumer = SQSConsumer(concurrency=total_messages)

    handler_calls = 0

    def handle_message(message):
        nonlocal handler_calls

        handler_calls += 1
        
        time.sleep(10)
        consumer.stop()

    consumer.consume(
        queue_name=queue_name,
        attempts=10
    )(handle_message)

    consumer.start()

    queue.delete()
    assert handler_calls == total_messages


def test_message_retries_and_failure():
    queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    failure_queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue = sqs.create_queue(QueueName=queue_name, Attributes={'FifoQueue': 'true'})
    failure_queue = sqs.create_queue(QueueName=failure_queue_name, Attributes={'FifoQueue': 'true'})

    # Sqs documentation says:
    # - "After you create a queue, you must wait at least one second after the queue is created to be able to use the queue."
    time.sleep(1)

    max_attempts = 55
    
    queue.send_message(
        MessageBody=json.dumps({"name": "test"}),
        MessageGroupId="test",
        MessageDeduplicationId=uuid.uuid4().hex
    )

    consumer = SQSConsumer()

    handler_calls = 0

    def handle_message(message):
        nonlocal handler_calls

        handler_calls += 1

        raise Exception("fake error")

    failure_calls = 0

    def handle_failure(message):
        nonlocal failure_calls

        failure_calls += 1

        consumer.stop()

    consumer.consume(
        queue_name=queue_name,
        failure_queue=failure_queue_name,
        attempts=max_attempts
    )(handle_message)

    consumer.consume(
        queue_name=failure_queue_name
    )(handle_failure)

    consumer.start()

    queue.delete()
    failure_queue.delete()
    assert handler_calls == max_attempts
    assert failure_calls == 1
