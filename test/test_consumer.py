import json
import asyncio
import string
import random
import time
import boto3
import uuid
from sqs_consumer import SQSConsumer



sqs = boto3.resource('sqs')


def test_consumer_stop():
    queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue = sqs.create_queue(QueueName=queue_name, Attributes={'FifoQueue': 'true'})

    try:
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

        async def handle_message(message):
            consumer.stop()

        consumer.consume(
            queue_name=queue_name
        )(handle_message)

        consumer.start()
    finally:
        queue.delete()


def test_multiple_consumers():
    queue_name_1 = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue_name_2 = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue_1 = sqs.create_queue(QueueName=queue_name_1, Attributes={'FifoQueue': 'true'})
    queue_2 = sqs.create_queue(QueueName=queue_name_2, Attributes={'FifoQueue': 'true'})

    try:

        # Sqs documentation says:
        # - "After you create a queue, you must wait at least one second after the queue is created to be able to use the queue."
        time.sleep(1)
        
        total_messages = 20

        for _ in range(total_messages):
            queue_1.send_message(
                MessageBody=json.dumps({"name": "test1"}),
                MessageGroupId="test1",
                MessageDeduplicationId=uuid.uuid4().hex
            )

        for _ in range(total_messages):
            queue_2.send_message(
                MessageBody=json.dumps({"name": "test2"}),
                MessageGroupId="test2",
                MessageDeduplicationId=uuid.uuid4().hex
            )

        consumer = SQSConsumer(concurrency=total_messages)

        handler_calls = 0

        async def handle_message_1(message):
            nonlocal handler_calls

            handler_calls += 1
            
            await asyncio.sleep(12)
            consumer.stop()

        consumer.consume(
            queue_name=queue_name_1,
        )(handle_message_1)

        async def handle_message_2(message):
            nonlocal handler_calls

            handler_calls += 1
            
            await asyncio.sleep(10)
            
        consumer.consume(
            queue_name=queue_name_2,
        )(handle_message_2)

        consumer.start()

        assert handler_calls == (total_messages * 2)
    finally:
        queue_1.delete()
        queue_2.delete()


def test_message_consumption():
    queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue = sqs.create_queue(QueueName=queue_name, Attributes={'FifoQueue': 'true'})

    try:
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

        async def handle_message(message):
            nonlocal total_messages
            total_messages += 1
            message_received.update(message)
            consumer.stop()

        consumer.consume(
            queue_name=queue_name,
        )(handle_message)

        consumer.start()

        assert total_messages == 1
        assert message_received == message_sent
    finally:
        queue.delete()


def test_message_concurrency():
    queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue = sqs.create_queue(QueueName=queue_name, Attributes={'FifoQueue': 'true'})

    try:

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

        consumer = SQSConsumer(concurrency=total_messages + 100)

        handler_calls = 0

        async def handle_message(message):
            nonlocal handler_calls

            handler_calls += 1
            
            await asyncio.sleep(10)
            consumer.stop()

        consumer.consume(
            queue_name=queue_name,
        )(handle_message)

        consumer.start()
        
        assert handler_calls == total_messages
    finally:
        queue.delete()


def test_message_retries_and_failure():
    queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    failure_queue_name = "%s.fifo" % "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    queue = sqs.create_queue(QueueName=queue_name, Attributes={'FifoQueue': 'true'})
    failure_queue = sqs.create_queue(QueueName=failure_queue_name, Attributes={'FifoQueue': 'true'})

    try:
        # Sqs documentation says:
        # - "After you create a queue, you must wait at least one second after the queue is created to be able to use the queue."
        time.sleep(1)

        max_attempts = 25
        
        queue.send_message(
            MessageBody=json.dumps({"name": "test"}),
            MessageGroupId="test",
            MessageDeduplicationId=uuid.uuid4().hex
        )

        consumer = SQSConsumer()

        handler_calls = 0

        async def handle_message(message):
            nonlocal handler_calls

            handler_calls += 1

            raise Exception("fake error")

        failure_calls = 0

        async def handle_failure(message):
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

        assert handler_calls == max_attempts
        assert failure_calls == 1
    finally:
        queue.delete()
        failure_queue.delete()
