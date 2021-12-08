""" AWS Simple Queue Service utilities.
"""

import logging
import asyncio
import json
import uuid
from aiobotocore.session import get_session
from asyncio_pool import AioPool


log = logging.getLogger(__name__)


class SQSConsumer():
    """ This class is able to consume multiple fifo queues from SQS.
    It uses `Asyncio` library in order to process the messages asynchronously with a predefined concurrency.
    It also is able to perform retries and give up messages, sending them to a failure queue.

    Example:
        consumer = SQSConsumer()
        @consumer.consume(
            queue_name="MyQueue.fifo",
            failure_queue="MyFailureQueue.fifo,
            attempts=10
        )
        async def handle_message(message):
            print(message)

        consumer.start()
    """

    def __init__(self, concurrency=100):
        self._concurrency = concurrency
        self._stop_consuming = False
        self._handers = {}

    def consume(self, queue_name, attempts=1, failure_queue=None):     
        """ Decorator to set a message handler to a queue.
        
        Parâmeters:
          - queue_name (str):       Queue name to read from.  
          - attempts (int):         Number of retries.
          - failure_queue (str):    Queue to send unsuccessful messages.
        """   

        def decorator(fn):
            if queue_name in self._handers:
                log.warning("Handler already registered for queue %s", queue_name)
                return

            self._handers[queue_name] = {"func": fn, "attempts": attempts, "failure_queue": failure_queue}
        return decorator

    async def _enqueue(self, sqs, queue_url, message_body, group_id):
        response = await sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body),
            MessageGroupId=group_id,
            MessageDeduplicationId=uuid.uuid4().hex
        )

        self._raise_for_status(response)
        
    async def _handle_message(self, sqs, queue_name, queue_url, message):        
        try:
            message_body = json.loads(message["Body"])
            if not self._stop_consuming:
                await self._handers[queue_name]["func"](message_body)
            else:
                # The consumer received stop consuming signal.
                # Sending the message back to queue.
                await self._enqueue(sqs, queue_url, message_body, message["Attributes"].get("MessageGroupId"))

        except Exception as ex:
            # If an exceptions occurs the message will be:
            # - Sent back to queue for retry
            # - Sent to failure queue after max attempts

            message_body["attempts"] = message_body.get("attempts", 0) + 1
            max_attempts = self._handers[queue_name]["attempts"]

            if message_body["attempts"] < max_attempts:
                log.warning("Retring message %s. Cause: %s", message["MessageId"], str(ex))
                log.exception(ex)

                await self._enqueue(sqs, queue_url, message_body, message["Attributes"].get("MessageGroupId"))
            else:
                log.error("Max attempts reached for message %s.", message["MessageId"])
                log.exception(ex)

                failure_queue = self._handers[queue_name]["failure_queue"]

                if failure_queue:
                    response = await sqs.get_queue_url(QueueName=failure_queue)
                    self._raise_for_status(response)

                    failure_queue_url = response['QueueUrl']
                    await self._enqueue(sqs, failure_queue_url, message_body, message["Attributes"].get("MessageGroupId"))

    async def _start_consuming(self, queue_name, sqs, pool):
        response = await sqs.get_queue_url(QueueName=queue_name)
        self._raise_for_status(response)

        queue_url = response['QueueUrl']
        
        while not self._stop_consuming:
            response = await sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1, AttributeNames=["All"])
            self._raise_for_status(response)

            messages = response.get("Messages", [])

            log.info("%s messages received from %s", len(messages), queue_name)
            if not messages:
                continue
            
            messages_to_delete = []
            try:
                for message in messages:
                    messages_to_delete.append({
                        'Id': message['MessageId'],
                        'ReceiptHandle': message['ReceiptHandle']
                    })

                    await pool.spawn(self._handle_message(sqs, queue_name, queue_url, message))

            finally:
                if messages_to_delete:
                    log.info("Deleting %s messages", len(messages_to_delete))
                    response = await sqs.delete_message_batch(
                        QueueUrl=queue_url,
                        Entries=messages_to_delete
                    )

                    self._raise_for_status(response)

    def _raise_for_status(self, response):
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception(response) 

    async def _start_async(self):
        session = get_session()
        consumers = []
        async with session.create_client('sqs') as sqs:
            async with AioPool(size=self._concurrency) as pool:
                for queue_name in self._handers.keys():
                    consumers.append(asyncio.create_task(self._start_consuming(queue_name, sqs, pool)))

                await asyncio.gather(*consumers)

    def start(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._start_async())

    def stop(self):
        self._stop_consuming = True

    def is_running(self):
        return not self._stop_consuming
