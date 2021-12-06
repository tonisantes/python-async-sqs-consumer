""" AWS Simple Queue Service utilities.
"""

import logging
import eventlet
import json
import boto3
import uuid


log = logging.getLogger(__name__)


class SQSConsumer():
    """ This class is able to consume multiple queues from sqs.
    It depends on a library (eventlet) to make the calls asynchronously with a predefined concurrency.
    It also is able to preform retries.

    Example:
        consumer = SQSConsumer()
        @consumer.consume(
            queue_name="MyQueue.fifo",
            failure_queue="MyFailureQueue.fifo,
            attempts=10
        )
        def handle_message(message):
            print(message)

        consumer.start()
    """

    def __init__(self, concurrency=100):
        self._sqs = boto3.resource('sqs')
        self._pool = eventlet.GreenPool(size=concurrency)
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

    def start(self):
        for queue_name in self._handers.keys():
            self._pool.resize(self._pool.size + 1)
            self._pool.spawn_n(self._start_consuming, queue_name)

        self._pool.waitall()

    def stop(self):
        self._stop_consuming = True

    def _enqueue(self, queue_name, message_body, group_id):
        queue = self._sqs.get_queue_by_name(QueueName=queue_name)
        queue.send_message(
            MessageBody=json.dumps(message_body),
            MessageGroupId=group_id,
            MessageDeduplicationId=uuid.uuid4().hex
        )
        
    def _handle_message(self, queue_name, message):        
        try:
            message_body = json.loads(message.body)
            if not self._stop_consuming:
                self._handers[queue_name]["func"](message_body)
            else:
                # The consumer received stop consuming signal.
                # Sending the message back to queue.
                self._enqueue(queue_name, message_body, message.attributes.get("MessageGroupId"))

        except Exception as ex:
            # If an exceptions occurs the message will be:
            # - Sent back to queue for retry
            # - Sent to failure queue after max attempts

            message_body["attempts"] = message_body.get("attempts", 0) + 1
            max_attempts = self._handers[queue_name]["attempts"]

            if message_body["attempts"] < max_attempts:
                log.info("Retring message %s. Cause: %s", message.message_id, str(ex))
                log.exception(ex)

                self._enqueue(queue_name, message_body, message.attributes.get("MessageGroupId"))
            else:
                log.info("Max attempts reached for message %s. Cause: %s", message.message_id, str(ex))
                failure_queue = self._handers[queue_name]["failure_queue"]

                if failure_queue:
                    # del message_body["attempts"]
                    self._enqueue(failure_queue, message_body, message.attributes.get("MessageGroupId"))

    def _start_consuming(self, queue_name):
        queue = self._sqs.get_queue_by_name(QueueName=queue_name)
        
        while not self._stop_consuming:
            messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1, AttributeNames=["All"])
            log.info("%s messages received from %s", len(messages), queue_name)
            if not messages:
                continue
            
            messages_to_delete = []
            try:
                for message in messages:
                    messages_to_delete.append({
                        'Id': message.message_id,
                        'ReceiptHandle': message.receipt_handle
                    })

                    self._pool.spawn_n(self._handle_message, queue_name, message)

            finally:
                log.info("Deleting %s messages", len(messages_to_delete))
                queue.delete_messages(Entries=messages_to_delete)
