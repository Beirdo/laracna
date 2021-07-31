import json
import logging
import uuid

import pika
from pika.adapters.blocking_connection import BlockingChannel

logger = logging.getLogger(__name__)


exchange_name = "laracna-%s" % uuid.uuid4()


class RabbitQueue(object):
    def __init__(self, key):
        if not key:
            raise KeyError("RabbitQueue key must be defined")

        self.exchange = exchange_name
        self.key = key

        self.connection = pika.BlockingConnection()
        self.channel = self.connection.channel()

        self.channel.exchange_declare(self.exchange, "direct")
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.key)

    def __del__(self):
        self.connection.close()

    def send(self, message):
        if isinstance(message, (list, dict)):
            message = json.dumps(message)
        if not isinstance(message, str):
            raise TypeError("Messages must be string, list or dict")

        self.channel.basic_publish(exchange=self.exchange, routing_key=self.key,
                              body=message.encode("utf-8"))

    def poll(self):
        for (method_frame, properties, body) in self.channel.consume(queue=self.queue_name, exclusive=True):
            try:
                message = json.loads(body)
            except Exception:
                message = body

            item = {
                "frame": method_frame,
                "properties": properties,
                "body": body,
                "message": message,
            }

            # Acknowledge the message
            self.channel.basic_ack(method_frame.delivery_tag)

            if "shutdown" in message:
                item = {}

            yield item

            if not item:
                break

        # Cancel the consumer and return any pending messages
        requeued_messages = self.channel.cancel()
        print('Requeued %i messages' % requeued_messages)

