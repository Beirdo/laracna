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

        with pika.BlockingConnection() as connection:
            channel = connection.channel()
            channel.exchange_declare(self.exchange, "direct")
            result = channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue
            channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.key)

    def send(self, message):
        with pika.BlockingConnection() as connection:
            channel = connection.channel()

            if isinstance(message, (list, dict)):
                message = json.dumps(message)
            if not isinstance(message, str):
                raise TypeError("Messages must be string, list or dict")

            channel.basic_publish(exchange=self.exchange, routing_key=self.key,
                                  body=message.encode("utf-8"))

    def poll(self):
        with pika.BlockingConnection() as connection:
            channel: BlockingChannel = connection.channel()

            for (method_frame, properties, body) in channel.consume(queue=self.queue_name, exclusive=True):
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
                channel.basic_ack(method_frame.delivery_tag)

                if "shutdown" in message:
                    item = {}

                yield item

                if not item:
                    break

            # Cancel the consumer and return any pending messages
            requeued_messages = channel.cancel()
            print('Requeued %i messages' % requeued_messages)

