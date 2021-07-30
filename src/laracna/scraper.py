import logging
import random
import time

import requests

from laracna.http_cache import HttpCache
from laracna.rabbit_queue import RabbitQueue

logger = logging.getLogger(__name__)


class Scraper(object):
    def __init__(self, min_delay=None, max_delay=None, callbacks=None, user_agent=None):
        if min_delay is None:
            min_delay = 1.0
        if max_delay is None:
            max_delay = 5.0
        if min_delay < 0.0:
            min_delay = 0.0
        if max_delay < min_delay:
            max_delay = min_delay

        self.min_delay = int(min_delay * 1000.0)
        self.max_delay = int(max_delay * 1000.0)

        if not callbacks:
            callbacks = {}
        self.callbacks = callbacks

        if not user_agent:
            user_agent = "Laracna/1.0 (gjhurlbu@gmail.com)"
        self.user_agent = user_agent

        self.incoming_queue = RabbitQueue("requests")
        self.outgoing_queue = RabbitQueue("results")
        self.cache = HttpCache()

    def grab(self, url, type_):
        message = {
            "type": type_,
            "url": url,
            "delay": random.randint(self.min_delay, self.max_delay),
        }
        self.incoming_queue.send(message)

    def scrape(self, start_url):
        # Prime the pump
        self.grab(start_url, "initial")

        session = requests.Session()
        session.headers.update({
            "User-Agent": self.user_agent,
        })

        for item in self.incoming_queue.poll():
            if not item:
                break

            message = item.get("message", {})
            delay = message.get("delay", None)
            url = message.get("url", None)
            type_ = message.get("type", None)

            if delay is None:
                delay = self.min_delay
            time.sleep(delay / 1000.0)

            (code, body) = self.cache.get(url)
            if body is None:
                try:
                    response = session.get(url)
                    self.cache.put(url, response.status_code, response.content)
                except Exception:
                    self.cache.put(url, 500, "")

                (code, body) = self.cache.get(url)

            callback = self.callbacks.get(type_, None)
            if callback is None:
                results = body
                chain_items = []
            elif hasattr(callback, "__call__"):
                (results, chain_items) = callback(code, body)
            else:
                raise NotImplementedError("No runnable callback for type %s" % type)

            for chain_item in chain_items:
                self.incoming_queue.send(chain_item)

            out_item = {
                "url": url,
                "type": type_,
                "code": code,
                "body": results,
            }
            self.outgoing_queue.send(out_item)



