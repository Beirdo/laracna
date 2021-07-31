import logging
import random
import time
from multiprocessing import Queue
from threading import Thread

import requests

from laracna.http_cache import HttpCache

logger = logging.getLogger(__name__)


class Scraper(object):
    def __init__(self, min_delay=None, max_delay=None, callbacks=None, user_agent=None, auth=None):
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

        if not auth:
            auth = None
        self.auth = auth

        self.incoming_queue = Queue()
        self.outgoing_queue = Queue()
        self.cache = HttpCache()
        self.scrape_thread = None
        self.abort = False

    def grab(self, url, type_):
        message = {
            "type": type_,
            "url": url,
            "delay": random.randint(self.min_delay, self.max_delay),
        }
        self.incoming_queue.put(message)

    def scrape(self, start_url):
        # Prime the pump
        self.grab(start_url, "initial")

        if not self.scrape_thread:
            self.abort = False
            self.scrape_thread = Thread(target=self.scrape_thread_loop, daemon=True)
            self.scrape_thread.start()

    def stop_scraping(self):
        self.abort = True
        self.incoming_queue.put({})
        self.scrape_thread.join()
        self.scrape_thread = None

    def scrape_thread_loop(self):
        session = requests.Session()
        session.headers.update({
            "User-Agent": self.user_agent,
        })
        session.auth = self.auth

        while not self.abort:
            try:
                item = self.incoming_queue.get()
            except Exception:
                continue

            if not item or self.abort:
                self.abort = True
                self.outgoing_queue.put({})
                return

            delay = item.get("delay", None)
            url = item.get("url", None)
            type_ = item.get("type", None)

            if delay is None:
                delay = self.min_delay
            delay /= 1000.0
            time.sleep(delay)

            cache_item = self.cache.get(url)
            if cache_item is None:
                try:
                    response = session.get(url)
                    self.cache.put(url, response.status_code, response.content)
                except Exception as e:
                    self.cache.put(url, 500, "")

                cache_item = self.cache.get(url)

            try:
                (code, body) = cache_item
            except Exception:
                continue

            callback = self.callbacks.get(type_, None)
            if callback is None:
                results = body
                chain_items = []
            elif hasattr(callback, "__call__"):
                (results, chain_items) = callback(code, body)
            else:
                raise NotImplementedError("No runnable callback for type %s" % type)

            for chain_item in chain_items:
                self.incoming_queue.put(chain_item)

            out_item = {
                "url": url,
                "type": type_,
                "code": code,
                "body": results,
            }
            self.outgoing_queue.put(out_item)
            print(out_item)

    def get_results(self):
        results = []
        while True:
            item = self.outgoing_queue.get()
            if not item:
                break
            results.append(item)

        return results

