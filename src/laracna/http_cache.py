import logging
import os
import sys
import time
from hashlib import md5

logger = logging.getLogger(__name__)


class HttpCache(object):
    def __init__(self, basedir=None, expiry=None):
        if not basedir:
            basedir = os.path.join(os.path.realpath(os.path.dirname(sys.argv[0])), ".cache")
        os.makedirs(basedir, 0o755, exist_ok=True)
        self.basedir = basedir

        if not expiry:
            expiry = 3600.0
        self.expiry = expiry

    @staticmethod
    def sanitize_url(url: str):
        d = md5(url.encode("utf-8"))
        return d.hexdigest()

    def get(self, url):
        filename = os.path.join(self.basedir, self.sanitize_url(url))
        try:
            ctime = os.path.getctime(filename)
        except Exception:
            ctime = 0.0

        if time.time() >= ctime + self.expiry:
            if os.path.exists(filename):
                os.unlink(filename)

        try:
            with open(filename, "r") as f:
                code = int(f.readline().strip())
                body = f.read()
                return {
                    "code": code,
                    "url": url,
                    "body": body,
                    "ctime": ctime,
                }
        except Exception:
            return None

    def put(self, url, code, body):
        filename = os.path.join(self.basedir, self.sanitize_url(url))
        try:
            with open(filename, "w") as f:
                f.write("\n".join([str(code), body.decode("utf-8")]))
        except Exception:
            raise
