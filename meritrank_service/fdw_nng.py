from enum import Enum

import pynng

import msgpack

DEFAULT_URL = 'tcp://127.0.0.1:10234'


class Request(Enum):
    SCORE = 1
    SCORES = 2
    GRAVITY_GRAPH = 3
    GLOBAL_SCORES = 4


class NNGListener:
    def __init__(self, callback, listen_url=None):
        self.listen_url = listen_url or DEFAULT_URL
        self.callback = callback
        self.received_messages_count = 0

    async def start_listener(self, max_count=None):
        with pynng.Rep0() as sock:
            sock.listen(self.listen_url)
            while True:
                if max_count is not None and self.received_messages_count >= max_count:
                    break
                msg = await sock.arecv_msg()
                self.received_messages_count += 1
                content = msgpack.loads(msg.bytes)
                rows = self.callback(*content)
                rows_serialized = msgpack.packb(rows)
                sock.send(rows_serialized)
