import sys
import anyio
import anyio.abc
import sniffio
import subprocess
from anyio import IncompleteRead, DelimiterNotFound
import asyncio
import pytest
from asyncwago.server import Server, open_server
from functools import partial
from inspect import iscoroutine
from typing import AsyncIterable

import logging

logging.basicConfig(level=logging.DEBUG)

# We can just use 'async def test_*' to define async tests.
# This also uses a virtual clock fixture, so time passes quickly and
# predictably.


class MockServerProtocol(anyio.abc.ByteStream):  # pylint: disable=abstract-method
    def __init__(self, server):
        self.server = server
        self._buffer = b""

    async def _recv(self):
        b = await self.server._sub_prot.stdout.receive(1024)
        if not b:
            return ""
        return b

    @property
    def buffered_data(self):
        return self._buffer

    async def receive(self, max_bytes: int) -> bytes:
        if not self._buffer:
            self._buffer = await self._recv()  # max_bytes)
            if not self._buffer:
                return None
        data, self._buffer = self._buffer[:max_bytes], self._buffer[max_bytes:]
        return data

    async def receive_exactly(self, nbytes: int) -> bytes:
        bytes_left = nbytes - len(self._buffer)
        while bytes_left > 0:
            chunk = await self._recv()
            if not chunk:
                raise IncompleteRead

            self._buffer += chunk
            bytes_left -= len(chunk)

        result = self._buffer[:nbytes]
        self._buffer = self._buffer[nbytes:]
        return result

    async def receive_until(self, delimiter: bytes, max_bytes: int) -> bytes:
        delimiter_size = len(delimiter)
        offset = 0
        while True:
            # Check if the delimiter can be found in the current buffer
            index = self._buffer.find(delimiter, offset)
            if index >= 0:
                found = self._buffer[:index]
                self._buffer = self._buffer[index + len(delimiter) :]
                return found

            # Check if the buffer is already at or over the limit
            if len(self._buffer) >= max_bytes:
                raise DelimiterNotFound(max_bytes)

            # Read more data into the buffer from the socket
            # read_size = max_bytes - len(self._buffer)
            data = await self._recv()
            if not data:
                raise IncompleteRead

            # Move the offset forward and add the new data to the buffer
            offset = max(len(self._buffer) - delimiter_size + 1, 0)
            self._buffer += data

    def receive_chunks(self, max_size: int) -> AsyncIterable[bytes]:
        raise NotImplementedError

    def receive_delimited_chunks(
        self, delimiter: bytes, max_chunk_size: int
    ) -> AsyncIterable[bytes]:
        raise NotImplementedError


    async def aclose(self):
        if self.server._sub_prot is not None:
            self.server._sub_prot.kill()

    async def send(self, data):
        await self.server._sub_prot.stdin.send(data)

    async def send_eof(self):
        await self.server._sub_prot.stdin.aclose()


class MockServer(Server):
    freq = 0.1
    _sub_prot = None
    _sub_trans = None

    def __init__(self, taskgroup):
        super().__init__(taskgroup, "nope.invalid")

    async def _connect(self):
        self._sub_prot = await anyio.open_process(
                [
                    "../wago-firmware/wago",
                    "-d",
                    "-D",
                    "-p0",
                    "-c",
                    "../wago-firmware/wago.sample.csv",
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=sys.stderr,
            )
        p = MockServerProtocol(server=self)  # pylint: disable=abstract-class-instantiated
        return p


@pytest.mark.anyio
@pytest.mark.parametrize('anyio_backend', ['trio'])
async def test_wago_mock():
    async with open_server(ServerClass=MockServer) as s:
        await s.simple_cmd("Dc")
        assert await s.read_input(1, 2) is False
        await s.simple_cmd("Ds")
        assert await s.read_input(1, 2) is True
        await s.simple_cmd("Dp")
        info = await s.describe()
        assert info == {"input": {1: 8}, "output": {2: 16, 3: 16}}

        # Yes I know that this is unlikely
        for _ in range(10):
            m = s.write_timed_output(2, 4, True, 2)
            await m.start()
            if await m.wait():
                break
        else:
            assert False, "We didn't get a sane output."
        m = s.write_timed_output(2, 4, True, 10)
        await m.start()
        assert not await m.wait()

        m = s.count_input(1, 3, interval=2)
        await m.start()
        async for msg in m:
            print(msg)
            if msg > 20:
                break
        await m.aclose()

        m = s.monitor_input(1, 3)
        await m.start()
        x = 0
        async for msg in m:
            print(msg)
            x += 1
            if x >= 10:
                break
