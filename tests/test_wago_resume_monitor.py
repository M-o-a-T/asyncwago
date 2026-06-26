"""Test resuming a monitor after disconnect/reconnect."""

import anyio
import pytest
import socket
import subprocess
import sys

from asyncwago.server import Server, open_server


def get_free_port():
    """Return an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RealServer(Server):
    """Connect to the real wago binary."""

    freq = 0.1

    def __init__(self, taskgroup, port):
        super().__init__(taskgroup, "127.0.0.1", port)

    async def _connect(self):
        return await anyio.connect_tcp(remote_host=self.host, remote_port=self.port)


@pytest.fixture
def wago_server_port():
    """Start the wago binary on a free port, yield the port, then tear it down."""
    port = get_free_port()
    proc = subprocess.Popen(
        ["/src/wago-firmware/wago", "-p", str(port), "-c", "/src/wago-firmware/wago.sample.csv"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        yield port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["trio"])
async def test_resume_timed_output(wago_server_port):
    """Create a timed output via a raw TCP connection, disconnect,
    then resume the monitor with Server.find_monitor."""
    port = wago_server_port

    # Give the server a moment to start listening
    await anyio.sleep(0.2)

    # Step 1: open raw TCP connection, send timed set command, then disconnect.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))
    # read initial greeting
    greeting = b""
    while not greeting.endswith(b"\n"):
        greeting += s.recv(1024)
    # send timed set, expect a monitor-created reply
    s.sendall(b"s 2 4 3\n")
    reply = b""
    while not reply.endswith(b"\n"):
        reply += s.recv(1024)
    assert b"!+" in reply, f"unexpected reply: {reply!r}"
    s.close()

    # Step 2: connect via Server and resume the monitor
    async with anyio.create_task_group() as tg:
        srv = RealServer(tg, port)
        await srv.start()
        try:
            m = await srv.find_monitor(2, 4)
            assert m is not None, "monitor not found"
            async with m:
                await m.wait()
        finally:
            tg.cancel_scope.cancel()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
