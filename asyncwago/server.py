"""
Basic Wago access
"""

import anyio
from anyio.streams.buffered import BufferedByteReceiveStream

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

import logging

logger = logging.getLogger(__name__)


class QueueBlocked:
    """Sentinel to show that the reader doesn't keep up"""


class WagoException(RuntimeError):
    """Base class for our exceptions"""

    pass


class WagoRejected(WagoException):
    """The controller rejected a command"""

    def __init__(self, line):
        super().__init__()
        self.line = line

    def __repr__(self):
        return "REJ:" + repr(self.line)


class WagoUnknown(WagoException):
    """The controller sent an unknoan command"""

    def __init__(self, line):
        super().__init__()
        self.line = line

    def __repr__(self):
        return "UNK:" + repr(self.line)


# ### replies ### #


class BaseReply:
    """Base class for replies from the controller"""

    line = None

    def __init__(self, line):
        self.line = line

    def __repr__(self):
        return "<%s:%s>" % (self.__class__.__name__, repr(self.line))


class UnknownReply(BaseReply):
    """Anything without a recognized prefix"""

    pass


class SimpleAckReply(BaseReply):
    """Reply starts with ``+``: Ack."""

    pass


class SimpleRejectReply(BaseReply):
    """Reply starts with ``-``: Reject."""

    pass


class SimpleInfo(BaseReply):
    """Reply starts with ``*``: Info."""

    pass


class MultilineInfo(SimpleAckReply):  # '='
    """Reply starts with ``=``: Multi-line."""

    def __init__(self, line):
        super().__init__(line)
        self.lines = []

    def __repr__(self):
        return "<%s:%s (+%d)>" % (self.__class__.__name__, repr(self.line), len(self.lines))


class MonitorReply(BaseReply):
    """Monitor reply base class."""

    mon = None

    def __init__(self, line):
        mon, line = line.lstrip().split(b" ", 1)
        self.mon = int(mon)
        super().__init__(line)

    def __repr__(self):
        return "<%s:%s %s>" % (self.__class__.__name__, self.mon, repr(self.line))


class MonitorCreated(MonitorReply):  # '!+'
    """Reply starts with ``!+``: monitor created."""

    pass


class MonitorCleared(MonitorReply):  # '!-'
    """Reply starts with ``!-``: monitor cleared."""

    pass


class MonitorSignal(MonitorReply):  # '!' without trailing + or -
    """Reply starts with ``!``: monitor signal."""

    pass


def decode_reply(line):
    """Decode a reply line."""
    if line[0] == b"+"[0]:
        return SimpleAckReply(line[1:])
    if line[0] == b"-"[0]:
        return SimpleRejectReply(line[1:])
    if line[0] == b"*"[0]:
        return SimpleInfo(line[1:])
    if line[0] == b"="[0]:
        return MultilineInfo(line[1:])
    if line[0] != b"!"[0]:
        return UnknownReply(line)
    if line[1] == b"+"[0]:
        return MonitorCreated(line[2:])
    if line[1] == b"-"[0]:
        return MonitorCleared(line[2:])
    return MonitorSignal(line[1:])


class BaseChat:
    """Base class for sending a command to the server."""

    result = None
    server = None

    def __init__(self, server):
        self.server = server
        self.event = anyio.Event()

    async def set(self, reply):
        """
        Set this reply, assuming that it matches.

        Returns:
        * `None` if the result doesn't match
        * `True` if the result finishes the request
        * `False` if the result is accepted but doesn't finish the request

        Override this, and return ``__super__`` if you want to return `True`.
        """
        self.result = reply
        self.event.set()
        return True

    def __repr__(self):
        if self.result is None:
            return "<%s>" % (self.__class__.__name__,)
        return "<%s =%s>" % (self.__class__.__name__, self.result)

    async def _send(self, server):
        raise NotImplementedError("send")

    async def start(self):
        """Run the command on this server"""
        await self.server._start_interact(self)

    async def aclose(self):
        """Call this when the server goes away"""
        self.event.set()
        self.server = None


class HelloChat(BaseChat):
    """Read the initial message from the server"""

    async def set(self, reply):
        if not isinstance(reply, SimpleInfo):
            return None
        return await super().set(reply)

    async def _send(self, server):
        pass


class SimpleChat(BaseChat):
    """Send a command that expects a single reply"""

    def __init__(self, server, cmd):
        super().__init__(server)
        self.cmd = cmd

    def __repr__(self):
        if self.result is None:
            return "<%s:%s>" % (self.__class__.__name__, self.cmd)
        return "<%s:%s =%s>" % (self.__class__.__name__, self.cmd, self.result)

    async def _send(self, server):
        await server._send(self.cmd)

    async def set(self, reply):
        if not isinstance(reply, (SimpleAckReply, SimpleRejectReply, MultilineInfo)):
            return None
        return await super().set(reply)

    async def wait(self):
        await self.event.wait()
        if isinstance(self.result, SimpleRejectReply):
            raise WagoRejected(self.result.line)
        return self.result

    async def aclose(self):
        if not self.event.is_set():
            self.result = SimpleRejectReply("server closed")
        else:
            assert self.result is not None
        await super().aclose()


class MonitorChat(SimpleChat):
    """Open a monitor.

    Override `process_signal` to do something with the resulting
    `MonitorSignal` event.
    """

    # None: not yet set, False: already closed
    mon = None

    def __init__(self, *args):
        self._did_setup = anyio.Event()
        super().__init__(*args)

    def __repr__(self):
        res = super().__repr__()
        res = res[:-1] + " mon=" + repr(self.mon) + res[-1]
        return res

    async def start(self):
        await super().start()
        await self._did_setup.wait()
        if isinstance(self.result, SimpleRejectReply):
            raise WagoRejected(self.result.line)

    async def set(self, reply):
        """Process replies"""
        if self.mon is None and isinstance(reply, SimpleRejectReply):
            self._did_setup.set()
            return await super().set(reply)
        if not isinstance(reply, MonitorReply):
            return None
        if isinstance(reply, MonitorCreated):
            if self.mon is False:
                # already cancelled.
                self.mon = reply.mon
                self.server.task_group.start_soon(self.aclose)
                return False
            if self.mon is not None:
                return None
            self.mon = reply.mon
            self._did_setup.set()
            return False
        if self.mon is None or reply.mon != self.mon:
            return None
        if isinstance(reply, MonitorCleared):
            self.mon = 0
            self.event.set()
            self._did_setup.set()  # just for safety
            # does not call super() because we want to keep the last reply
            return True
        assert isinstance(reply, MonitorSignal)
        await self.process_signal(reply)
        return False

    async def process_signal(self, reply):
        """Process `MonitorSignal` events.

        Override this; the default does nothing.
        """
        pass

    async def aclose(self):
        """Closes the iteration, i.e. shuts down the monitor."""
        server = self.server
        if self.mon is None:
            self.mon = False
        elif self.mon:
            m, self.mon = self.mon, 0
            try:
                await server.simple_cmd("m-" + str(m))
            except anyio.ClosedResourceError:
                pass
        self.event.set()
        self._did_setup.set()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *tb):
        with anyio.fail_after(2, shield=True):
            await self.aclose()


class TimedOutputChat(MonitorChat):
    """
    A monitor that expects a single trigger message.
    This is used for outputs that are auto-cleared after some time.

    `wait` will return True if the message has been seen.
    """

    result = False

    def __init__(self, server, card, port, value, duration1, duration2=None):
        if duration2 is None:
            cmd = "%s %s %s %s" % (
                "s" if value else "c",
                card,
                port,
                duration1,
            )
        else:
            cmd = "%s %s %s %s %s" % ("s" if value else "c", card, port, duration1, duration2)

        super().__init__(server, cmd)
        self.card = card
        self.port = port
        self.value = value

    async def process_signal(self, reply):
        self.result = True

    async def aclose(self):
        """Clear the wire"""
        with anyio.fail_after(2, shield=True):
            await super().aclose()
            try:
                await self.server.simple_cmd("c" if self.value else "s", self.card, self.port)
            except anyio.ClosedResourceError:
                pass


class InputMonitorChat(MonitorChat):
    """
    Watch an input, iterate the replies.

    Override `decode_signal` to check and convert the value.
    """

    qr = None
    qw = None
    _qc = 0
    _qlen = 10

    def __aiter__(self):
        return self

    async def __aenter__(self):
        if self.qw is None:
            self.qw,self.qr = anyio.create_memory_object_stream(self._qlen + 2)
        return await super().__aenter__()

    async def __anext__(self):
        if self.qr is None:
            raise StopAsyncIteration
        try:
            res = await self.qr.receive()
        except anyio.ClosedResourceError:
            raise StopAsyncIteration
        self._qc -= 1
        return res

    def decode_signal(self, line):
        if line in (b"H", b"1"):
            return True
        if line in (b"L", b"0"):
            return False
        return ValueError(line)

    async def set(self, reply):
        if not self.mon or not isinstance(reply, MonitorReply) or reply.mon != self.mon:
            return await super().set(reply)
        if isinstance(reply, MonitorCleared):
            if self.qw is not None:
                await self.qw.aclose()
                self.qw = self.qr = None
            return await super().set(reply)
        if isinstance(reply, MonitorSignal):
            if self.qw is not None:
                sz = self._qc
                if sz <= self._qlen:
                    await self.qw.send(self.decode_signal(reply.line))
                    self._qc += 1
                elif sz == self._qlen + 1:
                    await self.qw.send(QueueBlocked)
                    self._qc += 1
        return False


class InputCounterChat(InputMonitorChat):
    """Watch an input, count the changes."""

    def decode_signal(self, line):
        return int(line)


class PingChat(MonitorChat):
    _scope = None

    def __init__(self, server, freq=1):
        self._freq = freq
        self._wait = anyio.Event()
        super().__init__(server, "Da " + str(freq))

    def decode_signal(self, line):
        return int(line[line.rindex(" ") :])

    async def process_signal(self, reply):
        self._wait.set()

    async def ping_wait(self, *, task_status):
        with anyio.CancelScope() as sc:
            self._scope = sc
            task_status.started()
            while True:
                try:
                    with anyio.fail_after(self._freq * 3):
                        await self._wait.wait()
                except TimeoutError:
                    logger.error("PING missing %r", self)
                    raise
                self._wait = anyio.Event()

    async def start(self):
        await self.server.task_group.start(self.ping_wait)
        return await super().start()

    async def aclose(self):
        await super().aclose()
        if self._scope is not None:
            await self._scope.cancel()
        await super().aclose()


class Server:
    """This class collects all data required to talk to a single Wago
    controller.
    """

    freq = 0.05  # how often to poll input lines. Min 0.01
    ping_freq = 3  # how often to expect a Ping keepalive
    _ping = None

    chan = None

    def __init__(self, taskgroup, host, port=59995, freq=None, ping_freq=None):
        self.task_group = taskgroup
        self.host = host
        self.port = port
        if freq is not None:
            self.freq = freq
        if ping_freq is not None:
            self.ping_freq = freq

        self._ports = {}
        self._cmds = []
        self._send_lock = anyio.Lock()

    async def set_freq(self, freq: float = None):
        """
        Change the controller's time between main loop runs.
        """
        if freq is not None:
            self.freq = freq
        await self.simple_cmd("d", self.freq)

    async def set_ping_freq(self, ping_freq: float = None):
        """
        Change the time between two "ping" messages from the controller.
        """
        if ping_freq is not None:
            self.ping_freq = ping_freq
        if self._ping is not None:
            await self._ping.aclose()
        self._ping = PingChat(self, self.ping_freq)
        await self._ping.start()

    async def _connect(self):
        return await anyio.connect_tcp(remote_host=self.host, remote_port=self.port)

    async def start(self):
        """This task holds the communication with a controller."""
        self.chan = await self._connect()
        self.chan_r = BufferedByteReceiveStream(self.chan)
        tg = self.task_group
        await tg.start(self._init_chan)
        await tg.start(self._reader)

    async def aclose(self):
        if self.chan is not None:
            await self.chan.aclose()
            self.chan = self.chan_r = None

    async def _init_chan(self, *, task_status):
        """Set up the device's state."""
        await HelloChat(self).start()
        task_status.started()
        await self._set_state()

    async def _set_state(self):
        await self.set_freq()
        await self.set_ping_freq()

    async def _send(self, s):
        if self.chan is None:
            raise anyio.ClosedResourceError
        logger.debug("OUT: %s", s)
        if isinstance(s, str):
            s = s.encode("utf-8")
        await self.chan.send(s + b"\n")

    async def _reader(self, *, task_status):
        task_status.started()
        while True:
            if self.chan is None:
                return
            line = await self.chan_r.receive_until(delimiter=b"\n", max_bytes=512)
            line = decode_reply(line)
            logger.debug("IN: %s", line)

            if isinstance(line, MultilineInfo):
                while True:
                    li = await self.chan_r.receive_until(delimiter=b"\n", max_bytes=512)
                    logger.debug("IN:: %s", li)
                    if li == b".":
                        break
                    line.lines.append(li)
            await self._process_reply(line)

    async def simple_cmd(self, *args):
        """Send a simple command to this server.

        A simple command results in exactly one reply message.
        If the reply is negative, this call raises 'WagoRejected`.
        """
        s = " ".join(str(arg) for arg in args)
        res = SimpleChat(self, s)
        await res.start()
        return await res.wait()

    async def _start_interact(self, cmd):
        async with self._send_lock:
            self._cmds.append(cmd)
            await cmd._send(self)

    async def _process_reply(self, reply):
        for i, r in enumerate(self._cmds):
            res = await r.set(reply)
            if res is None:
                continue
            logger.debug("Did %s %s %s", r, reply, res)
            if res:
                del self._cmds[i]
            return
        if not isinstance(reply, MonitorCleared):
            # collision between settingup and tearing down a monitor?
            raise WagoUnknown(reply)

    # Actual accessors follow

    async def read_input(self, card, port):
        """Read the state of a bool input."""
        res = await self.simple_cmd("i", card, port)
        return bool(int(res.line))

    async def read_output(self, card, port):
        """Read the current state of a bool output."""
        res = await self.simple_cmd("I", card, port)
        return bool(int(res.line))

    async def write_output(self, card, port, value):
        """Change the state of a bool output."""
        await self.simple_cmd("s" if value else "c", card, port)
        return value

    async def describe(self):
        """Retrieve the interfaces attached to this server.

        Result::
            input:
                1: 8
            output:
                2: 16

        describes a device with one 8-wire input card 1, and one 16-wire
        output card 2.

        Ports are numbered ``1…n``, not ``0…n-1``!
        """
        res = {}
        info = await self.simple_cmd("Dp")
        for line in info.lines:
            # 2: digital output:750-5xx 16 => 00010 00000 00000 0
            # 1: digital input:750-4xx 8 <= 00000 000

            i, dig, io, n, _ = line.split(b" ", 4)
            if dig != b"digital":
                continue
            io = io[: io.index(b":")]
            if i[-1] == b":"[0]:
                i = i[:-1]
            res.setdefault(io.decode("ascii"), {})[int(i)] = int(n)
        return res

    def monitor_input(self, card, port, direction=None):
        """
        Monitor this input line.

        Direction is True/False/None for up/down/both.
        """
        return InputMonitorChat(
            self,
            "m+ %d %d %c" % (card, port, "*" if direction is None else "+" if direction else "-"),
        )

    def count_input(self, card, port, direction=None, interval=None):
        """
        Count pulses on this input line. Reports count per interval.

        Direction is True/False/None for up/down/both.
        """
        if interval is None:
            interval = max(10, self.freq)
        return InputCounterChat(
            self,
            "m# %d %d %c %f"
            % (card, port, "*" if direction is None else "+" if direction else "-", interval),
        )

    def write_timed_output(self, card, port, value, duration):
        """
        Set (or clear) an output for N seconds.

        This is an async context manager. Call ``await mgr.wait()`` to
        delay until the wire is reset. Cancelling will reset the wire.
        """
        return TimedOutputChat(self, card, port, value, duration)

    def write_pulsed_output(self, card, port, value, duration1, duration2):
        """
        Set (or clear) an output for N1 seconds, then clear(or set) for N2 seconds, repeat.

        This is an async context manager. Cancelling will reset the wire.
        """
        return TimedOutputChat(self, card, port, value, duration1, duration2)


@asynccontextmanager
async def open_server(*args, ServerClass=Server, **kwargs):
    async with anyio.create_task_group() as tg:
        s = ServerClass(*args, taskgroup=tg, **kwargs)
        try:
            await s.start()
            yield s
        finally:
            tg.cancel_scope.cancel()
            await s.aclose()
