"""
Basic Wago access
"""

import anyio
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
        self.line = line
    def __repr__(self):
        return "REJ:"+repr(self.line)

class WagoUnknown(WagoException):
    """The controller sent an unknoan command"""
    def __init__(self, line):
        self.line = line
    def __repr__(self):
        return "UNK:"+repr(self.line)

### replies

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
class MultilineInfo(SimpleAckReply): # '='
    """Reply starts with ``=``: Multi-line."""
    def __init__(self, line):
        super().__init__(line)
        self.lines = []
    def __repr__(self):
        return "<%s:%s (+%d)>" % \
                (self.__class__.__name__, repr(self.line), len(self.lines))

class MonitorReply(BaseReply):
    """Monitor reply base class."""
    mon = None
    def __init__(self, line):
        mon, line = line.lstrip().split(b' ',1)
        self.mon = int(mon)
        super().__init__(line)
    def __repr__(self):
        return "<%s:%s %s>" % (self.__class__.__name__, self.mon, repr(self.line))

class MonitorCreated(MonitorReply): # '!+'
    """Reply starts with ``!+``: monitor created."""
    pass
class MonitorCleared(MonitorReply): # '!-'
    """Reply starts with ``!-``: monitor cleared."""
    pass
class MonitorSignal(MonitorReply): # '!' without trailing + or -
    """Reply starts with ``!``: monitor signal."""
    pass

def decode_reply(line):
    """Decode a reply line."""
    if line[0] == b'+'[0]:
        return SimpleAckReply(line[1:])
    if line[0] == b'-'[0]:
        return SimpleRejectReply(line[1:])
    if line[0] == b'*'[0]:
        return SimpleInfo(line[1:])
    if line[0] == b'='[0]:
        return MultilineInfo(line[1:])
    if line[0] != b'!'[0]:
        return UnknownReply(line)
    if line[1] == b'+'[0]:
        return MonitorCreated(line[2:])
    if line[1] == b'-'[0]:
        return MonitorCleared(line[2:])
    return MonitorSignal(line[1:])

class BaseChat:
    """Base class for sending a command to the server."""
    result = None
    server = None

    def __init__(self):
        self.event = anyio.create_event()

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
        await self.event.set()
        return True

    def __repr__(self):
        if self.result is None:
            return "<%s>" % (self.__class__.__name__,)
        return "<%s =%s>" % (self.__class__.__name__,self.result)

    async def _send(self, server):
        raise NotImplementedError("send")

    async def interact(self, server):
        """Run the command on this server"""
        self.server = server
        await server._interact(self)

    async def aclose(self):
        """Call this when the server goes away"""
        await self.event.set()
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

    def __init__(self, cmd):
        super().__init__()
        self.cmd = cmd

    def __repr__(self):
        if self.result is None:
            return "<%s:%s>" % (self.__class__.__name__,self.cmd)
        return "<%s:%s =%s>" % (self.__class__.__name__,self.cmd,self.result)

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
    
    Override `decode_signal` to check and convert the value.

    Override `process_signal` to do something with the resulting
    `MonitorSignal` event.
    """

    # None: not yet set, False: already closed
    mon = None

    def __init__(self, *args):
        self._did_setup = anyio.create_event()
        super().__init__(*args)

    def __repr__(self):
        res = super().__repr__()
        res = res[:-1]+" mon="+repr(self.mon)+res[-1]
        return res

    async def interact(self, server):
        await super().interact(server)
        await self._did_setup.wait()
        if isinstance(self.result, SimpleRejectReply):
            raise WagoRejected(self.result.line)

    def decode_signal(self, line):
        """Override this to convert the input to something appropriate"""
        return line

    async def set(self, reply):
        """Process replies"""
        if not self.mon and isinstance(reply, SimpleRejectReply):
            await self._did_setup.set()
            return await super().set(reply)
        if not isinstance(reply, MonitorReply):
            return None
        if isinstance(reply, MonitorCreated):
            if self.mon is False:
                # already cancelled.
                self.mon = reply.mon
                await self.server.task_group.spawn(self.aclose)
                return False
            if self.mon is not None:
                return None
            self.mon = reply.mon
            await self._did_setup.set()
            return False
        if self.mon is None or reply.mon != self.mon:
            return None
        if isinstance(reply, MonitorCleared):
            await self.event.set()
            await self._did_setup.set()  # just for safety
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
            m,self.mon = self.mon,0
            await server.simple_cmd("m-"+str(m))
        await self.event.set()
        await self._did_setup.set()

class TimedOutputChat(MonitorChat):
    """A monitor that expects a single trigger message.
    This is used for outputs that are auto-cleared after some time.

    `wait` will return True if the message has been seen.
    """
    result = False
    async def process_signal(self, reply):
        self.result = True

class InputMonitorChat(MonitorChat):
    """Watch an input, iterate the replies."""
    q = None
    _qlen = 10

    def __aiter__(self):
        if self.q is None:
            self.q = anyio.create_queue(self._qlen+2)
        return self

    async def __anext__(self):
        if self.q is None:
            raise StopAsyncIteration
        res = await self.q.get()
        if res is None:
            self.q = None
            raise StopAsyncIteration
        return res

    async def set(self, reply):
        if self.mon in (None,False) or not isinstance(reply, MonitorSignal) \
                or reply.mon != self.mon:
            return await super().set(reply)

        if self.q is not None:
            sz = self.q.qsize()
            if sz < self._qlen:
                await self.q.put(self.decode_signal(reply.line))
            elif sz == self._qlen:
                await self.q.put(QueueBlocked)
        return False

    def decode_signal(self, line):
        if line in (b'H', b'1'):
            return True
        if line in (b'L', b'0'):
            return False
        return ValueError(line)

    async def set(self, reply):
        if not self.mon or not isinstance(reply, MonitorReply) \
                or reply.mon != self.mon:
            return await super().set(reply)
        if isinstance(reply, MonitorCleared):
            if self.q is not None:
                await self.q.put(None)
                self.q = None
            return await super().set(reply)
        if isinstance(reply, MonitorSignal):
            if self.q is not None:
                sz = self.q.qsize()
                if sz <= self._qlen:
                    await self.q.put(self.decode_signal(reply.line))
                elif sz == self._qlen+1:
                    await self.q.put(QueueBlocked)
        return False

class InputCounterChat(InputMonitorChat):
    """Watch an input, count the changes."""
    def decode_signal(self, line):
        return int(line)


class PingChat(MonitorChat):
    _scope = None

    def __init__(self, freq=1):
        self._freq = freq
        self._wait = anyio.create_event()
        super().__init__("Da "+str(freq))

    def decode_signal(self, line):
        return int(line[line.rindex(' '):])

    async def process_signal(self, reply):
        await self._wait.set()

    async def ping_wait(self, evt):
        async with anyio.open_cancel_scope() as sc:
            self._scope = sc
            await evt.set()
            while True:
                try:
                    async with anyio.fail_after(self._freq * 3):
                        await self._wait.wait()
                except TimeoutError:
                    logger.error("PING missing %r", self)
                    raise
                self._wait = anyio.create_event()

    async def interact(self, server):
        evt = anyio.create_event()
        await server.task_group.spawn(self.ping_wait, evt)
        await evt.wait()
        return await super().interact(server)

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
        self._send_lock = anyio.create_lock()

    async def set_freq(self, freq: float = None):
        """
        Change the controller's time between main loop runs.
        """
        if freq is not None:
            self.freq = freq
        await self.simple_cmd("d",self.freq)

    async def set_ping_freq(self, ping_freq: float = None):
        """
        Change the time between two "ping" messages from the controller.
        """
        if ping_freq is not None:
            self.ping_freq = ping_freq
        if self._ping is not None:
            await self._ping.aclose()
        self._ping = PingChat(self.ping_freq)
        await self._ping.interact(self)

    async def _connect(self):
        return await anyio.connect_tcp(address=self.host, port=self.port)

    async def start(self):
        """This task holds the communication with a controller."""
        self.chan = await self._connect()
        tg = self.task_group
        evt = anyio.create_event()
        await tg.spawn(self._init_chan, evt)
        await evt.wait()
        await tg.spawn(self._reader)

    async def aclose(self):
        if self.chan is not None:
            await self.chan.close()
            self.chan = None

    async def _init_chan(self, evt):
        """Set up the device's state."""
        await HelloChat().interact(self)
        await evt.set()
        await self._set_state()

    async def _set_state(self):
        await self.set_freq()
        await self.set_ping_freq()

    async def _send(self, s):
        if self.chan is None:
            return
        logger.debug("OUT: %s",s)
        if isinstance(s, str):
            s = s.encode("utf-8")
        await self.chan.send_all(s+b'\n')

    async def _reader(self):
        while True:
            async with anyio.fail_after(self.freq + 2):
                if self.chan is None:
                    return
                line = await self.chan.receive_until(delimiter=b'\n', max_size=512)
                line = decode_reply(line)
                logger.debug("IN: %s",line)

                if isinstance(line, MultilineInfo):
                    while True:
                        li = await self.chan.receive_until(delimiter=b'\n', max_size=512)
                        logger.debug("IN:: %s",li)
                        if li == b'.':
                            break
                        line.lines.append(li)
                await self._process_reply(line)

    async def simple_cmd(self, *args):
        """Send a simple command to this server.

        A simple command results in exactly one reply message.
        If the reply is negative, this call raises 'WagoRejected`.
        """
        s = " ".join(str(arg) for arg in args)
        res = SimpleChat(s)
        await res.interact(self)
        return await res.wait()

    async def _interact(self, cmd):
        async with self._send_lock:
            self._cmds.append(cmd)
            await cmd._send(self)

    async def _process_reply(self, reply):
        for i,r in enumerate(self._cmds):
            res = await r.set(reply)
            if res is None:
                continue
            logger.debug("Did %s %s %s",r,reply, res)
            if res:
                del self._cmds[i]
            return
        raise WagoUnknown(reply)
    
    # Actual accessors follow

    async def read_input(self, card, port):
        """Read the state of a bool input."""
        res = await self.simple_cmd("i",card,port)
        return bool(int(res.line))

    async def read_output(self, card, port):
        """Read the current state of a bool output."""
        res = await self.simple_cmd("I",card,port)
        return bool(int(res.line))

    async def write_output(self, card, port, value):
        """Change the state of a bool output."""
        res = await self.simple_cmd("s" if value else "c", card, port)
        return bool(int(res.line))

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
        for l in info.lines:
            # 2: digital output:750-5xx 16 => 00010 00000 00000 0
            # 1: digital input:750-4xx 8 <= 00000 000

            i,dig,io,n,_ = l.split(b" ",4)
            if dig != b'digital':
                continue
            io = io[:io.index(b':')]
            if i[-1] == b':'[0]:
                i = i[:-1]
            res.setdefault(io.decode('ascii'),{})[int(i)] = int(n)
        return res

    async def monitor_input(self, card, port, direction=None):
        """
        Monitor this input line.

        Direction is True/False/None for up/down/both.
        """
        mon = InputMonitorChat("m+ %d %d %c" % \
                (card, port, "*" if direction is None
                                 else '+' if direction else '-'))
        await mon.interact(self)
        return mon

    async def count_input(self, card, port, direction=None, interval=None):
        """
        Count pulses on this input line. Reports count per interval.

        Direction is True/False/None for up/down/both.
        """
        if interval is None:
            interval = max(10, self.freq)
        mon = InputCounterChat("m# %d %d %c %f" % \
                (card, port, "*" if direction is None
                                 else '+' if direction else '-', interval))
        await mon.interact(self)
        return mon

    async def write_timed_output(self, card, port, value, duration):
        """
        Set (or clear) an output for N seconds.
        """
        timed = TimedOutputChat("%s %s %s %s" % ("s" if value else "c", card, port, duration))
        await timed.interact(self)
        return timed

    async def write_pulsed_output(self, card, port, value, duration1, duration2):
        """
        Set (or clear) an output for N1 seconds, then clear(or set) for N2 seconds, repeat.
        """
        timed = TimedOutputChat("%s %s %s %s %s" % ("s" if value else "c", card, port, duration1, duration2))
        await timed.interact(self)
        return timed


@asynccontextmanager
async def open_server(*args, ServerClass=Server, **kwargs):
    async with anyio.create_task_group() as tg:
        s = ServerClass(*args, taskgroup=tg, **kwargs)
        try:
            await s.start()
            yield s
        finally:
            await tg.cancel_scope.cancel()
            await s.aclose()

