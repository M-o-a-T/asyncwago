"""
Basic Wago access
"""

import anyio
from async_generator import asynccontextmanager
from async_generator import async_generator, yield_

import logging
logger = logging.getLogger(__name__)

class QueueBlocked:
    """Sentinel to show that the reader doesn't keep up"""

class WagoException(RuntimeError):
    pass

class WagoRejected(WagoException):
    def __init__(self, line):
        self.line = line
    def __repr__(self):
        return "REJ:"+repr(self.line)

class WagoUnknown(WagoException):
    def __init__(self, line):
        self.line = line
    def __repr__(self):
        return "UNK:"+repr(self.line)

### replies

class BaseReply:
    line = None
    def __init__(self, line):
        self.line = line
    def __repr__(self):
        return "<%s:%s>" % (self.__class__.__name__, repr(self.line))
class UnknownReply(BaseReply): # ???
    pass
class SimpleAckReply(BaseReply): # '+'
    pass
class SimpleRejectReply(BaseReply): # '?'
    pass
class SimpleInfo(BaseReply): # '*'
    pass
class MultilineInfo(SimpleAckReply): # '='
    def __init__(self, line):
        super().__init__(line)
        self.lines = []
    def __repr__(self):
        return "<%s:%s (+%d)>" % \
                (self.__class__.__name__, repr(self.line), len(self.lines))

class MonitorReply(BaseReply): # '!+'
    mon = None
    def __init__(self, line):
        mon, line = line.lstrip().split(b' ',1)
        self.mon = int(mon)
        super().__init__(line)
    def __repr__(self):
        return "<%s:%s %s>" % (self.__class__.__name__, self.mon, repr(self.line))

class MonitorCreated(MonitorReply): # '!+'
    pass
class MonitorCleared(MonitorReply): # '!-'
    pass
class MonitorSignal(MonitorReply): # '!' without trailing + or -
    pass

def decode_reply(line):
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
        """
        self.result = reply
        await self.event.set()
        return True

    def __repr__(self):
        if self.result is None:
            return "<%s>" % (self.__class__.__name__,)
        return "<%s =%s>" % (self.__class__.__name__,self.result)

    async def repeat(self, server):
        """Re-enqueue to this server"""
        return

    async def _send(self, server):
        raise NotImplementedError("send")

    async def interact(self, server):
        """Start an interaction on this server"""
        self.server = server
        await server._interact(self)

    async def aclose(self):
        """Call this when the server goes away"""
        self.event.set()
        self.server = None


class HelloChat(BaseChat):
    """Read the initial message from the server"""
    async def set(self, reply):
        if not isinstance(reply, SimpleInfo):
            return None
        self.result = reply
        await self.event.set()
        return True

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
        self.result = reply
        await self.event.set()
        return True

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
    """Open a monitor"""

    # None: not yet set, False: already closed
    mon = None

    def __init__(self, *args):
        self._did_setup = anyio.create_event()
        super().__init__(*args)

    async def interact(self, server):
        await super().interact(server)
        await self._did_setup.wait()
        if isinstance(self.result, SimpleRejectReply):
            raise WagoRejected(self.result.line)

    def decode_signal(self, line):
        """Override this to convert the text to something appropriate"""
        return line

    async def set(self, reply):
        if not self.mon and isinstance(reply, SimpleRejectReply):
            self.result = reply
            await self.event.set()
            await self._did_setup.set()
            return True
        if not isinstance(reply, MonitorReply):
            return None
        if isinstance(reply, MonitorCreated):
            if self.mon is False:
                # already cancelled.
                self.mon = reply.mon
                await self.server.tg.spawn(self.aclose)
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
            return True
        assert isinstance(reply, MonitorSignal)
        await self.process_signal(reply)
        return False

    async def process_signal(self, reply):
        pass

    async def aclose(self):
        """Closes the iteration, i.e. shuts down the monitor."""
        server = self.server
        await super().aclose()
        if self.mon is None:
            self.mon = False
        elif server is not None:
            await server.simple_cmd("m-"+str(self.mon))
        await self._did_setup.set()

class TimedOutputChat(MonitorChat):
    """A monitor that expects a single trigger message.
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
        """Override this to convert the text to something appropriate"""
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
            return super().set(reply)
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
    def __init__(self, freq=1):
        super().__init__("Da "+str(freq))

    def decode_signal(self, line):
        return int(line[line.rindex(' '):])

class Server:
    """This class collects all data required to talk to a single Wago
    controller.
    """
    freq = 5 # how often to poll input lines. Min 0.01

    def __init__(self, taskgroup, host, port=59995, freq=None):
        self.task_group = taskgroup
        self.host = host
        self.port = port
        if freq is not None:
            self.freq = freq

        self._ports = {}
        self._cmds = []
        self._send_lock = anyio.create_lock()

    async def _connect(self):
        return await anyio.connect_tcp(address=self.host, port=self.port)

    async def start(self):
        """This task holds the communication with a controller."""
        self.chan = await self._connect()
        tg = self.task_group
        await tg.spawn(self._init_chan)
        await tg.spawn(self._reader)

    async def aclose(self):
        await self.chan.close()

    async def _init_chan(self):
        """Set up the device's state."""
        await HelloChat().interact(self)

        await self.simple_cmd("d",self.freq)
        ping = PingChat()
        await ping.interact(self)

    async def _send(self, s):
        logger.debug("OUT: %s",s)
        if isinstance(s, str):
            s = s.encode("utf-8")
        await self.chan.send_all(s+b'\n')

    async def _reader(self):
        while True:
            async with anyio.fail_after(self.freq + 2):
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
        res = await self.simple_cmd("i",card,port)
        return bool(int(res.line))

    async def read_output(self, card, port):
        res = await self.simple_cmd("I",card,port)
        return bool(int(res.line))

    async def write_output(self, card, port, value):
        res = await self.simple_cmd("s" if value else "c", card, port)
        return bool(int(res.line))

    async def describe(self):
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
        """
        mon = InputMonitorChat("m+ %d %d %c" % \
                (card, port, "*" if direction is None
                                 else '+' if direction else '-'))
        await mon.interact(self)
        return mon

    async def count_input(self, card, port, direction=None, interval=None):
        """
        Monitor this input line.
        """
        if interval is None:
            interval = max(10, self.freq)
        mon = InputCounterChat("m# %d %d %c %f" % \
                (card, port, "*" if direction is None
                                 else '+' if direction else '-', interval))
        await mon.interact(self)
        return mon

    async def write_timed_output(self, card, port, value, duration):
        timed = TimedOutputChat("%s %s %s %s" % ("s" if value else "c", card, port, duration))
        await timed.interact(self)
        return timed


@asynccontextmanager
@async_generator
async def open_server(*args, ServerClass=Server, **kwargs):
    async with anyio.create_task_group() as tg:
        s = ServerClass(*args, taskgroup=tg, **kwargs)
        try:
            await s.start()
            await yield_(s)
        finally:
            await tg.cancel_scope.cancel()
            await s.aclose()

