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
class MultilineReply(SimpleAckReply): # '='
    def __init__(self, line):
        super().__init__(line)
        self.lines = []
    def __repr__(self):
        return "<%s:%s (+%d)>" % \
                (self.__class__.__name__, repr(self.line), len(self.lines))

class MonitorReply(BaseReply): # '!+'
    def __init__(self, line):
        mon, line = line.lstrip().split(b' ',1)
        self.mon = int(mon)
        super().__init__(line)

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
    reply = None

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
        self.reply = reply
        await self.event.set()
        return True

    def __repr__(self):
        if self.reply is None:
            return "<%s>" % (self.__class__.__name__,)
        return "<%s =%s>" % (self.__class__.__name__,self.reply)

    async def repeat(self, server):
        """Re-enqueue to this server"""
        return

    async def _send(self, server):
        raise NotImplementedError("send")

    async def interact(self, server):
        """Start an interaction on this server"""
        self.server = server
        await server._interact(self)

    def close(self):
        """Only close when the connection to this server goes away."""
        self.event.set()
        self.server = None


class HelloChat(BaseChat):
    async def set(self, reply):
        if not isinstance(reply, SimpleInfo):
            return None
        self.reply = reply
        await self.event.set()
        return True

    async def _send(self, server):
        pass

class SimpleChat(BaseChat):
    reply = None
    def __init__(self, cmd):
        super().__init__()
        self.cmd = cmd

    def __repr__(self):
        if self.reply is None:
            return "<%s:%s>" % (self.__class__.__name__,self.cmd)
        return "<%s:%s =%s>" % (self.__class__.__name__,self.cmd,self.reply)

    async def _send(self, server):
        await server._send(self.cmd)

    async def set(self, reply):
        if not isinstance(reply, (SimpleAckReply, SimpleRejectReply)):
            return None
        self.reply = reply
        await self.event.set()
        return True

    async def wait(self):
        await self.event.wait()
        if isinstance(self.reply, SimpleRejectReply):
            raise WagoRejected(self.reply.line)
        return self.reply

    def close(self):
        if not self.event.is_set():
            self.reply = SimpleRejectReply("server closed")
        super().close()

class MonitorChat(SimpleChat):
    """Open a monitor, iterate the replies."""

    # None: not yet set, False: already closed
    mon = None
    q = None
    _qlen = 10
    server = None

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

    def decode_signal(self, line):
        """Override this to convert the text to something appropriate"""
        return line

    async def set(self, reply):
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
            return False
        if self.mon is None or reply.mon != self.mon:
            return None
        if isinstance(reply, MonitorCleared):
            self.event.set()
            return True

        assert isinstance(reply, MonitorSignal)
        if self.q is not None:
            sz = self.q.qsize()
            if sz < self._qlen:
                await self.q.put(self.decode_signal(reply.line))
            elif sz == self._qlen:
                await self.q.put(QueueBlocked)
        return False

    async def aclose(self):
        """Closes the iteration, i.e. shuts down the monitor."""
        await super().aclose()
        if self.mon is None:
            self.mon = False
        else:
            await self.server.simple_cmd("m-"+str(self.mon))

class PingChat(MonitorChat):
    def __init__(self, freq=1):
        super().__init__("Da "+str(freq))

    async def interact(self, server):
        await super().interact(server)


    def decode_signal(self, line):
        return int(line[line.rindex(' '):])

class Server:
    """This class collects all data required to talk to a single Wago
    controller.
    """
    freq = 5 # how often to poll input lines. Min 0.01

    def __init__(self, taskgroup, host, port=5995, freq=None):
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
            async with anyio.fail_after(5):
                line = await self.chan.receive_until(delimiter=b'\n', max_size=512)
                line = decode_reply(line)
                logger.debug("IN: %s",line)

                if isinstance(line, MultilineReply):
                    while True:
                        li = self.chan.receive_until(delimiter=b'\n', max_size=512)
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
            if res:
                del self._cmds[i]
            return
        raise WagoUnknown(reply)

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

