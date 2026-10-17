"""Public connection lifecycle types and managed observer dispatch."""

import asyncio
import inspect
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum
from functools import partial
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aiokafka.conn import AIOKafkaConnection

__all__ = [
    "ConnectionCloseReason",
    "ConnectionClosedCallback",
    "ConnectionFailedCallback",
    "ConnectionGroup",
    "ConnectionInfo",
    "ConnectionOpenedCallback",
]

log = logging.getLogger(__name__)


class ConnectionCloseReason(IntEnum):
    """Reason a Kafka connection was closed.

    ``AUTH_FAILURE`` is used during authentication by the low-level connection.
    Public observers receive authentication errors through
    ``on_connection_failed``, rather than ``on_connection_closed``.
    """

    CONNECTION_BROKEN = 0
    CONNECTION_TIMEOUT = 1
    OUT_OF_SYNC = 2
    IDLE_DROP = 3
    SHUTDOWN = 4
    AUTH_FAILURE = 5


class ConnectionGroup(IntEnum):
    """Purpose of a connection to a Kafka node."""

    DEFAULT = 0
    COORDINATION = 1


@dataclass(frozen=True)
class ConnectionInfo:
    """Immutable description of one connection attempt and its connection.

    ``connection_id`` is an opaque identifier, unique to each attempt.
    ``node_id`` is ``None`` for bootstrap connections. The description stays
    unchanged for the lifetime of the connection, including after bootstrap.
    """

    client_id: str
    connection_id: str
    node_id: int | None
    host: str
    port: int
    connection_group: ConnectionGroup


ConnectionOpenedCallback = Callable[[ConnectionInfo], Awaitable[None] | None]
ConnectionClosedCallback = Callable[
    [ConnectionInfo, ConnectionCloseReason | None, BaseException | None],
    Awaitable[None] | None,
]
ConnectionFailedCallback = Callable[[ConnectionInfo, Exception], Awaitable[None] | None]


class _ConnectionCallbacks:
    # One deadline for all observers, independent of request_timeout_ms.
    _SHUTDOWN_TIMEOUT = 5.0
    # An attempt emits either failed or opened/closed, so limiting the
    # number of pending connections bounds both tasks and retained events.
    _MAX_PENDING_CONNECTIONS = 128

    def __init__(
        self,
        opened: ConnectionOpenedCallback | None,
        closed: ConnectionClosedCallback | None,
        failed: ConnectionFailedCallback | None,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._callbacks = {"opened": opened, "closed": closed, "failed": failed}
        for event, callback in self._callbacks.items():
            if callback is not None and not callable(callback):
                raise TypeError(f"on_connection_{event} must be callable or None")
        self._loop = loop if loop is not None else asyncio.get_running_loop()
        self._queues: dict[str, deque[tuple[str, tuple[Any, ...]]]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False
        self._cancelled = False
        self._overflow_warned = False

    def emit(self, event: str, info: ConnectionInfo, *args: Any) -> None:
        if self._closed or self._callbacks[event] is None:
            return
        if self._loop.is_closed():
            log.warning(
                "Dropping on_connection_%s callback: event loop is closed", event
            )
            return
        connection_id = info.connection_id
        if connection_id not in self._queues:
            if len(self._tasks) >= self._MAX_PENDING_CONNECTIONS:
                if not self._overflow_warned:
                    self._overflow_warned = True
                    log.warning(
                        "Connection observer limit reached (%s pending connections); "
                        "dropping notifications for other connections",
                        self._MAX_PENDING_CONNECTIONS,
                    )
                return
            self._overflow_warned = False
            self._queues[connection_id] = deque()
            task = self._loop.create_task(self._run(connection_id))
            self._tasks[connection_id] = task
            task.add_done_callback(partial(self._discard, connection_id))
        self._queues[connection_id].append((event, (info, *args)))

    async def _run(self, connection_id: str) -> None:
        try:
            # Also defer synchronous callbacks with an eager task factory.
            await asyncio.sleep(0)
            queue = self._queues[connection_id]
            while queue and not self._cancelled:
                event, args = queue.popleft()
                callback = self._callbacks[event]
                assert callback is not None
                try:
                    result = callback(*args)
                    if inspect.isawaitable(result):
                        await result
                except asyncio.CancelledError:
                    task = asyncio.current_task()
                    if task is not None and task.cancelling():
                        raise
                    log.exception("Error in on_connection_%s callback", event)
                except Exception:
                    log.exception("Error in on_connection_%s callback", event)
        finally:
            self._discard(connection_id, asyncio.current_task())

    def _discard(self, connection_id: str, task: asyncio.Task[None] | None) -> None:
        # The done callback also covers cancellation before _run starts. A new
        # event may already have installed a different task for this connection.
        if self._tasks.get(connection_id) is task:
            self._queues.pop(connection_id, None)
            self._tasks.pop(connection_id, None)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # A callback may itself stop its client. Never wait for that task here.
        current = asyncio.current_task()
        pending = {task for task in self._tasks.values() if task is not current}
        if not pending:
            return
        try:
            # close() may be entered from a finally block after its caller has
            # already received CancelledError while closing transports.
            if current is None or not current.cancelling():
                _, pending = await asyncio.wait(pending, timeout=self._SHUTDOWN_TIMEOUT)
        finally:
            if pending:
                self._cancelled = True
                for queue in self._queues.values():
                    queue.clear()
                for task in pending:
                    task.cancel()
                # Allow cooperative cleanup without waiting indefinitely for
                # a callback that suppresses cancellation.
                await asyncio.sleep(0)
                for connection_id, task in tuple(self._tasks.items()):
                    if task.done():
                        self._discard(connection_id, task)


class _ConnectionObserver:
    """Track establishment and closure, including a close during negotiation."""

    def __init__(
        self,
        info: ConnectionInfo,
        callbacks: _ConnectionCallbacks,
        on_close: Callable[["AIOKafkaConnection", ConnectionCloseReason | None], None]
        | None,
    ) -> None:
        self._info = info
        self._callbacks = callbacks
        self._on_close = on_close
        self._established = False
        self._close_event: (
            tuple[ConnectionCloseReason | None, BaseException | None] | None
        ) = None

    def on_close(
        self, conn: "AIOKafkaConnection", reason: ConnectionCloseReason | None
    ) -> None:
        if self._on_close is not None:
            self._on_close(conn, reason)
        event = (reason, conn._close_exception)
        if self._established:
            self._callbacks.emit("closed", self._info, *event)
        else:
            self._close_event = event

    def opened(self) -> None:
        self._established = True
        self._callbacks.emit("opened", self._info)
        # Negotiation can succeed just as the peer closes the connection.
        if self._close_event is not None:
            event, self._close_event = self._close_event, None
            self._callbacks.emit("closed", self._info, *event)

    def failed(self, exc: Exception) -> None:
        self._close_event = None
        self._callbacks.emit("failed", self._info, exc)
