"""Public connection lifecycle types and managed observer dispatch."""

import asyncio
import inspect
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

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
    """Reason an established connection was closed."""

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

    def __init__(
        self,
        opened: ConnectionOpenedCallback | None,
        closed: ConnectionClosedCallback | None,
        failed: ConnectionFailedCallback | None,
    ) -> None:
        self._callbacks = {"opened": opened, "closed": closed, "failed": failed}
        for event, callback in self._callbacks.items():
            if callback is not None and not callable(callback):
                raise TypeError(f"on_connection_{event} must be callable or None")
        self._queues: dict[str, deque[tuple[str, tuple[Any, ...]]]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False
        self._cancelled = False

    def emit(self, event: str, info: ConnectionInfo, *args: Any) -> None:
        if self._closed or self._callbacks[event] is None:
            return
        connection_id = info.connection_id
        if connection_id not in self._queues:
            self._queues[connection_id] = deque()
            self._tasks[connection_id] = asyncio.create_task(self._run(connection_id))
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
            _, pending = await asyncio.wait(pending, timeout=self._SHUTDOWN_TIMEOUT)
        finally:
            if pending:
                self._cancelled = True
                for task in pending:
                    task.cancel()
                # Allow cooperative cleanup without waiting indefinitely for
                # a callback that suppresses cancellation.
                await asyncio.sleep(0)
