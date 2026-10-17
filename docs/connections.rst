.. _connection-lifecycle:

Connection lifecycle callbacks
==============================

:class:`~aiokafka.AIOKafkaConsumer`, :class:`~aiokafka.AIOKafkaProducer`, and
``AIOKafkaClient`` accept three optional observer callbacks:

* ``on_connection_opened(connection)`` runs after a connection is established,
  including API version discovery and any TLS/SASL negotiation.
* ``on_connection_closed(connection, reason, error)`` reports an established
  connection closing. ``reason`` is a :class:`~aiokafka.ConnectionCloseReason`
  or ``None``; ``error`` is the underlying exception when available, otherwise
  ``None``.
* ``on_connection_failed(connection, error)`` reports a failed establishment
  attempt, including failures during bootstrap and background recovery.
  A cancelled attempt does not emit a failure notification. Request errors
  after establishment retain their existing handling; they do not emit this
  notification.

Each callback receives an immutable :class:`~aiokafka.ConnectionInfo` with
``client_id``, ``connection_id``, ``node_id``, ``host``, ``port``, and
``connection_group``. Bootstrap connections have ``node_id=None``, even if
they are retained for subsequent metadata requests. ``ConnectionGroup.DEFAULT``
identifies ordinary broker connections and ``ConnectionGroup.COORDINATION``
identifies coordination connections. Both enums and ``ConnectionInfo`` are
available directly from ``aiokafka``.

A connection's opening and closing notifications share the same opaque
``connection_id``. Each new attempt, including retries and replacements, gets
a new identifier. Reusing a connection emits no additional opening event.
Failed establishment emits only a failed notification, even if a socket was
opened before negotiation failed.

These notifications describe individual connections. An opening notification
does not establish cluster-wide availability, completed group coordination, or
resumed message processing. Bootstrap sockets may close normally as soon as
metadata is obtained. A close with ``reason=None`` can represent this normal
cleanup. ``IDLE_DROP`` identifies idle removal, ``SHUTDOWN`` identifies deliberate
client shutdown, and ``CONNECTION_BROKEN`` or ``CONNECTION_TIMEOUT`` identify
transport failures or request timeouts. ``OUT_OF_SYNC`` reports a mismatched
request/response correlation ID. ``AUTH_FAILURE`` is retained for compatibility
with the low-level connection's closing reasons. Authentication fails before
establishment completes, so public observers receive it only through
``on_connection_failed(connection, error)``. Handle authentication alerts there;
``on_connection_closed`` never reports ``AUTH_FAILURE``.

For example, the same observers can be supplied to a consumer or producer:

.. code-block:: python

    import logging

    from aiokafka import AIOKafkaProducer, ConnectionCloseReason, ConnectionInfo

    logger = logging.getLogger("kafka.connections")


    def opened(connection: ConnectionInfo) -> None:
        logger.info("Opened Kafka connection %s to %s:%s",
                    connection.connection_id, connection.host, connection.port)


    async def closed(
        connection: ConnectionInfo,
        reason: ConnectionCloseReason | None,
        error: BaseException | None,
    ) -> None:
        logger.info("Closed Kafka connection %s: %s (%r)",
                    connection.connection_id, reason, error)


    def failed(connection: ConnectionInfo, error: Exception) -> None:
        logger.warning("Kafka connection attempt %s failed: %r",
                       connection.connection_id, error)


    async def send():
        producer = AIOKafkaProducer(
            bootstrap_servers="localhost:9092",
            on_connection_opened=opened,
            on_connection_closed=closed,
            on_connection_failed=failed,
        )
        try:
            await producer.start()
            await producer.send_and_wait("events", b"hello")
        finally:
            await producer.stop()

Dispatch and shutdown
---------------------

Callbacks may be synchronous functions or return awaitables. Public type aliases
``ConnectionOpenedCallback``, ``ConnectionClosedCallback``, and
``ConnectionFailedCallback`` are available from ``aiokafka``.

Observers run in managed background tasks. Their exceptions are logged and do
not change connection management, retries, or request results. For each
connection, notifications run in order: the closing callback waits for the
opening callback to finish. Different connections have independent dispatch and
no ordering guarantee. By the time a callback runs, the connection may already
have closed. Callback completion is not required for ``start()`` or a request
to return.

Dispatch is limited to **128 connections with pending callback work per client**,
including callbacks that are currently running. Each connection attempt emits
at most an opening and a closing event, or a single failure event. If the limit
is reached, notifications for additional connections are dropped before
creating tasks or retaining their exceptions. Closing events for connections
already being dispatched remain queued behind their opening event. Kafka
connection attempts and requests continue without waiting for observer capacity.

A warning is logged on the first dropped notification in each overload episode.
Delivery resumes for new notifications when capacity becomes available; dropped
notifications are not replayed. Observers must tolerate missing notifications,
including a closing event whose opening event was dropped during overload.
This bound also applies when a callback never finishes.

Keep callbacks short and avoid blocking the event loop. Defer longer work to
application tasks. Callbacks are never awaited by connection establishment,
including while its connection lock is held.

On ``stop()`` (or ``AIOKafkaClient.close()``), connections close first, then
pending observers have up to **five seconds in total** to finish. Remaining
observer tasks are cancelled, and their queued notifications are discarded.
Cancelling ``stop()`` or ``close()`` skips the remaining grace period and
immediately requests cancellation of pending observers. Applications with a
shorter termination budget can use ``asyncio.wait_for(client.stop(), timeout)``.
This grace period is independent of Kafka's ``request_timeout_ms``.

Cancellation is cooperative: callbacks must propagate cancellation promptly;
shutdown does not wait indefinitely for a callback that suppresses it. If an
observer itself stops the client, shutdown does not wait for that observer.
A callback that suppresses cancellation can outlive ``stop()`` and retains its
running task until it finishes. Stop the client before closing its event loop,
and ensure callbacks propagate cancellation after any short cleanup. Arbitrary
callback code cannot be forcibly terminated by asyncio. Closing a low-level
connection between runs of its owning loop queues the notification on that
loop; callbacks cannot be delivered after the loop has closed.

With all three callbacks left as ``None``, no observer tasks are created.
