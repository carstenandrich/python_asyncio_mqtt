#!/usr/bin/python3
# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# (c) 2024 Carsten Andrich

import asyncio
import logging
import selectors
import socket
from typing import Self

from paho.mqtt import MQTTException
from paho.mqtt.client import MQTTErrorCode
import paho.mqtt.client as mqttc

_logger = logging.getLogger(__name__)


class AsyncMqttClient:
    _LOOP_MISC_INTERVAL = 1.0

    def __init__(
        self,
        host: str,
        port: int = 1883,
        async_connect: bool = False,
        client_id: str | None = None,
        connect_delay: list[float] = [0., 1., 2., 3., 5., 10., 15., 30., 60.],
        connect_timeout: float = 5.,
        keepalive: int = 60,
        so_sndbuf_size: int = 8192
    ):
        self._host: str = str(host)
        self._port: int = int(port)
        self._async_connect: bool = bool(async_connect)
        self._client_id: str | None = None if client_id is None else str(client_id)
        self._connect_delay: list[float] = [
            float(secs) if secs >= 0. else 0. for secs in connect_delay
        ]
        self._keepalive: int = int(keepalive)
        self._so_sndbuf_size: int = int(so_sndbuf_size)

        self._connect_errors: int = 0
        self._connected: bool = False
        self._selector: selectors.BaseSelector = selectors.DefaultSelector()
        self._socket: socket.socket | None = None
        self._task_connect: asyncio.Future | None = None
        self._task_queue_get: asyncio.Task | None = None
        self._task_worker: asyncio.Task | None = None

        self.client = mqttc.Client(mqttc.CallbackAPIVersion.VERSION2, client_id=self._client_id)
        self.client.connect_timeout = connect_timeout
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self._on_message
        self.client.on_socket_open = self._on_socket_open
        self.client.on_socket_close = self._on_socket_close
        self.client.on_socket_register_write = self._on_socket_register_write
        self.client.on_socket_unregister_write = self._on_socket_unregister_write

        self.subscribe = self.client.subscribe

    async def __aenter__(self) -> Self:
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue()

        # mqttc.Client.connect_async() prepares for mqttc.Client.reconnect()
        self.client.connect_async(self._host, self._port, self._keepalive)

        # connect asynchronously in background. will obediently attempt to
        # connect to a non-existing broker forever.
        if self._async_connect:
            # mqttc.Client.reconnect() will be called as necessary by _worker()
            self._task_worker = self._loop.create_task(self._worker())
            return self

        # connect synchronously. only return when MQTT connection is fully
        # established (CONNACK from broker) or has failed.
        self._task_connect = asyncio.Future()
        # try to open socket. will raise OSError on failure.
        rc = await self._loop.run_in_executor(None, self.client.reconnect)
        if rc != mqttc.MQTTErrorCode.MQTT_ERR_SUCCESS:
            raise MQTTException(f"paho.mqtt.client.Client.reconnect() failed with {rc!r}")

        # create background worker task. required for timeout logic to run.
        self._task_worker = self._loop.create_task(self._worker())
        # await invocation of on_connect() or on_disconnect() callback.
        # will implicity raise MQTTException upon connection failure.
        await self._task_connect
        self._task_connect = None
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.disconnect()
        try:
            # worker task will always be cancelled when disconnecting
            await self._task_worker
        except asyncio.exceptions.CancelledError:
            pass

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> mqttc.MQTTMessage:
        if self._queue is None:
            raise StopAsyncIteration
        if self._task_queue_get is not None:
            raise RuntimeError("__anext__() called again before previous call returned")

        self._task_queue_get = asyncio.create_task(self._queue.get())
        try:
            return await self._task_queue_get
        except asyncio.exceptions.CancelledError:
            raise StopAsyncIteration from None
        finally:
            self._task_queue_get = None

    async def disconnect(self):
        # mqttc.Client.disconnect() is blocking, so run in executor thread
        await self._loop.run_in_executor(None, self.client.disconnect)

        # if mqttc.Client is not currently connected, e.g., sleeping between
        # reconnect() attemps, mqttc.Client.on_disconnect() will not be called
        # and we must cancel the worker explicitly
        if not self._task_worker.done():
            self._task_worker.cancel()
            if self._task_queue_get is not None:
                self._task_queue_get.cancel()
            self._queue = None

    def publish(self, *args, **kwargs) -> (mqttc.MQTTErrorCode, int):
        # discard messages before on_connect() or after of_disconnect()
        # NOTE: the MQTTv5 specification permits PUBLISH before CONNACK
        # https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html#_Toc514345345
        if not self._connected:
            return (mqttc.MQTTErrorCode.MQTT_ERR_NO_CONN, -1)

        # discard this message if writing to the socket would block to prevent
        # the accumulation of a growing backlog of unsent messages.
        # NOTE: mqttc.Client only limits backlog of queued QoS>0 messages
        # https://github.com/eclipse/paho.mqtt.python/blob/v2.1.0/src/paho/mqtt/client.py#L1772
        if not self._selector.select(0):
            return (mqttc.MQTTErrorCode.MQTT_ERR_QUEUE_SIZE, -1)

        msg_info = self.client.publish(*args, **kwargs)
        return (msg_info.rc, msg_info.mid)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        self._connected = True
        self._connect_errors = 0

        # awake __aenter__() when connecting synchronously
        if self._task_connect is not None:
            self._task_connect.set_result(True)

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._connected = False

        # quit on intentional disconnect via mqttc.Client.disconnect()
        # NOTE: will also quit if brokers sends DISCONNECT with with reason code
        #       0x00 ("Normal disconnection"). the standard does not prohibit a
        #       broker from doing so:
        #       https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html#_Toc514345477
        if reason_code == mqttc.MQTTErrorCode.MQTT_ERR_SUCCESS:
            self._task_worker.cancel()
            if self._task_queue_get is not None:
                self._task_queue_get.cancel()
            self._queue = None
        else:
            # on_disconnect() is invoked if connecting fails after the socket
            # has already been opened successfully, but the broker does not
            # respond with positive CONNACK, e.g., on authentication failure.
            self._connect_errors += 1

            # awake __aenter__() when connecting synchronously
            if self._task_connect is not None:
                self._task_connect.set_exception(MQTTException(
                    f"paho.mqtt.client.Client.on_disconnect() invoked with {reason_code!r}"
                ))

    def on_reconnect_error(self, reason_code, exception):
        pass

    def _on_message(self, client, userdata, msg):
        self._queue.put_nowait(msg)

    def _on_socket_open(self, client, userdata, sock):
        assert self._socket is None
        self._socket = sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self._so_sndbuf_size)
        self._loop.add_reader(sock, self.client.loop_read)
        self._selector.register(sock, selectors.EVENT_WRITE)

    def _on_socket_close(self, client, userdata, sock):
        assert sock is self._socket
        self._socket = None
        self._loop.remove_reader(sock)
        self._selector.unregister(sock)

    def _on_socket_register_write(self, client, userdata, sock):
        self._loop.add_writer(sock, self.client.loop_write)

    def _on_socket_unregister_write(self, client, userdata, sock):
        self._loop.remove_writer(sock)

    async def _reconnect(self) -> mqttc.MQTTErrorCode:
        self._reconnect_exception = None
        try:
            # mqttc.Client.reconnect() is blocking, so run in executor thread
            # FIXME: blocks cancellation until reconnect() returns/raises
            return await self._loop.run_in_executor(None, self.client.reconnect)
        # translate ConnectionRefusedError, i.e., OSError(errno.ECONNREFUSED)
        except ConnectionRefusedError as e:
            self._reconnect_exception = e
            return mqttc.MQTTErrorCode.MQTT_ERR_CONN_REFUSED
        # translate other typical network errors, e.g., TimeoutError,
        # OSError(errno.ENETDOWN), OSError(errno.ENETUNREACH),
        # OSError(errno.EHOSTUNREACH), socket.gaierror, etc.
        except OSError as e:
            self._reconnect_exception = e
            return mqttc.MQTTErrorCode.MQTT_ERR_NO_CONN
        # translate unexpected exceptions
        except Exception as e:
            self._reconnect_exception = e
            return mqttc.MQTTErrorCode.MQTT_ERR_UNKNOWN

    async def _worker(self):
        while True:
            if self._socket is not None:
                rc = self.client.loop_misc()
                if rc == mqttc.MQTTErrorCode.MQTT_ERR_SUCCESS:
                    await asyncio.sleep(self._LOOP_MISC_INTERVAL)
                    continue

            # socket is closed or loop_misc() failed. wait before trying to
            # reconnect(), incrementally increasing wait times.
            if 0 <= self._connect_errors < len(self._connect_delay):
                await asyncio.sleep(self._connect_delay[self._connect_errors])
            else:
                await asyncio.sleep(self._connect_delay[-1])

            # try to reconnect()
            rc = await self._reconnect()
            if rc == mqttc.MQTTErrorCode.MQTT_ERR_SUCCESS:
                # socket is now open, but the MQTT handshake concluded by a
                # CONNACK from the broker has not yet finished.
                # if the handshake succeeds, on_connect() will be called back,
                # otherwise, on_disconnect() will be called. starting now, we
                # must periodically call loop_misc() to detect timeouts.
                pass
            else:
                # opening socket failed, e.g., due to network error or because
                # the server refused the connection. on_disconnect() callback
                # will not be invoked.
                self._connect_errors += 1
                self.on_reconnect_error(rc, self._reconnect_exception)


class AsyncMqttClientLog(AsyncMqttClient):
    def on_connect(self, client, userdata, flags, reason_code, properties):
        super().on_connect(client, userdata, flags, reason_code, properties)
        _logger.info(f"AsyncMqttClient connected with {reason_code!r}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        super().on_disconnect(client, userdata, flags, reason_code, properties)
        if reason_code == mqttc.MQTTErrorCode.MQTT_ERR_SUCCESS:
            _logger.info(f"AsyncMqttClient disconnected with {reason_code!r}")
        else:
            _logger.error(f"AsyncMqttClient disconnected with {reason_code!r}")

    def on_reconnect_error(self, reason_code, exception):
        super().on_reconnect_error(reason_code, exception)
        if exception:
            _logger.error(f"AsyncMqttClient reconnect failed with {reason_code!r} and exception <{exception!r}>")
        else:
            _logger.error(f"AsyncMqttClient reconnect failed with {reason_code!r}")
