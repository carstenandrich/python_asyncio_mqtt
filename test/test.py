#!/usr/bin/python3
# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# (c) 2024 Carsten Andrich

import asyncio
import os
import time
import unittest

from asyncio_mqtt import AsyncMqttClient, MQTTException, MQTTErrorCode
from .mqtt_server import MqttServer

# reduce _LOOP_MISC_INTERVAL to accelerate unit tests
AsyncMqttClient._LOOP_MISC_INTERVAL = 0.1


class HttpServer(MqttServer):
    async def _serve_client(self, reader, writer):
        writer.write(b"HTTP/1.0 400 Bad Request\r\n\r\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()


class BlackholeServer(MqttServer):
    async def _serve_client(self, reader, writer):
        while True:
            if not await reader.read(4096):
                break
        writer.close()
        await writer.wait_closed()


class CloggedServer(MqttServer):
    async def _serve_client(self, reader, writer):
        self._client_tasks.add(asyncio.current_task())
        try:
            if not await self._handshake(reader, writer):
                return
            await asyncio.sleep(1)
        except asyncio.exceptions.CancelledError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            self._client_tasks.remove(asyncio.current_task())


class TestAsyncMqttClient(unittest.IsolatedAsyncioTestCase):
    _TOPIC = "test_topic"
    _PAYLOAD = b"test_payload"

    async def test_connect(self):
        async with \
        MqttServer() as srv, \
        AsyncMqttClient(*srv.getsockname()) as c:
            self.assertTrue(c._connected)

    async def test_connect_fail_http(self):
        with self.assertRaises(MQTTException):
            async with \
            HttpServer() as srv, \
            AsyncMqttClient(*srv.getsockname()) as c:
                pass

    async def test_connect_fail_keepalive(self):
        with self.assertRaises(MQTTException):
            async with \
            BlackholeServer() as srv, \
            AsyncMqttClient(*srv.getsockname(), keepalive=1) as c:
                pass

    @unittest.skipUnless(os.name == "posix", "dunno if this works on win32")
    async def test_connect_fail_enetunreach(self):
        with self.assertRaises(OSError):
            async with AsyncMqttClient("224.0.0.0") as c:
                pass

    @unittest.skipUnless(os.name == "posix", "dunno if this works on win32")
    async def test_connect_fail_timeout(self):
        with self.assertRaises(OSError):
            async with AsyncMqttClient("169.254.0.0", connect_timeout=0.1) as c:
                pass

    async def test_connect_async(self):
        async with \
        MqttServer() as srv, \
        AsyncMqttClient(*srv.getsockname(), async_connect=True) as c:
            await asyncio.sleep(0.1)
            self.assertTrue(c._connected)

    async def test_connect_async_fail(self):
        async with \
        HttpServer() as srv, \
        AsyncMqttClient(*srv.getsockname(), async_connect=True) as c:
            await asyncio.sleep(0.1)
            self.assertFalse(c._connected)

    async def test_disconnect_external(self):
        async with \
        MqttServer() as srv, \
        AsyncMqttClient(*srv.getsockname()) as c:
            asyncio.get_running_loop().call_later(0.1, c.client.disconnect)
            async for msg in c:
                pass

    async def test_disconnect_socket_open(self):
        async with \
        BlackholeServer() as srv, \
        AsyncMqttClient(*srv.getsockname(), async_connect=True, keepalive=1) as c:
            await asyncio.sleep(0.1)
            self.assertTrue(c._socket is not None)
            await c.disconnect()

    @unittest.skipUnless(os.name == "posix", "dunno if this works on win32")
    async def test_disconnect_socket_closed(self):
        async with AsyncMqttClient("169.254.0.0", async_connect=True, connect_timeout=0.5) as c:
            await asyncio.sleep(0.1)
            self.assertTrue(c._socket is None)
            await c.disconnect()

    async def test_reconnect(self):
        async with MqttServer() as srv:
            sockname = srv.getsockname()

        async with AsyncMqttClient(*sockname, async_connect=True, connect_delay=[0., 0.1], connect_timeout=0.1) as c:
            await asyncio.sleep(0.1)
            self.assertFalse(c._connected)

            async with MqttServer(*sockname) as srv:
                await asyncio.sleep(0.2)
                self.assertTrue(c._connected)
            await asyncio.sleep(0.1)
            self.assertFalse(c._connected)

            async with MqttServer(*sockname) as srv:
                await asyncio.sleep(0.2)
                self.assertTrue(c._connected)
            await asyncio.sleep(0.1)
            self.assertFalse(c._connected)

    async def test_publish_clogged(self):
        async with \
        CloggedServer() as srv, \
        AsyncMqttClient(*srv.getsockname(), keepalive=1) as c:
            payload = bytes(1024)
            for _ in range(128):
                rc, mid = c.publish(self._TOPIC, payload)
                if rc == MQTTErrorCode.MQTT_ERR_QUEUE_SIZE:
                    return
                # must await for send callback to be invoked by event loop
                await asyncio.sleep(0.0001)
            self.fail("publish() never returned MQTTErrorCode.MQTT_ERR_QUEUE_SIZE")

    async def test_subscribe_publish_break(self):
        async with \
        MqttServer() as srv, \
        AsyncMqttClient(*srv.getsockname(), keepalive=1) as c:
            c.subscribe(self._TOPIC)

            rc, mid = c.publish(self._TOPIC, self._PAYLOAD)
            self.assertEqual(rc, MQTTErrorCode.MQTT_ERR_SUCCESS)

            async for msg in c:
                self.assertEqual(msg.topic, self._TOPIC)
                self.assertEqual(msg.payload, self._PAYLOAD)
                break

    async def test_subscribe_publish_disconnect(self):
        async with \
        MqttServer() as srv, \
        AsyncMqttClient(*srv.getsockname()) as c:
            c.subscribe(self._TOPIC)

            rc, mid = c.publish(self._TOPIC, self._PAYLOAD)
            self.assertEqual(rc, MQTTErrorCode.MQTT_ERR_SUCCESS)

            async for msg in c:
                self.assertEqual(msg.topic, self._TOPIC)
                self.assertEqual(msg.payload, self._PAYLOAD)
                # schedule disconnect() as task, to test cancellation of
                # __anext__(), which is about to block this coroutine
                task_disconnect = asyncio.create_task(c.disconnect())
            await task_disconnect
