#!/usr/bin/python3
# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# (c) 2024 Carsten Andrich

import asyncio
import os
import socket
import threading
from typing import Self


class MqttServer:
    _TYPE_CONNECT = 0x10
    _TYPE_PUBLISH = 0x30
    _TYPE_SUBSCRIBE = 0x82
    _TYPE_SUBACK = 0x90

    def __init__(self, host: str = "127.0.0.1", port: int = 0, timeout: float = 5.):
        self._host = host
        self._port = port
        self._timeout = timeout

        # spawn worker thread
        self._thread = threading.Thread(target=asyncio.run, args=(self._mqtt_server(),))
        self._started = threading.Event()

    def __enter__(self) -> Self:
        self._thread.start()
        self._started.wait()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._loop.call_soon_threadsafe(self._server.close)
        # asyncio.Server.close() will leave existing client connections open, so
        # cancel each associtated client task individually
        self._loop.call_soon_threadsafe(self._cancel_clients)
        self._thread.join()

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_value, traceback):
        return self.__exit__(exc_type, exc_value, traceback)

    def getsockname(self):
        return self._sockname

    def _cancel_clients(self):
        # client tasks will remove themselves from set, so iterate over a copy
        for client_task in self._client_tasks.copy():
            client_task.cancel()

    async def _mqtt_server(self):
        self._loop = asyncio.get_running_loop()
        self._server = await asyncio.start_server(self._serve_client, #_timeout,
                                                  self._host, self._port,
                                                  limit=8192,
                                                  reuse_port=True if os.name == "posix" else False)
        self._client_tasks = set()
        self._sockname = self._server.sockets[0].getsockname()
        self._started.set()

        async with self._server:
            await self._server.wait_closed()

    async def _read_packet(self, reader) -> (int, bytes):
        msg_type, msg_len = await reader.readexactly(2)
        if msg_len < 128:
            payload = await reader.readexactly(msg_len)
        else:
            raise NotImplementedError("message longer than 127 bytes")

        return msg_type, payload

    async def _handshake(self, reader, writer) -> bool:
        msg_type, msg_payload = await self._read_packet(reader)
        if msg_type == self._TYPE_CONNECT:
            # basic CONNACK without any options
            writer.write(b"\x20\x02\x00\x00")
            await writer.drain()
            return True
        else:
            return False

    async def _serve_client_timeout(self, reader, writer):
        await asyncio.wait_for(self._serve_client(reader, writer), timeout=self._timeout)

    async def _serve_client(self, reader, writer):
        self._client_tasks.add(asyncio.current_task())
        try:
            if not await self._handshake(reader, writer):
                return

            while True:
                msg_type, msg_payload = await self._read_packet(reader)

                if msg_type == self._TYPE_PUBLISH:
                    buf = bytearray()
                    buf.append(msg_type)
                    buf.append(len(msg_payload))
                    buf.extend(msg_payload)
                    writer.write(buf)
                    await writer.drain()
                elif msg_type == self._TYPE_SUBSCRIBE:
                    buf = bytearray()
                    buf.append(self._TYPE_SUBACK)
                    buf.append(3)
                    buf.extend(msg_payload[0:2])
                    buf.append(0)
                    writer.write(buf)
                    await writer.drain()
                else:
                    break
        except asyncio.exceptions.CancelledError:
            pass
        except asyncio.exceptions.IncompleteReadError:
            # ignore client closing connection without sending complete request
            # will legitimately occur when client closes connection
            pass
        finally:
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            self._client_tasks.remove(asyncio.current_task())


if __name__ == "__main__":
    import time

    with MqttServer() as srv:
        print(srv.getsockname())
        time.sleep(3600)
