#!/usr/bin/python3
# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# (c) 2024 Carsten Andrich

import asyncio
import asyncio_mqtt as mqttc
import logging
import os
import signal
import time
import uuid

_logger = logging.getLogger(__name__)
topic = str(uuid.uuid4())


class Example(mqttc.AsyncMqttClientLog):
    def on_connect(self, client, userdata, flags, reason_code, properties):
        super().on_connect(client, userdata, flags, reason_code, properties)
        self.client.subscribe(topic)


async def publish(client):
    while True:
        rc, mid = client.publish(topic, str(time.time_ns()))
        if rc != mqttc.MQTTErrorCode.MQTT_ERR_SUCCESS:
            _logger.error(f"MQTT publish failed: {rc!r}")
        await asyncio.sleep(1)

async def main():
    # set up signal handler for graceful termination on POSIX platforms
    if os.name == "posix":
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT,  asyncio.current_task().cancel)
        loop.add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)

    async with Example("test.mosquitto.org", keepalive=5) as client:
        asyncio.create_task(publish(client))
        async for msg in client:
            print(f"{msg.topic}: {msg.payload.decode()}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
