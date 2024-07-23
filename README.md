# Python asyncio wrapper for Paho MQTT Client

Minimal wrapper to use the [Paho MQTT Client](https://github.com/eclipse/paho.mqtt.python/) with [Python asyncio](https://docs.python.org/3/library/asyncio.html).
Initially inspired by the [`loop_asyncio.py`](https://github.com/eclipse/paho.mqtt.python/blob/master/examples/loop_asyncio.py) example of the Paho Client.


## Features

* Automatic background reconnect: Wrapper will automatically and continuously try to reconnect in the background when the connection is lost.
  No user intervention is necessary. Outbound messages are discarded while no connection is active.
  The wrapper will quit upon [`DISCONNECT`](https://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html#_Toc514345477) from broker with reason code `0x00` (*Normal disconnection*).
  Delays between reconnect attempts can be configured arbitrarily.
* Asynchronous iterator (`async for`) to consume inbound message instead of `on_message()` callback.
  The iterator persists over automatic reconnects and will only stop upon client-side termination.
* Synchronous (default) or asynchronous initial connection support:
  * In synchronous mode, the first connection attempt must succeed or an exception will be raised. Best for (semi-)interactive applications.
  * In asynchronous mode, will try to connect indefinitely. Intended primarily for system services.
* Backlog prevention:
  The Paho MQTT Client [keeps an unlimited backlog](https://github.com/eclipse/paho.mqtt.python/blob/v2.1.0/src/paho/mqtt/client.py#L1772) of unsent QoS=0 messages, only limiting the number of in-flight QoS>0 messages.
  If the upstream bandwidth drops below the average data rate of outbound messages, the backlog may grow indefinitely.
  When periodically publishing data over an unstable connection, backlogged stale data will clog the connection and increase upstream message latency.
  Instead, this wrapper will drop outbound messages if the socket send buffer ([`SO_SNDBUF`](https://manpages.debian.org/bookworm/manpages/socket.7.en.html#SO_SNDBUF)) is full.
  If a message was dropped, `publish()` returns `MQTTErrorCode.MQTT_ERR_QUEUE_SIZE`.
* Optional logging of background events (connect, disconnect, failed reconnect attempt) via the Python [`logging`](https://docs.python.org/3/library/logging.html) facility.
  Use `asyncio_mqtt.AsyncMqttClientLog` instead of `asyncio_mqtt.AsyncMqttClient` to enable logging.


## Usage

### Minimal Example

```python3
import asyncio
import asyncio_mqtt as mqttc

# need to override on_connect() to reliably (re-)subscribe on (re-)connection
class Example(mqttc.AsyncMqttClient):
    def on_connect(self, client, userdata, flags, reason_code, properties):
        super().on_connect(client, userdata, flags, reason_code, properties)
        self.client.subscribe("topic")

# exemplary coroutine periodically publishing messages
async def publish(client):
    while True:
        rc, mid = client.publish("another topic", b"some data")
        if rc != mqttc.MQTTErrorCode.MQTT_ERR_SUCCESS:
            pass
        await asyncio.sleep(1)

async def main():
    async with Example("test.mosquitto.org", keepalive=5) as client:
        asyncio.create_task(publish(client))
        async for msg in client:
            print(f"{msg.topic}: {msg.payload}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Advanced Example with Logging

See [`example.py`](./example.py) for a more advanced example with logging.


## Development

Run unit tests with `python3 -m unittest discover -v`.
