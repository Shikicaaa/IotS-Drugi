import asyncio
import json
import argparse
import time
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt_client
except ImportError:
    mqtt_client = None

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None


class Stats:
    def __init__(self):
        self.sent = 0
        self.failed = 0

        self.connected = False

        self.disconnect_time = None
        self.reconnect_time = None

        self.start_time = time.time()

    def report(self):
        print("\n========== Scenario B Report ==========")

        print(f"Sent: {self.sent}")
        print(f"Failed: {self.failed}")

        if self.disconnect_time and self.reconnect_time:
            recovery = (
                self.reconnect_time - self.disconnect_time
            ).total_seconds()

            print(f"Recovery Time: {recovery:.2f}s")

        print("=======================================\n")


stats = Stats()


def on_connect(client, userdata, flags, reason_code, properties=None):
    global stats

    if stats.connected is False:
        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"MQTT CONNECTED"
        )
    else:
        stats.reconnect_time = datetime.now()

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"MQTT RECONNECTED"
        )

    stats.connected = True


def on_disconnect(client, userdata, flags, reason_code, properties=None):
    global stats

    stats.connected = False
    stats.disconnect_time = datetime.now()

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] "
        f"MQTT DISCONNECTED"
    )


def mqtt_connect():

    client = mqtt_client.Client(
        mqtt_client.CallbackAPIVersion.VERSION2
    )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    client.reconnect_delay_set(
        min_delay=1,
        max_delay=5
    )

    client.connect("localhost", 1883)

    client.loop_start()

    return client


def kafka_connect():
    return Producer({
        "bootstrap.servers": "localhost:9094",
        "acks": "all"
    })


async def run_mqtt(rate):

    client = mqtt_connect()

    seq = 0

    print("\nMQTT Scenario B started.")
    print("1. Wait until messages are flowing.")
    print("2. Stop Mosquitto container.")
    print("3. Wait 30 seconds.")
    print("4. Start Mosquitto container.")
    print("5. Observe reconnect.\n")

    while True:

        payload = {
            "device_id": "scenario_b_device",
            "seq": seq,
            "time": datetime.now(
                timezone.utc
            ).isoformat()
        }

        try:

            info = client.publish(
                "sensor/data",
                json.dumps(payload),
                qos=1
            )

            if info.rc == mqtt_client.MQTT_ERR_SUCCESS:
                stats.sent += 1
            else:
                stats.failed += 1

        except Exception:
            stats.failed += 1

        seq += 1

        if seq % 100 == 0:

            print(
                f"[MQTT] sent={stats.sent} "
                f"failed={stats.failed} "
                f"connected={stats.connected}"
            )

        await asyncio.sleep(1 / rate)


async def run_kafka(rate):

    producer = kafka_connect()

    seq = 0

    print("\nKafka Scenario B started.")
    print("1. Wait until messages are flowing.")
    print("2. Stop Kafka container.")
    print("3. Wait 30 seconds.")
    print("4. Start Kafka container.")
    print("5. Observe recovery.\n")

    while True:

        payload = {
            "device_id": "scenario_b_device",
            "seq": seq,
            "time": datetime.now(
                timezone.utc
            ).isoformat()
        }

        try:

            producer.produce(
                "sensor_data",
                value=json.dumps(payload).encode()
            )

            producer.poll(0)

            stats.sent += 1

        except Exception:

            stats.failed += 1

        seq += 1

        if seq % 100 == 0:

            print(
                f"[Kafka] sent={stats.sent} "
                f"failed={stats.failed}"
            )

        await asyncio.sleep(1 / rate)


async def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--broker",
        choices=["mqtt", "kafka"],
        required=True
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=10
    )

    args = parser.parse_args()

    try:

        if args.broker == "mqtt":
            await run_mqtt(args.rate)

        elif args.broker == "kafka":
            await run_kafka(args.rate)

    except KeyboardInterrupt:

        stats.report()


if __name__ == "__main__":
    asyncio.run(main())