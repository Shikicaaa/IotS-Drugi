import asyncio
import json
import argparse
import time
import random
import sys
import csv
import os
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path

try:
    import paho.mqtt.client as mqtt_lib
except ImportError:
    mqtt_lib = None

try:
    from confluent_kafka import Producer, KafkaException
except ImportError:
    Producer = None

@dataclass
class Stats:
    sent: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.monotonic)

    def throughput(self) -> float:
        elapsed = time.monotonic() - self.start_time
        return self.sent / elapsed if elapsed > 0 else 0.0

    def loss_pct(self) -> float:
        total = self.sent + self.failed
        return (self.failed / total * 100) if total > 0 else 0.0


def make_payload(device_id: str) -> bytes:
    now = datetime.now(timezone.utc).isoformat()
    data = {
        "device_id": device_id,
        "time": now,
        "temperature_c": round(random.uniform(20.0, 30.0), 2),
        "humidity_percent": round(random.uniform(40.0, 60.0), 2),
        "tvoc_ppb": random.randint(0, 200),
        "eco2_ppm": random.randint(400, 700),
        "raw_h2": random.randint(12000, 15000),
        "raw_ethanol": random.randint(15000, 20000),
        "pressure_hpa": round(random.uniform(930.0, 945.0), 2),
        "pm10": round(random.uniform(5.0, 20.0), 2),
        "pm25": round(random.uniform(1.0, 15.0), 2),
        "nc05": round(random.uniform(2.0, 30.0), 2),
        "nc10": round(random.uniform(5.0, 25.0), 2),
        "nc25": round(random.uniform(2.0, 10.0), 2),
        "fire_alarm": False,
    }
    return json.dumps(data).encode("utf-8")


async def mqtt_device(device_id: str, broker: str, port: int, qos: int,
                       rate: float, duration: float, stats: Stats,
                       semaphore: asyncio.Semaphore):
    if mqtt_lib is None:
        raise RuntimeError("paho-mqtt is not installed")

    connected = asyncio.Event()
    loop = asyncio.get_running_loop()

    client = mqtt_lib.Client(mqtt_lib.CallbackAPIVersion.VERSION2,
                              client_id=f"sim_{device_id}")

    def on_connect(c, userdata, flags, reason_code, properties):
        if reason_code == 0:
            connected.set()

    def on_disconnect(c, userdata, disconnect_flags, reason_code, properties):
        pass

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    try:
        client.connect(broker, port, keepalive=60)
        client.loop_start()

        try:
            await asyncio.wait_for(connected.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            async with semaphore:
                stats.failed += 1
            client.loop_stop()
            client.disconnect()
            return

        deadline = time.monotonic() + duration
        interval = 1.0 / rate if rate > 0 else 0

        while time.monotonic() < deadline:
            payload = make_payload(device_id)
            result = client.publish("sensor/data", payload, qos=qos)

            if result.rc == mqtt_lib.MQTT_ERR_SUCCESS:
                async with semaphore:
                    stats.sent += 1
            else:
                async with semaphore:
                    stats.failed += 1

            if interval > 0:
                await asyncio.sleep(interval)

    except (Exception, asyncio.CancelledError):
        async with semaphore:
            stats.failed += 1
    finally:
        try:
            client.loop_stop(timeout=1)
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass


# Shared producer — prima producer kao parametar umesto da ga pravi sam
async def kafka_device(device_id: str, producer: "Producer",
                        rate: float, duration: float, stats: Stats,
                        semaphore: asyncio.Semaphore):
    deadline = time.monotonic() + duration
    interval = 1.0 / rate if rate > 0 else 0

    def delivery_callback(err, msg):
        if err:
            pass

    try:
        while time.monotonic() < deadline:
            payload = make_payload(device_id)
            try:
                producer.produce(
                    "sensor_data",
                    value=payload,
                    key=device_id.encode("utf-8"),
                    callback=delivery_callback,
                )
                producer.poll(0)
                async with semaphore:
                    stats.sent += 1
            except BufferError:
                producer.poll(0.1)
                async with semaphore:
                    stats.failed += 1

            if interval > 0:
                await asyncio.sleep(interval)

    except Exception:
        async with semaphore:
            stats.failed += 1


async def run_mqtt(num_devices: int, broker: str, port: int, qos: int,
                   rate: float, duration: float) -> Stats:
    stats = Stats()
    sem = asyncio.Semaphore(1)

    print(f"\n[MQTT] Pokretanje {num_devices} uredjaja | broker={broker}:{port} "
          f"| QoS={qos} | rate={rate} msg/s/uredjaj | trajanje={duration}s")
    print("  cekam na konekcije...")

    conn_limit = asyncio.Semaphore(min(num_devices, 500))

    async def guarded_device(did):
        async with conn_limit:
            await mqtt_device(did, broker, port, qos, rate, duration, stats, sem)

    tasks = [
        asyncio.create_task(guarded_device(f"mqtt_dev_{i:06d}"))
        for i in range(num_devices)
    ]

    async def progress():
        while True:
            await asyncio.sleep(5)
            print(f"  [{time.strftime('%H:%M:%S')}] Poslato: {stats.sent:,} | "
                  f"Neuspesno: {stats.failed:,} | "
                  f"Throughput: {stats.throughput():,.1f} msg/s")

    prog_task = asyncio.create_task(progress())

    await asyncio.gather(*tasks, return_exceptions=True)
    prog_task.cancel()

    return stats


async def run_kafka(num_devices: int, broker_url: str, acks: str,
                    rate: float, duration: float) -> Stats:
    if Producer is None:
        raise RuntimeError("confluent_kafka is not installed")

    stats = Stats()
    sem = asyncio.Semaphore(1)

    print(f"\n[Kafka] Pokretanje {num_devices} uredjaja | broker={broker_url} "
          f"| acks={acks} | rate={rate} msg/s/uredjaj | trajanje={duration}s")

    # Jedan shared producer za sve uredjaje — resava problem "Too many open files"
    shared_producer = Producer({
        "bootstrap.servers": broker_url,
        "acks": acks,
        "linger.ms": 5,
        "batch.size": 65536,
        "compression.type": "lz4",
    })

    tasks = [
        asyncio.create_task(
            kafka_device(f"kafka_dev_{i:06d}", shared_producer,
                         rate, duration, stats, sem)
        )
        for i in range(num_devices)
    ]

    async def progress():
        while True:
            await asyncio.sleep(5)
            print(f"  [{time.strftime('%H:%M:%S')}] Poslato: {stats.sent:,} | "
                  f"Neuspesno: {stats.failed:,} | "
                  f"Throughput: {stats.throughput():,.1f} msg/s")

    prog_task = asyncio.create_task(progress())
    await asyncio.gather(*tasks, return_exceptions=True)
    prog_task.cancel()

    # Flush na kraju da se posalju sve poruke iz bafera
    remaining = shared_producer.flush(timeout=10)
    if remaining > 0:
        async with sem:
            stats.failed += remaining

    return stats


async def main():
    parser = argparse.ArgumentParser(
        description="Scenario A Massive Sensor Ingestion benchmark"
    )
    parser.add_argument("--broker", choices=["mqtt", "kafka"], required=True)
    parser.add_argument("--devices", type=int, default=100,
                        help="(100 / 1000 / 10000)")
    parser.add_argument("--duration", type=float, default=30.0,
                        help="testa u sekundama")
    parser.add_argument("--rate", type=float, default=10.0,
                        help="Poruke po sekundi po uredjaju")

    # MQTT params
    parser.add_argument("--mqtt-broker", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=0)

    # Kafka params
    parser.add_argument("--kafka-broker", default="localhost:9094")
    parser.add_argument("--acks", choices=["0", "1", "all"], default="1")

    args = parser.parse_args()

    t0 = time.monotonic()

    if args.broker == "mqtt":
        stats = await run_mqtt(
            num_devices=args.devices,
            broker=args.mqtt_broker,
            port=args.mqtt_port,
            qos=args.qos,
            rate=args.rate,
            duration=args.duration,
        )
        config_label = f"QoS={args.qos}"
    else:
        stats = await run_kafka(
            num_devices=args.devices,
            broker_url=args.kafka_broker,
            acks=args.acks,
            rate=args.rate,
            duration=args.duration,
        )
        config_label = f"acks={args.acks}"

    elapsed = time.monotonic() - t0
    total = stats.sent + stats.failed
    throughput = stats.sent / elapsed if elapsed > 0 else 0
    loss_pct = (stats.failed / total * 100) if total > 0 else 0.0

    print("\n" + "=" * 60)
    print("REZULTATI SCENARIO A")
    print("=" * 60)
    print(f"  Broker:          {args.broker.upper()} ({config_label})")
    print(f"  Broj uredjaja:    {args.devices:,}")
    print(f"  Rate/uredjaj:     {args.rate} msg/s")
    print(f"  Trajanje:        {elapsed:.1f}s")
    print(f"  Ukupno poslato:  {stats.sent:,}")
    print(f"  Neuspesno:       {stats.failed:,}")
    print(f"  Throughput:      {throughput:,.1f} msg/s")
    print(f"  Izgubljen %:     {loss_pct:.2f}%")
    print("=" * 60)

    row = {
        "timestamp": datetime.now().isoformat(),
        "broker": args.broker,
        "config": config_label,
        "devices": args.devices,
        "rate_per_device": args.rate,
        "duration_s": round(elapsed, 1),
        "sent": stats.sent,
        "failed": stats.failed,
        "throughput_msg_s": round(throughput, 1),
        "loss_pct": round(loss_pct, 2),
    }

if __name__ == "__main__":
    asyncio.run(main())