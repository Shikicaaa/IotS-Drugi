import asyncio
import random
import sys
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

try:
    import paho.mqtt.client as mqtt_client
except ImportError:
    mqtt_client = None

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None

def get_kafka_producer(broker_url="localhost:9094", acks="1"):
    if Producer is None:
        raise Exception("confluent_kafka is not installed")
    return Producer({
        "bootstrap.servers": broker_url,
        "acks": acks
    })

def get_mqtt_client(broker_url="localhost", port=1883):
    if mqtt_client is None:
        raise Exception("paho-mqtt is not installed")
    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
    client.connect(broker_url, port)
    client.loop_start()
    return client

async def run_simulation(broker_type, mqtt_qos, kafka_acks, rate, device_id):
    is_fire = True
    fire_duration_left = 60

    print(f"Broker: {broker_type} (Device ID: {device_id})")
    
    kafka_prod = None
    mqtt_cli = None
    
    if broker_type in ["kafka", "both"]:
        kafka_prod = get_kafka_producer(acks=kafka_acks)
    if broker_type in ["mqtt", "both"]:
        mqtt_cli = get_mqtt_client()

    print("Started (Ctrl + C to stop)...")
    
    while True:
        now = datetime.now(timezone.utc)

        if not is_fire and random.random() < 0.00001:
            print("\n🔥 WARNING: Fire detected!")
            is_fire = True
            fire_duration_left = random.randint(30, 120)

        if is_fire:
            temp = round(random.uniform(66.0, 150.0), 2)
            hum = round(random.uniform(5.0, 15.0), 2)
            tvoc = random.randint(2100, 15000)
            eco2 = random.randint(3100, 10000)
            pm25 = round(random.uniform(155.0, 600.0), 2)
            pm10 = round(random.uniform(310.0, 1000.0), 2)
            raw_ethanol = random.randint(30000, 45000)
            fire_alarm = True

            fire_duration_left -= 1
            if fire_duration_left <= 0:
                print("\nCondition stabilized. Fire is extinguished. Returning to normal.")
                is_fire = False
        else:
            temp = round(random.uniform(20.0, 25.0), 2)
            hum = round(random.uniform(40.0, 50.0), 2)
            tvoc = random.randint(0, 100)
            eco2 = random.randint(400, 600)
            pm25 = round(random.uniform(1.0, 15.0), 2)
            pm10 = round(random.uniform(5.0, 20.0), 2)
            raw_ethanol = random.randint(15000, 20000)
            fire_alarm = False

        nc05 = round(pm25 * 2.5, 2)
        nc10 = round(pm10 * 1.5, 2)
        nc25 = round(pm10 * 0.5, 2)

        payload = {
            "device_id": device_id,
            "time": now.isoformat(),
            "temperature_c": temp,
            "humidity_percent": hum,
            "tvoc_ppb": tvoc,
            "eco2_ppm": eco2,
            "raw_h2": random.randint(12000, 15000),
            "raw_ethanol": raw_ethanol,
            "pressure_hpa": round(random.uniform(930.0, 945.0), 2),
            "pm10": pm10,
            "pm25": pm25,
            "nc05": nc05,
            "nc10": nc10,
            "nc25": nc25,
            "fire_alarm": fire_alarm
        }

        payload_json = json.dumps(payload)

        # Publish MQTT
        if broker_type in ["mqtt", "both"]:
            mqtt_cli.publish("sensor/data", payload_json, qos=mqtt_qos)
            
        # Publish Kafka
        if broker_type in ["kafka", "both"]:
            kafka_prod.produce(
                "sensor_data", 
                value=payload_json.encode('utf-8'),
                key=str(device_id).encode('utf-8')
            )
            kafka_prod.poll(0)

        status_icon = "FIRE" if fire_alarm else "OK"
        if rate <= 10:
            print(f"[{now.strftime('%H:%M:%S')}] {status_icon} | Temp: {temp}C | Broker: {broker_type}")

        if rate > 0:
            await asyncio.sleep(1.0 / rate)
        elif broker_type in ["kafka", "both"]:
            kafka_prod.flush()

def main():
    parser = argparse.ArgumentParser(description="IoT Sensor Simulator for MQTT and Kafka")
    parser.add_argument("--broker", type=str, choices=["mqtt", "kafka", "both"], default="kafka", help="Choose the broker")
    parser.add_argument("--rate", type=float, default=1.0, help="Messages per second for this device")
    parser.add_argument("--device-id", type=str, default="sensor_01", help="Device identifier")
    parser.add_argument("--mqtt-qos", type=int, choices=[0, 1, 2], default=0, help="MQTT QoS level")
    parser.add_argument("--kafka-acks", type=str, choices=["0", "1", "all"], default="1", help="Kafka acks parameter")
    
    args = parser.parse_args()

    try:
        asyncio.run(run_simulation(
            broker_type=args.broker, 
            mqtt_qos=args.mqtt_qos,
            kafka_acks=args.kafka_acks,
            rate=args.rate, 
            device_id=args.device_id
        ))
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")
    finally:
        if kafka_prod:
            kafka_prod.flush()
        if mqtt_cli:
            mqtt_cli.loop_stop()
            mqtt_cli.disconnect()

if __name__ == "__main__":
    main()