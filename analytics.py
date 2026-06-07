import asyncio
import json
import argparse
import sys
import time
from pathlib import Path
from collections import deque
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt_client
except ImportError:
    mqtt_client = None

try:
    from confluent_kafka import Consumer, Producer, KafkaError
except ImportError:
    Consumer = None
    Producer = None

WINDOW_SIZE_SECONDS = 10
FIRE_TEMP_THRESHOLD = 66.0
HUMIDITY_THRESHOLD = 15.0
TVOC_THRESHOLD = 2100
ECO2_THRESHOLD = 3100
PM25_THRESHOLD = 155.0
PM10_THRESHOLD = 310.0
RAW_ETHANOL_THRESHOLD = 30000

class AnalyticsService:
    def __init__(self, broker_type, mqtt_broker="localhost", kafka_broker="localhost:9094"):
        self.broker_type = broker_type
        self.mqtt_broker = mqtt_broker
        self.kafka_broker = kafka_broker
        self.window = deque()
        self.window_lock = asyncio.Lock()
        
        if broker_type == "kafka":
            self.alarm_producer = Producer({"bootstrap.servers": self.kafka_broker})
        else:
            self.alarm_mqtt = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
            self.alarm_mqtt.connect(self.mqtt_broker, 1883)
            self.alarm_mqtt.loop_start()

    def is_fire_condition(self, metrics):
        avg_temp = metrics.get("avg_temp", 0)
        avg_hum = metrics.get("avg_hum", 0)
        avg_tvoc = metrics.get("avg_tvoc", 0)
        avg_eco2 = metrics.get("avg_eco2", 0)
        avg_pm25 = metrics.get("avg_pm25", 0)
        avg_pm10 = metrics.get("avg_pm10", 0)
        avg_raw_ethanol = metrics.get("avg_raw_ethanol", 0)

        return avg_temp > FIRE_TEMP_THRESHOLD and avg_hum < HUMIDITY_THRESHOLD and avg_tvoc > TVOC_THRESHOLD and avg_eco2 > ECO2_THRESHOLD and avg_pm25 > PM25_THRESHOLD and avg_pm10 > PM10_THRESHOLD and avg_raw_ethanol > RAW_ETHANOL_THRESHOLD

    async def trigger_alarm(self, metrics):
        alarm_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "CRITICAL",
            "message": f"FIRE DETECTED! Window avg temp: {metrics['avg_temp']:.2f}°C",
            "metrics": metrics
        }
        
        payload_json = json.dumps(alarm_payload)
        print(f"\n[ALARM] {alarm_payload['message']}")

        if self.broker_type == "kafka":
            self.alarm_producer.produce("fire_alarms", value=payload_json.encode('utf-8'))
            self.alarm_producer.poll(0)
        else:
            self.alarm_mqtt.publish("sensor/alarms", payload_json, qos=1)

    async def process_window(self):
        while True:
            await asyncio.sleep(WINDOW_SIZE_SECONDS)
            
            async with self.window_lock:
                if not self.window:
                    continue
                
                current_window = list(self.window)
                self.window.clear()

            total_temp = sum(data.get("temperature_c", 0) for data in current_window)
            total_hum = sum(data.get("humidity_percent", 0) for data in current_window)
            tvoc_values = sum(data.get("tvoc_ppb", 0) for data in current_window)
            eco2_values = sum(data.get("eco2_ppm", 0) for data in current_window)
            pm25_values = sum(data.get("pm25", 0) for data in current_window)
            pm10_values = sum(data.get("pm10", 0) for data in current_window)
            raw_ethanol_values = sum(data.get("raw_ethanol", 0) for data in current_window)
            pressure_values = sum(data.get("pressure_hpa", 0) for data in current_window)
            
            avg_temp = total_temp / len(current_window)
            avg_hum = total_hum / len(current_window)
            avg_tvoc = tvoc_values / len(current_window)
            avg_eco2 = eco2_values / len(current_window)
            avg_pm25 = pm25_values / len(current_window)
            avg_pm10 = pm10_values / len(current_window)
            avg_raw_ethanol = raw_ethanol_values / len(current_window)
            avg_pressure = pressure_values / len(current_window)

            metrics = {
                "reading_count": len(current_window),
                "avg_temp": avg_temp,
                "avg_hum": avg_hum,
                "avg_tvoc": avg_tvoc,
                "avg_eco2": avg_eco2,
                "avg_pm25": avg_pm25,
                "avg_pm10": avg_pm10,
                "avg_raw_ethanol": avg_raw_ethanol,
                "avg_pressure": avg_pressure
            }
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Analiziran prozor od {WINDOW_SIZE_SECONDS}s: {len(current_window)} poruka, Prosek temp: {avg_temp:.2f}°C")
            
            if self.is_fire_condition(metrics):
                await self.trigger_alarm(metrics)

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            asyncio.run_coroutine_threadsafe(self.add_to_window(payload), self.loop)
        except Exception as e:
            pass

    async def add_to_window(self, data):
        async with self.window_lock:
            self.window.append(data)

    async def run_mqtt(self):
        self.loop = asyncio.get_running_loop()
        
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        client.on_message = self.on_mqtt_message
        client.connect(self.mqtt_broker, 1883)
        client.subscribe("sensor/data", qos=1)
        client.loop_start()

        print(f"Analytics Service (MQTT) started. Tumbling window: {WINDOW_SIZE_SECONDS}s")
        
        asyncio.create_task(self.process_window())
        
        try:
            while True:
                await asyncio.sleep(1)
        finally:
            client.loop_stop()
            client.disconnect()
            self.alarm_mqtt.loop_stop()
            self.alarm_mqtt.disconnect()

    async def run_kafka(self):
        conf = {
            'bootstrap.servers': self.kafka_broker,
            'group.id': 'analytics-service-group',
            'auto.offset.reset': 'earliest'
        }

        consumer = Consumer(conf)
        consumer.subscribe(["sensor_data"])

        print(f"Analytics Service (Kafka) started. Tumbling window: {WINDOW_SIZE_SECONDS}s")
        
        asyncio.create_task(self.process_window())

        try:
            while True:
                msg = consumer.poll(timeout=0.1)
                if msg is None:
                    await asyncio.sleep(0.01)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Kafka error: {msg.error()}")
                        break

                try:
                    payload = json.loads(msg.value().decode('utf-8'))
                    await self.add_to_window(payload)
                except Exception as e:
                    pass
                
        finally:
            consumer.close()
            self.alarm_producer.flush()

async def main():
    parser = argparse.ArgumentParser(description="IoT Analytics Service (Stream Processing)")
    parser.add_argument("--broker", type=str, choices=["mqtt", "kafka"], required=True, help="Choose the broker to listen to")
    parser.add_argument("--kafka-broker", type=str, default="localhost:9094", help="Kafka broker address")
    parser.add_argument("--mqtt-broker", type=str, default="localhost", help="MQTT broker address")
    
    args = parser.parse_args()

    service = AnalyticsService(
        broker_type=args.broker,
        mqtt_broker=args.mqtt_broker,
        kafka_broker=args.kafka_broker
    )
    
    try:
        if args.broker == "mqtt":
            await service.run_mqtt()
        elif args.broker == "kafka":
            await service.run_kafka()
    except KeyboardInterrupt:
        print("\nAnalytics service stopped.")

if __name__ == "__main__":
    asyncio.run(main())