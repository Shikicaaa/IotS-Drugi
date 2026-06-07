import asyncio
import json
import argparse
import sys
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt_client
except ImportError:
    mqtt_client = None

try:
    from confluent_kafka import Consumer, KafkaError
except ImportError:
    Consumer = None

class NotificationService:
    def __init__(self, broker_type, mqtt_broker="localhost", kafka_broker="localhost:9094"):
        self.broker_type = broker_type
        self.mqtt_broker = mqtt_broker
        self.kafka_broker = kafka_broker
        
        self.notif_count = 0
        self.last_alarm_time = None
        self.lock = asyncio.Lock()

    async def _handle_alarm(self, payload):
        async with self.lock:
            now = datetime.now(timezone.utc)
            
            if self.last_alarm_time and (now - self.last_alarm_time).total_seconds() > 30:
                print("\n[INFO] Previous incident has been resolved. Resetting anti-spam counter.")
                self.notif_count = 0
                
            self.last_alarm_time = now
            
            if self.notif_count < 2:
                self.notif_count += 1
                avg_temp = payload.get("metrics", {}).get("avg_temp", "N/A")
                print(f"\n [EMAIL] RECEPIENT: stefanovicandrijasd@gmail.com | SUBJECT: FIRE ALERT | MESSAGE: FIRE DETECTED! Average temperature: {avg_temp:.2f}°C.")
                print(f"   (Message {self.notif_count}/2 sent)\n")
            else:
                print(f"[MUTED] Message suppressed to prevent spamming. (Incident ongoing for {(now - self.last_alarm_time).total_seconds():.1f} seconds)")

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            asyncio.run_coroutine_threadsafe(self._handle_alarm(payload), self.loop)
        except Exception as e:
            pass

    async def run_mqtt(self):
        if mqtt_client is None:
            raise Exception("paho-mqtt nije instaliran")
            
        self.loop = asyncio.get_running_loop()
        
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        client.on_message = self.on_mqtt_message
        client.connect(self.mqtt_broker, 1883)
        client.subscribe("sensor/alarms", qos=1)
        client.loop_start()

        print(f"Notification Service (MQTT) pokrenut. Sluša topiku 'sensor/alarms'")
        
        try:
            while True:
                await asyncio.sleep(1)
        finally:
            client.loop_stop()
            client.disconnect()

    async def run_kafka(self):
        if Consumer is None:
            raise Exception("confluent_kafka nije instaliran")

        conf = {
            'bootstrap.servers': self.kafka_broker,
            'group.id': 'notification-service-group',
            'auto.offset.reset': 'earliest'
        }

        consumer = Consumer(conf)
        consumer.subscribe(["fire_alarms"])

        print(f"Notification Service (Kafka) pokrenut. Sluša topiku 'fire_alarms'.")

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
                    await self._handle_alarm(payload)
                except Exception as e:
                    pass
                
        finally:
            consumer.close()

async def main():
    parser = argparse.ArgumentParser(description="IoT Notification Service (Anti-spam alerting)")
    parser.add_argument("--broker", type=str, choices=["mqtt", "kafka"], required=True, help="Koji broker se koristi")
    parser.add_argument("--kafka-broker", type=str, default="localhost:9094", help="Kafka broker adresa")
    parser.add_argument("--mqtt-broker", type=str, default="localhost", help="MQTT broker adresa")
    
    args = parser.parse_args()

    service = NotificationService(
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
        print("\nNotification service stopped.")

if __name__ == "__main__":
    asyncio.run(main())