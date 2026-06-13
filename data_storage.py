import asyncio
import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
import dateutil.parser

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

try:
    import paho.mqtt.client as mqtt_client
except ImportError:
    mqtt_client = None

try:
    from confluent_kafka import Consumer, KafkaError
except ImportError:
    Consumer = None

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from backend.services.env import DATABASE_URL
from backend.models.database.sensor_data import SensorData

LOCAL_DB_URL = DATABASE_URL.replace("@db:", "@localhost:")
print("LOCAL_DB_URL =", LOCAL_DB_URL)
engine = create_async_engine(LOCAL_DB_URL, echo=False)
SessionLocal = async_sessionmaker(bind=engine)

BATCH_SIZE = 500

class StorageService:
    def __init__(self, broker_type, mqtt_broker="localhost", kafka_broker="localhost:9094"):
        self.broker_type = broker_type
        self.mqtt_broker = mqtt_broker
        self.kafka_broker = kafka_broker
        self.message_batch = []
        self.db_lock = asyncio.Lock()

    async def save_batch(self):
        async with self.db_lock:
            if not self.message_batch:
                return
            
            batch_to_save = self.message_batch[:]
            self.message_batch.clear()

        async with SessionLocal() as db:
            records = []
            for msg in batch_to_save:
                try:
                    data = json.loads(msg)
                    # Convert string to datetime
                    record_time = dateutil.parser.isoparse(data['time'])
                    record = SensorData(
                        time=record_time,
                        temperature_c=data.get('temperature_c'),
                        humidity_percent=data.get('humidity_percent'),
                        tvoc_ppb=data.get('tvoc_ppb'),
                        eco2_ppm=data.get('eco2_ppm'),
                        raw_h2=data.get('raw_h2'),
                        raw_ethanol=data.get('raw_ethanol'),
                        pressure_hpa=data.get('pressure_hpa'),
                        pm10=data.get('pm10'),
                        pm25=data.get('pm25'),
                        nc05=data.get('nc05'),
                        nc10=data.get('nc10'),
                        nc25=data.get('nc25'),
                        fire_alarm=data.get('fire_alarm')
                    )
                    records.append(record)
                except Exception as e:
                    print(f"Error parsing message: {e}")

            if records:
                db.add_all(records)
                await db.commit()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Saved batch of {len(records)} records to DB.")

    def on_mqtt_message(self, client, userdata, msg):
        payload = msg.payload.decode()
        asyncio.run_coroutine_threadsafe(self._process_message(payload), self.loop)

    async def _process_message(self, payload):
        async with self.db_lock:
            self.message_batch.append(payload)
            should_save = len(self.message_batch) >= BATCH_SIZE

        if should_save:
            await self.save_batch()

    async def run_mqtt(self, qos):
        if mqtt_client is None:
            raise Exception("paho-mqtt is not installed")
        
        self.loop = asyncio.get_running_loop()
        
        client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        client.on_message = self.on_mqtt_message
        client.connect(self.mqtt_broker, 1883)
        client.subscribe("sensor/data", qos=qos)
        client.loop_start()

        print(f"MQTT Storage Service started. Subscribed to 'sensor/data' with QoS {qos}.")
        
        try:
            while True:
                await asyncio.sleep(1)
        finally:
            client.loop_stop()
            client.disconnect()

    async def run_kafka(self):
        if Consumer is None:
            raise Exception("confluent_kafka is not installed")

        conf = {
            'bootstrap.servers': self.kafka_broker,
            'group.id': 'storage-service-group',
            'auto.offset.reset': 'earliest'
        }

        consumer = Consumer(conf)
        consumer.subscribe(["sensor_data"])

        print("Kafka Storage Service started. Subscribed to 'sensor_data'.")

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

                payload = msg.value().decode('utf-8')
                await self._process_message(payload)
                
        finally:
            consumer.close()
            await self.save_batch()

async def main():
    parser = argparse.ArgumentParser(description="IoT Data Storage Service")
    parser.add_argument("--broker", type=str, choices=["mqtt", "kafka"], required=True, help="Choose the broker")
    parser.add_argument("--mqtt-qos", type=int, choices=[0, 1, 2], default=1, help="MQTT QoS level")
    parser.add_argument("--kafka-broker", type=str,  default="localhost:9094,localhost:9096,localhost:9098", help="Kafka broker address")
    parser.add_argument("--mqtt-broker", type=str, default="localhost", help="MQTT broker address")
    
    args = parser.parse_args()

    service = StorageService(
        broker_type=args.broker,
        mqtt_broker=args.mqtt_broker,
        kafka_broker=args.kafka_broker
    )
    
    try:
        if args.broker == "mqtt":
            await service.run_mqtt(args.mqtt_qos)
        elif args.broker == "kafka":
            await service.run_kafka()
    except KeyboardInterrupt:
        print("\nStorage service stopped. Saving remaining records...")
        await service.save_batch()

if __name__ == "__main__":
    asyncio.run(main())