docker exec -it iots-2-kafka1-1 bash
docker exec -it iots-2-kafka2-1 bash
docker exec -it iots-2-kafka3-1 bash

/opt/kafka/bin/kafka-producer-perf-test.sh \
 --topic sensor_data \
 --num-records 1500 \
 --record-size 200 \
 --throughput 50 \
 --producer-props bootstrap.servers=localhost:9092

/opt/kafka/bin/kafka-producer-perf-test.sh \
 --topic sensor_data \
 --num-records 50000 \
 --record-size 200 \
 --throughput 5000 \
 --producer-props bootstrap.servers=localhost:9092

/opt/kafka/bin/kafka-producer-perf-test.sh \
 --topic sensor_data \
 --num-records 1500 \
 --record-size 200 \
 --throughput 50 \
 --producer-props bootstrap.servers=localhost:9092
