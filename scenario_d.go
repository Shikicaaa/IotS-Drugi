package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/twmb/franz-go/pkg/kgo"
)

type SensorPayload struct {
	DeviceID        string  `json:"device_id"`
	Time            string  `json:"time"`
	EmitTS          int64   `json:"emit_ts"`
	TemperatureC    float64 `json:"temperature_c"`
	HumidityPercent float64 `json:"humidity_percent"`
	TvocPpb         int     `json:"tvoc_ppb"`
	Eco2Ppm         int     `json:"eco2_ppm"`
	RawH2           int     `json:"raw_h2"`
	RawEthanol      int     `json:"raw_ethanol"`
	PressureHpa     float64 `json:"pressure_hpa"`
	Pm10            float64 `json:"pm10"`
	Pm25            float64 `json:"pm25"`
	Nc05            float64 `json:"nc05"`
	Nc10            float64 `json:"nc10"`
	Nc25            float64 `json:"nc25"`
	FireAlarm       bool    `json:"fire_alarm"`
}

type AlarmPayload struct {
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Metrics   map[string]interface{} `json:"metrics"`
}

type LatencyResult struct {
	Round     int
	EmitTS    time.Time
	ReceiveTS time.Time
	Latency   time.Duration
}

func firePayload(deviceID string) SensorPayload {
	now := time.Now().UTC()
	pm25 := rand.Float64()*445 + 155
	pm10 := rand.Float64()*690 + 310
	return SensorPayload{
		DeviceID:        deviceID,
		Time:            now.Format(time.RFC3339Nano),
		EmitTS:          now.UnixNano(),
		TemperatureC:    rand.Float64()*84 + 66,
		HumidityPercent: rand.Float64()*10 + 5,
		TvocPpb:         rand.Intn(12900) + 2100,
		Eco2Ppm:         rand.Intn(6900) + 3100,
		RawH2:           rand.Intn(3000) + 12000,
		RawEthanol:      rand.Intn(15000) + 30000,
		PressureHpa:     rand.Float64()*15 + 930,
		Pm10:            pm10,
		Pm25:            pm25,
		Nc05:            pm25 * 2.5,
		Nc10:            pm10 * 1.5,
		Nc25:            pm10 * 0.5,
		FireAlarm:       true,
	}
}

func printStats(results []LatencyResult, brokerName string, config string) {
	if len(results) == 0 {
		fmt.Println("[WARN] Nema rezultata za prikaz.")
		return
	}

	latencies := make([]float64, len(results))
	for i, r := range results {
		latencies[i] = float64(r.Latency.Milliseconds())
	}
	sort.Float64s(latencies)

	sum := 0.0
	for _, v := range latencies {
		sum += v
	}
	avg := sum / float64(len(latencies))

	variance := 0.0
	for _, v := range latencies {
		diff := v - avg
		variance += diff * diff
	}
	stddev := math.Sqrt(variance / float64(len(latencies)))

	p50 := percentile(latencies, 50)
	p90 := percentile(latencies, 90)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	minL := latencies[0]
	maxL := latencies[len(latencies)-1]

	fmt.Println("SCENARIO D — REZULTATI MERENJA")
	fmt.Printf("  Broker:        %s (%s)\n", brokerName, config)
	fmt.Printf("  Merenja:       %d uspešnih\n", len(results))
	fmt.Printf("  Min latencija:  %6.1f ms\n", minL)
	fmt.Printf("  Max latencija:  %6.1f ms\n", maxL)
	fmt.Printf("  Avg latencija:  %6.1f ms\n", avg)
	fmt.Printf("  Std Dev:        %6.1f ms\n", stddev)
	fmt.Printf("  p50:            %6.1f ms\n", p50)
	fmt.Printf("  p90:            %6.1f ms\n", p90)
	fmt.Printf("  p95:            %6.1f ms  ← referentna metrika\n", p95)
	fmt.Printf("  p99:            %6.1f ms\n", p99)

	windowSec := 10.0
	fmt.Printf("\n  [NAPOMENA] Analytics koristi Tumbling Window od %.0fs.\n", windowSec)
	fmt.Printf("  Minimalna očekivana latencija = window + broker propagacija.\n")
	fmt.Printf("  Ako je avg ≈ %.0f–%.0f s — sistem radi ispravno.\n", windowSec, windowSec+2)
	fmt.Println()

	// Tabela po rundama
	fmt.Println("  Runda │ Emit time           │ Receive time        │ Latencija")
	for _, r := range results {
		fmt.Printf("  %5d │ %s │ %s │ %8.1f ms\n",
			r.Round,
			r.EmitTS.Format("15:04:05.000"),
			r.ReceiveTS.Format("15:04:05.000"),
			float64(r.Latency.Milliseconds()),
		)
	}
	fmt.Println()
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}


func runMQTT(mqttBroker string, qos byte, rounds int, timeout time.Duration) ([]LatencyResult, error) {
	results := make([]LatencyResult, 0, rounds)
	var mu sync.Mutex

	// Buffered channel: callback stavlja (receiveTS, alarmTS) parove
	type alarmEvent struct {
		receiveTS time.Time
		alarmTS   time.Time
	}
	alarmCh := make(chan alarmEvent, 32)

	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s:1883", mqttBroker)).
		SetClientID("scenario_d_go_" + fmt.Sprintf("%d", time.Now().UnixNano())).
		SetCleanSession(true).
		SetConnectTimeout(10 * time.Second)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("MQTT connect greška: %w", token.Error())
	}
	defer func() {
		client.Disconnect(500)
	}()

	fmt.Printf("[MQTT] Konekcija na %s uspostavljena.\n", mqttBroker)

	token := client.Subscribe("sensor/alarms", qos, func(_ mqtt.Client, msg mqtt.Message) {
		receiveTS := time.Now()
		var alarm AlarmPayload
		alarmTS := receiveTS // fallback ako parsiranje ne uspe
		if err := json.Unmarshal(msg.Payload(), &alarm); err == nil {
			if t, err2 := time.Parse(time.RFC3339Nano, alarm.Timestamp); err2 == nil {
				alarmTS = t
			}
		}
		select {
		case alarmCh <- alarmEvent{receiveTS: receiveTS, alarmTS: alarmTS}:
		default: // kanal pun — odbaci, neće se desiti pri normalnom radu
		}
	})
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("MQTT subscribe greška: %w", token.Error())
	}

	fmt.Printf("[MQTT] Pretplaćen na 'sensor/alarms' | QoS=%d\n", qos)
	fmt.Printf("[MQTT] Pokrećem %d rundi merenja (timeout po rundi: %v)...\n\n", rounds, timeout)

	for round := 1; round <= rounds; round++ {
		payload := firePayload(fmt.Sprintf("scenario_d_%d", round))
		emitTS := time.Unix(0, payload.EmitTS)

		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("JSON marshal greška: %w", err)
		}

		pubToken := client.Publish("sensor/data", qos, false, data)
		pubToken.Wait()
		if pubToken.Error() != nil {
			fmt.Printf("  [Runda %d] Publish greška: %v — preskačem.\n", round, pubToken.Error())
			continue
		}

		fmt.Printf("  [Runda %d] Fire poruka poslata u %s...\n",
			round, emitTS.Format("15:04:05.000"))

		// Čekaj alarm koji je nastao POSLE nego što smo poslali poruku
		deadline := time.NewTimer(timeout)
		alarmReceived := false
		for !alarmReceived {
			select {
			case ev := <-alarmCh:
				if ev.alarmTS.Before(emitTS) {
					fmt.Printf("  [Runda %d] Ignorisan zastareli alarm (alarm=%s < emit=%s).\n",
						round, ev.alarmTS.Format("15:04:05.000"), emitTS.Format("15:04:05.000"))
					continue
				}
				latency := ev.receiveTS.Sub(emitTS)
				result := LatencyResult{
					Round:     round,
					EmitTS:    emitTS,
					ReceiveTS: ev.receiveTS,
					Latency:   latency,
				}
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
				fmt.Printf("  [Runda %d] Alarm primljen! Latencija: %.1f ms\n",
					round, float64(latency.Milliseconds()))
				alarmReceived = true
			case <-deadline.C:
				fmt.Printf("  [Runda %d] Timeout (%v) - alarm nije stigao.\n", round, timeout)
				alarmReceived = true // izađi iz petlje
			}
		}
		deadline.Stop()

		if round < rounds {
			fmt.Printf("  Čekam 11s (analytics window)...\n")
			time.Sleep(11 * time.Second)
		}
	}

	return results, nil
}


func runKafka(kafkaBroker string, acks string, rounds int, timeout time.Duration) ([]LatencyResult, error) {
	results := make([]LatencyResult, 0, rounds)

	ctx := context.Background()

	var requiredAcks kgo.Acks
	switch acks {
	case "0":
		requiredAcks = kgo.NoAck()
	case "1":
		requiredAcks = kgo.LeaderAck()
	case "all":
		requiredAcks = kgo.AllISRAcks()
	default:
		requiredAcks = kgo.LeaderAck()
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBroker),
		kgo.RequiredAcks(requiredAcks),
		kgo.ProducerBatchMaxBytes(1_000_000),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		return nil, fmt.Errorf("Kafka producer greška: %w", err)
	}
	defer producer.Close()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBroker),
		kgo.ConsumeTopics("fire_alarms"),
		kgo.ConsumerGroup("scenario_d_go_"+fmt.Sprintf("%d", time.Now().UnixNano())),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, fmt.Errorf("Kafka consumer greška: %w", err)
	}
	defer consumer.Close()

	fmt.Printf("[Kafka] Konekcija na %s | acks=%s\n", kafkaBroker, acks)
	fmt.Printf("[Kafka] Pokrećem %d rundi merenja (timeout po rundi: %v)...\n\n", rounds, timeout)

	for round := 1; round <= rounds; round++ {
		payload := firePayload(fmt.Sprintf("scenario_d_kafka_%d", round))
		emitTS := time.Unix(0, payload.EmitTS)

		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("JSON marshal greška: %w", err)
		}

		record := &kgo.Record{
			Topic: "sensor_data",
			Key:   []byte(payload.DeviceID),
			Value: data,
		}

		if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
			fmt.Printf("  [Runda %d] Kafka produce greška: %v — preskačem.\n", round, err)
			continue
		}

		fmt.Printf("  [Runda %d] Fire poruka poslata u %s...\n",
			round, emitTS.Format("15:04:05.000"))

		alarmReceived := false
		deadline := time.Now().Add(timeout)

		for time.Now().Before(deadline) && !alarmReceived {
			pollCtx, cancel := context.WithDeadline(ctx, deadline)
			fetches := consumer.PollFetches(pollCtx)
			cancel()

			fetches.EachRecord(func(r *kgo.Record) {
				if alarmReceived {
					return
				}
				if r.Timestamp.Before(emitTS) {
					return
				}
				receiveTS := time.Now()
				var alarm AlarmPayload
				if err := json.Unmarshal(r.Value, &alarm); err == nil {
					latency := receiveTS.Sub(emitTS)
					result := LatencyResult{
						Round:     round,
						EmitTS:    emitTS,
						ReceiveTS: receiveTS,
						Latency:   latency,
					}
					results = append(results, result)
					alarmReceived = true
					fmt.Printf("  [Runda %d] Alarm primljen! Latencija: %.1f ms\n",
						round, float64(latency.Milliseconds()))
				}
			})
		}

		if !alarmReceived {
			fmt.Printf("  [Runda %d] Timeout (%v) — alarm nije stigao.\n", round, timeout)
		}

		if round < rounds {
			fmt.Printf("  Čekam 11s (analytics window)...\n")
			time.Sleep(11 * time.Second)
		}
	}

	return results, nil
}

func checkTCP(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

func main() {
	broker := flag.String("broker", "mqtt", "Tip brokera: mqtt ili kafka")
	mqttAddr := flag.String("mqtt-broker", "localhost", "MQTT broker adresa")
	kafkaAddr := flag.String("kafka-broker", "localhost:9094", "Kafka broker adresa (host:port)")
	qos := flag.Int("qos", 1, "MQTT QoS nivo (0, 1 ili 2)")
	acks := flag.String("acks", "1", "Kafka acks (0, 1 ili all)")
	rounds := flag.Int("rounds", 5, "Broj rundi merenja")
	timeoutSec := flag.Float64("timeout", 30.0, "Timeout po rundi u sekundama (mora biti > 10s zbog window-a)")
	flag.Parse()

	timeout := time.Duration(*timeoutSec * float64(time.Second))

	fmt.Println("SCENARIO D - Real-Time Alerting Latency Benchmark")
	fmt.Printf("  Broker:    %s\n", *broker)
	fmt.Printf("  Runde:     %d\n", *rounds)
	fmt.Printf("  Timeout:   %.1fs po rundi\n", *timeoutSec)
	fmt.Println()

	if timeout <= 10*time.Second {
		fmt.Println("[WARN] Timeout mora biti > 10s (analytics window = 10s). Postavljam na 30s.")
		timeout = 30 * time.Second
	}

	var results []LatencyResult
	var err error
	var configLabel string

	switch *broker {
	case "mqtt":
		configLabel = fmt.Sprintf("QoS=%d", *qos)
		addr := fmt.Sprintf("%s:1883", *mqttAddr)
		if e := checkTCP(addr, 5*time.Second); e != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] MQTT broker nije dostupan na %s: %v\n", addr, e)
			os.Exit(1)
		}
		results, err = runMQTT(*mqttAddr, byte(*qos), *rounds, timeout)

	case "kafka":
		configLabel = fmt.Sprintf("acks=%s", *acks)
		if e := checkTCP(*kafkaAddr, 5*time.Second); e != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Kafka nije dostupan na %s: %v\n", *kafkaAddr, e)
			os.Exit(1)
		}
		results, err = runKafka(*kafkaAddr, *acks, *rounds, timeout)

	default:
		fmt.Fprintf(os.Stderr, "[ERROR] Nepoznat broker: %s. Koristite 'mqtt' ili 'kafka'.\n", *broker)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		os.Exit(1)
	}

	printStats(results, *broker, configLabel)
}