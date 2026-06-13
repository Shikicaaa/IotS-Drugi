# IoT Mikroservisi – Projekat 2
## Uporedna evaluacija MQTT-a i Kafke

---

## CILJ PROJEKTA

Istražiti performanse, skalabilnost i ograničenja **message broker sistema** zasnovanih na **publish-subscribe modelu** u IoT mikroservisnim arhitekturama.

Fokus:
- Trade-off: **kašnjenje vs. pouzdanost**
- Pogodnost za **edge** i **cloud** okruženja

**Ulazni podaci:** Isti IoT dataset i model podataka iz Projekta 1 (uz mogućnost proširenja atributa)

---

## TEHNIČKI PREDUSLOVI

| Zahtev | Detalj |
|---|---|
| Kontejnerizacija | Kompletan sistem mora biti pokrenut pomoću **Docker Compose** |
| Tehnologije | Koristiti **najmanje dve** (ASP.NET Core, Node.js, Spring Boot, FastAPI, itd.) |

---

## ARHITEKTURA SISTEMA

Sistem je **asinhron, event-driven mikroservisni** i sastoji se od tri obavezne komponente:

---

### 1. Data Ingestion Service
> **Uloga:** Simulira IoT uređaje i šalje podatke u realnom vremenu

- Šalje podatke na odgovarajući broker:
  - MQTT: na odgovarajući **MQTT topic**
  - Kafka: na odgovarajući **Kafka topic**

---

### 2. Data Storage Service
> **Uloga:** Prima poruke sa brokera i čuva ih u bazu podataka

- Pretplaćen na broker
- Skladišti podatke u **PostgreSQL** bazu

**⚠️ VAŽNA OPTIMIZACIJA ZA STRESS-TESTOVE (Scenariji A i C):**
- Implementirati **batching** (grupni upis na svakih **500 poruka**)
- **ILI** privremeno isključiti upis u bazu
- Cilj: sprečiti da I/O podsistem postane usko grlo umesto samog brokera

---

### 3. Analytics Service (Stream Processing)
> **Uloga:** Analizira tok podataka u realnom vremenu

- Pretplaćen na tok podataka
- Implementira **Tumbling Window** (fiksni vremenski prozor) od **10 sekundi**

**Logika:**
- Za svaki prozor od 10 sekundi → izračunati **prosečnu vrednost senzora** (npr. temperature)
- Ako je prosek prozora **> definisanog praga** (npr. > 50°C) → ispisati **kritičan alarm (Alert)** u log

---

## DVA MESSAGE BROKER-A – PARALELNA IMPLEMENTACIJA

Ista arhitektura mora biti implementirana za oba brokera.

---

### MQTT (Mosquitto)

| Zadatak | Opis |
|---|---|
| QoS nivoi | Testirati **QoS 0**, **QoS 1** i **QoS 2** |
| Analiza | Efekti garancije isporuke na latenciju: *at most once / at least once / exactly once* |

---

### Apache Kafka

| Zadatak | Opis |
|---|---|
| Režim | Koristiti **KRaft režim** (bez Zookeeper-a) – radi uštede memorije na lokalnim mašinama |
| Acks parametri | Testirati **acks=0**, **acks=1** i **acks=all** |
| Analiza | Razumeti pojam **Consumer Lag-a** i **particionisanja** |

---

## EKSPERIMENTALNI SCENARIJI

### Scenario A – Massive Sensor Ingestion

**Opis:** Simulirati paralelni rad velikog broja uređaja

| Broj uređaja | Metrike za praćenje |
|---|---|
| 100 | Maksimalni throughput (poruke/s) |
| 1.000 | Procenat izgubljenih poruka |
| 10.000 | — |

---

### Scenario B – Edge Connectivity Failures

**Opis:** Simulirati mrežni prekid i pratiti oporavak

- Alat: `docker network disconnect`
- Trajanje prekida: **30 sekundi**

| Broker | Recovery mehanizam |
|---|---|
| MQTT | Trajne pretplate (persistent sessions) |
| Kafka | Pomeranje offset-a (offset replay) |

---

### Scenario C – Burst Event Load

**Opis:** Nagli skok opterećenja

- Skok sa **50 na 5.000 poruka/s** u trajanju od nekoliko sekundi

Pratiti:
- Formiranje reda čekanja (**backlog**)
- **Backpressure** ponašanje
- **Recovery time** (vreme povratka u normalu)

---

### Scenario D – Real-Time Alerting

**Opis:** Merenje end-to-end latencije

- Od: trenutak kada simulator generiše **kritičnu vrednost**
- Do: trenutak kada Analytics Service ispiše **alarm**

---

## MERENJE PERFORMANSI – OBAVEZNI ALATI

| Broker | Alat |
|---|---|
| MQTT | **emqtt-bench** (zvanični) **ILI** k6 sa MQTT ekstenzijom |
| Kafka | **kafka-producer-perf-test.sh** (nativni) **ILI** k6 sa xk6-kafka dodatkom |
| Resursi | **docker stats** (obavezno) · opciono: Prometheus + Grafana stack |

**Metrike resursa:** CPU, RAM, mrežni saobraćaj po kontejneru

---

## ANALIZA POUZDANOSTI – OBAVEZNA PITANJA

U tehničkom izveštaju odgovoriti na sledeća inženjerska pitanja:

**Pitanje 1:**
> Zašto je MQTT idealan za postavljanje na samim edge uređajima (senzorima), a zašto postaje neadekvatan kada je potrebna istorijska analitika velikih podataka?

**Pitanje 2:**
> Zašto Kafka dominira u data-intensive cloud sistemima? Kolika je "cena" njene skalabilnosti u pogledu resursa i da li je realno pokretati je na hardverski ograničenim edge serverima?

**Pitanje 3:**
> Popuniti uporednu tabelu performansi na osnovu sprovedenih eksperimenata:

| Metrika | MQTT (QoS 0) | MQTT (QoS 1) | MQTT (QoS 2) | Kafka (acks=0) | Kafka (acks=1) | Kafka (acks=all) |
|---|---|---|---|---|---|---|
| Throughput (msg/s) | | | | | | |
| p95 Latencija (ms) | | | | | | |
| CPU footprint | | | | | | |
| RAM footprint | | | | | | |

---

## OČEKIVANI REZULTATI (DELIVERABLES)

| # | Deliverable |
|---|---|
| 1 | **Git repozitorijum** |
| 2 | **Docker Compose konfiguracija** |
| 3 | **Konfiguracija brokera** (MQTT + Kafka) |
| 4 | **Benchmark skripte** |
| 5 | **Eksperimentalni podaci** (sirovi rezultati merenja) |
| 6 | **Tehnički izveštaj** sa: opisom rada, uporednom tabelom i odgovorima na kritička pitanja |

---

*Predmet: Internet stvari i servisa · Projekat 2*
