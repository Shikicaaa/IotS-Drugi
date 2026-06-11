# Beleske scenario A


## Kafka 100 Korisnika

### acks = 0
  Broker:          KAFKA (acks=0)

  Broj uredjaja:    100

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.1s

  Ukupno poslato:  29,700

  Neuspesno:       0

  Throughput:      985.9 msg/s

  Izgubljen %:     0.00%


### acks = 1

  Broker:          KAFKA (acks=1)

  Broj uredjaja:    100

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.0s

  Ukupno poslato:  29,700

  Neuspesno:       0

  Throughput:      988.5 msg/s
  
  Izgubljen %:     0.00%

### acks = all

  Broker:          KAFKA (acks=all)

  Broj uredjaja:    100

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.1s

  Ukupno poslato:  29,696

  Neuspesno:       0

  Throughput:      986.3 msg/s

  Izgubljen %:     0.00%

## Kafka 1000 Korisnika
  
### acks = 0
  Broker:          KAFKA (acks=0)

  Broj uredjaja:    1,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.1s

  Ukupno poslato:  297,000

  Neuspesno:       0

  Throughput:      9,861.5 msg/s

  Izgubljen %:     0.00%

### acks = 1
  Broker:          KAFKA (acks=1)

  Broj uredjaja:    1,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.1s

  Ukupno poslato:  297,000

  Neuspesno:       0

  Throughput:      9,858.9 msg/s

  Izgubljen %:     0.00%

### acks = all
  Broker:          KAFKA (acks=all)

  Broj uredjaja:    1,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.1s
  
  Ukupno poslato:  297,000

  Neuspesno:       0

  Throughput:      9,860.7 msg/s

  Izgubljen %:     0.00%

## Kafka 10000 Korisnika

### acks = 0

  Broker:          KAFKA (acks=0)
  
  Broj uredjaja:    10,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.4s

  Ukupno poslato:  1,028,040

  Neuspesno:       0

  Throughput:      33,777.3 msg/s

  Izgubljen %:     0.00%

### acks = 1

  Broker:          KAFKA (acks=1)

  Broj uredjaja:    10,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.4s

  Ukupno poslato:  1,009,053

  Neuspesno:       0

  Throughput:      33,149.9 msg/s

  Izgubljen %:     0.00%

### acks = all
  Broker:          KAFKA (acks=all)
  
  Broj uredjaja:    10,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.4s

  Ukupno poslato:  1,022,855

  Neuspesno:       0

  Throughput:      33,616.6 msg/s

  Izgubljen %:     0.00%

---
## MQTT 100 Korisnika

### QoS = 0
  Broker:          MQTT (QoS=0)

  Broj uredjaja:    100

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.2s

  Ukupno poslato:  29,713

  Neuspesno:       0

  Throughput:      984.1 msg/s

  Izgubljen %:     0.00%

### QoS = 1
  Broker:          MQTT (QoS=1)

  Broj uredjaja:    100
  
  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.2s

  Ukupno poslato:  29,704

  Neuspesno:       0

  Throughput:      984.7 msg/s

  Izgubljen %:     0.00%

### QoS = 2
  Broker:          MQTT (QoS=2)

  Broj uredjaja:    100

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.2s

  Ukupno poslato:  29,703

  Neuspesno:       0

  Throughput:      984.0 msg/s

  Izgubljen %:     0.00%

## MQTT 1000 Korisnika
### QoS = 0
  Broker:          MQTT (QoS=0)

  Broj uredjaja:    1,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.6s

  Ukupno poslato:  100,603

  Neuspesno:       661

  Throughput:      3,288.3 msg/s

  Izgubljen %:     0.65%

### QoS = 1
  Broker:          MQTT (QoS=1)

  Broj uredjaja:    1,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.7s

  Ukupno poslato:  100,799

  Neuspesno:       661

  Throughput:      3,286.2 msg/s

  Izgubljen %:     0.65%
### QoS = 2
  Broker:          MQTT (QoS=2)

  Broj uredjaja:    1,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.7s

  Ukupno poslato:  91,668

  Neuspesno:       661

  Throughput:      2,983.1 msg/s

  Izgubljen %:     0.72%
## MQTT 10000 Korisnika
### QoS = 0
  Broker:          MQTT (QoS=0)

  Broj uredjaja:    10,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        34.7s

  Ukupno poslato:  98,946

  Neuspesno:       9,661

  Throughput:      2,854.9 msg/s

  Izgubljen %:     8.90%

### QoS = 1
  Broker:          MQTT (QoS=1)
  
  Broj uredjaja:    10,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        33.9s

  Ukupno poslato:  99,892

  Neuspesno:       9,661

  Throughput:      2,944.6 msg/s

  Izgubljen %:     8.82%
### QoS = 2
  Broker:          MQTT (QoS=2)

  Broj uredjaja:    10,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        34.9s

  Ukupno poslato:  91,358

  Neuspesno:       9,661

  Throughput:      2,614.7 msg/s
  
  Izgubljen %:     9.56%
