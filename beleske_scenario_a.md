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

### acks = all sa 1 replikom

  Broker:          KAFKA (acks=all)

  Broj uredjaja:    100

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.1s

  Ukupno poslato:  29,696

  Neuspesno:       0

  Throughput:      986.3 msg/s

  Izgubljen %:     0.00%

### acks = all sa 3 replike
  Broker:          KAFKA (acks=all)

  Broj uredjaja:    100

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.1s

  Ukupno poslato:  29,700

  Neuspesno:       0

  Throughput:      988.3 msg/s

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

### acks = all sa 1 replikom
  Broker:          KAFKA (acks=all)

  Broj uredjaja:    1,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.1s
  
  Ukupno poslato:  297,000

  Neuspesno:       0

  Throughput:      9,860.7 msg/s

  Izgubljen %:     0.00%

### acks = all sa 3 replike
  Broker:          KAFKA (acks=all)

  Broj uredjaja:    1,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.2s

  Ukupno poslato:  295,000

  Neuspesno:       0

  Throughput:      9,784.2 msg/s

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

### acks = all sa 1 replikom
  Broker:          KAFKA (acks=all)
  
  Broj uredjaja:    10,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.4s

  Ukupno poslato:  1,022,855

  Neuspesno:       0

  Throughput:      33,616.6 msg/s

  Izgubljen %:     0.00%

  ### acks = all sa 3 replike
  Broker:          KAFKA (acks=all)

  Broj uredjaja:    10,000

  Rate/uredjaj:     10.0 msg/s

  Trajanje:        30.5s

  Ukupno poslato:  877,970

  Neuspesno:       0

  Throughput:      28,755.3 msg/s

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



## Opšti zaključak:

Kafka linearno raste od 1K do 10K poruka u sekundi, ali sa klasterom i acks = all, dolazi do overhead-a replikacije, dok sa 1 brokerom daje isti throughput jer nemamo replikaciju.

## MQTT vs Kafka throughput

MQTT na 1000 uređaja daje 3000 poruka dok kafka daje skoro 10000, što ynači da je kafka 3x propusnija pri istom opterećenju. Na 10k uređaja MQTT pada na 2900 sa gubicima 9.56%, dok kafka drži poruke bez ijedne izgubljene poruke. To je zbog kafkine mogucnosti da refire poruke, a ne fire and forget.
