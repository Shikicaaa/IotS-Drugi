## Bitno za uociti

Runda 1 uvek ima anomalnu latenciju (11s, 18s, timeout), to je zato što analytics servis ima tumbling window koji teče nezavisno.
Runde 2 i 3 su pouzdanije jer se sinhronizuju sa window ritmom.

## Latencija
Oba brokera su zanemarljivo brza za samu propagaciju poruke (1–5ms). Dominantni faktor latencije u end-to-end scenariju je analytics window od 10s što znači da za real-time alerting izbor brokera ne utiče na latenciju, već arhitektura stream processinga.
