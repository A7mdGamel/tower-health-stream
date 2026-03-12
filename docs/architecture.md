# Tower Health Stream — Architecture

## Overview
Real-time telecom network monitoring pipeline that detects tower degradation before customers notice.

## Pipeline Flow
```
Simulator → tower_events → Job 1 → valid_events → Job 2 → regional_alerts → Job 3 → enriched_alerts
                                  → dead_letter_queue
```

## Kafka Topics
| Topic | Description | Partitions |
|-------|-------------|------------|
| tower_events | Raw events from simulator | 3 |
| valid_events | Clean events after quality check | 3 |
| dead_letter_queue | Invalid events | 1 |
| regional_alerts | Tower-level and area-level alerts | 3 |
| enriched_alerts | Alerts with weather data | 3 |

## Jobs

### Job 1 — data_quality.py (Flink)
- Source: tower_events
- Validates: signal_strength (-120 to 0), latency_ms (> 0), connected_users (>= 0)
- Valid → valid_events
- Invalid → dead_letter_queue

### Job 2 — anomaly_detection.py (Flink)
- Source: valid_events
- Detects: WEAK_SIGNAL, HIGH_DROP_RATE, HIGH_LATENCY, OVERLOADED
- Regional aggregation: 3+ towers in same area within 10 minutes → REGIONAL_OUTAGE
- Sink: regional_alerts

### Job 3 — weather_enrichment.py (Python)
- Source: regional_alerts
- Fetches weather from Open-Meteo API (lat/lon per tower)
- Classifies weather impact: LOW / MEDIUM / HIGH
- Sink: enriched_alerts

## Tech Stack
| Tool | Version | Role |
|------|---------|------|
| Apache Kafka | 4.0.0 | Message broker |
| Apache Flink | 2.0.0 | Stream processing |
| Python | 3.12 | Simulator + enrichment |
| Docker | latest | Infrastructure |

## Running the Pipeline
```bash
# Start infrastructure
docker compose -f docker/docker-compose.yml up -d

# Start Flink Jobs
docker exec flink-jobmanager flink run -py /opt/flink/jobs/data_quality.py
docker exec flink-jobmanager flink run -py /opt/flink/jobs/anomaly_detection.py

# Start enrichment service
python3 flink_jobs/weather_enrichment.py

# Start simulator
python3 simulator/tower_simulator.py
```

## Ports
| Service | Port |
|---------|------|
| Kafka | 9092 |
| Flink Dashboard | 8081 |
| Kafka UI | 8080 |