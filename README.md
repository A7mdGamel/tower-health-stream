# Tower Health Stream

[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.0.0-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-2.0.0-E6526F?style=flat-square&logo=apacheflink&logoColor=white)](https://flink.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.3-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Latest-3E8FD3?style=flat-square&logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.11.7-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Grafana](https://img.shields.io/badge/Grafana-11.4.0-F46800?style=flat-square&logo=grafana&logoColor=white)](https://grafana.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com/)
[![License](https://img.shields.io/badge/License-MIT%20with%20Attribution-green?style=flat-square)](LICENSE)

**Real-Time Telecom Network Intelligence Pipeline**

A production-grade streaming data pipeline that detects cell tower degradation before customers experience service disruption. Built on a modern open-source data stack, it processes live telemetry from 10,192 real Egyptian cell towers, enriches alerts with live weather data, and visualizes the results on an interactive real-time map.

---

## Overview

The pipeline ingests raw tower telemetry in real time, validates signal quality, detects anomalies, correlates failures with weather conditions, and surfaces actionable insights through a live Grafana dashboard. Every layer of the stack handles a distinct responsibility — from message brokering to batch transformation to stream enrichment.

| Metric | Value |
|--------|-------|
| Real towers monitored | 10,192 (Egypt, MCC=602) |
| Event throughput | 10,192 events every 5 seconds |
| Anomaly rate simulated | 5% of events |
| Operators covered | Vodafone, e&, Orange, WE |
| Dashboard refresh rate | Every 5 seconds |
| Weather API | Open-Meteo (free, no API key required) |

---

## Architecture

The pipeline is organized into four stages: ingestion, stream processing, batch analytics, and visualization.

```
OpenCelliD Dataset (10,192 towers)
          |
          v
Tower Simulator  ------>  Kafka: tower_events
                                    |
                          Flink Job 1: Data Quality
                          /                        \
              Kafka: valid_events         Kafka: dead_letter_queue
                    |
          Flink Job 2: Anomaly Detection
                    |
          Kafka: regional_alerts
          /                    \
Weather Enrichment         Alerts Sink
          |                    |
Kafka: enriched_alerts    PostgreSQL: realtime_alerts
          |                    |
Enriched Alerts Sink           |
          |                    |
PostgreSQL: enriched_alerts    |
          \                   /
              Grafana Dashboard
              (http://localhost:3000)
```

### Stage 1 — Ingestion

- The Tower Simulator reads all 10,192 towers from the OpenCelliD dataset and emits a full cycle of events every 5 seconds to the Kafka topic `tower_events`.
- Apache Kafka 4.0.0 (KRaft mode, no ZooKeeper) acts as the central message broker, holding five topics: `tower_events`, `valid_events`, `dead_letter_queue`, `regional_alerts`, and `enriched_alerts`.

### Stage 2 — Stream Processing (Apache Flink 2.0.0)

- **Data Quality Job:** Validates each incoming event against signal, latency, and user thresholds. Valid events are forwarded to `valid_events`; invalid events are routed to `dead_letter_queue`.
- **Anomaly Detection Job:** Classifies valid events into four alert types — `WEAK_SIGNAL`, `HIGH_DROP_RATE`, `HIGH_LATENCY`, `OVERLOADED`. Regional aggregation uses a 10-minute tumbling window to detect `REGIONAL_OUTAGE` when three or more towers in the same area are simultaneously degraded. Alerts are published to `regional_alerts`.
- **Weather Enrichment Service:** Consumes `regional_alerts`, calls the Open-Meteo API per tower location, computes a `weather_impact` rating (HIGH, MEDIUM, LOW), appends a human-readable conclusion, and publishes the enriched event to `enriched_alerts`.

### Stage 3 — Storage and Batch Analytics

- **Apache Iceberg (REST Catalog):** Persists valid events as a managed table in a columnar format, enabling time-travel queries and schema evolution.
- **Apache Airflow:** Orchestrates two DAGs — a monthly OpenCelliD data refresh and an hourly weather batch job that samples 100 towers and caches current weather readings.
- **dbt (dbt-core 1.11.7 + dbt-duckdb):** Runs three SQL transformation models against DuckDB to produce aggregated analytics tables: `tower_health_scores`, `operator_performance`, and `regional_risk_index`.

### Stage 4 — Visualization (Grafana 11.4.0)

- All dbt model outputs are exported to PostgreSQL, which serves as the Grafana data source.
- Two real-time sink scripts (`alerts_sink.py` and `enriched_alerts_sink.py`) continuously write from Kafka into PostgreSQL, allowing the dashboard to reflect live state.

---

## Technology Stack

| Layer | Technology | Version | Role |
|-------|-----------|---------|------|
| Message Broker | Apache Kafka (KRaft) | 4.0.0 | Durable, partitioned event streaming — no ZooKeeper |
| Stream Processing | Apache Flink | 2.0.0 | Stateful anomaly detection and windowed aggregations |
| Table Storage | Apache Iceberg + REST Catalog | Latest | ACID-compliant open table format for valid events |
| Orchestration | Apache Airflow | 2.10.3 | Scheduled batch pipelines and DAG management |
| Transformation | dbt-core + dbt-duckdb | 1.11.7 | Declarative SQL models compiled against DuckDB |
| Analytical DB | DuckDB | 1.5.0 | Embedded OLAP engine powering dbt transformations |
| Visualization | Grafana | 11.4.0 | Real-time dashboards with geo-map and table panels |
| Serving DB | PostgreSQL | 13 | Grafana data source for both static and live data |
| Infrastructure | Docker Compose | Latest | Fully containerized local development environment |
| Simulation and Enrichment | Python | 3.12 | Tower simulator, weather enrichment, alert sinks |

---

## Data Source

**OpenCelliD** is the world's largest open-source database of cell tower locations, maintained by Unwired Labs. The dataset used in this project contains all Egyptian cell towers (MCC=602), cleaned to remove operator codes outside the four major providers.

| Operator | Towers | Network |
|----------|--------|---------|
| Vodafone Egypt | 4,791 | LTE / UMTS / GSM |
| e& (Etisalat) | 2,774 | LTE / UMTS / GSM |
| Orange Egypt | 2,212 | LTE / UMTS / GSM |
| WE (Telecom Egypt) | 415 | LTE / UMTS / GSM |
| **Total** | **10,192** | All radio types |

---

## Kafka Topics

| Topic | Partitions | Producer | Consumer | Purpose |
|-------|-----------|---------|---------|---------|
| tower_events | 3 | Tower Simulator | Flink Job 1 | Raw telemetry from all towers |
| valid_events | 3 | Flink Job 1 | Flink Job 2 / Iceberg Sink | Quality-validated events only |
| dead_letter_queue | 1 | Flink Job 1 | Monitoring | Malformed or out-of-range events |
| regional_alerts | 3 | Flink Job 2 | Weather Enrichment / Alerts Sink | Tower and regional anomaly alerts |
| enriched_alerts | 3 | Weather Enrichment | Enriched Alerts Sink | Alerts with live weather context |

---

## dbt Transformation Models

| Model | Description | Key Output Columns |
|-------|------------|-------------------|
| tower_health_scores | Assigns a health score to every tower based on radio technology. LTE towers score highest, GSM lowest. | tower_id, operator, radio, health_score, performance_rank |
| operator_performance | Aggregates LTE, UMTS, and GSM tower counts per operator and ranks them by 4G coverage percentage. | operator, total_towers, lte_pct, umts_pct, gsm_pct, performance_rank |
| regional_risk_index | Calculates a risk score per geographic area based on the proportion of legacy (non-LTE) towers. | area, total_towers, lte_coverage_pct, risk_level, risk_score |

---

## Grafana Dashboard

The Tower Health Dashboard is accessible at `http://localhost:3000` and contains six panels:

| Panel | Type | Data Source | Refresh |
|-------|------|------------|---------|
| Tower Alerts Map | Geomap | realtime_alerts JOIN enriched_alerts (PostgreSQL) | 5 seconds (live) |
| Real-time Alerts | Table | realtime_alerts (PostgreSQL) | 5 seconds (live) |
| Weather Impact Alerts | Table | enriched_alerts (PostgreSQL) | 5 seconds (live) |
| Tower Health Scores by Operator | Bar Chart | tower_health_scores (dbt model) | On dbt run |
| Operator Performance | Bar Chart | operator_performance (dbt model) | On dbt run |
| Regional Risk Index | Bar Chart | regional_risk_index (dbt model) | On dbt run |

### Map Color Coding

| Color | Alert Type | Meaning |
|-------|-----------|---------|
| Yellow | WEAK_SIGNAL | Signal strength below -90 dBm |
| Red | HIGH_DROP_RATE | Call drop rate above 5% |
| Orange | HIGH_LATENCY | Latency above 200ms |
| Blue | OVERLOADED | Weak signal with more than 800 connected users |
| Purple | WEATHER_RELATED | Any alert where weather impact is MEDIUM or HIGH |

---

## Running the Pipeline Locally

### Prerequisites

- Docker and Docker Compose
- Python 3.12
- Required Python packages: `kafka-python`, `psycopg2`, `pyiceberg`, `duckdb`, `pandas`
- dbt-core and dbt-duckdb

### 1. Start Infrastructure

```bash
git clone https://github.com/A7md-Waly/tower-health-stream.git
cd tower-health-stream
docker compose -f docker/docker-compose.yml up -d
```

### 2. Create Kafka Topics

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic tower_events \
  --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic valid_events \
  --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic dead_letter_queue \
  --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic regional_alerts \
  --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic enriched_alerts \
  --partitions 3 --replication-factor 1
```

### 3. Add Tower Data

Download Egypt towers (MCC=602) from [OpenCelliD](https://opencellid.org/) and place the CSV file at `data/towers_raw.csv`, then run:

```bash
python3 simulator/data_cleaner.py
# Output: data/towers_clean.csv (10,192 towers)
```

### 4. Submit Flink Jobs

```bash
docker exec flink-jobmanager flink run -py /opt/flink/jobs/data_quality.py
docker exec flink-jobmanager flink run -py /opt/flink/jobs/anomaly_detection.py
```

### 5. Start Python Services (each in a separate terminal)

```bash
python3 simulator/tower_simulator.py
python3 flink_jobs/weather_enrichment.py
python3 iceberg/iceberg_sink.py
python3 grafana/alerts_sink.py
python3 grafana/enriched_alerts_sink.py
```

### 6. Run dbt Transformations

```bash
cd dbt_project && dbt run
```

### Service Endpoints

| Service | URL | Credentials |
|---------|-----|------------|
| Grafana Dashboard | http://localhost:3000 | admin / admin |
| Kafka UI | http://localhost:8080 | No login required |
| Flink Dashboard | http://localhost:8081 | No login required |
| Airflow | http://localhost:8082 | admin / admin |
| Iceberg REST Catalog | http://localhost:8182 | No login required |

---

## Project Structure

```
tower-health-stream/
|
|-- simulator/
|   |-- tower_simulator.py          Emits real tower telemetry to Kafka every 5 seconds
|   `-- data_cleaner.py             Cleans raw OpenCelliD CSV into towers_clean.csv
|
|-- flink_jobs/
|   |-- data_quality.py             Flink job: signal validation and dead-letter routing
|   |-- anomaly_detection.py        Flink job: alert classification and regional aggregation
|   |-- weather_enrichment.py       Enriches alerts with live Open-Meteo weather data
|   `-- jars/
|       `-- flink-sql-connector-kafka-4.0.1-2.0.jar
|
|-- iceberg/
|   `-- iceberg_sink.py             Writes valid events to the Iceberg table
|
|-- dags/
|   |-- opencellid_refresh.py       Airflow DAG: monthly tower data refresh
|   `-- weather_batch.py            Airflow DAG: hourly weather sampling for 100 towers
|
|-- dbt_project/
|   `-- models/
|       |-- tower_health_scores.sql
|       |-- operator_performance.sql
|       `-- regional_risk_index.sql
|
|-- grafana/
|   |-- alerts_sink.py              Streams regional alerts to PostgreSQL for Grafana
|   `-- enriched_alerts_sink.py     Streams weather-enriched alerts to PostgreSQL
|
|-- docker/
|   |-- docker-compose.yml          Full infrastructure definition (9 services)
|   `-- Dockerfile.flink            Custom Flink image with PyFlink and Kafka connector JAR
|
|-- data/                           Not tracked in Git (add to .gitignore)
|   `-- towers_clean.csv            10,192 Egyptian towers
|
`-- docs/
    `-- architecture.md
```

---

## Anomaly Detection Thresholds

| Alert Type | Condition | Severity |
|-----------|-----------|---------|
| WEAK_SIGNAL | signal_strength < -90 dBm | Medium |
| HIGH_DROP_RATE | call_drop_rate > 5% | High |
| HIGH_LATENCY | latency_ms > 200ms | Medium |
| OVERLOADED | signal < -90 dBm AND connected_users > 800 | High |
| REGIONAL_OUTAGE | 3 or more towers in same area with alerts in a 10-minute window | Critical |

---

## Weather Impact Classification

Weather enrichment is performed per tower location using the Open-Meteo API (free, no API key required). Precipitation, wind speed, and cloud cover are evaluated to determine whether weather is a contributing factor to the detected anomaly.

| Impact Level | Condition | Conclusion |
|-------------|-----------|-----------|
| HIGH | Rain > 5 mm/h or wind > 40 km/h | Weather is the main cause |
| MEDIUM | Rain > 1 mm/h or wind > 20 km/h or cloud cover > 80% | Weather may be a partial cause |
| LOW | All other conditions | Technical issue — not weather related |

---

## License

This project is open source. You are free to use, study, modify, and build upon it for any purpose — personal, academic, or commercial — subject to one condition:

**Attribution is required.** Any use or derivative of this project must include clear and visible credit to the original author:

> This project is based on Tower Health Stream, originally created by Ahmed Waly (github.com/A7md-Waly/tower-health-stream).

See the [LICENSE](LICENSE) file for full terms.

---

## Author

Ahmed Waly — [github.com/A7md-Waly](https://github.com/A7md-Waly)
