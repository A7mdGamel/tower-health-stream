import os
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

os.environ['PYFLINK_PYTHON'] = '/usr/bin/python3'

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=settings)

t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar")

# Source: valid_events
t_env.execute_sql("""
    CREATE TABLE valid_events (
        tower_id        BIGINT,
        operator        STRING,
        radio           STRING,
        lat             DOUBLE,
        lon             DOUBLE,
        area            BIGINT,
        ts              STRING,
        signal_strength DOUBLE,
        latency_ms      DOUBLE,
        connected_users INT,
        call_drop_rate  DOUBLE,
        is_peak_hour    BOOLEAN,
        is_anomaly      BOOLEAN,
        event_time      AS TO_TIMESTAMP(ts),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'valid_events',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id'          = 'anomaly-detection-group',
        'scan.startup.mode'            = 'latest-offset',
        'format'                       = 'json',
        'json.ignore-parse-errors'     = 'true'
    )
""")

# Sink: regional_alerts
t_env.execute_sql("""
    CREATE TABLE regional_alerts (
        tower_id        BIGINT,
        operator        STRING,
        lat             DOUBLE,
        lon             DOUBLE,
        area            BIGINT,
        ts              STRING,
        signal_strength DOUBLE,
        latency_ms      DOUBLE,
        connected_users INT,
        call_drop_rate  DOUBLE,
        alert_type      STRING
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'regional_alerts',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format'                       = 'json'
    )
""")

# Sink: area_alerts (regional aggregation)
t_env.execute_sql("""
    CREATE TABLE area_alerts (
        area            BIGINT,
        operator        STRING,
        window_start    TIMESTAMP(3),
        window_end      TIMESTAMP(3),
        affected_towers BIGINT,
        avg_signal      DOUBLE,
        avg_drop_rate   DOUBLE,
        alert_type      STRING
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'regional_alerts',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format'                       = 'json'
    )
""")

print("Job starting...")

# Job 1: Tower-level anomaly detection
t_env.execute_sql("""
    INSERT INTO regional_alerts
    SELECT
        tower_id,
        operator,
        lat,
        lon,
        area,
        ts,
        signal_strength,
        latency_ms,
        connected_users,
        call_drop_rate,
        CASE
            WHEN signal_strength < -90 AND connected_users > 800 THEN 'OVERLOADED'
            WHEN signal_strength < -90 THEN 'WEAK_SIGNAL'
            WHEN call_drop_rate > 0.05 THEN 'HIGH_DROP_RATE'
            WHEN latency_ms > 200 THEN 'HIGH_LATENCY'
            ELSE 'DEGRADED'
        END AS alert_type
    FROM valid_events
    WHERE signal_strength < -90
       OR call_drop_rate > 0.05
       OR latency_ms > 200
""")

# Job 2: Regional aggregation — 3+ towers in same area within 10 minutes
t_env.execute_sql("""
    INSERT INTO area_alerts
    SELECT
        area,
        operator,
        TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS window_start,
        TUMBLE_END(event_time,   INTERVAL '10' MINUTE) AS window_end,
        COUNT(DISTINCT tower_id)                        AS affected_towers,
        AVG(signal_strength)                            AS avg_signal,
        AVG(call_drop_rate)                             AS avg_drop_rate,
        'REGIONAL_OUTAGE'                               AS alert_type
    FROM valid_events
    WHERE signal_strength < -90
       OR call_drop_rate > 0.05
    GROUP BY
        area,
        operator,
        TUMBLE(event_time, INTERVAL '10' MINUTE)
    HAVING COUNT(DISTINCT tower_id) >= 3
""")