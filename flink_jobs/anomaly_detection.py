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
        is_anomaly      BOOLEAN
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
        alert_type      STRING
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'regional_alerts',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format'                       = 'json'
    )
""")

print("Job starting...")

# Detect anomalies and classify them
t_env.execute_sql("""
    INSERT INTO regional_alerts
    SELECT
        tower_id,
        operator,
        radio,
        lat,
        lon,
        area,
        ts,
        signal_strength,
        latency_ms,
        connected_users,
        call_drop_rate,
        is_peak_hour,
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