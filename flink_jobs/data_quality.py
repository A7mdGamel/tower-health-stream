import os
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

os.environ['PYFLINK_PYTHON'] = '/usr/bin/python3'

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=settings)

t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar")

t_env.execute_sql("""
    CREATE TABLE tower_events (
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
        'topic'                        = 'tower_events',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id'          = 'quality-group',
        'scan.startup.mode'            = 'latest-offset',
        'format'                       = 'json',
        'json.ignore-parse-errors'     = 'true'
    )
""")

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
        'format'                       = 'json'
    )
""")

t_env.execute_sql("""
    CREATE TABLE dead_letter (
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
        'topic'                        = 'dead_letter_queue',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format'                       = 'json'
    )
""")

print("Job starting...")

t_env.execute_sql("""
    INSERT INTO valid_events
    SELECT * FROM tower_events
    WHERE signal_strength >= -120
      AND signal_strength <= 0
      AND latency_ms > 0
      AND connected_users >= 0
""")

t_env.execute_sql("""
    INSERT INTO dead_letter
    SELECT * FROM tower_events
    WHERE signal_strength < -120
       OR signal_strength > 0
       OR latency_ms <= 0
       OR connected_users < 0
""")