import os
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# Use python3 inside container
os.environ['PYFLINK_PYTHON'] = '/usr/bin/python3'

# 1. Setup Environment
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=settings)

# 2. Add Kafka JAR (Correct filename)
t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar")

# 3. Create Source Table
t_env.execute_sql("""
    CREATE TABLE tower_events (
        tower_id STRING,
        signal_strength INT,
        latency_ms INT,
        connected_users INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'tower_events',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'quality-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 4. Create Sink Table
t_env.execute_sql("""
    CREATE TABLE anomalies_sink (
        tower_id STRING,
        signal_strength INT,
        latency_ms INT,
        connected_users INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'anomalies',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format' = 'json'
    )
""")

# 5. Execute Logic
print("Job starting...")
t_env.execute_sql("""
    INSERT INTO anomalies_sink
    SELECT tower_id, signal_strength, latency_ms, connected_users
    FROM tower_events
    WHERE signal_strength >= -120 AND signal_strength <= 0
      AND latency_ms > 0
      AND connected_users >= 0
""")