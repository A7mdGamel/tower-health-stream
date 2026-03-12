import json
import pyarrow as pa
from kafka import KafkaConsumer
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, StringType, DoubleType, BooleanType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder

# Connect to Iceberg REST catalog
catalog = RestCatalog(
    name="rest",
    uri="http://localhost:8182",
)

# Create namespace if not exists
try:
    catalog.create_namespace("telecom")
except Exception:
    pass

# Define schema
schema = Schema(
    NestedField(1,  "tower_id",        LongType(),    required=True),
    NestedField(2,  "operator",         StringType(),  required=False),
    NestedField(3,  "radio",            StringType(),  required=False),
    NestedField(4,  "lat",              DoubleType(),  required=False),
    NestedField(5,  "lon",              DoubleType(),  required=False),
    NestedField(6,  "area",             LongType(),    required=False),
    NestedField(7,  "ts",               StringType(),  required=False),
    NestedField(8,  "signal_strength",  DoubleType(),  required=False),
    NestedField(9,  "latency_ms",       DoubleType(),  required=False),
    NestedField(10, "connected_users",  LongType(),    required=False),
    NestedField(11, "call_drop_rate",   DoubleType(),  required=False),
    NestedField(12, "is_peak_hour",     BooleanType(), required=False),
    NestedField(13, "is_anomaly",       BooleanType(), required=False),
)

# Create table if not exists
try:
    table = catalog.create_table(
        identifier="telecom.valid_events",
        schema=schema,
    )
    print("Table created: telecom.valid_events")
except Exception:
    table = catalog.load_table("telecom.valid_events")
    print("Table loaded: telecom.valid_events")

# Kafka consumer
consumer = KafkaConsumer(
    'valid_events',
    bootstrap_servers='localhost:9092',
    group_id='iceberg-sink-group',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Iceberg sink started...")

batch = []
BATCH_SIZE = 100

for msg in consumer:
    event = msg.value
    batch.append({
        "tower_id":        event.get("tower_id"),
        "operator":        event.get("operator"),
        "radio":           event.get("radio"),
        "lat":             event.get("lat"),
        "lon":             event.get("lon"),
        "area":            event.get("area"),
        "ts":              event.get("ts") or event.get("timestamp"),
        "signal_strength": event.get("signal_strength"),
        "latency_ms":      event.get("latency_ms"),
        "connected_users": event.get("connected_users"),
        "call_drop_rate":  event.get("call_drop_rate"),
        "is_peak_hour":    event.get("is_peak_hour"),
        "is_anomaly":      event.get("is_anomaly"),
    })

    if len(batch) >= BATCH_SIZE:
        df = pa.Table.from_pylist(batch, schema=schema.as_arrow())
        table.append(df)
        print(f"Written {len(batch)} records to Iceberg")
        batch = []