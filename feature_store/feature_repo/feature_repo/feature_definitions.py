#C:\Users\gehlo\Desktop\ML\Stock_Agent\feature_store\feature_repo\feature_repo\feature_definitions.py
from feast import Entity, FileSource, FeatureView, Field
from feast.types import Float32
from datetime import timedelta
from feast.value_type import ValueType

ticker = Entity(
    name="ticker",
    join_keys=["ticker"],
    value_type=ValueType.STRING,
)

feature_source = FileSource(
    path="../../../data/processed/features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)


feature_view = FeatureView(
    name="stock_features",
    entities=[ticker],
    ttl=timedelta(days=7),
    source=feature_source,
    schema=[
        Field(name="return_1d", dtype=Float32),
        Field(name="return_5d", dtype=Float32),
        Field(name="return_20d", dtype=Float32),
        Field(name="ma_ratio", dtype=Float32),
        Field(name="volatility_20d", dtype=Float32),
        Field(name="rsi_14", dtype=Float32),
        Field(name="bb_width", dtype=Float32),
        Field(name="volume_ratio", dtype=Float32),
    ],
)

