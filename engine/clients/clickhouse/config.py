from engine.base_client.distances import Distance

CLICKHOUSE_DATABASE_NAME = "Benchmark"
CLICKHOUSE_DEFAULT_PORT = "8123"
CLICKHOUSE_DEFAULT_USER = "default"
CLICKHOUSE_DEFAULT_PASSWD = ""

DISTANCE_MAPPING = {
    Distance.L2: "L2Distance",
    Distance.DOT: "cosineDistance",
    Distance.COSINE: "cosineDistance"
}

H5_COLUMN_TYPES_MAPPING = {
    "float64": "Float64",
    "float32": "Float32",
    "float": "Float64",
    "int32": "Int32",
    "int": "Int32",
    "integer": "Int32",
    "text": "Nullable(String)",
    "string": "String",
    "blob": "String",
    "geo": "Tuple(Float64, Float64)",
    "keyword": "LowCardinality(String)",
    "boolean": "Boolean",
}


def convert_H52ClickHouseType(h5_column_type: str):
    clickhouse_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if clickhouse_type is None:
        raise RuntimeError(f"🐛 clickhouse doesn't support h5 column type: {h5_column_type}")
    return clickhouse_type
