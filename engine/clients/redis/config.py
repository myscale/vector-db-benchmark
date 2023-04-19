from redis.commands.search.field import GeoField, NumericField, TextField, VectorField


REDIS_INDEX_NAME = "benchmark"
REDIS_PORT = 6380

H5_COLUMN_TYPES_MAPPING = {
    "int": NumericField,
    "int32": NumericField,
    "keyword": TextField,
    "text": TextField,
    "string": TextField,
    "str": TextField,
    "float": NumericField,
    "float64": NumericField,
    "float32": NumericField,
    "geo": GeoField,
}


def convert_H52RedisType(h5_column_type: str):
    redis_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if redis_type is None:
        raise RuntimeError(f"🐛 redis doesn't support h5 column type: {h5_column_type}")
    return redis_type
