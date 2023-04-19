from pyproximase import DataType

from engine.base_client.distances import Distance

PROXIMA_INDEX_NAME = "benchmark"
# PINECONE_NAME_SPACE="vector_ns"

PROXIMA_COLLECTION_NAME = "Benchmark"
PROXIMA_GRPC_PORT = "16000"
PROXIMA_HTTP_PORT = "16001"
PROXIMA_DEFAULT_USER = "default"
PROXIMA_DEFAULT_PASSWD = ""

# Fixme find a way to use distance in Proxima
DISTANCE_MAPPING = {
    Distance.L2: "euclidean",
    Distance.DOT: "dotproduct",
    Distance.COSINE: "cosine"
}

H5_COLUMN_TYPES_MAPPING = {
    "float64": DataType.FLOAT,
    "float32": DataType.FLOAT,
    "float": DataType.FLOAT,
    "int32": DataType.INT32,
    "int64": DataType.INT64,
    "int": DataType.INT32,
    "integer": DataType.INT32,
    "text": DataType.STRING,
    "keyword": DataType.STRING,
    "string": DataType.STRING,
    "boolean": DataType.BOOL,
}


def convert_H52ProximaType(h5_column_type: str):
    proxima_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if proxima_type is None:
        raise RuntimeError(f"🐛 proxima doesn't support h5 column type: {h5_column_type}")
    return proxima_type
