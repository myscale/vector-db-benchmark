from pymilvus import DataType, connections
from engine.base_client.distances import Distance

MILVUS_COLLECTION_NAME = "Benchmark"
MILVUS_DEFAULT_ALIAS = "bench"
MILVUS_DEFAULT_PORT = "19530"

DISTANCE_MAPPING = {
    Distance.L2: "L2",
    Distance.DOT: "IP",
    # Milvus does not support cosine. Cosine is equal to IP of normalized vectors
    Distance.COSINE: "IP"
    # Jaccard, Tanimoto, Hamming distance, Superstructure and Substructure are also available
}

H5_COLUMN_TYPES_MAPPING = {
    "int": DataType.INT64,
    "int32": DataType.INT64,
    "keyword": DataType.VARCHAR,
    "text": DataType.VARCHAR,
    "string": DataType.VARCHAR,
    "str": DataType.VARCHAR,
    "float": DataType.DOUBLE,
    "float64": DataType.DOUBLE,
    "float32": DataType.DOUBLE,
    "geo": DataType.UNKNOWN,
}

DTYPE_EXTRAS = {
    "keyword": {"max_length": 500},
    "text": {"max_length": 5000},
}

DTYPE_DEFAULT = {
    DataType.INT64: 0,
    DataType.VARCHAR: "---MILVUS DOES NOT ACCEPT EMPTY STRINGS---",
    DataType.FLOAT: 0.0,
}


def convert_H52MilvusType(h5_column_type: str):
    milvus_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if milvus_type is None:
        raise RuntimeError(f"🐛 milvus doesn't support h5 column type: {h5_column_type}")
    return milvus_type


def process_connection_params(connection_params_with_default_host: dict):
    if connection_params_with_default_host.get("cloud_mode", False) is not False:
        return connections.connect(
            alias=MILVUS_DEFAULT_ALIAS,
            uri=connection_params_with_default_host.get('end_point',
                                                        connection_params_with_default_host.get('default_host')),
            user=connection_params_with_default_host.get('cloud_user', ""),
            password=connection_params_with_default_host.get('cloud_password', ""),
            secure=connection_params_with_default_host.get('cloud_secure', False)
        )
    else:
        return connections.connect(
            alias=MILVUS_DEFAULT_ALIAS,
            host=connection_params_with_default_host.get('host',
                                                         connection_params_with_default_host.get('default_host')),
            port=connection_params_with_default_host.get('port', MILVUS_DEFAULT_PORT),
            user=connection_params_with_default_host.get('user', ""),
            password=connection_params_with_default_host.get('password', "")
        )