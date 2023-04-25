from qdrant_client.http import models as rest

QDRANT_COLLECTION_NAME = "Benchmark"

H5_COLUMN_TYPES_MAPPING = {
    "int": rest.PayloadSchemaType.INTEGER,
    "int32": rest.PayloadSchemaType.INTEGER,
    "keyword": rest.PayloadSchemaType.KEYWORD,
    "text": rest.PayloadSchemaType.TEXT,
    "string": rest.PayloadSchemaType.TEXT,
    "str": rest.PayloadSchemaType.TEXT,
    "float": rest.PayloadSchemaType.FLOAT,
    "float64": rest.PayloadSchemaType.FLOAT,
    "float32": rest.PayloadSchemaType.FLOAT,
    "geo": rest.PayloadSchemaType.GEO,
}


def convert_H52QdrantType(h5_column_type: str):
    qdrant_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if qdrant_type is None:
        raise RuntimeError(f"🐛 qdrant doesn't support h5 column type: {h5_column_type}")
    return qdrant_type


def process_connection_params(connection_params: dict):
    if connection_params['host'].startswith("http"):
        connection_params['url'] = connection_params['host'] + ":" + str(connection_params['port'])
        connection_params.pop('host', '')
        connection_params.pop('port', '')
        connection_params.pop('grpc_port', '')
        connection_params.pop('prefer_grpc', '')
        if connection_params.get('api_key', None) is None:
            raise RuntimeError("please set api key for your qdrant client")
    else:
        connection_params.pop('api_key', '')
    return connection_params
