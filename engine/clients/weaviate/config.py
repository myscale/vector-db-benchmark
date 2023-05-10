import weaviate
from weaviate import Client

WEAVIATE_CLASS_NAME = "Benchmark"
WEAVIATE_DEFAULT_PORT = 8090

H5_COLUMN_TYPES_MAPPING = {
    "int": "int",
    "keyword": "string",
    "text": "string",
    "float": "number",
    "float64": "number",
    "float32": "number",
    "geo": "geoCoordinates",
}


# reference: https://weaviate.io/developers/weaviate/configuration/datatypes
def convert_H52WeaviateType(h5_column_type: str):
    py_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if py_type is None:
        raise RuntimeWarning(f"myscale doesn't support h5 column type: {h5_column_type}")
    return py_type


def generateWeaviateClient(connection_params: dict, host: str):
    host = connection_params.pop('host', host)
    if host.startswith("http"):
        # cloud mode
        api_key = connection_params.pop('api_key', '<your_api_key>')
        auth_client_secret = weaviate.auth.AuthApiKey(api_key=api_key)
        connection_params.pop('port', '')
        return Client(url=host, auth_client_secret=auth_client_secret, **connection_params)
    else:
        # local mode
        url = f"http://{host}:{connection_params.pop('port', WEAVIATE_DEFAULT_PORT)}"
        connection_params.pop('api_key', '<your_api_key>')
        return Client(url=url, **connection_params)
