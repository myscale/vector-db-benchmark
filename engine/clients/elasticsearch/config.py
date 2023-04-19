ELASTIC_PORT = 9200
ELASTIC_INDEX = "bench"
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "passwd"


H5_COLUMN_TYPES_MAPPING = {
    "int": "long",
    "geo": "geo_point",
    "float64": "float",
    "float32": "float",
    "text": "text",
    "string": "text",
    "keyword": "text",
    "boolean": "boolean",
}


def convert_H52ClickHouseType(h5_column_type: str):
    mqdb_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if mqdb_type is None:
        raise RuntimeError(f"🐛 elastic doesn't support h5 column type: {h5_column_type}")
    return mqdb_type


def process_connection_params(connection_params: dict, default_host: str):
    host = connection_params.get("host", default_host)
    port = connection_params.get("port", ELASTIC_PORT)
    user = connection_params.get("user", ELASTIC_USER)
    password = connection_params.get("password", ELASTIC_PASSWORD)
    init_client = {**connection_params}
    init_client.pop("host", '')
    init_client.pop("port", '')
    init_client.pop("user", '')
    init_client.pop("password", '')

    init_params = {
        **{
            "verify_certs": False,
            "request_timeout": 90,
            "retry_on_timeout": True,
        },
        **init_client,
    }
    return host, port, user, password, init_params
