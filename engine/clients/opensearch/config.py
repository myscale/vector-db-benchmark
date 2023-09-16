from engine.base_client.distances import Distance


OPENSEARCH_PORT = 443
OPENSEARCH_INDEX = "bench"
OPENSEARCH_USER = "opensearch"
OPENSEARCH_PASSWORD = "passwd"

DISTANCE_MAPPING = {
    Distance.L2: "l2",
    Distance.COSINE: "cosinesimil",
    Distance.DOT: "innerproduct",
}

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


def process_connection_params(connection_params: dict, default_host: str):
    host = connection_params.get("host", default_host)
    port = connection_params.get("port", OPENSEARCH_PORT)
    user = connection_params.get("user", OPENSEARCH_USER)
    password = connection_params.get("password", OPENSEARCH_PASSWORD)
    aws_secret_access_key = connection_params.get("aws_secret_access_key", "NONE")
    aws_access_key_id = connection_params.get("aws_access_key_id", "NONE")
    region = connection_params.get("region", "NONE")
    service = connection_params.get("service", "NONE")
    init_client = {**connection_params}
    init_client.pop("host", '')
    init_client.pop("port", '')
    init_client.pop("user", '')
    init_client.pop("password", '')
    init_client.pop("aws_secret_access_key", '')
    init_client.pop("aws_access_key_id", '')
    init_client.pop("region", '')
    init_client.pop("service", '')

    init_params = {
        **{
            "verify_certs": True,
            "request_timeout": 90,
            "retry_on_timeout": True,
        },
        **init_client,
    }
    return host, port, user, password, aws_secret_access_key, aws_access_key_id, region, service, init_params
