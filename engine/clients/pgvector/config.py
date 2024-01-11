from engine.base_client.distances import Distance

PGVECTOR_PORT = 5432
PGVECTOR_INDEX = "bench"
PGVECTOR_DATABASE = "postgres"
PGVECTOR_USER = "root"
PGVECTOR_PASSWORD = "123456"

DISTANCE_MAPPING_CREATE = {
    Distance.L2: "vector_l2_ops",
    Distance.COSINE: "vector_cosine_ops",
    Distance.DOT: "vector_ip_ops",
}

DISTANCE_MAPPING_CREATE_RUST = {
    Distance.L2: "vector_l2_ops",
    Distance.COSINE: "vector_dot_ops",
    Distance.DOT: "vector_cos_ops",
}

DISTANCE_MAPPING_SEARCH = {
    Distance.L2: "<->",
    Distance.COSINE: "<=>",
    Distance.DOT: "<#>",
}

H5_COLUMN_TYPES_MAPPING = {
    "int": "integer",
    # TODO support geo extension
    "float64": "double precision",
    "float32": "real",
    "float": "real",
    "text": "text",
    "string": "varchar(256)",
    "keyword": "varchar(256)",
    "boolean": "boolean",
}


def process_connection_params(connection_params: dict, default_host: str):
    database = PGVECTOR_DATABASE
    host = connection_params.get("host", default_host)
    port = connection_params.get("port", PGVECTOR_PORT)
    user = connection_params.get("user", PGVECTOR_USER)
    password = connection_params.get("password", PGVECTOR_PASSWORD)

    return database, host, port, user, password
