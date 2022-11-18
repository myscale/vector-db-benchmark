from engine.base_client.distances import Distance

MQDB_DATABASE_NAME = "Benchmark"
MQDB_DEFAULT_PORT = "8123"
MQDB_DEFAULT_USER = "default"
MQDB_DEFAULT_PASSWD = ""

DISTANCE_MAPPING = {
    Distance.L2: "L2",
    Distance.DOT: "IP",
    Distance.COSINE: "cosine"
}