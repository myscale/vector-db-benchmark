import random
import string

from engine.base_client.distances import Distance

MQDB_DATABASE_NAME = "Benchmark"
MQDB_DEFAULT_PORT = "8123"
MQDB_DEFAULT_USER = "default"
MQDB_DEFAULT_PASSWD = ""

DISTANCE_MAPPING = {
    Distance.L2: "L2",
    Distance.DOT: "IP",
    Distance.COSINE: "COSINE"  # cosine 索引存在问题, 在对数据归一化后 IP=COSINE
}

H5_COLUMN_TYPES_MAPPING = {
    "float64": "Float64",
    "float32": "Float32",
    "float": "Float64",
    "int32": "Int32",
    "int": "Int32",
    "integer": "Int32",
    "text": "Nullable(String)",  # 有些大文本字段是 null
    "string": "String",
    "blob": "String",
    "geo": "Tuple(Float64, Float64)",  # 经纬度使用 Point 存储, Point == Tuple(Float64, Float64)
    "keyword": "LowCardinality(String)",  # TODO 处理 ann-filter payload 字段是 null
    "boolean": "Boolean",
}


def convert_H52ClickHouseType(h5_column_type: str):
    mqdb_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if mqdb_type is None:
        raise RuntimeError(f"🐛 mqdb doesn't support h5 column type: {h5_column_type}")
    return mqdb_type


def get_random_string(length: int):
    random_list = []
    for i in range(length):
        random_list.append(random.choice(string.ascii_uppercase + string.digits))
    return ''.join(random_list)
