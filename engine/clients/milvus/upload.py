from typing import List, Optional

from pymilvus import Collection, Connections

from engine.base_client.upload import BaseUploader
from engine.clients.milvus.config import (
    DISTANCE_MAPPING,
    MILVUS_COLLECTION_NAME,
    MILVUS_DEFAULT_ALIAS,
    DTYPE_DEFAULT, process_connection_params,
)


class MilvusUploader(BaseUploader):
    client: Connections = None
    upload_params = {}
    collection: Collection = None
    distance: str = None

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        connection_params["default_host"] = host
        cls.client = process_connection_params(connection_params_with_default_host=connection_params)
        cls.collection = Collection(MILVUS_COLLECTION_NAME, using=MILVUS_DEFAULT_ALIAS)
        cls.upload_params = upload_params
        cls.distance = DISTANCE_MAPPING[distance]

    @classmethod
    def upload_batch(
            cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):
        if metadata is not None:
            field_values = [
                [
                    payload.get(field_schema.name) or DTYPE_DEFAULT[field_schema.dtype]
                    for payload in metadata
                ]
                for field_schema in cls.collection.schema.fields
                if field_schema.name not in ["id", "vector"]
            ]
        else:
            field_values = []
        while True:
            try:
                cls.collection.insert([ids, vectors] + field_values)
                break
            except Exception as e:
                print(f"🐛 milvus upload exception: {e}")

    @classmethod
    def post_upload(cls, distance):
        index_params = {
            "metric_type": cls.distance,
            "index_type": cls.upload_params["index_type"],
            "params": {**cls.upload_params.get("index_params", {})},
        }
        print(f"before build index, index in collection:{cls.collection.indexes}")
        print(f"trying to create index, index params is {index_params}, waiting for index build finish...")
        cls.collection.create_index(field_name="vector", index_params=index_params)
        print(f"after build index, index in collection:{cls.collection.indexes}, trying load collection in memory")
        cls.collection.load()
        print("collection loaded into memory")
        return {}
