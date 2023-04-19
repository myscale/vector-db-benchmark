import time

import redis
from redis.commands.search.field import VectorField

from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.redis.config import *


class RedisConfigurator(BaseConfigurator):
    DISTANCE_MAPPING = {
        Distance.L2: "L2",
        Distance.COSINE: "COSINE",
        Distance.DOT: "IP",
    }

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(connection_params.get('host', host),
                         collection_params, connection_params)

        self.client = redis.Redis(host=connection_params.get('host', host),
                                  port=connection_params.get('port', REDIS_PORT),
                                  db=0)

    def clean(self):
        index = self.client.ft()
        try:
            index.dropindex(delete_documents=True)
        except redis.ResponseError as e:
            print(f"drop redis index exception: {e}")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        self.clean()
        search_namespace = self.client.ft()
        payload_fields = [
            convert_H52RedisType(extra_columns_type[index])(
                name=extra_columns_name[index],
            )
            for index in range(0, len(extra_columns_name))
        ]
        print(f"redis recreate with payload_fields :{payload_fields}")
        search_namespace.create_index(
            fields=[
                VectorField(
                    name="vector",
                    algorithm="HNSW",
                    attributes={
                        "TYPE": "FLOAT32",
                        "DIM": vector_size,
                        "DISTANCE_METRIC": self.DISTANCE_MAPPING[distance],
                        **self.collection_params.get("hnsw_config", {}),
                    },
                )
            ]
            + payload_fields
        )


if __name__ == "__main__":
    pass
