from typing import List, Tuple

from pymilvus import Collection, Connections

from engine.base_client.search import BaseSearcher
from engine.clients.milvus.config import (
    DISTANCE_MAPPING,
    MILVUS_COLLECTION_NAME,
    MILVUS_DEFAULT_ALIAS,
    process_connection_params,
)
from engine.clients.milvus.parser import MilvusConditionParser


class MilvusSearcher(BaseSearcher):
    search_params = {}
    client: Connections = None
    collection: Collection = None
    distance: str = None
    parser = MilvusConditionParser()

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        connection_params["default_host"] = host
        cls.client = process_connection_params(connection_params_with_default_host=connection_params)
        cls.collection = Collection(MILVUS_COLLECTION_NAME, using=MILVUS_DEFAULT_ALIAS)
        cls.search_params = search_params
        cls.distance = DISTANCE_MAPPING[distance]

    @classmethod
    def search_one(cls, vector, meta_conditions, top, schema) -> List[Tuple[int, float]]:
        param = {"metric_type": cls.distance, "params": cls.search_params["params"]}
        while True:
            try:
                res = cls.collection.search(
                    data=[vector],
                    anns_field="vector",
                    param=param,
                    limit=top,
                    expr=cls.parser.parse(meta_conditions),
                    # use output_fields to get extra columns, refer: https://github.com/milvus-io/pymilvus/issues/1149
                    # output_fields=["probability"]  # for debug
                )
                break
            except Exception as e:
                print(f"🐛 milvus search got exception: {e}, param is {param}")

        # Here is sample to get output_fields value
        # debug_res = []
        # for hits in res:
        #     debug_res.append([(hit.entity.value_of_field("probability")) for hit in hits])
        # print(debug_res)
        return list(zip(res[0].ids, res[0].distances))
