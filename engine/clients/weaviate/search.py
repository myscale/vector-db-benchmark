import time
import uuid
from typing import List, Tuple

from weaviate import Client

from engine.base_client.search import BaseSearcher
from engine.clients.weaviate.config import WEAVIATE_CLASS_NAME, WEAVIATE_DEFAULT_PORT, generateWeaviateClient
from engine.clients.weaviate.parser import WeaviateConditionParser


class WeaviateSearcher(BaseSearcher):
    search_params = {}
    client: Client = None
    parser = WeaviateConditionParser()

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        cls.client = generateWeaviateClient(connection_params={**connection_params}, host=host)
        cls.search_params = search_params

    @classmethod
    def conditions_to_filter(cls, _meta_conditions):
        # ToDo: implement
        return None

    @classmethod
    def search_one(cls, vector, meta_conditions, top, schema) -> List[Tuple[int, float]]:
        while True:
            try:
                where_conditions = cls.parser.parse(meta_conditions)
                query = cls.client.query.get(
                    WEAVIATE_CLASS_NAME, ["_additional {id distance}"]
                    # WEAVIATE_CLASS_NAME, ["_additional {id vector distance}", "probability"]  # for debug, payload vector
                ).with_near_vector({"vector": vector})

                is_geo_query = False  # geo 数据集目前未测试, 未支持
                if where_conditions is not None:
                    operands = where_conditions["operands"]
                    is_geo_query = any(
                        operand["operator"] == "WithinGeoRange" for operand in operands
                    )
                    query = query.with_where(where_conditions)
                    # print(f"weaviate search with filter: {where_conditions}")

                query_obj = query.with_limit(top)
                if is_geo_query:
                    # weaviate can't handle geo queries in python due to excess quotes in generated queries
                    gql_query = query_obj.build()
                    for field in ("geoCoordinates", "latitude", "longitude", "distance", "max"):
                        gql_query = gql_query.replace(f'"{field}"', field)  # get rid of quotes
                    response = cls.client.query.raw(gql_query)
                else:
                    response = query_obj.do()
                res = response["data"]["Get"][WEAVIATE_CLASS_NAME]

                id_score_pairs: List[Tuple[int, float]] = []
                for obj in res:
                    description = obj["_additional"]
                    score = description["distance"]
                    id_ = uuid.UUID(hex=description["id"]).int
                    id_score_pairs.append((id_, score))
                return id_score_pairs
            except Exception as e:
                print(f"weaviate search exception is {e}")

    def setup_search(self, host, distance, connection_params: dict, search_params: dict):
        search_params = {
            'vectorIndexConfig': search_params.get("vectorIndexConfig", {})
        }
        self.init_client(host=host,
                         distance=distance,
                         connection_params=connection_params,
                         search_params=search_params)
        self.client.schema.update_config(WEAVIATE_CLASS_NAME, search_params)
