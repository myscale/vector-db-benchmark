import time
import uuid
from typing import List, Tuple

from elasticsearch import Elasticsearch

from engine.base_client.search import BaseSearcher
from engine.clients.elasticsearch.config import (
    ELASTIC_INDEX, process_connection_params,
)
from engine.clients.elasticsearch.parser import ElasticConditionParser


class ClosableElastic(Elasticsearch):
    def __del__(self):
        self.close()


class ElasticSearcher(BaseSearcher):
    parser = ElasticConditionParser()
    search_params = {}
    client: Elasticsearch = None

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        host, port, user, password, init_params = process_connection_params(connection_params, host)
        cls.client: Elasticsearch = Elasticsearch(f"http://{host}:{port}", basic_auth=(user, password), **init_params)
        cls.search_params = search_params['params']

    @classmethod
    def search_one(cls, vector, meta_conditions, top, schema) -> List[Tuple[int, float]]:
        try:
            knn = {
                "field": "vector",
                "query_vector": vector,
                "k": top,
                **{"num_candidates": 100, **cls.search_params},
            }

            meta_conditions = cls.parser.parse(meta_conditions)
            if meta_conditions:
                knn["filter"] = meta_conditions

            source_excludes = ['vector']
            if schema is not None:
                source_excludes.extend(list(schema.keys()))
            res = cls.client.search(
                index=ELASTIC_INDEX,
                knn=knn,
                size=top,
                source_excludes=source_excludes
            )

            re = [
                (uuid.UUID(hex=hit["_id"]).int, hit["_score"])
                for hit in res["hits"]["hits"]
            ]
            return re
        except Exception as e:
            raise RuntimeError(f"🐛 elastic search exception in search_one, {e}")
