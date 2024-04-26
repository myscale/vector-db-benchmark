import time
import uuid
from typing import List, Tuple, Optional

from elasticsearch import Elasticsearch

from dataset_reader.base_reader import Query
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
        user, password, init_params = process_connection_params(connection_params, host)
        cls.client: Elasticsearch = Elasticsearch(basic_auth=(user, password), **init_params)
        cls.search_params = search_params['params']

    @classmethod
    def search_one(cls, vector, meta_conditions, top, schema, query: Query) -> List[Tuple[int, float]]:
        if query.query_text is not None:
            return cls.hybrid_search(meta_conditions, top, schema, query)
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

    @classmethod
    def hybrid_search(cls, meta_conditions, top: Optional[int], schema, query: Query) -> List[Tuple[int, float]]:
        # search_params_dict = cls.search_params["params"]
        text_search = cls.search_params.get("only_text_search", False)
        if text_search:
            return cls.text_search(meta_conditions, top, schema, query)
        else:
            raise NotImplementedError

    @classmethod
    def text_search(cls, meta_conditions, top, schema, query) -> List[Tuple[int, float]]:
        try:
            # 构建文本搜索查询
            query = {
                # "query": {
                "match": {
                    f"{query.query_text_column}": query.query_text
                }
                # }
            }
            # TODO 后续完善可以使用 filter
            source_excludes = ['vector']
            if schema is not None:
                source_excludes.extend(list(schema.keys()))

            # 执行文本搜索
            res = cls.client.search(
                index=ELASTIC_INDEX,
                query=query,
                size=top,
                source_excludes=source_excludes
            )

            # 处理搜索结果
            re = [
                (uuid.UUID(hex=hit["_id"]).int, hit["_score"])
                for hit in res["hits"]["hits"]
            ]
            return re
        except Exception as e:
            raise RuntimeError(f"🐛 Elasticsearch exception in text_search: {e}")
