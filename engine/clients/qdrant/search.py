import time
from typing import List, Optional, Tuple

from qdrant_client import QdrantClient
from qdrant_client.http import models as rest
import multiprocessing as mp

from engine.base_client.search import BaseSearcher
from engine.clients.qdrant.config import QDRANT_COLLECTION_NAME, process_connection_params
from engine.clients.qdrant.parser import QdrantConditionParser


class QdrantSearcher(BaseSearcher):
    search_params = {}
    client: QdrantClient = None
    parser = QdrantConditionParser()

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        connection_params['host'] = host if connection_params.get('host', None) is None else connection_params['host']
        connection_params = process_connection_params(connection_params)
        cls.client: QdrantClient = QdrantClient(**connection_params)
        cls.search_params = search_params

    @classmethod
    def search_one(cls, vector, meta_conditions, top, schema) -> List[Tuple[int, float]]:
        while True:
            try:
                res = cls.client.search(
                    collection_name=QDRANT_COLLECTION_NAME,
                    query_vector=vector,
                    query_filter=cls.parser.parse(meta_conditions),
                    limit=top,
                    # We need qdrant to not return structured data payload.
                    with_payload=False,
                    search_params=rest.SearchParams(
                        **cls.search_params.get("params", {})
                    ),
                )

                # print([hit.payload for hit in res])
                return [(hit.id, hit.score) for hit in res]
            except Exception as e:
                print(e)
