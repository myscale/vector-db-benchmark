from typing import List, Optional, Tuple

import pinecone

from engine.base_client import BaseSearcher
from engine.clients.pinecone.config import *
from engine.clients.pinecone.parser import PineconeConditionParser


class PineconeSearcher(BaseSearcher):
    search_params = {}
    parser = PineconeConditionParser()
    distance: str = None
    index: pinecone.Index = None

    @classmethod
    def init_client(
            cls, host: str, distance, connection_params: dict, search_params: dict
    ):
        pinecone.init(api_key=connection_params.get("api-key", PINECONE_API_KEY),
                      environment=connection_params.get("environment", PINECONE_ENVIRONMENT))
        cls.index = pinecone.Index(index_name=PINECONE_INDEX_NAME)

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema) -> List[Tuple[int, float]]:
        while True:
            try:
                query_response = cls.index.query(
                    # namespace=PINECONE_NAME_SPACE,  # FixMe Determine whether to add namespace for Pinecone.
                    top_k=top,
                    include_values=False,
                    include_metadata=False,
                    vector=vector,
                    filter=cls.parser.parse(meta_conditions)
                )
                break
            except Exception as e:
                print(f"pinecone search_one exception 🐛 {e}")
        res_list = []
        for result_op in query_response["matches"]:
            # print(result_op)
            res_list.append((int(result_op["id"]), float(result_op["score"])))
        # print(res_list)
        return res_list
