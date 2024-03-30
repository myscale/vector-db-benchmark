import time
from typing import List, Optional, Tuple

from pyproximase import Client, QueryRequest, DataType, SqlQueryRequest

from dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from engine.clients.proxima.config import *
from engine.clients.proxima.parser import ProximaConditionParser


class ProximaSearcher(BaseSearcher):
    client: Client = None
    parser = ProximaConditionParser()

    @classmethod
    def init_client(
            cls, host: str, distance, connection_params: dict, search_params: dict
    ):
        cls.client: Client = Client(connection_params.get("host", host), PROXIMA_GRPC_PORT)

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema, query: Query) -> List[Tuple[int, float]]:
        if query.query_text is not None:
            raise NotImplementedError
        knn_param = QueryRequest.KnnQueryParam(column_name='vector',
                                               features=vector,
                                               data_type=DataType.VECTOR_FP32)
        query_request = QueryRequest(collection_name=PROXIMA_COLLECTION_NAME,
                                     topk=top,
                                     # topk=5,  # for debug
                                     knn_param=knn_param,
                                     query_filter=cls.parser.parse(meta_conditions))

        status, search_res = cls.client.query(query_request)  # str(status): success
        res = []
        for i, result in enumerate(search_res.results):
            for doc in result:
                # forward_values = ','.join(f'{k}={v}' for k, v in doc.forward_column_values.items())
                # print(f'primary_key={doc.primary_key}, score={doc.score}, forward_column_values=[{forward_values}]')
                res.append((doc.primary_key, doc.score))
        return res
