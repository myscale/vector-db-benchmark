import random
import time
import uuid
from typing import List, Tuple

import boto3
import tqdm
from opensearchpy import OpenSearch
from opensearchpy import RequestsHttpConnection, AWSV4SignerAuth

from dataset_reader.base_reader import Query
from engine.base_client.search import BaseSearcher
from engine.clients.opensearch.config import (
    OPENSEARCH_INDEX, process_connection_params,
)
from engine.clients.opensearch.parser import OpenSearchConditionParser


class ClosableOpenSearch(OpenSearch):
    def __del__(self):
        self.close()


class OpenSearchSearcher(BaseSearcher):
    search_params = {}
    client: OpenSearch = None
    parser = OpenSearchConditionParser()

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        host, port, user, password, aws_secret_access_key, aws_access_key_id, region, service, init_params = process_connection_params(connection_params, host)
        session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        credentials = session.get_credentials()
        auth = AWSV4SignerAuth(credentials, region, service)
        cls.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )
        cls.search_params = search_params['params']

    @classmethod
    def search_one(cls, vector, meta_conditions, top, schema, query: Query) -> List[Tuple[int, float]]:
        if query.query_text is not None:
            raise NotImplementedError
        if not cls.client:
            raise RuntimeError("cls.client has not been initialized. Please call init_client first.")
        while True:
            try:
                query = {
                    "knn": {
                        "vector": {
                            "vector": vector,
                            "k": top,
                        }
                    }
                }

                meta_conditions = cls.parser.parse(meta_conditions)
                if meta_conditions:
                    query = {
                        "bool": {
                            "must": [query],
                            "filter": meta_conditions,
                        }
                    }
                source_excludes = ['vector']
                if schema is not None:
                    source_excludes.extend(list(schema.keys()))
                res = cls.client.search(
                    index=OPENSEARCH_INDEX,
                    body={
                        "query": query,
                        "size": top,
                    },
                    _source_excludes=source_excludes,
                    params={
                        "timeout": 60,
                    },
                )
                # print(cls.client.indices.get_settings(index="bench"))
                return [
                    (uuid.UUID(hex=hit["_id"]).int, hit["_score"])
                    for hit in res["hits"]["hits"]
                ]
            except Exception as e:
                print(f"🐛 open search exception in search_one, {e}")
                time.sleep(1)

    def setup_search(self, host, distance, connection_params: dict, search_params: dict, dataset_config):
        if search_params and search_params['params']:
            while True:
                try:
                    self.init_client(host=host,
                                     distance=distance,
                                     connection_params=connection_params,
                                     search_params=search_params)
                    self.client.indices.put_settings(
                        body=search_params['params'], index=OPENSEARCH_INDEX
                    )
                    vectors = [[random.uniform(-1, 1) for _ in range(dataset_config.vector_size)] for _ in range(64)]
                    print("Trying to use 64 random search to warm up OpenSearch")
                    for vector in tqdm.tqdm(vectors):
                        results = OpenSearchSearcher.search_one(vector, None, 100, None)
                        if not results:
                            raise Exception(f"Can't get any results when warm up OpenSearch")
                    break
                except Exception as e:
                    print(f"Exception happened when warming up search for OpenSearch:{e}")
                    time.sleep(10)
