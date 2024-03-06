import time
import uuid
from typing import List, Optional

from elasticsearch import Elasticsearch

from engine.base_client.upload import BaseUploader
from engine.clients.elasticsearch.config import (
    ELASTIC_INDEX,
    process_connection_params,
)


class ClosableElastic(Elasticsearch):
    def __del__(self):
        self.close()


class ElasticUploader(BaseUploader):
    client: Elasticsearch = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        host, port, user, password, init_params = process_connection_params(connection_params, host)
        cls.client = Elasticsearch(f"http://{host}:{port}", basic_auth=(user, password), **init_params)
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(
        cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):
        if metadata is None:
            metadata = [{}] * len(vectors)
        operations = []
        for idx, vector, payload in zip(ids, vectors, metadata):
            vector_id = uuid.UUID(int=idx).hex

            operations.append({"index": {"_id": vector_id}})
            if payload:
                operations.append({"vector": vector, **payload})
            else:
                operations.append({"vector": vector})

        cls.client.bulk(
            index=ELASTIC_INDEX,
            operations=operations,
        )

    @classmethod
    def post_upload(cls, _distance):
        cls.client.indices.forcemerge(index=ELASTIC_INDEX, wait_for_completion=True)
        # There may be issues with `wait_for_completion`, so we need wait it manually.
        print("Waiting for lucene merges finished....")
        while True:
            stats = cls.client.indices.stats(index=ELASTIC_INDEX)
            merges = stats["indices"][ELASTIC_INDEX]["primaries"]["merges"]
            current_merges = merges["current"]

            if current_merges == 0:
                break
            else:
                time.sleep(3)
        print("Lucene merges completed. No segments are currently merging.")
        return {}
