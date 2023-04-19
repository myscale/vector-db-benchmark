import time
from typing import List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http.models import Batch, CollectionStatus

from engine.base_client.upload import BaseUploader
from engine.clients.qdrant.config import QDRANT_COLLECTION_NAME, process_connection_params


class QdrantUploader(BaseUploader):
    client = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        connection_params['host'] = host if connection_params.get('host', None) is None else connection_params['host']
        connection_params['prefer_grpc'] = connection_params.get('prefer_grpc', True)
        connection_params = process_connection_params(connection_params)
        cls.client = QdrantClient(**connection_params)
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(
            cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):
        while True:
            try:
                cls.client.upsert(
                    collection_name=QDRANT_COLLECTION_NAME,
                    points=Batch(
                        ids=ids,
                        vectors=vectors,
                        payloads=[payload or {} for payload in metadata],
                    ),
                )
                break
            except Exception as e:
                print(e)

    @classmethod
    def post_upload(cls, _distance):
        cls.wait_collection_green()
        return {}

    @classmethod
    def wait_collection_green(cls):
        wait_time = 5.0
        total = 0
        while True:
            time.sleep(wait_time)
            total += wait_time
            collection_info = cls.client.get_collection(QDRANT_COLLECTION_NAME)
            if collection_info.status != CollectionStatus.GREEN:
                continue
            time.sleep(wait_time)
            collection_info = cls.client.get_collection(QDRANT_COLLECTION_NAME)
            if collection_info.status == CollectionStatus.GREEN:
                break
        return total
