import time
import uuid
from typing import List, Optional
from opensearchpy import NotFoundError, OpenSearch
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3
from opensearchpy import OpenSearch

from engine.base_client.upload import BaseUploader
from engine.clients.opensearch.config import (
    OPENSEARCH_INDEX,
    process_connection_params,
)


class ClosableOpenSearch(OpenSearch):
    def __del__(self):
        self.close()


class OpenSearchUploader(BaseUploader):
    client: OpenSearch = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
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

        while True:
            try:
                cls.client.bulk(
                    index=OPENSEARCH_INDEX,
                    body=operations,
                    params={
                        "timeout": 300,
                    },
                )
                break
            except Exception as e:
                time.sleep(3)
                print(f"Exception happened while upload_batch:{e}")


    @classmethod
    def post_upload(cls, _distance):
        time_start = time.time()
        while True:
            try:
                cls.client.indices.forcemerge(index=OPENSEARCH_INDEX)
                break
            except Exception as e:
                print(f"Exception happened while merging:{e}")
        print(f"OpenSearch merging finished, time:{time.time()-time_start}")
        return {}
