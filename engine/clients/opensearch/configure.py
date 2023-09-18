import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

from engine.base_client.configure import BaseConfigurator
from engine.clients.opensearch.config import *


# reference: https://opensearch.org/docs/latest/search-plugins/knn/knn-index/
class OpenSearchConfigurator(BaseConfigurator):

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        host, port, user, password, aws_secret_access_key, aws_access_key_id, region, service, init_params = process_connection_params(
            connection_params, host)
        session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        credentials = session.get_credentials()
        auth = AWSV4SignerAuth(credentials, region, service)
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )

    def clean(self):
        try:
            self.client.indices.delete(
                index=OPENSEARCH_INDEX,
                params={
                    "timeout": 300,
                },
            )
        except Exception as e:
            print(f"Exception happened when clean index:{e}")

    # TODO unified dataset info
    def recreate(self, distance: str, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):

        try:
            self.client.indices.create(
                index=OPENSEARCH_INDEX,
                body={
                    "settings": {
                        "index": {
                            "knn": True,
                        },
                        "number_of_replicas": 0
                    },
                    "mappings": {
                        "properties": {
                            "vector": {
                                "type": "knn_vector",
                                "dimension": vector_size,
                                "method": {
                                    **{
                                        "name": "hnsw",
                                        "engine": "nmslib",  # supports: lucene faiss nmslib
                                        "space_type": DISTANCE_MAPPING[distance],
                                        "parameters": {
                                            "m": 16,
                                            "ef_construction": 100,
                                            **collection_params.get("index_options")
                                        },
                                    }
                                    ,
                                },
                            },
                            **{
                                extra_columns_name[i]: {
                                    # The mapping is used only for several types, as some of them
                                    # overlap with the ones used internally.
                                    "type": H5_COLUMN_TYPES_MAPPING.get(extra_columns_type[i], extra_columns_type[i]),
                                    "index": True,
                                }
                                for i in range(0, len(extra_columns_name))
                            },
                        }
                    },
                },
                params={
                    "timeout": 600,
                },
                cluster_manager_timeout="15m",
            )
        except Exception as e:
            print(f"Exception happened when recreate index:{e}")
