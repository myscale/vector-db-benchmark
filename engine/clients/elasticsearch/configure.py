from elasticsearch import Elasticsearch, NotFoundError

from engine.base_client import IncompatibilityError
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.elasticsearch.config import (
    ELASTIC_INDEX,
    ELASTIC_PASSWORD,
    ELASTIC_PORT,
    ELASTIC_USER, H5_COLUMN_TYPES_MAPPING, process_connection_params, DISTANCE_MAPPING,
)


class ElasticConfigurator(BaseConfigurator):

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        user, password, init_params = process_connection_params(connection_params, host)
        self.client = Elasticsearch(basic_auth=(user, password), **init_params)

    def clean(self):
        try:
            self.client.indices.delete(index=ELASTIC_INDEX, timeout="5m", master_timeout="5m")
        except NotFoundError:
            pass

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):

        self.client.indices.create(
            index=ELASTIC_INDEX,
            mappings={
                "properties": {
                    "vector": {
                        "type": "dense_vector",
                        "dims": vector_size,
                        "index": not collection_params.get("only_text_search", False),
                        **(
                            {
                                "similarity": DISTANCE_MAPPING[distance],
                                "index_options": {
                                    **{
                                        "type": "hnsw",
                                        "m": 16,
                                        "ef_construction": 100,
                                    },
                                    **collection_params.get("index_options"),
                                },
                            }
                            if not collection_params.get("only_text_search", False)
                            else {}
                        ),
                    },
                    **{
                        extra_columns_name[i]: {
                            # The mapping is used only for several types, as some of them
                            # overlap with the ones used internally.
                            "type": H5_COLUMN_TYPES_MAPPING.get(extra_columns_type[i], extra_columns_type[i]),
                            "index": True,
                            **(
                                {
                                    "similarity": "BM25"
                                }
                                if H5_COLUMN_TYPES_MAPPING.get(extra_columns_type[i], extra_columns_type[i]) == "text"
                                else {}
                            ),
                        }
                        for i in range(0, len(extra_columns_name))
                    }
                }
            },
        )

    def execution_params(self, distance, vector_size) -> dict:
        print(f"execution_params:{DISTANCE_MAPPING[distance]}")
        return {"normalize": DISTANCE_MAPPING[distance] == 'dot_product'}
