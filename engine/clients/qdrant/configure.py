from qdrant_client import QdrantClient
from qdrant_client.http import models as rest

from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.qdrant.config import QDRANT_COLLECTION_NAME, convert_H52QdrantType, process_connection_params


class QdrantConfigurator(BaseConfigurator):
    DISTANCE_MAPPING = {
        Distance.L2: rest.Distance.EUCLID,
        Distance.COSINE: rest.Distance.COSINE,
        Distance.DOT: rest.Distance.DOT,
    }

    def __init__(self, host, collection_params: dict, connection_params: dict):
        # fixme 修改 host 为 connection_params 中的 host
        super().__init__(host, collection_params, connection_params)
        connection_params['host'] = host if connection_params.get('host', None) is None else connection_params['host']
        connection_params = process_connection_params(connection_params)
        self.client = QdrantClient(**connection_params)

    def clean(self):
        self.client.delete_collection(collection_name=QDRANT_COLLECTION_NAME)

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        print("distance {}, vector_size {}, collection_params {}".format(distance, vector_size, collection_params))
        self.client.recreate_collection(
            collection_name=QDRANT_COLLECTION_NAME,
            vectors_config=rest.VectorParams(size=vector_size, distance=self.DISTANCE_MAPPING.get(distance)),
            **self.collection_params
        )
        for index in range(0, len(extra_columns_name)):
            self.client.create_payload_index(
                collection_name=QDRANT_COLLECTION_NAME,
                field_name=extra_columns_name[index],
                field_schema=convert_H52QdrantType(extra_columns_type[index]),
            )
