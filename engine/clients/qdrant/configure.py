from qdrant_client import QdrantClient
from qdrant_client.http import models as rest

from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.qdrant.config import QDRANT_COLLECTION_NAME, convert_H52QdrantType, process_connection_params


# generate sq index config
def generate_scalar_quantization(quantization_config: dict):
    _type = rest.ScalarType.INT8
    if quantization_config.get("type", "int8") == "int8":
        _type = rest.ScalarType.INT8
    else:
        # FixMe Support more type
        raise RuntimeError("quantization_config needs fix")
    return rest.ScalarQuantization(
        scalar=rest.ScalarQuantizationConfig(
            type=_type,
            quantile=0.99,
            always_ram=True,
        )
    )


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
        print(f"## distance {distance} ##")
        print(f"## vector_size {vector_size} ##")
        print(f"## collection_params.keys {collection_params.keys()} ##")
        print(f"## collection_params {collection_params} ##")
        print(f"## collection_params {self.collection_params} ##")

        if "hnsw_config" in list(collection_params.keys()):
            res = self.client.recreate_collection(
                collection_name=QDRANT_COLLECTION_NAME,
                vectors_config=rest.VectorParams(size=vector_size, distance=self.DISTANCE_MAPPING.get(distance)),
                **self.collection_params
            )
            print(f"recreate collection from hnsw_config finished: {res}")
        elif "quantization_config" in list(collection_params.keys()):
            quantization_config = self.collection_params.pop("quantization_config", {})
            print(generate_scalar_quantization(quantization_config=quantization_config))
            print(self.collection_params)
            res = self.client.recreate_collection(
                collection_name=QDRANT_COLLECTION_NAME,
                vectors_config=rest.VectorParams(size=vector_size, distance=self.DISTANCE_MAPPING.get(distance)),
                quantization_config=generate_scalar_quantization(quantization_config=quantization_config),
                **self.collection_params
            )
            print(f"recreate collection from quantization_config finished: {res}")

        for index in range(0, len(extra_columns_name)):
            self.client.create_payload_index(
                collection_name=QDRANT_COLLECTION_NAME,
                field_name=extra_columns_name[index],
                field_schema=convert_H52QdrantType(extra_columns_type[index]),
            )
