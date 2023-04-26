from weaviate import Client

from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.weaviate.config import WEAVIATE_CLASS_NAME, WEAVIATE_DEFAULT_PORT, convert_H52WeaviateType, \
    generateWeaviateClient


class WeaviateConfigurator(BaseConfigurator):
    DISTANCE_MAPPING = {
        Distance.L2: "l2-squared",
        Distance.COSINE: "cosine",
        Distance.DOT: "dot",
    }

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        self.client = generateWeaviateClient(connection_params={**connection_params}, host=host)

    def clean(self):
        classes = self.client.schema.get()
        for cl in classes["classes"]:
            if cl["class"] == WEAVIATE_CLASS_NAME:
                self.client.schema.delete_class(WEAVIATE_CLASS_NAME)

    def recreate(self, distance, vector_size, collection_params, connection_params,  extra_columns_name, extra_columns_type):
        # 记录结构化字段
        structured_list = [
            {
                "name": extra_columns_name[i],
                "dataType": [
                    convert_H52WeaviateType(extra_columns_type[i]),
                ],
                "indexInverted": True,
            }
            for i in range(0, len(extra_columns_name))
        ]
        print(f"weaviate structured columns and types: {structured_list}")
        self.client.schema.create_class(
            {
                "class": WEAVIATE_CLASS_NAME,
                "vectorizer": "none",
                "properties": structured_list,
                # Defaults to hnsw, can be omitted in schema definition since this is the only available type for now
                "vectorIndexType": "hnsw",
                "vectorIndexConfig": {
                    **{
                        "vectorCacheMaxObjects": 1000000000,
                        "distance": self.DISTANCE_MAPPING.get(distance),
                    },
                    **collection_params["vectorIndexConfig"],
                },
            }
        )
