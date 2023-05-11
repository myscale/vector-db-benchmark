from engine.base_client.configure import BaseConfigurator
from engine.clients.pinecone.config import *
import pinecone


class PineconeConfigurator(BaseConfigurator):
    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        pinecone.init(api_key=connection_params.get("api-key", PINECONE_API_KEY),
                      environment=connection_params.get("environment", PINECONE_ENVIRONMENT))

    def clean(self):
        if PINECONE_INDEX_NAME in pinecone.list_indexes():
            pinecone.delete_index(PINECONE_INDEX_NAME)
        else:
            print("pinecone has already been deleted")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        print(f"distance {DISTANCE_MAPPING[distance]}, vector_size {vector_size}, collection_params {collection_params}")
        if PINECONE_INDEX_NAME in pinecone.list_indexes():
            pinecone.delete_index(PINECONE_INDEX_NAME)

        # metadata_config = {
        # "indexed": index_column_list
        # }
        pinecone.create_index(name=PINECONE_INDEX_NAME,
                              dimension=vector_size,
                              metric=DISTANCE_MAPPING[distance],
                              pod_type=collection_params["pod_type"],
                              pods=collection_params.get("pods", 1)
                              # FixMe Determine whether to add an index to the Pinecone meta field.
                              # metadata_config=metadata_config
                              )

    def execution_params(self, distance, vector_size) -> dict:
        return {"normalize": DISTANCE_MAPPING[distance] == Distance.COSINE}
