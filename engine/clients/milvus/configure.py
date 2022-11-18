from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusException,
    connections,
)
from pymilvus.orm import utility

from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.milvus.config import (
    MILVUS_COLLECTION_NAME,
    MILVUS_DEFAULT_ALIAS,
    MILVUS_DEFAULT_PORT,
)


class MilvusConfigurator(BaseConfigurator):
    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        # 初始化 client
        print("init milvus connect client")
        self.client = connections.connect(
            alias=MILVUS_DEFAULT_ALIAS,
            host=host,
            port=str(connection_params.pop("port", MILVUS_DEFAULT_PORT)),
            **connection_params
        )
        print("established milvus connection")

    def clean(self):
        try:
            print("drop milvus collection {}".format(MILVUS_COLLECTION_NAME))
            utility.drop_collection(MILVUS_COLLECTION_NAME, using=MILVUS_DEFAULT_ALIAS)
            utility.has_collection(MILVUS_COLLECTION_NAME, using=MILVUS_DEFAULT_ALIAS)
        except MilvusException:
            pass

    def recreate(
        self,
        distance,
        vector_size,
        collection_params,
    ):
        # 重新创建 collection 并删除所有的 index
        idx = FieldSchema(
            name="id",
            dtype=DataType.INT64,
            is_primary=True,
        )
        vector = FieldSchema(
            name="vector",
            dtype=DataType.FLOAT_VECTOR,
            dim=vector_size,
        )
        schema = CollectionSchema(
            fields=[idx, vector], description=MILVUS_COLLECTION_NAME
        )

        collection = Collection(
            name=MILVUS_COLLECTION_NAME,
            schema=schema,
            using=MILVUS_DEFAULT_ALIAS,
        )

        for index in collection.indexes:
            index.drop()

    def execution_params(self, distance, vector_size):
        """
        COSINE 距离需要做归一化
        """
        return {"normalize": distance == Distance.COSINE}
