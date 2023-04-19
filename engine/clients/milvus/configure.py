import time
from multiprocessing import get_context

from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections, Connections,
)
from pymilvus.exceptions import DataTypeNotSupportException
from pymilvus.orm import utility

from engine.base_client import IncompatibilityError
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.milvus.config import *


class MilvusConfigurator(BaseConfigurator):
    client: Connections = None

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)

    @classmethod
    def init_client(cls, connection_params_with_default_host: dict):
        print(connection_params_with_default_host)
        cls.client = process_connection_params(connection_params_with_default_host)

    def clean(self):
        pass

    @classmethod
    def sub_recreate(cls, vector_size, extra_columns_name, extra_columns_type, ):
        try:
            print(f"dropping current milvus collection "
                  f"[{MILVUS_COLLECTION_NAME} in {utility.list_collections(using=MILVUS_DEFAULT_ALIAS)}]")
            utility.drop_collection(MILVUS_COLLECTION_NAME, timeout=10, using=MILVUS_DEFAULT_ALIAS)
            utility.has_collection(MILVUS_COLLECTION_NAME, using=MILVUS_DEFAULT_ALIAS)
        except Exception as e:
            print(e)

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
        fields = [idx, vector]

        for index in range(0, len(extra_columns_name)):
            try:
                field_schema = FieldSchema(
                    name=extra_columns_name[index],
                    dtype=convert_H52MilvusType(extra_columns_type[index]),
                    **DTYPE_EXTRAS.get(extra_columns_type[index], {}),
                )
                fields.append(field_schema)
            except DataTypeNotSupportException as e:
                print(f"🐛 {e}")
                raise IncompatibilityError(e)

        schema = CollectionSchema(fields=fields, description=MILVUS_COLLECTION_NAME)
        collection = Collection(name=MILVUS_COLLECTION_NAME, schema=schema, using=MILVUS_DEFAULT_ALIAS)
        for index in collection.indexes:
            index.drop()

        connections.disconnect(alias=MILVUS_DEFAULT_ALIAS)
        connections.remove_connection(alias=MILVUS_DEFAULT_ALIAS)

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        print(f"distance {distance}, vector_size {vector_size}, collection_params {collection_params}")
        ctx = get_context(None)
        connection_params['default_host'] = self.host
        with ctx.Pool(
                processes=1,
                initializer=self.__class__.init_client,
                initargs=(connection_params,),
        ) as pool:
            pool.apply(func=self.sub_recreate, args=(vector_size, extra_columns_name, extra_columns_type,))

        print(f"after recreate, connections in main process:[{connections.list_connections()}]")

    def execution_params(self, distance, vector_size):
        return {"normalize": distance == Distance.COSINE}
