import time
from multiprocessing import get_context

from pyproximase import Client, IndexColumnParam, DataType, IndexType, CollectionConfig, ForwardColumnParam

from engine.base_client import BaseConfigurator
from engine.clients.proxima.config import *


class ProximaConfigurator(BaseConfigurator):
    client = None

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)

    @classmethod
    def init_client(cls, host, connection_params):
        host = connection_params.get("host", host)
        grpc_port = connection_params.get("grpc_port", PROXIMA_GRPC_PORT)
        cls.client = Client(host, grpc_port)

    def clean(self):
        print("TODO restart proxima server")

    @classmethod
    def sub_recreate(cls, vector_size, collection_params, extra_columns_name, extra_columns_type):
        index_type_str = collection_params.get('index_config', {}).get('index_type', '')
        index_type = IndexType.PROXIMA_GRAPH_INDEX
        if index_type_str == 'PROXIMA_GRAPH_INDEX':
            index_type = IndexType.PROXIMA_GRAPH_INDEX
        elif index_type_str == 'PROXIMA_QC_INDEX':
            index_type = IndexType.PROXIMA_QC_INDEX
        elif index_type_str == 'INVERT_INDEX':
            index_type = IndexType.INVERT_INDEX

        index_column = IndexColumnParam(name='vector',
                                        dimension=vector_size,
                                        data_type=DataType.VECTOR_FP32,
                                        index_type=index_type)
        forward_column_params = []
        for index in range(0, len(extra_columns_name)):
            forward_op = ForwardColumnParam(column_name=extra_columns_name[index],
                                            data_type=convert_H52ProximaType(extra_columns_type[index]))
            forward_column_params.append(forward_op)

        print(f"proxima forward_column_params is {forward_column_params}")
        # 配置 collection
        collection_config = CollectionConfig(collection_name=PROXIMA_COLLECTION_NAME,
                                             index_column_params=[index_column],
                                             forward_column_params=forward_column_params)
        drop_status = str(cls.client.drop_collection(PROXIMA_COLLECTION_NAME))
        while True:
            if drop_status == 'success' or (PROXIMA_COLLECTION_NAME not in cls.client.list_collections()):
                break
            else:
                time.sleep(2)
                print(f"proxima waiting drop finished -- drop status: {drop_status}")
                drop_status = str(cls.client.drop_collection(PROXIMA_COLLECTION_NAME))

        create_status = str(cls.client.create_collection(collection_config))
        while True:
            if create_status == "success" or (PROXIMA_COLLECTION_NAME in cls.client.list_collections()):
                break
            else:
                time.sleep(2)
                print(f"create failed.. current create status: {create_status}")
                create_status = str(cls.client.create_collection(collection_config))

        status, collection_info = cls.client.describe_collection(PROXIMA_COLLECTION_NAME)
        print(f"proxima recreate finished, collection status: {collection_info}, collection info: {status}")
        cls.client.close()

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        print("trying recreate proxima collections in sub process")
        ctx = get_context(None)
        with ctx.Pool(
                processes=1,
                initializer=self.__class__.init_client,
                initargs=(self.host, connection_params,),
        ) as pool:
            pool.apply(
                func=self.sub_recreate,
                args=(vector_size, collection_params, extra_columns_name, extra_columns_type,)
            )

    def execution_params(self, distance, vector_size) -> dict:
        return {"normalize": DISTANCE_MAPPING[distance] == Distance.COSINE}
