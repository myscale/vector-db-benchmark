from myscaledb import Client

from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.mqdb.config import *


class MqdbConfigurator(BaseConfigurator):
    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        # 初始化 client
        print("init mqdb connection")
        self.client = Client(url='http://{}:{}/'.format(host, MQDB_DEFAULT_PORT),
                             user=MQDB_DEFAULT_USER,
                             password=MQDB_DEFAULT_PASSWD)
        print("mqdb connection established")

    def clean(self):
        if self.client.is_alive():
            self.client.close()

    def recreate(self, distance, vector_size, collection_params):
        print("distance {}, vector_size {}, collection_params {}".format(distance, vector_size, collection_params))
        # 删除之前的数据库表
        drop_table = "DROP TABLE IF EXISTS {}".format(MQDB_DATABASE_NAME)
        self.client.execute(drop_table)
        # 重新创建数据库表
        create_table = "create table default.{}(id Int32, vector FixedArray(Float32, {})) engine MergeTree order by id".format(
            MQDB_DATABASE_NAME, vector_size)
        self.client.execute(create_table)

    def execution_params(self, distance, vector_size) -> dict:
        """COSINE 需要做归一化"""
        # TODO 确认此处距离映射没问题
        return {"normalize": DISTANCE_MAPPING[distance] == Distance.COSINE}
