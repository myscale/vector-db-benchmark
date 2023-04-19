import time
import clickhouse_connect

from multiprocessing import get_context
from clickhouse_connect.driver.client import Client
from engine.base_client.configure import BaseConfigurator
from engine.clients.mqdb.config import *


class MqdbConfigurator(BaseConfigurator):
    client: Client = None

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)

    @classmethod
    def init_client(cls, connection_params):
        cls.client = clickhouse_connect.get_client(host=connection_params.get('host', "127.0.0.1"),
                                                   port=connection_params.get('port', 8123),
                                                   username=connection_params.get("user", MQDB_DEFAULT_USER),
                                                   password=connection_params.get("password", MQDB_DEFAULT_PASSWD))

    def clean(self):
        pass

    @classmethod
    def sub_recreate(cls, params):
        shard = params.get("shard", 1)
        replicate = params.get("replicate", 1)
        vector_size = params.get("vector_size", 960)

        # get payloads data
        extra_columns_name = params.get("extra_columns_name", [])
        extra_columns_type = params.get("extra_columns_type", [])
        structured_columns = ""
        for col_index in range(0, len(extra_columns_name)):
            structured_columns += f"{extra_columns_name[col_index]} {convert_H52ClickHouseType(extra_columns_type[col_index])}, "

        # TODO Subsequently, based on the payloads data of the HDF5 dataset, this function will continue to be improved.
        if shard == 1 and replicate == 1:
            drop_table = f"DROP TABLE IF EXISTS default.{MQDB_DATABASE_NAME} sync"
            print(drop_table)
            cls.client.command(drop_table)
            create_table = f"create table default.{MQDB_DATABASE_NAME}(id UInt32, vector Array(Float32), {structured_columns} CONSTRAINT check_length CHECK length(vector) = {vector_size}) engine MergeTree order by id"
            print(create_table)
            cls.client.command(create_table)
        else:
            cluster = "{cluster}"
            drop_table1 = f"DROP TABLE IF EXISTS replicas.{MQDB_DATABASE_NAME} on cluster '{cluster}'"
            drop_table2 = f"DROP TABLE IF EXISTS default.{MQDB_DATABASE_NAME} on cluster '{cluster}'"
            drop_database = f"DROP DATABASE IF EXISTS replicas ON CLUSTER '{cluster}'"
            print("drop table replica: " + drop_table1)
            cls.client.command(drop_table1)

            print("drop table distribute: " + drop_table2)
            cls.client.command(drop_table2)

            print("drop database replicas: " + drop_database)
            cls.client.command(drop_database)

            time.sleep(2)
            create_database = f"CREATE DATABASE IF NOT EXISTS replicas on cluster '{cluster}'"
            print("create database: " + create_database)
            cls.client.command(create_database)

            create_table = f"create table replicas.{MQDB_DATABASE_NAME} on cluster '{cluster}' (id UInt32, vector Array(Float32), {structured_columns} CONSTRAINT check_length CHECK length(vector) = {vector_size}) "
            create_table += " ENGINE = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/replicas/"
            create_table += f"{MQDB_DATABASE_NAME}"
            create_table += "', '{replica}') ORDER BY id"
            print("create replicated table: " + create_table)
            cls.client.command(create_table)

            create_distribute = f"create table default.{MQDB_DATABASE_NAME} on cluster '{cluster}' (id UInt32, vector Array(Float32), {structured_columns} CONSTRAINT check_length CHECK length(vector) = {vector_size}) engine Distributed('{cluster}', 'replicas', '{MQDB_DATABASE_NAME}', rand())"
            print("create distributed table: " + create_distribute)
            cls.client.command(create_distribute)
        print("myscale recreate finished!")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        print(f"distance {distance}, vector_size {vector_size}, collection_params {collection_params}")
        ctx = get_context(None)
        with ctx.Pool(
                processes=1,
                initializer=self.__class__.init_client,
                initargs=(connection_params,),
        ) as pool:
            pool.apply(func=self.sub_recreate,
                       args=({
                                 "shard": collection_params.get("shard", 1),
                                 "replicate": collection_params.get("replicate", 1),
                                 "vector_size": vector_size,
                                 "extra_columns_name": extra_columns_name,
                                 "extra_columns_type": extra_columns_type
                             },))

    def execution_params(self, distance, vector_size) -> dict:
        print(f"execution_params:{DISTANCE_MAPPING[distance]}")
        return {"normalize": DISTANCE_MAPPING[distance] == 'COSINE'}
