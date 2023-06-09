import time
import clickhouse_connect

from multiprocessing import get_context
from clickhouse_connect.driver.client import Client
from engine.base_client.configure import BaseConfigurator
from engine.clients.clickhouse.config import *


class ClickHouseConfigurator(BaseConfigurator):
    client: Client = None

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)

    @classmethod
    def init_client(cls, connection_params):
        cls.client = clickhouse_connect.get_client(host=connection_params.get('host', "127.0.0.1"),
                                                   port=connection_params.get('port', 8123),
                                                   username=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                                                   password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD))
        cls.client.query("SET allow_experimental_annoy_index = 1")

    def clean(self):
        pass

    @classmethod
    def sub_recreate(cls, distance, vector_size, collection_params, extra_columns_name, extra_columns_type):
        shard = collection_params.get("shard", 1)
        replicate = collection_params.get("replicate", 1)
        use_optimize = collection_params.get("optimizers_config").get("optimize_final", True)

        # get payloads data
        structured_columns = ""
        for col_index in range(0, len(extra_columns_name)):
            structured_columns += f"{extra_columns_name[col_index]} {convert_H52ClickHouseType(extra_columns_type[col_index])}, "

        # generate vector index
        index_distance = f"{distance}"
        num_trees = collection_params.get('index_params', {})['num_trees']
        granularity = collection_params.get('index_params', {})['granularity']

        # referhttps://github.com/ClickHouse/ClickHouse/blob/42914484b3cd42208d32d983d2a7d7d670541edb/tests/queries/0_stateless/02354_annoy_index.sql#L111
        # vector_index_inner = f"index {CLICKHOUSE_DATABASE_NAME} vector type {collection_params['index_type']}('{index_distance}', {num_trees}) GRANULARITY {granularity},"
        vector_index_inner = f"index {CLICKHOUSE_DATABASE_NAME} vector type {collection_params['index_type']}('{num_trees}') GRANULARITY {granularity},"
        if use_optimize:
            vector_index_inner = ""

        # TODO Subsequently, based on the payloads data of the HDF5 dataset, this function will continue to be improved.
        if shard == 1 and replicate == 1:
            drop_table = f"DROP TABLE IF EXISTS default.{CLICKHOUSE_DATABASE_NAME} sync"
            print(f">>> {drop_table}")
            cls.client.command(drop_table)
            create_table = f"create table default.{CLICKHOUSE_DATABASE_NAME}(id UInt32, vector Array(Float32), {structured_columns} {vector_index_inner} CONSTRAINT check_length CHECK length(vector) = {vector_size}) engine MergeTree order by id"
            print(f">>> {create_table}")
            cls.client.command(create_table)
        else:
            cluster = "{cluster}"
            drop_table1 = f"DROP TABLE IF EXISTS replicas.{CLICKHOUSE_DATABASE_NAME} on cluster '{cluster}' sync"
            drop_table2 = f"DROP TABLE IF EXISTS default.{CLICKHOUSE_DATABASE_NAME} on cluster '{cluster}' sync"
            drop_database = f"DROP DATABASE IF EXISTS replicas ON CLUSTER '{cluster}' sync"
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
            create_table = f"create table replicas.{CLICKHOUSE_DATABASE_NAME} on cluster '{cluster}' (id UInt32, vector Array(Float32), {structured_columns} {vector_index_inner} CONSTRAINT check_length CHECK length(vector) = {vector_size})"
            create_table += " ENGINE = ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/replicas/"
            create_table += f"{CLICKHOUSE_DATABASE_NAME}"
            create_table += "', '{replica}') ORDER BY id"
            print("create replicated table: " + create_table)
            cls.client.command(create_table)
            # Fixme Does distribute need vector_index_inner ？
            create_distribute = f"create table default.{CLICKHOUSE_DATABASE_NAME} on cluster '{cluster}' (id UInt32, vector Array(Float32), {structured_columns}) CONSTRAINT check_length CHECK length(vector) = {vector_size}) engine Distributed('{cluster}', 'replicas', '{CLICKHOUSE_DATABASE_NAME}', rand())"
            print("create distributed table: " + create_distribute)
            cls.client.command(create_distribute)
        print("clickhouse recreate finished!")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        print(f"## distance {distance}  ##")
        print(f"## vector_size {vector_size}  ##")
        print(f"## collection_params {collection_params}  ##")
        ctx = get_context(None)
        with ctx.Pool(
                processes=1,
                initializer=self.__class__.init_client,
                initargs=(connection_params,),
        ) as pool:
            pool.apply(func=self.sub_recreate,
                       args=(DISTANCE_MAPPING[distance], vector_size, collection_params, extra_columns_name, extra_columns_type,))

    def execution_params(self, distance, vector_size) -> dict:
        print(f"execution_params:{DISTANCE_MAPPING[distance]}")
        return {"normalize": DISTANCE_MAPPING[distance] == 'cosineDistance'}
