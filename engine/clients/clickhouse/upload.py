import time
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from engine.base_client import BaseUploader
from engine.clients.clickhouse.config import *


class ClickHouseUploader(BaseUploader):
    client: Client = None
    upload_params = {}
    distance: str = None

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        if cls.client is not None:
            print(cls.client.ping())
        try:
            cls.client = clickhouse_connect.get_client(host=connection_params.get('host', '127.0.0.1'),
                                                       port=connection_params.get('port', 8123),
                                                       username=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                                                       password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD))
        except Exception as e:
            print(f"ClickHouse get exception: {e}")
        cls.upload_params = upload_params
        cls.distance = DISTANCE_MAPPING[distance]

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        if len(ids) != len(vectors):
            raise RuntimeError("clickhouse batch upload unhealthy")
        # Getting the names of structured data columns based on the first meta information.
        col_list = ['id', 'vector']
        if metadata[0] is not None:
            for col_name in list(metadata[0].keys()):
                col_list.append(str(col_name))

        res = []
        for i in range(0, len(ids)):
            temp_list = [ids[i], vectors[i]]
            if metadata[i] is not None:
                for col_name in list(metadata[i].keys()):
                    value = metadata[i][col_name]
                    # Determining if the data is a dictionary type of latitude and longitude.
                    if isinstance(value, dict) and ('lon' and 'lat') in list(value.keys()):
                        # Keep the correct order of latitude and longitude. 🙆‍
                        temp_list.append(tuple([value.get('lon'), value.get('lat')]))
                    else:
                        temp_list.append(value)
            res.append(temp_list)

        while True:
            try:
                cls.client.insert(CLICKHOUSE_DATABASE_NAME, res, column_names=col_list)
                break
            except Exception as e:
                print(f"ClickHouse upload exception {e}")
                time.sleep(3)

    @classmethod
    def post_upload(cls, distance):
        print("ClickHouse post upload: distance {}, cls.distance {}".format(distance, cls.distance))
        # Create vector index
        use_optimize = cls.upload_params.get("optimizers_config").get("optimize_final", True)
        if use_optimize:
            # prepare index create str
            index_distance = f"{distance}"
            num_trees = cls.upload_params.get('index_params', {})['num_trees']
            granularity = cls.upload_params.get('index_params', {})['granularity']

            index_create_str = f"alter table {CLICKHOUSE_DATABASE_NAME} " \
                               f"add index {CLICKHOUSE_DATABASE_NAME} " \
                               f"vector type {cls.upload_params['index_type']}('{index_distance}', '{num_trees}') GRANULARITY {granularity}"
            # optimize table
            optimize_str = f"optimize table {CLICKHOUSE_DATABASE_NAME} final"
            print(f">>> {optimize_str}")
            optimize_begin_time = time.time()
            try:
                cls.client.command(optimize_str)
            except Exception as e:
                print(f"exp: {e}, clickhouse may run by multi replicates mode..")
                optimize_str = f"optimize table replicas.{CLICKHOUSE_DATABASE_NAME} on cluster '{'{cluster}'}' final"
                print(f">>> {optimize_str}")
                cls.client.command(optimize_str)
            print("optimize table finished, time consume {}".format(time.time() - optimize_begin_time))
            print(f">>> {index_create_str}")
            cls.client.command(index_create_str)
        # waiting for vector index create finished
        # shard = cls.upload_params.get("shard", 1)
        # replicate = cls.upload_params.get("replicate", 1)
        # check_index_status = f"select status from system.vector_indices where table='{MYSCALE_DATABASE_NAME}'"
        # if shard!=1 or replicate!=1:
        #     cluster="{cluster}"
        #     check_index_status = f"select status from clusterAllReplicas('{cluster}', system.vector_indices) where table = '{MYSCALE_DATABASE_NAME}'"
        # print(f">>> {check_index_status}")
        # while True:
        #     time.sleep(5)
        #     try:
        #         res = cls.client.query(check_index_status)
        #         if len(res.result_rows) == 0:
        #             print("you haven't build index")
        #         print("{}".format(res.result_rows[0][0]), end=".", flush=True)
        #         if str(res.result_rows[0][0]) == "Built":
        #             break
        #     except Exception as e:
        #         print(e)
        #         time.sleep(3)
        #         continue
        return {}
