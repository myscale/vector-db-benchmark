import random
import string
import time
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from engine.base_client import BaseUploader
from engine.clients.mqdb.config import *


def get_random_string(length: int):
    random_list = []
    for i in range(length):
        random_list.append(random.choice(string.ascii_uppercase + string.digits))
    return ''.join(random_list)


class MqdbUploader(BaseUploader):
    client: Client = None
    upload_params = {}
    distance: str = None

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        if cls.client is not None:
            print(cls.client.ping())
        try:
            cls.client = clickhouse_connect.get_client(host=connection_params.get('host', '127.0.0.1'),
                                                       port=connection_params.get('port', 8123),
                                                       username=connection_params.get("user", MQDB_DEFAULT_USER),
                                                       password=connection_params.get("password", MQDB_DEFAULT_PASSWD))
        except Exception as e:
            print(f"MyScale get exception: {e}")
        cls.upload_params = upload_params
        cls.distance = DISTANCE_MAPPING[distance]

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        if len(ids) != len(vectors):
            raise RuntimeError("mqdb batch upload unhealthy")
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
                cls.client.insert(MQDB_DATABASE_NAME, res, column_names=col_list)
                break
            except Exception as e:
                print(f"MyScale upload exception {e}")
                time.sleep(3)

    @classmethod
    def post_upload(cls, distance):
        print("MyScale post upload: distance {}, cls.distance {}".format(distance, cls.distance))
        # Create vector index
        index_parameter_str = "\'metric_type={}\'".format('IP' if cls.distance == 'COSINE' else cls.distance)
        for key in cls.upload_params.get("index_params", {}).keys():
            index_parameter_str += ("'{} = {}'" if index_parameter_str == "" else ",'{}={}'").format(
                key, cls.upload_params.get("index_params", {})[key])

        index_create_str = "alter table {} add vector index {} vector type {}({})".format(
            MQDB_DATABASE_NAME,
            "{}_{}".format(MQDB_DATABASE_NAME, get_random_string(4)),
            cls.upload_params["index_type"],
            index_parameter_str
        )
        print(index_create_str)
        # optimize table
        optimize_str = "optimize table {} final".format(MQDB_DATABASE_NAME)
        print("trying to optimize table final: {}".format(optimize_str))
        optimize_begin_time = time.time()
        try:
            cls.client.command(optimize_str)
        except Exception as e:
            print(e)
            print("mqdb may run by multi replicates mode..")
            optimize_str = f"optimize table replicas.{MQDB_DATABASE_NAME} on cluster '{'{cluster}'}' final"
            cls.client.command(optimize_str)

        print("optimize table finished, time consume {}".format(time.time() - optimize_begin_time))
        print("trying to create vector index: {}".format(index_create_str))
        cls.client.command(index_create_str)
        # waiting for vector index create finished
        check_index_status = "select status from system.vector_indices where table='{}'".format(MQDB_DATABASE_NAME)
        print("trying to check vector index build status: {}".format(check_index_status))
        while True:
            time.sleep(5)
            try:
                res = cls.client.query(check_index_status)
                if len(res.result_rows) == 0:
                    print("you haven't build index")
                print("{}".format(res.result_rows[0][0]), end=".", flush=True)
                if str(res.result_rows[0][0]) == "Built":
                    break
            except Exception as e:
                print(e)
                time.sleep(3)
                continue
        return {}
