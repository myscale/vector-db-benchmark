import datetime
import math
import string
import random
import time
from typing import List, Optional

from myscaledb import Client
import multiprocessing as mp

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
    def get_mp_start_method(cls):
        return "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        print("mqdb upload init_client")
        cls.client: Client = Client(url='http://{}:{}/'.format(host, MQDB_DEFAULT_PORT),
                                    user=MQDB_DEFAULT_USER,
                                    password=MQDB_DEFAULT_PASSWD)
        cls.upload_params = upload_params
        # 映射符合 MQDB 的距离参数
        cls.distance = DISTANCE_MAPPING[distance]

    @classmethod
    def upload_batch(
            cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]
    ):
        if len(ids) != len(vectors):
            print("batch upload unhealthy")

        # TODO 感觉此处可以换成元组
        values_str = ""
        for i in range(0, min(len(ids), len(vectors))):
            values_str += "({}, {})".format(ids[i], vectors[i]) if values_str == "" else ", ({}, {})".format(
                ids[i],
                vectors[i]
            )
        insert_str = "insert into {} (*) values {}".format(MQDB_DATABASE_NAME, values_str)
        cls.client.execute(insert_str)

    @classmethod
    def post_upload(cls, distance):
        print("mqdb post upload: distance {}, cls.distance {}".format(distance,cls.distance))
        # 创建索引命令
        index_parameter_str = "\'metric_type={}\'".format(cls.distance)
        print(index_parameter_str)
        for key in cls.upload_params.get("index_params", {}).keys():
            index_parameter_str += ("'{} = {}'" if index_parameter_str == "" else ",'{}={}'").format(
                key, cls.upload_params.get("index_params", {})[key])

        index_create_str = "alter table {} add vector index {} vector type {}({})".format(
            MQDB_DATABASE_NAME,
            "{}_{}".format(MQDB_DATABASE_NAME, get_random_string(4)),
            cls.upload_params["index_type"],
            index_parameter_str
            # "\'metric_type=L2\'"
        )
        # optimize table
        optimize_str = "optimize table {} final".format(MQDB_DATABASE_NAME)
        print("trying to optimize table final: {}".format(optimize_str))
        optimize_begin_time=time.time()
        cls.client.execute(optimize_str)
        print("optimize table finished, time consume {}".format(time.time()-optimize_begin_time))
        # - - - - - -
        print("trying to create vector index: {}".format(index_create_str))
        cls.client.execute(index_create_str)
        # 阻塞等待索引创建完成
        check_index_status = "select status from system.vector_indices where table='{}'".format(MQDB_DATABASE_NAME)
        print("trying to check vector index build status: {}".format(check_index_status))
        while True:
            time.sleep(3)
            res = cls.client.fetch(check_index_status)
            if len(res) == 0:
                print("you haven't build index")
            print("{}".format(res[0].get("status")), end=".", flush=True)
            if str(res[0].get("status")) == "Built":
                break
        return {}
