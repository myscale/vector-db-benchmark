from typing import List, Optional, Tuple

from engine.base_client import BaseSearcher
from myscaledb.client import Client
import multiprocessing as mp

from engine.clients.mqdb.config import *


class MqdbSearcher(BaseSearcher):
    search_params = {}
    client: Client = None

    @classmethod
    def init_client(
            cls, host: str, distance, connection_params: dict, search_params: dict
    ):
        cls.client: Client = Client(url='http://{}:{}/'.format(host, MQDB_DEFAULT_PORT),
                                    user=MQDB_DEFAULT_USER,
                                    password=MQDB_DEFAULT_PASSWD)
        cls.distance = DISTANCE_MAPPING[distance]
        cls.search_params = search_params

    # forkserver 和 spawn 是 python 启动进程的不同方式，优先使用 forkserver，spawn 比较慢
    @classmethod
    def get_mp_start_method(cls):
        return "forkserver" if "forkserver" in mp.get_all_start_methods() else "spawn"

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        # param = {"metric_type": cls.distance, "params": cls.search_params["params"]}

        search_params_dict = cls.search_params["params"]
        par = "\'topK={}\'".format(top)
        for key in search_params_dict.keys():
            par += ", \'{}={}\'".format(key, search_params_dict[key])

        search_str = "SELECT id, distance({})(vector, {}) as dis FROM {}".format(par, vector, MQDB_DATABASE_NAME)
        # print(search_str)
        res = cls.client.fetch(search_str)
        res_list = []
        # fo=open("record_before_search.log",'a')
        # fo.write("\n{}\n".format(vector))
        for res_id_dis in res:
            res_list.append((int(dict(res_id_dis).get('id')), float(dict(res_id_dis).get('dis'))))
            # fo.write("id:{}--distance{}\n".format(
            #     int(dict(res_id_dis).get('id')),
            #     float(dict(res_id_dis).get('dis'))
            # ))
        # print("res_list size is {}".format(len(res_list)))
        # fo.close()
        return res_list
