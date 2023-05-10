import time
from typing import List, Optional, Tuple
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from engine.base_client import BaseSearcher
from engine.clients.myscale.config import *
from engine.clients.myscale.parser import MyScaleConditionParser


class MyScaleSearcher(BaseSearcher):
    search_params = {}
    client: Client = None
    distance: str = None
    host: str = None
    parser = MyScaleConditionParser()

    @classmethod
    def init_client(
            cls, host: str, distance, connection_params: dict, search_params: dict
    ):
        cls.client = clickhouse_connect.get_client(host=connection_params.get('host', '127.0.0.1'),
                                                   port=connection_params.get('port', 8123),
                                                   username=connection_params.get("user", MYSCALE_DEFAULT_USER),
                                                   password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD))
        cls.host = host
        cls.distance = DISTANCE_MAPPING[distance]
        cls.search_params = search_params

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema) -> List[Tuple[int, float]]:
        search_params_dict = cls.search_params["params"]
        par = ""
        for key in search_params_dict.keys():
            par += ", \'{}={}\'".format(key, search_params_dict[key])
        if par != "":
            par = par[2:]

        search_str = f"SELECT id, distance({par})(vector, {vector}) as dis FROM {MYSCALE_DATABASE_NAME}"

        if meta_conditions is not None:
            search_str += f" prewhere {cls.parser.parse(meta_conditions=meta_conditions)}"

        if cls.distance == "IP":
            search_str += f" order by dis DESC limit {top}"
        else:
            search_str += f" order by dis limit {top}"

        res_list = []
        while True:
            try:
                res = cls.client.query(search_str)
                break
            except Exception as e:
                raise RuntimeError(e)

        for res_id_dis in res.result_rows:
            res_list.append((res_id_dis[0], res_id_dis[1]))

        return res_list
