import time
from typing import List, Optional, Tuple
import clickhouse_connect
from clickhouse_connect.driver.client import Client

from dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from engine.clients.clickhouse.config import *
from engine.clients.clickhouse.parser import ClickHouseConditionParser


class ClickHouseSearcher(BaseSearcher):
    search_params = {}
    client: Client = None
    distance: str = None
    host: str = None
    parser = ClickHouseConditionParser()

    @classmethod
    def init_client(
            cls, host: str, distance, connection_params: dict, search_params: dict
    ):
        cls.client = clickhouse_connect.get_client(host=connection_params.get('host', '127.0.0.1'),
                                                   port=connection_params.get('port', 8123),
                                                   username=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                                                   password=connection_params.get("password",
                                                                                  CLICKHOUSE_DEFAULT_PASSWD))
        cls.host = host
        cls.distance = DISTANCE_MAPPING[distance]
        cls.search_params = search_params

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema, query: Query) -> List[Tuple[int, float]]:
        if query.query_text is not None:
            raise NotImplementedError
        # ClickHouse annoy index only support search parameter: annoy_index_search_k_nodes
        search_params_dict = cls.search_params["params"]

        par = ""
        for key in search_params_dict.keys():
            par += ", {}={}".format(key, search_params_dict[key])
        if par != "":
            par = par[2:]

        search_str = f"SELECT id, {cls.distance}(vector, {vector}) as score FROM {CLICKHOUSE_DATABASE_NAME}"

        if meta_conditions is not None:
            search_str += f" where {cls.parser.parse(meta_conditions=meta_conditions)}"

        # Code: 115. DB::Exception: Unknown setting annoy_index_search_k_nodes. (UNKNOWN_SETTING) (version 23.4.2.11 (official build))
        # search_str += f" order by {cls.distance}(vector, {vector}) limit {top} SETTINGS {par}"
        # search_str += f" order by {cls.distance}(vector, {vector}) ASC limit {top}"
        if cls.distance == "L2Distance":
            search_str += f" order by score ASC limit {top}"
        else:
            search_str += f" order by score ASC limit {top}"

        res_list = []
        while True:
            try:
                res = cls.client.query(search_str)
                break
            except Exception as e:
                raise RuntimeError(e)

        for res_id_dis in res.result_rows:
            res_list.append((res_id_dis[0], 0.0))

        return res_list
