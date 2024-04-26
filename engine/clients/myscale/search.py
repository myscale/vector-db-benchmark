import re
import time
from typing import List, Optional, Tuple
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from ranx import Run, fuse
from ranx.normalization import rank_norm, min_max_norm

from dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from engine.clients.myscale.config import *
from engine.clients.myscale.min_max_inverted import min_max_norm_inverted
from engine.clients.myscale.parser import MyScaleConditionParser


def remove_punctuation(input_string):
    translator = str.maketrans('', '', string.punctuation)
    return input_string.translate(translator)


class MyScaleSearcher(BaseSearcher):
    search_params = {}
    client: Client = None
    distance: str = None
    host: str = None
    parser = MyScaleConditionParser()
    connection_params: dict = {}

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
        cls.connection_params = connection_params

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
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

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema, query: Query) -> List[
        Tuple[int, float]]:
        if query.query_text is not None and cls.connection_params.get("tantivy_idx_cols", None) is not None:
            if cls.search_params["params"].get("only_text_search", False):
                return cls.text_search(top, query)
            else:
                # return cls.hybrid_search(top, query)
                return cls.hybrid_search_with_ranx(top, query)
        else:
            return cls.vector_search(vector, meta_conditions, top)

    @classmethod
    def hybrid_search(cls, top: Optional[int], query: Query) -> List[Tuple[int, float]]:
        search_params_dict = cls.search_params["params"]
        dense_alpha = search_params_dict.get("dense_alpha", 1)
        fusion_type = search_params_dict.get("fusion_type", "RRF")
        fusion_weight = search_params_dict.get("fusion_weight", 0.5)
        fusion_k = search_params_dict.get("fusion_k", 60)

        search_str = f"""
        SELECT
            id,
            HybridSearch('dense_alpha={dense_alpha}', 'fusion_type={fusion_type}', 'fusion_weight={fusion_weight}', 'fusion_k={fusion_k}')(vector, {query.query_text_column}, %s, %s) AS dis
        FROM {MYSCALE_DATABASE_NAME}
        ORDER BY dis DESC
        LIMIT {top}
        """
        # print(search_str)
        params = [query.vector, remove_punctuation(query.query_text)]
        res_list = []
        while True:
            try:
                res = cls.client.query(search_str, params)
                break
            except Exception as e:
                print(search_str)
                raise RuntimeError(e)

        for res_id_dis in res.result_rows:
            res_list.append((res_id_dis[0], res_id_dis[1]))

        return res_list

    @classmethod
    def hybrid_search_with_ranx(cls, top: Optional[int], query: Query) -> List[Tuple[int, float]]:
        vector_search_results: List[Tuple[int, float]] = cls.vector_search(
            vector=query.vector,
            meta_conditions=query.meta_conditions,
            top=top*10
        )
        text_search_results: List[Tuple[int, float]] = cls.text_search(
            top=top*10,
            query=query
        )

        # 检查搜索结果是否为空
        if not vector_search_results and not text_search_results:
            print("both vector and text search are empty")
            return []
        elif not vector_search_results:
            print("vector search is empty")
            return text_search_results  # 如果vector search结果为空,返回text search结果
        elif not text_search_results:
            print("text search is empty")
            return vector_search_results  # 如果text search结果为空,返回vector search结果

        text_dict = {"query-0": {str(row[0]): float(row[1]) for row in text_search_results}}
        max_value = max(float(row[3]) for row in vector_search_results)
        vector_dict = {"query-0": {str(row[0]): max_value - float(row[3]) for row in vector_search_results}}

        vector_run = min_max_norm(Run(vector_dict, name="vector"))
        bm25_run = min_max_norm(Run(text_dict, name="text"))

        combined_run = fuse(
            runs=[vector_run, bm25_run],
            method="rrf",
            params={'k': 10}
        )
        fused_results = []
        for doc_id, score in combined_run.get_doc_ids_and_scores()[0].items():
            fused_results.append((int(doc_id), score))

        return fused_results[:top]

    @classmethod
    def text_search(cls, top: Optional[int], query: Query) -> List[Tuple[int, float]]:
        # TODO: handle filter condition
        search_str = f"""
        SELECT
            id,
            TextSearch({query.query_text_column},'{remove_punctuation(query.query_text)}') AS dis
        FROM {MYSCALE_DATABASE_NAME}
        ORDER BY dis DESC
        LIMIT {top}
        """
        res_list = []
        while True:
            try:
                res = cls.client.query(search_str)
                break
            except Exception as e:
                print(search_str)
                raise RuntimeError(e)

        for res_id_dis in res.result_rows:
            res_list.append((res_id_dis[0], res_id_dis[1]))

        return res_list
