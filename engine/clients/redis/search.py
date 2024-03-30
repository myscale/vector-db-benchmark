import time
from typing import List, Tuple

import numpy as np
import redis
from redis.commands.search.query import Query

from dataset_reader import base_reader
from engine.base_client.search import BaseSearcher
from engine.clients.redis.config import REDIS_PORT
from engine.clients.redis.parser import RedisConditionParser


class RedisSearcher(BaseSearcher):
    search_params = {}
    client = None
    parser = RedisConditionParser()

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        cls.client = redis.Redis(host=connection_params.get('host', host),
                                 port=connection_params.get('port', REDIS_PORT),
                                 db=0, socket_keepalive=True, socket_timeout=20,
                                 retry_on_timeout=True)
        cls.search_params = search_params
        cls.client.info()

    @classmethod
    def search_one(cls, vector, meta_conditions, top, schema, query: base_reader.Query) -> List[Tuple[int, float]]:
        if query.query_text is not None:
            raise NotImplementedError
        conditions = cls.parser.parse(meta_conditions)
        if conditions is None:
            prefilter_condition = "*"
            params = {}
        else:
            prefilter_condition, params = conditions
        q = (
            Query(f"({prefilter_condition})=>[KNN $K @vector $vec_param EF_RUNTIME $EF AS vector_score]")
            # Query("((@probability:[-inf 0.9] @probability:[0.65 +inf]) (@probability:[0.799529802394401 0.799529802394401] | @probability:[-inf 0.7] @probability:[0.69 +inf]))=>[KNN $K @vector $vec_param EF_RUNTIME $EF AS vector_score]")
            # Query("(@probability:[-inf 0.9] @probability:[0.65 +inf]) (@probability:[0.799529802394401 0.799529802394401] | @probability:[-inf 0.7] @probability:[0.69 +inf])=>[KNN $K @vector $vec_param EF_RUNTIME $EF AS vector_score]")
            .sort_by("vector_score", asc=False)
            .paging(0, top)
            .return_fields("vector_score")
            # .return_fields("probability")  # got return filed
            # .return_fields("vector")  # got bytes return filed
            .dialect(2)
        )
        params_dict = {
            "vec_param": np.array(vector).astype(np.float32).tobytes(),
            "K": top,
            "EF": cls.search_params["params"]["ef"],
            **params,
        }
        try:
            results = cls.client.ft().search(q, query_params=params_dict)
            ans = [(int(result.id), float(result.vector_score)) for result in results.docs]
            # ans_with_probability = [(int(result.id),float(result.vector_score), float(result.probability)) for result in results.docs]
        except Exception as e:
            raise RuntimeError(f"redis search get exception: {e}")
        return ans
