import time
from typing import List, Optional

import numpy as np
import redis

from engine.base_client.upload import BaseUploader
from engine.clients.redis.config import REDIS_PORT
from engine.clients.redis.helper import convert_to_redis_coords


class RedisUploader(BaseUploader):
    client = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        cls.client = redis.Redis(host=connection_params.get('host', host),
                                 port=connection_params.get('port', REDIS_PORT),
                                 db=0)
        cls.upload_params = upload_params

    @classmethod
    def upload_batch(
        cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):

        p = cls.client.pipeline(transaction=False)
        for i in range(len(ids)):
            idx = ids[i]
            vec = vectors[i]
            meta = metadata[i] if metadata is not None and metadata[i] is not None else {}
            payload = {
                k: v
                for k, v in meta.items()
                if v is not None and not isinstance(v, dict)
            }
            # Redis treats geopoints differently and requires putting them as
            # a comma-separated string with lat and lon coordinates
            geopoints = {
                k: ",".join(map(str, convert_to_redis_coords(v["lon"], v["lat"])))
                for k, v in meta.items()
                if isinstance(v, dict)
            }
            cls.client.hset(
                str(idx),
                mapping={
                    "vector": np.array(vec).astype(np.float32).tobytes(),
                    **payload,
                    **geopoints,
                },
            )
        while True:
            try:
                p.execute()
                break
            except Exception as e:
                print(f"redis upload exception: {e}")

    @classmethod
    def post_upload(cls, _distance):
        return {}
