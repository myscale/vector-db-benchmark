import time
from multiprocessing import get_context
from typing import Iterable, List, Optional, Tuple

import tqdm

from dataset_reader.base_reader import Record
from engine.base_client.utils import iter_batches


class BaseUploader:
    client = None

    def __init__(self, host, connection_params, upload_params):
        self.host = host
        self.connection_params = connection_params
        self.upload_params = upload_params

    @classmethod
    def get_mp_start_method(cls):
        return None

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params: dict, upload_params: dict,
                    extra_columns_name: list, extra_columns_type: list):
        raise NotImplementedError()

    def upload(
            self,
            distance,
            vector_count,
            records: Iterable[Record],
            extra_columns_name: list,
            extra_columns_type: list
    ) -> dict:
        latencies = []
        start = time.perf_counter()
        parallel = self.upload_params.get("parallel", 8)
        batch_size = self.upload_params.get("batch_size", 64)
        print(f"upload parallel: {parallel}, batch size: {batch_size}, upload params dict: { self.upload_params}")

        ctx = get_context(self.get_mp_start_method())
        with ctx.Pool(
                processes=int(parallel),
                initializer=self.__class__.init_client,
                initargs=(
                        self.host,
                        distance,
                        vector_count,
                        self.connection_params,
                        self.upload_params,
                        extra_columns_name,
                        extra_columns_type,
                ),
        ) as pool:
            latencies = list(
                pool.imap(
                    self.__class__._upload_batch,
                    iter_batches(tqdm.tqdm(records), batch_size),
                )
            )
        upload_time = time.perf_counter() - start

        print("Upload time consume: {}".format(upload_time))

        wait_index_begin = time.perf_counter()
        ctx = get_context("forkserver")  # When use None, sometimes it will be blocked.
        with ctx.Pool(
                processes=1,
                initializer=self.__class__.init_client,
                initargs=(self.host,
                          distance,
                          self.connection_params,
                          self.upload_params,
                          extra_columns_name,
                          extra_columns_type,),
        ) as pool:
            pool.apply(func=self.post_upload, args=(distance,))

        total_time = time.perf_counter() - start
        post_time = time.perf_counter() - wait_index_begin

        return {
            # "post_upload": post_upload_stats,
            "post_upload": post_time,
            "upload_time": upload_time,
            "total_time": total_time,
            "latencies": latencies,
        }

    # Upload data[ids, vectors, metadata] and return time consume
    @classmethod
    def _upload_batch(
            cls, batch: Tuple[List[int], List[list], List[Optional[dict]]]
    ) -> float:
        ids, vectors, metadata = batch
        start = time.perf_counter()
        cls.upload_batch(ids, vectors, metadata)
        return time.perf_counter() - start

    @classmethod
    def post_upload(cls, distance):
        return {}

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        raise NotImplementedError()
