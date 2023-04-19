import json
import os
import sys
from typing import Iterator, List, Optional
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from dataset_reader.base_reader import Query
from dataset_reader.json_reader import JSONReader


class AnnCompoundReader(JSONReader):
    """
    A reader created specifically to read the format used in
    https://github.com/qdrant/ann-filtering-benchmark-datasets, in which vectors
    and their metadata are stored in separate files.
    """

    VECTORS_FILE = "vectors.npy"
    QUERIES_FILE = "tests.jsonl"

    def read_vectors(self) -> Iterator[List[float]]:
        vectors = np.load(str(self.path / self.VECTORS_FILE))
        for vector in vectors:
            if self.normalize:
                vector = vector / np.linalg.norm(vector)
            yield vector.tolist()

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        if query_meta is not None:
            raise RuntimeError("Not support multi query meta in jsonl datasets 😭...")
        count = 0
        while True:
            exit_flag = 0
            with open(self.path / self.QUERIES_FILE) as payloads_fp:
                for idx, row in enumerate(payloads_fp):
                    row_json = json.loads(row)
                    vector = np.array(row_json["query"])
                    if self.normalize:
                        vector /= np.linalg.norm(vector)
                    count += 1
                    if count > times:
                        exit_flag = 1
                        break
                    yield Query(
                        vector=vector.tolist(),
                        meta_conditions=row_json["conditions"],
                        expected_result=row_json["closest_ids"],
                        expected_scores=row_json["closest_scores"],
                    )
            if exit_flag == 1:
                break


# if __name__ == "__main__":
#     from benchmark.dataset_config import DatasetConfig
#     from pathlib import Path
#
#     my_test = AnnCompoundReader(
#         path=Path("/Users/mochix/workspace_mqdb_bench/vector-db-benchmark/datasets"),
#         dataset_config=DatasetConfig(name="arxiv-titles-384-angular-filters",
#                                      result_group="single-search",
#                                      vector_size=384,
#                                      distance='cosine',
#                                      type='tar',
#                                      path="arxiv-titles-384-angular/arxiv",
#                                      schema={
#                                          "update_date_ts": "int",
#                                          "label": "keyword"
#                                      }),
#         normalize=True
#     )
#     for i in my_test.read_data():
#         print(i)
