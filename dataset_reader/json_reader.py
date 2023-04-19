import json
import os
import sys
from pathlib import Path
from typing import Iterator, List, Optional, Tuple
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from benchmark.dataset_config import DatasetConfig
from dataset_reader.base_reader import BaseReader, Query, Record

VECTORS_FILE = "vectors.jsonl"
PAYLOADS_FILE = "payloads.jsonl"
QUERIES_FILE = "queries.jsonl"
NEIGHBOURS_FILE = "neighbours.jsonl"


class JSONReader(BaseReader):
    def __init__(self, path: Path, dataset_config: DatasetConfig, normalize=False):
        self.path = (path / dataset_config.path)
        print(self.path)
        self.dataset_config = dataset_config
        self.normalize = normalize

    def read_payloads(self) -> Iterator[dict]:
        if not (self.path / PAYLOADS_FILE).exists():
            while True:
                yield {}
        valid_columns = list(self.dataset_config.schema.keys())
        with open(self.path / PAYLOADS_FILE, "r") as json_fp:
            for json_line in json_fp:
                line = json.loads(json_line)
                # filter out non-stored fields
                re_line = dict([(col, line.get(col)) for col in valid_columns])
                yield re_line

    def read_vectors(self) -> Iterator[List[float]]:
        with open(self.path / VECTORS_FILE, "r") as json_fp:
            for json_line in json_fp:
                vector = json.loads(json_line)
                if self.normalize:
                    vector = vector / np.linalg.norm(vector)
                yield vector

    def read_neighbours(self) -> Iterator[Optional[List[int]]]:
        if not (self.path / NEIGHBOURS_FILE).exists():
            while True:
                yield None

        with open(self.path / NEIGHBOURS_FILE, "r") as json_fp:
            for json_line in json_fp:
                line = json.loads(json_line)
                yield line

    def read_query_vectors(self) -> Iterator[List[float]]:
        with open(self.path / QUERIES_FILE, "r") as json_fp:
            for json_line in json_fp:
                vector = json.loads(json_line)
                if self.normalize:
                    vector /= np.linalg.norm(vector)
                yield vector

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        count = 0
        while True:
            exit_flag = 0
            for idx, (vector, neighbours) in enumerate(
                    zip(self.read_query_vectors(), self.read_neighbours())
            ):
                count += 1
                if count > times:
                    exit_flag = 1
                    break
                yield Query(vector=vector, meta_conditions=None, expected_result=neighbours)
            if exit_flag == 1:
                break

    def read_data(self) -> Iterator[Record]:
        """
        Returns:Record(id=0, vector=[0.29879.....378], metadata={})
        """
        for idx, (vector, payload) in enumerate(
                zip(self.read_vectors(), self.read_payloads())
        ):
            yield Record(id=idx, vector=vector, metadata=payload)

    def read_column_name_type(self) -> Tuple[list, list]:
        """ Get the payloads data name and type """
        extra_columns = []
        extra_columns_type = []
        for extra_column in list(self.dataset_config.schema.keys()):
            extra_columns.append(extra_column)
            extra_columns_type.append(self.dataset_config.schema.get(extra_column))

        return extra_columns, extra_columns_type
