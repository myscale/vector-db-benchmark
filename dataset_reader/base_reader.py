from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple


@dataclass
class Record:
    id: int
    vector: List[float]
    metadata: Optional[dict]


@dataclass
class Query:
    vector: List[float]
    meta_conditions: Optional[dict]
    expected_result: Optional[List[int]]
    expected_scores: Optional[List[float]] = None


# 提供两个基础读取数据集的函数
class BaseReader:
    def read_data(self) -> Iterator[Record]:
        raise NotImplementedError()

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        # query_meta: to support the datasets gist-960-euclidean-probability-l2
        raise NotImplementedError()

    def prefetch(self, vector, *items) -> List:
        raise NotImplementedError()

    def read_column_name_type(self) -> Tuple[list, list]:
        raise NotImplementedError()

