import ast
import math
from typing import Iterator, Optional, Tuple, List

import h5py
import numpy as np

from benchmark.dataset_config import DatasetConfig
from dataset_reader.base_reader import BaseReader, Query, Record
from dataset_reader.utils import convert_H52py

HDF5_BATCH_PART_SIZE = 200000


def convert_bytes_to_str(text):
    if isinstance(text, bytes):
        text = text.decode('utf-8')
    if not isinstance(text, str):
        text = str(text)
    return text


class AnnH5Reader(BaseReader):
    def __init__(self, dataset_dir, dataset_config: DatasetConfig, normalize=False):
        self.dataset_dir = dataset_dir
        self.dataset_config = dataset_config
        self.normalize = normalize
        # 初始化 query_files
        if self.dataset_config.query_files is not None:
            query_files = [
                {
                    "path": self.dataset_dir / query_file_op["path"],
                    "meta": query_file_op["meta"],
                    "score_type": query_file_op["score_type"],
                    "queries": query_file_op["queries"],
                } for
                query_file_op in self.dataset_config.query_files]
        else:
            query_files = [{"path": self.dataset_dir / self.dataset_config.path,
                            "meta": None,
                            "queries": self.dataset_config.queries}]
        self.query_files = query_files

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        for query_path in self.query_files:
            # skip mismatched query path
            if query_meta is not None and query_meta != query_path["meta"]:
                continue
            with h5py.File(query_path["path"], "r") as query_data:
                # initialize the filtering criteria
                if "filter" in list(query_data.keys()):
                    filter_conditions = query_data["filter"]
                else:
                    filter_conditions = [None] * len(query_data["test"])

                if "neighbors" in list(query_data.keys()):
                    neighbors = query_data["neighbors"]
                else:
                    neighbors = [None] * len(query_data["test"])

                if "distances" in list(query_data.keys()):
                    distances = query_data["distances"]
                else:
                    distances = [None] * len(query_data["test"])

                if query_path["score_type"] is not None:
                    st = query_path["score_type"]
                elif self.dataset_config.score_type is not None:
                    st = self.dataset_config.score_type
                else:
                    st = "default"
                score_type = [st] * len(query_data["test"])

                # for hybrid_query, currently, only support single column query.
                query_columns_in_hdf5 = query_data.attrs.get("query_columns_in_hdf5", [])
                query_columns_type = query_data.attrs.get("query_columns_type", [])
                query_columns_in_table = query_data.attrs.get("query_columns_in_table", [])

                if len(query_columns_in_hdf5) != 0:
                    query_texts = [convert_bytes_to_str(bytes_str) for bytes_str in
                                   query_data[query_columns_in_hdf5[0]]]
                else:
                    query_texts = [None] * len(query_data["test"])

                if len(query_columns_in_table) != 0:
                    query_text_columns = [query_columns_in_table[0]] * len(query_data["test"])
                else:
                    query_text_columns = [None] * len(query_data["test"])

                count = 0
                while True:
                    exit_flag = 0

                    for vector, expected_result, expected_scores, score_type, filter_condition, query_text, query_text_column in zip(
                            query_data["test"], neighbors, distances, score_type, filter_conditions,
                            query_texts, query_text_columns):
                        if self.normalize:
                            vector /= np.linalg.norm(vector)
                        count += 1
                        if count > times:
                            exit_flag = 1
                            break
                        yield Query(
                            vector=vector.tolist(),
                            meta_conditions=ast.literal_eval(
                                filter_condition.decode("ascii",
                                                        "ignore")).get("conditions",
                                                                       None) if filter_condition is not None else None,
                            expected_result=expected_result.tolist() if expected_result is not None else [],
                            expected_scores=expected_scores.tolist() if expected_scores is not None else [],
                            score_type=score_type,
                            query_text=query_text,
                            query_text_column=query_text_column,
                        )
                    if exit_flag == 1:
                        break
            break  # Avoid meaningless loops

    def read_data(self) -> Iterator[Record]:
        with h5py.File(self.dataset_dir / self.dataset_config.path, "r") as train_data:
            extra_columns = train_data.attrs.get("extra_columns",
                                                 []) if self.dataset_config.result_group == "hybrid_search" else []
            extra_columns_type = train_data.attrs.get("extra_columns_type",
                                                      []) if self.dataset_config.result_group == "hybrid_search" else []
            # get origin train datasets length
            data_size = train_data["train"].shape[0]
            # default use one batch_part
            batch_parts = 1

            if data_size > HDF5_BATCH_PART_SIZE:
                batch_parts = math.ceil(data_size / HDF5_BATCH_PART_SIZE)
            block_size = data_size // batch_parts

            global_idx = 0  # Add this line to initialize the global index.
            vectors_limit = -1
            vector_count = 0
            for i in range(batch_parts):
                print(
                    f"\nbatch_part: cur-{i + 1}/total-{batch_parts}, data_size:{data_size}, HDF5_BATCH_PART_SIZE: {HDF5_BATCH_PART_SIZE}")
                start = i * block_size
                # To handle the case of uneven data sizes, we allow the last block to contain all the remaining data.
                if i == batch_parts - 1:
                    end = data_size
                else:
                    end = start + block_size
                # avoid mess memory consume
                data_block = train_data["train"][start:end]

                if 0 < vectors_limit <= vector_count:
                    break

                for vector in data_block:
                    # normalize the vector for some distance
                    if self.normalize:
                        vector /= np.linalg.norm(vector)

                    # read payload data
                    extra_columns_data = {}
                    for col_name, col_type in zip(extra_columns, extra_columns_type):
                        if col_type == "text" or "string":
                            extra_columns_data[col_name] = convert_bytes_to_str(train_data[col_name][global_idx])
                        else:
                            extra_columns_data[col_name] = convert_H52py(col_type)(train_data[col_name][global_idx])

                    if 0 < vectors_limit <= vector_count:
                        break
                    yield Record(id=global_idx,
                                 vector=vector.tolist()
                                 if (len(vector) == self.dataset_config.vector_size)
                                 else np.random.uniform(0, 1, self.dataset_config.vector_size).tolist(),
                                 metadata=None if len(extra_columns_data.keys()) == 0 else extra_columns_data)
                    global_idx += 1
                    vector_count += 1

    def read_column_name_type(self) -> Tuple[list, list]:
        """ Get the payloads data name and type """
        with h5py.File(self.dataset_dir / self.dataset_config.path, "r") as train_data:
            extra_columns = train_data.attrs.get("extra_columns", [])
            extra_columns_type = train_data.attrs.get("extra_columns_type", [])
            return extra_columns, extra_columns_type

    def get_query_files(self) -> List[dict]:
        return self.query_files
