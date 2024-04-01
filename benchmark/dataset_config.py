from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

{
    "name": "ms-macro-768-5m-cosine-dev-query.hdf5",
    "meta": {"type": "dev"},
    "queries": 6980,
    "score_type": "mrr",
    "path": "downloaded/ms-macro-768-5m-cosine/ms-macro-768-5m-cosine-dev-query.hdf5",
    "link": "https://mqdb-release-1253802058.cos.ap-beijing.myqcloud.com/datasets/ms-macro-768-5m-cosine-dev-query.hdf5"
}


@dataclass
class QueryConfig:
    name: str
    meta: dict
    queries: int
    path: str
    link: str
    score_type: Optional[str] = "default"


@dataclass
class DatasetConfig:
    name: str  # dataset name
    result_group: str  # single_search or hybrid_search, if hybrid_search, extra_columns will be stored into table.
    vector_size: int  # vector size
    vector_count: int  # vector count
    queries: int  # number of vectors used for vector search
    distance: str  # cosine、l2
    type: str  # h5 or jsonl
    path: str  # file path
    # dataset group, such as arxiv_title_no_filter and arxiv_title_filter belong to one group
    group_name: Optional[str] = None
    score_type: Optional[str] = "default"  # default, average, dcg, ndcg.
    tag: [str] = None  # dataset tag, we use tag to differentiate dataset in one group
    link: Optional[str] = None  # dataset link, use it to download file
    query_files: Optional[List[QueryConfig]] = None  # hybrid_search queries dataset (they use same train dataset)
    schema: Optional[Dict[str, str]] = field(default_factory=dict)
