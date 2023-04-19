from typing import Optional, Dict
from dataclasses import dataclass, field


@dataclass
class DatasetConfig:
    name: str           # dataset name
    result_group: str   # single_search or hybrid_search
    vector_size: int    # vector size
    queries: int        # number of vectors used for vector search
    distance: str       # cosine、l2
    type: str           # h5 or jsonl
    path: str           # file path
    # dataset group, such as arxiv_title_no_filter and arxiv_title_filter belong to one group
    group_name: Optional[str] = None
    tag: [str] = None   # dataset tag, we use tag to differentiate dataset in one group
    link: Optional[str] = None              # dataset link, use it to download file
    query_file_path: Optional[list] = None  # hybrid_search queries dataset (they use same train dataset)
    schema: Optional[Dict[str, str]] = field(default_factory=dict)

