import math
from typing import Any, Iterable, List
from dataset_reader.base_reader import Record


def iter_batches(records: Iterable[Record], n: int) -> Iterable[Any]:
    ids = []
    vectors = []
    metadata = []

    for record in records:
        ids.append(record.id)
        vectors.append(record.vector)
        metadata.append(record.metadata)

        if len(vectors) >= n:
            yield [ids, vectors, metadata]
            ids, vectors, metadata = [], [], []
    if len(ids) > 0:
        yield [ids, vectors, metadata]


# Intersect Precision, IP
def intersect_precision(actual_ids: List[int], expected_ids: List[int], limit: int):
    ans_len = min(len(actual_ids), len(expected_ids), limit)

    expected_set = set(expected_ids[:limit])
    actual_set = set(actual_ids[:limit])
    intersect_len = len(actual_set.intersection(expected_set))

    return intersect_len / ans_len


# Average Precision, AP
def average_precision(actual_ids: List[int], expected_ids: List[int], limit: int):
    ans_len = min(len(actual_ids), len(expected_ids), limit)
    expected_set = set(expected_ids[:limit])

    precision_sum = 0
    num_hits = 0

    for i, id in enumerate(actual_ids[:limit]):
        if id in expected_set:
            num_hits += 1
            precision_sum += num_hits / (i + 1)

    if num_hits == 0:
        return 0

    return precision_sum / ans_len


# Discounted Cumulative Gain, DCG
def dcg(actual_ids: List[int], expected_ids: List[int], limit: int, expected_scores: List[Any] = None):
    dcg_score = 0
    for i, id in enumerate(actual_ids[:limit]):
        if id in expected_ids[:limit]:
            # rel_score = expected_scores[expected_ids.index(id)]
            rel_score = 1
            dcg_score += rel_score / math.log2(i + 2)
    return dcg_score


# Normalized Discounted Cumulative Gain, NDCG
def ndcg(actual_ids: List[int], expected_ids: List[int], limit: int):
    idcg_score = dcg(expected_ids, expected_ids, limit)
    dcg_score = dcg(expected_ids, actual_ids, limit)
    if idcg_score == 0:
        return 0
    return dcg_score / idcg_score


# MRR
def mrr(actual_ids: List[int], expected_ids: List[int], limit: int):
    actual_ids = actual_ids[:limit]
    if expected_ids[0] in actual_ids:
        return 1 / (actual_ids.index(expected_ids[0])+1)
    else:
        return 0
