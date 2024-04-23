from numba import njit, prange
from numba.typed import List as TypedList

from ranx import Run

from ranx.normalization.common import (
    extract_scores,
    safe_min,
    safe_max,
    to_unicode,
    convert_results_dict_list_to_run,
    create_empty_results_dict,
    create_empty_results_dict_list
)


# LOW LEVEL FUNCTIONS ==========================================================
@njit(cache=True)
def _min_max_norm_inverted(results):
    """Apply `min-max norm` to a given results dictionary."""
    scores = extract_scores(results)
    min_score = safe_min(scores)
    max_score = safe_max(scores)
    denominator = max_score - min_score
    denominator = max(denominator, 1e-9)

    normalized_results = create_empty_results_dict()
    for doc_id in results.keys():
        doc_id = to_unicode(doc_id)
        normalized_results[doc_id] = (max_score - results[doc_id]) / (denominator)

    return normalized_results


@njit(cache=True, parallel=True)
def _min_max_norm_inverted_parallel(run):
    """Apply `min_max norm` to a each results dictionary of a run in parallel."""
    q_ids = TypedList(run.keys())

    normalized_run = create_empty_results_dict_list(len(q_ids))
    for i in prange(len(q_ids)):
        normalized_run[i] = _min_max_norm_inverted(run[q_ids[i]])

    return convert_results_dict_list_to_run(q_ids, normalized_run)


# HIGH LEVEL FUNCTIONS =========================================================
def min_max_norm_inverted(run: Run) -> Run:
    """Apply `min_max norm` to a given run.

    Args:
        run (Run): Run to be normalized.

    Returns:
        Run: Normalized run.
    """
    normalized_run = Run()
    normalized_run.name = run.name
    normalized_run.run = _min_max_norm_inverted_parallel(run.run)
    return normalized_run
