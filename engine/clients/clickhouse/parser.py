from typing import Any, List, Optional, Dict

from engine.base_client.parser import BaseConditionParser, FieldValue


class ClickHouseConditionParser(BaseConditionParser):

    def build_condition(
            self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        clauses = []
        if or_subfilters is not None and len(or_subfilters) > 0:
            clauses.append("(" + " or ".join(or_subfilters) + ")")
        if and_subfilters is not None and len(and_subfilters) > 0:
            clauses.append("(" + " and ".join(and_subfilters) + ")")
        return " and ".join(clauses)

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        return f"{column_name} = '{expression_value}'"

    def build_range_filter(
            self,
            column_name: str,
            lt: Optional[FieldValue],
            gt: Optional[FieldValue],
            lte: Optional[FieldValue],
            gte: Optional[FieldValue],
    ) -> Any:
        if lt is not None and gt is not None and lt < gt:
            raise RuntimeError(f"dataset condition error! lt:{lt}, gt:{gt}")
        if lte is not None and gte is not None and lte < gte:
            raise RuntimeError(f"dataset condition error! lt:{lte}, gt:{gte}")
        range_filter = []
        if lt is not None:
            range_filter.append(f"{column_name}<{lt}")
        if gt is not None:
            range_filter.append(f"{column_name}>{gt}")
        if lte is not None:
            range_filter.append(f"{column_name}<={lte}")
        if gte is not None:
            range_filter.append(f"{column_name}>={gte}")
        return "(" + " and ".join(range_filter) + ")"

    def build_geo_filter(
            self, column_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        return f"geoDistance( {lon}, {lat}, {column_name}.1, {column_name}.2 )<{radius}"

