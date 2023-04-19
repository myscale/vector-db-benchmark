from typing import Any, List, Optional

from qdrant_client.http import models as rest

from engine.base_client import IncompatibilityError
from engine.base_client.parser import BaseConditionParser, FieldValue


class PineconeConditionParser(BaseConditionParser):
    def build_condition(
            self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        pinecone_filter = {}
        if and_subfilters is not None:
            pinecone_filter["$and"] = and_subfilters
        if or_subfilters is not None:
            pinecone_filter["$or"] = or_subfilters
        return pinecone_filter

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        return {column_name: {"$eq": expression_value}}

    def build_range_filter(
            self,
            column_name: str,
            lt: Optional[FieldValue],
            gt: Optional[FieldValue],
            lte: Optional[FieldValue],
            gte: Optional[FieldValue],
    ) -> Any:
        value_range = {}
        if lt is not None:
            value_range["$lt"] = lt
        if gt is not None:
            value_range["$gt"] = gt
        if lte is not None:
            value_range["$lte"] = lte
        if gte is not None:
            value_range["$gte"] = gte
        return {column_name: value_range}

    def build_geo_filter(
            self, column_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        raise IncompatibilityError
