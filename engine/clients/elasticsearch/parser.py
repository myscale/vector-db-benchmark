from typing import Any, List, Optional

from engine.base_client.parser import BaseConditionParser, FieldValue


class ElasticConditionParser(BaseConditionParser):
    def build_condition(
        self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        if and_subfilters is None and or_subfilters is None:
            return None
        return {
            "bool": {
                "must": and_subfilters,
                "should": or_subfilters,
            }
        }

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        return {"match": {column_name: expression_value}}

    def build_range_filter(
            self,
            column_name: str,
            lt: Optional[FieldValue],
            gt: Optional[FieldValue],
            lte: Optional[FieldValue],
            gte: Optional[FieldValue],
    ) -> Any:
        return {"range": {column_name: {"lt": lt, "gt": gt, "lte": lte, "gte": gte}}}

    def build_geo_filter(
            self, column_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        return {
            "geo_distance": {
                "distance": f"{radius}m",
                column_name: {"lat": lat, "lon": lon},
            }
        }
