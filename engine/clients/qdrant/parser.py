from typing import Any, List, Optional

from qdrant_client.http import models as rest

from engine.base_client.parser import BaseConditionParser, FieldValue


class QdrantConditionParser(BaseConditionParser):
    def build_condition(
            self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        return rest.Filter(
            should=or_subfilters,
            must=and_subfilters,
        )

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        if isinstance(expression_value, float):
            # Qdrant does not support matching on float types,
            # they need to be converted to range conditions using lte and gte.
            return self.build_range_filter(column_name=column_name, lt=None, gt=None, lte=expression_value, gte=expression_value)
        return rest.FieldCondition(
            key=column_name,
            match=rest.MatchValue(value=expression_value),
        )

    def build_range_filter(
            self,
            column_name: str,
            lt: Optional[FieldValue],
            gt: Optional[FieldValue],
            lte: Optional[FieldValue],
            gte: Optional[FieldValue],
    ) -> Any:
        return rest.FieldCondition(
            key=column_name,
            range=rest.Range(
                lt=lt,
                gt=gt,
                gte=gte,
                lte=lte,
            ),
        )

    def build_geo_filter(
            self, column_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        return rest.FieldCondition(
            key=column_name,
            geo_radius=rest.GeoRadius(
                center=rest.GeoPoint(
                    lon=lon,
                    lat=lat,
                ),
                radius=radius,
            ),
        )
