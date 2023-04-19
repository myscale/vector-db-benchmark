from enum import Enum
from typing import Any, Dict, List, Optional, Union


class FilterType(str, Enum):
    FULL_MATCH = "match"
    RANGE = "range"
    GEO = "geo"


FieldValue = Union[str, int, float]
MetaConditions = Dict[str, List[Any]]


class BaseConditionParser:
    def parse(self, meta_conditions: Optional[MetaConditions]) -> Optional[Any]:
        """
        The parse method accepts the meta conditions stored in a dict-like
        internal benchmark structure and converts it into the representation
        used by a specific engine.

        The internal representation has the following structure:
        {
            "or": [
                {"a": {"match": {"value": 80}}},
                {"a": {"match": {"value": 2}}}
            ]
        }

        There is always an operator ("and" / "or") and a list of operands.

        :param meta_conditions:
        :return:
        """
        if meta_conditions is None or 0 == len(meta_conditions):
            # print("base parser meta is None")
            return None
        return self.build_condition(
            and_subfilters=self.create_condition_subfilters(meta_conditions.get("and")),
            or_subfilters=self.create_condition_subfilters(meta_conditions.get("or")),
        )

    def build_condition(
        self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        """
        Combination and logical "and" and "or" conditions
        Args:
            and_subfilters:
            or_subfilters:
        """
        raise NotImplementedError

    def create_condition_subfilters(self, condition_list) -> Optional[List[Any]]:
        if condition_list is None:
            return None

        output_filters = []
        for entry in condition_list:
            for column_name, column_filters in entry.items():
                for expression_type, expression_value_dict in column_filters.items():
                    condition = self.build_filter(
                        column_name=column_name,
                        expression_type=FilterType(expression_type),
                        expression_value_dict=expression_value_dict
                    )
                    output_filters.append(condition)
        return output_filters

    def build_filter(
        self, column_name: str, expression_type: FilterType, expression_value_dict: Dict[str, Any]
    ):
        if FilterType.FULL_MATCH == expression_type:
            return self.build_exact_match_filter(
                column_name, expression_value=expression_value_dict.get("value")
            )
        if FilterType.RANGE == expression_type:
            return self.build_range_filter(
                column_name,
                lt=expression_value_dict.get("lt"),    # less than '<'
                gt=expression_value_dict.get("gt"),    # grater than '>'
                lte=expression_value_dict.get("lte"),  # less than or equal '<='
                gte=expression_value_dict.get("gte"),  # greater than or equal '>='
            )
        if FilterType.GEO == expression_type:
            return self.build_geo_filter(
                column_name,
                lon=expression_value_dict.get("lon"),
                lat=expression_value_dict.get("lat"),
                radius=expression_value_dict.get("radius"),
            )
        raise NotImplementedError

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        raise NotImplementedError

    def build_range_filter(
        self,
        column_name: str,
        lt: Optional[FieldValue],
        gt: Optional[FieldValue],
        lte: Optional[FieldValue],
        gte: Optional[FieldValue],
    ) -> Any:
        raise NotImplementedError

    def build_geo_filter(
        self, column_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        raise NotImplementedError
