import json
import time
from typing import Any, List, Optional

from engine.base_client import IncompatibilityError
from engine.base_client.parser import BaseConditionParser, FieldValue
from pyproximase import DataType, QueryRequest

PY_TYPE_2_PROXIMA_TYPE = {
    float: DataType.FLOAT,
    int: DataType.INT32,
    str: DataType.STRING,
    bool: DataType.BOOL
}


class ProximaConditionParser(BaseConditionParser):
    def build_condition(
            self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        final_or_node = None
        final_and_node = None
        if or_subfilters is not None and len(or_subfilters) > 0:
            final_or_node = QueryRequest.QueryFilterNode(
                logic_type=QueryRequest.QueryLogicType.OR,
                expressions=[],
                filter_nodes=or_subfilters
            )

        if and_subfilters is not None and len(and_subfilters) > 0:
            final_and_node = QueryRequest.QueryFilterNode(
                logic_type=QueryRequest.QueryLogicType.AND,
                expressions=[],
                filter_nodes=and_subfilters
            )

        multi_nodes = []
        if final_or_node is not None:
            multi_nodes.append(final_or_node)
        if final_and_node is not None:
            multi_nodes.append(final_and_node)
        incorporate_node = QueryRequest.QueryFilterNode(
            logic_type=QueryRequest.QueryLogicType.AND,
            expressions=[],
            filter_nodes=multi_nodes
        )
        return QueryRequest.QueryFilter(filter_node=incorporate_node)

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        query_filter_and_node = QueryRequest.QueryFilterNode(
            logic_type=QueryRequest.QueryLogicType.AND,
            expressions=[
                QueryRequest.QueryFilterExpression(
                    column_name=column_name, rel_type=QueryRequest.QueryRelType.EQ, value=expression_value,
                    value_type=PY_TYPE_2_PROXIMA_TYPE.get(type(expression_value), DataType.UNDEFINED))
            ],
        )
        return query_filter_and_node

    def build_range_filter(
            self,
            column_name: str,
            lt: Optional[FieldValue],
            gt: Optional[FieldValue],
            lte: Optional[FieldValue],
            gte: Optional[FieldValue],
    ) -> Any:
        expressions = []
        if lt is not None:
            expressions.append(QueryRequest.QueryFilterExpression(
                column_name=column_name, rel_type=QueryRequest.QueryRelType.LT, value=lt,
                value_type=PY_TYPE_2_PROXIMA_TYPE.get(type(lt), DataType.UNDEFINED)))
        if gt is not None:
            expressions.append(QueryRequest.QueryFilterExpression(
                column_name=column_name, rel_type=QueryRequest.QueryRelType.GT, value=gt,
                value_type=PY_TYPE_2_PROXIMA_TYPE.get(type(gt), DataType.UNDEFINED)))
        if lte is not None:
            expressions.append(QueryRequest.QueryFilterExpression(
                column_name=column_name, rel_type=QueryRequest.QueryRelType.LE, value=lte,
                value_type=PY_TYPE_2_PROXIMA_TYPE.get(type(lte), DataType.UNDEFINED)))
        if gte is not None:
            expressions.append(QueryRequest.QueryFilterExpression(
                column_name=column_name, rel_type=QueryRequest.QueryRelType.GE, value=gte,
                value_type=PY_TYPE_2_PROXIMA_TYPE.get(type(gte), DataType.UNDEFINED)))

        query_filter_and_node = QueryRequest.QueryFilterNode(
            logic_type=QueryRequest.QueryLogicType.AND,
            expressions=expressions
        )
        return query_filter_and_node

    def build_geo_filter(
            self, column_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        raise IncompatibilityError
