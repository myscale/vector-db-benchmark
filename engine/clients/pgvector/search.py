import time
from typing import List, Optional, Tuple
import psycopg2

from engine.base_client import BaseSearcher
from engine.clients.pgvector.config import *
from engine.clients.pgvector.parser import PGVectorConditionParser


class PGVectorSearcher(BaseSearcher):
    search_params = {}
    conn = None
    cur = None
    distance: str = None
    host: str = None
    parser = PGVectorConditionParser()
    engine_type: str = None

    @classmethod
    def init_client(cls, host: str, distance, connection_params: dict, search_params: dict):
        cls.engine_type = connection_params.get("engine_type", "c")
        database, host, port, user, password = process_connection_params(connection_params, host)
        cls.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        cls.cur = cls.conn.cursor()
        cls.host = host
        cls.distance = DISTANCE_MAPPING_SEARCH[distance]
        cls.search_params = search_params

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema) -> List[Tuple[int, float]]:
        with cls.conn.cursor() as cur:
            # start transaction
            cur.execute("BEGIN;")
            # set index create parameter
            for key in cls.search_params["params"].keys():
                cur.execute(f"SET LOCAL {key} = {cls.search_params['params'][key]};")


            meta_conditions = cls.parser.parse(meta_conditions)
            if meta_conditions:
                search_command = f"SELECT id, vector {cls.distance} '{vector}' as dis FROM {PGVECTOR_INDEX} where {meta_conditions} order by dis ASC LIMIT {top}"
                cur.execute(search_command)
            else:
                search_command = f"SELECT id, vector {cls.distance} '{vector}' as dis FROM {PGVECTOR_INDEX} order by dis ASC LIMIT {top}"
                cur.execute(search_command)
            results = cur.fetchall()
            cur.execute("COMMIT;")
            return [(row[0], row[1] * (-1 if cls.distance == '<#>' else 1)) for row in results]
