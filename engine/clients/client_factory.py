from abc import ABC
from typing import List, Type

from engine.base_client.client import (
    BaseClient,
    BaseConfigurator,
    BaseSearcher,
    BaseUploader,
)
from engine.clients.elasticsearch import (
    ElasticConfigurator,
    ElasticSearcher,
    ElasticUploader,
)
from engine.clients.milvus import MilvusConfigurator, MilvusSearcher, MilvusUploader
from engine.clients.myscale.configure import MyScaleConfigurator
from engine.clients.myscale.search import MyScaleSearcher
from engine.clients.myscale.upload import MyScaleUploader
from engine.clients.clickhouse.configure import ClickHouseConfigurator
from engine.clients.clickhouse.search import ClickHouseSearcher
from engine.clients.clickhouse.upload import ClickHouseUploader
from engine.clients.pinecone.configure import PineconeConfigurator
from engine.clients.pinecone.search import PineconeSearcher
from engine.clients.pinecone.upload import PineconeUploader
from engine.clients.proxima.configure import ProximaConfigurator
from engine.clients.proxima.search import ProximaSearcher
from engine.clients.proxima.upload import ProximaUploader
from engine.clients.opensearch import (
    OpenSearchConfigurator,
    OpenSearchSearcher,
    OpenSearchUploader,
)
from engine.clients.qdrant import QdrantConfigurator, QdrantSearcher, QdrantUploader
from engine.clients.redis import RedisConfigurator, RedisSearcher, RedisUploader
from engine.clients.weaviate import (
    WeaviateConfigurator,
    WeaviateSearcher,
    WeaviateUploader,
)

from engine.clients.pgvector import (
    PGVectorSearcher,
    PGVectorConfigurator,
    PGVectorUploader,
)

ENGINE_CONFIGURATORS = {
    "myscale": MyScaleConfigurator,
    "clickhouse": ClickHouseConfigurator,
    "qdrant": QdrantConfigurator,
    "weaviate": WeaviateConfigurator,
    "milvus": MilvusConfigurator,
    "zilliz": MilvusConfigurator,
    "elastic": ElasticConfigurator,
    "opensearch": OpenSearchConfigurator,
    "redis": RedisConfigurator,
    "pinecone": PineconeConfigurator,
    "proxima": ProximaConfigurator,
    "pgvector": PGVectorConfigurator
}

ENGINE_UPLOADERS = {
    "myscale": MyScaleUploader,
    "clickhouse": ClickHouseUploader,
    "qdrant": QdrantUploader,
    "weaviate": WeaviateUploader,
    "milvus": MilvusUploader,
    "zilliz": MilvusUploader,
    "elastic": ElasticUploader,
    "opensearch": OpenSearchUploader,
    "redis": RedisUploader,
    "pinecone": PineconeUploader,
    "proxima": ProximaUploader,
    "pgvector": PGVectorUploader
}

ENGINE_SEARCHERS = {
    "myscale": MyScaleSearcher,
    "clickhouse": ClickHouseSearcher,
    "qdrant": QdrantSearcher,
    "weaviate": WeaviateSearcher,
    "milvus": MilvusSearcher,
    "zilliz": MilvusSearcher,
    "elastic": ElasticSearcher,
    "opensearch": OpenSearchSearcher,
    "redis": RedisSearcher,
    "pinecone": PineconeSearcher,
    "proxima": ProximaSearcher,
    "pgvector": PGVectorSearcher
}


class ClientFactory(ABC):
    def __init__(self, host):
        self.host = host

    def _create_configurator(self, experiment) -> BaseConfigurator:
        engine_configurator_class = ENGINE_CONFIGURATORS[experiment["engine"]]
        engine_configurator = engine_configurator_class(
            self.host,
            # for myscale and clickhouse, append upload_params to collection_params
            collection_params={**experiment.get("collection_params", {})} if experiment["engine"] not in ["myscale", "clickhouse"] else {**experiment.get("collection_params", {}), **experiment.get("upload_params", {})},
            connection_params={**experiment.get("connection_params", {})},
        )
        return engine_configurator

    def _create_uploader(self, experiment) -> BaseUploader:
        engine_uploader_class = ENGINE_UPLOADERS[experiment["engine"]]
        engine_uploader = engine_uploader_class(
            self.host,
            # for myscale and clickhouse, append upload_params to collection_params
            connection_params={**experiment.get("connection_params", {})},
            upload_params={**experiment.get("upload_params", {})} if experiment["engine"] not in ["myscale", "clickhouse"] else {**experiment.get("upload_params", {}), **experiment.get("collection_params", {})},
        )
        return engine_uploader

    def _create_searchers(self, experiment) -> List[BaseSearcher]:
        engine_searcher_class: Type[BaseSearcher] = ENGINE_SEARCHERS[
            experiment["engine"]
        ]

        engine_searchers = [
            engine_searcher_class(
                self.host,
                # for pgvector, append upload_params to connection_params
                connection_params={**experiment.get("connection_params", {})} if experiment["engine"] not in [
                    "pgvector"] else {**experiment.get("connection_params", {}), **experiment.get("upload_params", {})},
                search_params=search_params,
            )
            for search_params in experiment.get("search_params", [{}])
        ]
        return engine_searchers

    def build_client(self, experiment, dataset_name, dataset_config):
        meta = {
            "engine": {
                "name": experiment["engine"],
                "branch": experiment["branch"],
                "version": experiment["version"],
                "commit": experiment["commit"],
                "link": experiment["link"],
                "remark": experiment["remark"],
                "other": experiment["other"],
            },
            "index_type": experiment["index_type"],
            "dataset": dataset_name,
            "dataset_group": dataset_config["group_name"],
            "dataset_tag": dataset_config["tag"],
            "platform": experiment["platform"],
            "time_stamp": experiment["time_stamp"],
            "run_date": experiment["run_date"],
        }

        return BaseClient(
            name=experiment["name"],  # example: milvus-m-16-ef-128
            meta=meta,
            configurator=self._create_configurator(experiment),
            uploader=self._create_uploader(experiment),
            # init n search obj from search.py
            searchers=self._create_searchers(experiment),
        )
