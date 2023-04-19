from abc import ABC
from typing import List, Type

from engine.base_client.client import (
    BaseClient,
    BaseConfigurator,
    BaseSearcher,
    BaseUploader,
)
from engine.clients.elasticsearch.configure import ElasticConfigurator
from engine.clients.elasticsearch.search import ElasticSearcher
from engine.clients.elasticsearch.upload import ElasticUploader
from engine.clients.milvus import MilvusConfigurator, MilvusSearcher, MilvusUploader
from engine.clients.mqdb.configure import MqdbConfigurator
from engine.clients.mqdb.search import MqdbSearcher
from engine.clients.mqdb.upload import MqdbUploader
from engine.clients.pinecone.configure import PineconeConfigurator
from engine.clients.pinecone.search import PineconeSearcher
from engine.clients.pinecone.upload import PineconeUploader
from engine.clients.proxima.configure import ProximaConfigurator
from engine.clients.proxima.search import ProximaSearcher
from engine.clients.proxima.upload import ProximaUploader
from engine.clients.qdrant import QdrantConfigurator, QdrantSearcher, QdrantUploader
from engine.clients.redis.configure import RedisConfigurator
from engine.clients.redis.search import RedisSearcher
from engine.clients.redis.upload import RedisUploader
from engine.clients.weaviate import (
    WeaviateConfigurator,
    WeaviateSearcher,
    WeaviateUploader,
)

ENGINE_CONFIGURATORS = {
    "mqdb": MqdbConfigurator,
    "qdrant": QdrantConfigurator,
    "weaviate": WeaviateConfigurator,
    "milvus": MilvusConfigurator,
    "elastic": ElasticConfigurator,
    "redis": RedisConfigurator,
    "pinecone": PineconeConfigurator,
    "proxima": ProximaConfigurator,
}

ENGINE_UPLOADERS = {
    "mqdb": MqdbUploader,
    "qdrant": QdrantUploader,
    "weaviate": WeaviateUploader,
    "milvus": MilvusUploader,
    "elastic": ElasticUploader,
    "redis": RedisUploader,
    "pinecone": PineconeUploader,
    "proxima": ProximaUploader,
}

ENGINE_SEARCHERS = {
    "mqdb": MqdbSearcher,
    "qdrant": QdrantSearcher,
    "weaviate": WeaviateSearcher,
    "milvus": MilvusSearcher,
    "elastic": ElasticSearcher,
    "redis": RedisSearcher,
    "pinecone": PineconeSearcher,
    "proxima": ProximaSearcher,
}


class ClientFactory(ABC):
    def __init__(self, host):
        self.host = host

    def _create_configurator(self, experiment) -> BaseConfigurator:
        engine_configurator_class = ENGINE_CONFIGURATORS[experiment["engine"]]
        engine_configurator = engine_configurator_class(
            self.host,
            collection_params={**experiment.get("collection_params", {})},
            connection_params={**experiment.get("connection_params", {})},
        )
        return engine_configurator

    def _create_uploader(self, experiment) -> BaseUploader:
        engine_uploader_class = ENGINE_UPLOADERS[experiment["engine"]]
        engine_uploader = engine_uploader_class(
            self.host,
            connection_params={**experiment.get("connection_params", {})},
            upload_params={**experiment.get("upload_params", {})},
        )
        return engine_uploader

    def _create_searchers(self, experiment) -> List[BaseSearcher]:
        engine_searcher_class: Type[BaseSearcher] = ENGINE_SEARCHERS[
            experiment["engine"]
        ]

        engine_searchers = [
            engine_searcher_class(
                self.host,
                connection_params={**experiment.get("connection_params", {})},
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
