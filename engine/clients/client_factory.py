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
}

ENGINE_UPLOADERS = {
    "mqdb": MqdbUploader,
    "qdrant": QdrantUploader,
    "weaviate": WeaviateUploader,
    "milvus": MilvusUploader,
    "elastic": ElasticUploader,
    "redis": RedisUploader,
}

ENGINE_SEARCHERS = {
    "mqdb": MqdbSearcher,
    "qdrant": QdrantSearcher,
    "weaviate": WeaviateSearcher,
    "milvus": MilvusSearcher,
    "elastic": ElasticSearcher,
    "redis": RedisSearcher,
}


class ClientFactory(ABC):
    def __init__(self, host):
        self.host = host

    def _create_configurator(self, experiment) -> BaseConfigurator:
        # 找到对应的配置类实现
        engine_configurator_class = ENGINE_CONFIGURATORS[experiment["engine"]]
        engine_configurator = engine_configurator_class(
            self.host,
            collection_params={**experiment.get("collection_params", {})},
            connection_params={**experiment.get("connection_params", {})},
        )
        return engine_configurator

    def _create_uploader(self, experiment) -> BaseUploader:
        # 初始化 uploader
        engine_uploader_class = ENGINE_UPLOADERS[experiment["engine"]]
        engine_uploader = engine_uploader_class(
            self.host,
            # 使用 ** 作为前缀，多余的参数会被认为是字典
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
        print("create {} engine searchers".format(len(engine_searchers)))
        return engine_searchers

    def build_client(self, experiment):
        # experiment 对应每个需要测试的 single-node-config-op
        print("build_client: client factory trying to build client..")
        print("build_client: experiment is {}".format(experiment))
        return BaseClient(
            name=experiment["name"],  # milvus-m-16-ef-128
            # 初始化引擎配置--configure.py
            configurator=self._create_configurator(experiment),
            # 初始化上传类--upload.py
            uploader=self._create_uploader(experiment),
            # 初始化 15 个搜索类--search.py
            searchers=self._create_searchers(experiment),
        )
