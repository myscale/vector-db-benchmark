import functools
import json
from datetime import datetime
from typing import List

from benchmark import ROOT_DIR
from benchmark.dataset import Dataset
from engine.base_client.configure import BaseConfigurator
from engine.base_client.search import BaseSearcher
from engine.base_client.upload import BaseUploader

RESULTS_DIR = ROOT_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)


class BaseClient:
    def __init__(
            self,
            name: str,   # name of the experiment, for example: myscale-m-16-ef-128...
            meta: dict,  # information of your engine version
            configurator: BaseConfigurator,
            uploader: BaseUploader,
            searchers: List[BaseSearcher],
    ):
        self.name = name
        self.meta = meta
        self.configurator = configurator
        self.uploader = uploader
        self.searchers = searchers
        index_create_parameter = {}
        if self.name.startswith("myscale") or self.name.startswith("milvus") or self.name.startswith("zilliz") or self.name.startswith("mqdb"):
            index_create_parameter = self.uploader.upload_params["index_params"]
        elif self.name.startswith("qdrant"):
            index_create_parameter = self.configurator.collection_params["hnsw_config"]
            if self.configurator.collection_params.get("optimizers_config", None) is not None:
                index_create_parameter["optimizers_config"] = self.configurator.collection_params["optimizers_config"]
        elif self.name.startswith("pinecone"):
            index_create_parameter = self.configurator.collection_params["pod_type"]
        elif self.name.startswith("redis"):
            index_create_parameter = self.configurator.collection_params["hnsw_config"]
        elif self.name.startswith("weaviate"):
            index_create_parameter = self.configurator.collection_params["vectorIndexConfig"]
        elif self.name.startswith("elastic"):
            index_create_parameter = self.configurator.collection_params["index_options"]
        elif self.name.startswith("proxima"):
            index_create_parameter = self.uploader.upload_params.get("index_params", {})
        self.index_create_parameter = index_create_parameter
        print(f"experiment information: [name ⚙️ {name}, init {len(self.searchers)} searchers, {1} uploader]")

    def save_search_and_upload_results(
            self, search_results: dict, search_id: int, search_params: dict,
            upload_params: dict, upload_results: dict, result_group: str
    ):
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        experiments_file = (f"{self.name}-search-{search_id}-parallel-{search_params['parallel']}"
                            f"-top-{search_params['top']}-{timestamp}.json")
        print(f"trying to save search result, file name is {experiments_file}")
        with open(RESULTS_DIR / experiments_file, "w") as out:
            out.write(
                json.dumps({
                    "result_group": result_group,  # single search or hybrid search
                    "meta": self.meta,
                    "index_create_parameter": self.index_create_parameter,
                    "index_search_parameter": search_params,
                    "data_upload_parameter": {
                        "parallel": upload_params.get("parallel", 16),
                        "batch_size": upload_params.get("batch_size", 64),
                        "optimizers": upload_params.get("optimizers", {})
                    },
                    "search_results": search_results,
                    "upload_results": upload_results,
                }, indent=2)
            )

    def save_upload_results(self, results: dict, upload_params: dict, result_group: str):
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        experiments_file = f"{self.name}-upload-{timestamp}.json"
        print(f"trying to save upload result, file name is {experiments_file}")
        with open(RESULTS_DIR / experiments_file, "w") as out:
            upload_stats = {
                "result_group": result_group,  # single search or hybrid search
                "meta": self.meta,
                "index_create_parameter": self.index_create_parameter,
                "data_upload_parameter": {
                    "parallel": upload_params.get("parallel", 1),
                    "batch_size": upload_params.get("batch_size", 64),
                    "optimizers": upload_params.get("optimizers", {})
                },
                "results": results,
            }
            out.write(json.dumps(upload_stats, indent=2))

    def run_experiment(self, dataset: Dataset, skip_upload: bool = False):
        execution_params = self.configurator.execution_params(
            distance=dataset.config.distance, vector_size=dataset.config.vector_size
        )
        print("got execution_params is {}".format(execution_params))

        reader = dataset.get_reader(execution_params.get("normalize", False), dataset_config=dataset.config)

        upload_stats = {}
        if not skip_upload:
            print("Experiment stage: Configure")
            extra_columns_name, extra_columns_type = reader.read_column_name_type()
            self.configurator.configure(
                distance=dataset.config.distance,
                vector_size=dataset.config.vector_size,
                extra_columns_name=extra_columns_name,
                extra_columns_type=extra_columns_type
            )

            print("Experiment stage: Upload")
            upload_stats = self.uploader.upload(
                distance=dataset.config.distance, records=reader.read_data(),
                extra_columns_name=extra_columns_name, extra_columns_type=extra_columns_type
            )
            self.save_upload_results(
                upload_stats,
                upload_params={
                    **self.uploader.upload_params,
                    **self.configurator.collection_params,
                },
                result_group=dataset.config.result_group
            )

        print("Experiment stage: Search")
        for search_id, searcher in enumerate(self.searchers):
            search_params = {**searcher.search_params}
            get_queries = functools.partial(reader.read_queries)

            search_stats = searcher.search_all(
                dataset.config.distance, get_queries, dataset.config.queries, dataset.config.schema
            )

            self.save_search_and_upload_results(
                search_results=search_stats, search_id=search_id, search_params=search_params,
                upload_params={
                    **self.uploader.upload_params,
                    **self.configurator.collection_params,
                },
                upload_results=upload_stats,
                result_group=dataset.config.result_group
            )
        print("Experiment stage: Done")
