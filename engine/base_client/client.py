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
        name: str,  # name of the experiment, 例 mqdb-m-16-ef-128
        configurator: BaseConfigurator,  # configure.py 实例
        uploader: BaseUploader,  # upload.py 实例
        searchers: List[BaseSearcher],  # search.py 实例, 有多少个搜索参数就有多少个实例
    ):
        self.name = name
        self.configurator = configurator
        self.uploader = uploader
        self.searchers = searchers

    # 保存搜索结果
    def save_search_results(
        self, dataset_name: str, results: dict, search_id: int, search_params: dict
    ):
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        experiments_file = (
            f"{self.name}-{dataset_name}-search-{search_id}-{timestamp}.json"
        )
        print("trying to save search result, file name is {}".format(f"{self.name}-{dataset_name}-search-{search_id}-{timestamp}.json"))
        with open(RESULTS_DIR / experiments_file, "w") as out:
            out.write(
                json.dumps({"params": search_params, "results": results}, indent=2)
            )

    # 保存上传结果
    def save_upload_results(
        self, dataset_name: str, results: dict, upload_params: dict
    ):
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        experiments_file = f"{self.name}-{dataset_name}-upload-{timestamp}.json"
        print("trying to save upload result, file name is {}".format(f"{self.name}-{dataset_name}-upload-{timestamp}.json"))
        with open(RESULTS_DIR / experiments_file, "w") as out:
            upload_stats = {
                "params": upload_params,
                "results": results,
            }
            out.write(json.dumps(upload_stats, indent=2))

    def run_experiment(self, dataset: Dataset, skip_upload: bool = False):
        # 执行参数
        execution_params = self.configurator.execution_params(
            distance=dataset.config.distance, vector_size=dataset.config.vector_size
        )
        # 获得 reader 对象
        print("got reader normalize is {}".format(execution_params))
        reader = dataset.get_reader(execution_params.get("normalize", False))

        if not skip_upload:
            print("Experiment stage: Configure")
            # 创建表，drop 已经存在的索引
            self.configurator.configure(
                distance=dataset.config.distance,
                vector_size=dataset.config.vector_size,
            )

            print("Experiment stage: Upload")
            # netx(records)= Record(id=0, vector=[-0.11...], metadata=None)
            upload_stats = self.uploader.upload(
                distance=dataset.config.distance, records=reader.read_data()
            )
            self.save_upload_results(
                dataset.config.name,
                upload_stats,
                upload_params={
                    **self.uploader.upload_params,
                    **self.configurator.collection_params,
                },
            )

        print("Experiment stage: Search")
        for search_id, searcher in enumerate(self.searchers):
            search_params = {**searcher.search_params}
            # search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
            # print("Experiment stage-: Search params {}".format(search_params))

            # TODO 修改 search all 参数，新增 topK
            search_stats = searcher.search_all(
                dataset.config.distance, reader.read_queries()
            )
            self.save_search_results(
                dataset.config.name, search_stats, search_id, search_params
            )
        print("Experiment stage: Done")
