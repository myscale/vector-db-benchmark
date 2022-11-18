import fnmatch

import typer

from benchmark.config_read import read_dataset_config, read_engine_configs
from benchmark.dataset import Dataset
from engine.base_client import IncompatibilityError
from engine.clients.client_factory import ClientFactory

app = typer.Typer()


@app.command()
def run(
    engines: str = "*",
    datasets: str = "*",
    host: str = "localhost",
    skip_upload: bool = False,
):

    """
    Example:
        python3 run --engines *-m-16-* --datasets glove-*
    """
    all_engines = read_engine_configs()
    all_datasets = read_dataset_config()

    selected_engines = {
        name: config
        for name, config in all_engines.items()
        if fnmatch.fnmatch(name, engines)
    }
    selected_datasets = {
        name: config
        for name, config in all_datasets.items()
        if fnmatch.fnmatch(name, datasets)
    }
    print("select engine {}".format(selected_engines))
    print("select dataset {}".format(selected_datasets))
    # 对每个选中的 engine-engine 配置，轮流 run 对应的数据集
    for engine_name, engine_config in selected_engines.items():
        for dataset_name, dataset_config in selected_datasets.items():
            print(f"Running experiment: {engine_name} - {dataset_name}")
            client = ClientFactory(host).build_client(engine_config)
            # 测试前下载数据集
            dataset = Dataset(dataset_config)
            dataset.download()
            try:
                print("trying to run experiment")
                print("dataset vector_size:{}".format(dataset.config.vector_size))
                print("dataset distance:{}".format(dataset.config.distance))
                client.run_experiment(dataset, skip_upload)
            except IncompatibilityError as e:
                print(f"Skipping {engine_name} - {dataset_name}, incompatible params")
                continue


if __name__ == "__main__":
    app()
