import glob
import json
import os

from benchmark import DATASETS_DIR, ROOT_DIR


def read_engine_configs() -> dict:
    """
    拼接所有引擎配置 \n
    Returns: 合并 pwd/experiments/configurations 下的 json 文件
    """
    all_configs = {}
    # 引擎配置目录，就是 pwd/experiments/configurations 目录下的 json 文件
    engines_config_dir = os.path.join(ROOT_DIR, "experiments", "configurations")
    # 返回所有待匹配文件列表
    config_files = glob.glob(os.path.join(engines_config_dir, "*.json"))
    for config_file in config_files:
        with open(config_file, "r") as fd:
            configs = json.load(fd)
            for config in configs:
                all_configs[config["name"]] = config

    return all_configs


def read_dataset_config():
    """
    读取 datasets/datasets.json 为 dict
    Returns: dict

    """
    all_configs = {}
    datasets_config_path = DATASETS_DIR / "datasets.json"
    with open(datasets_config_path, "r") as fd:
        configs = json.load(fd)
        for config in configs:
            all_configs[config["name"]] = config
    return all_configs

# print(read_engine_configs())
# print(read_dataset_config())