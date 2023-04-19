import glob
import json
import os
import time

from benchmark import DATASETS_DIR, ROOT_DIR


def read_engine_configs() -> dict:
    """ Concatenate all engine configurations """
    all_configs = {}
    engines_config_dir = os.path.join(ROOT_DIR, "experiments", "configurations")
    config_files = glob.glob(os.path.join(engines_config_dir, "*.json"))
    for config_file in config_files:
        time_stamp = int(time.time() * 1000)
        run_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_stamp / 1000))
        with open(config_file, "r") as fd:
            configs = json.load(fd)
            for config in configs:
                # add time stamp for config file
                config["time_stamp"] = time_stamp
                config["run_date"] = run_date
                all_configs[config["name"]] = config
        print(f"load config file {config_file.split('/')[-1]}, run_date {run_date}, time_stamp {time_stamp}")
        time.sleep(1)

    return all_configs


def read_dataset_config():
    all_configs = {}
    datasets_config_path = DATASETS_DIR / "datasets.json"
    with open(datasets_config_path, "r") as fd:
        configs = json.load(fd)
        for config in configs:
            all_configs[config["name"]] = config
    return all_configs

# print(read_engine_configs())
# print(read_dataset_config())
