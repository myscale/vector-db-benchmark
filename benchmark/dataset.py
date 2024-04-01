import os
import sys
import shutil
import tarfile
import urllib.request

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from dataset_reader.ann_compound_reader import AnnCompoundReader
from dataset_reader.json_reader import JSONReader
from benchmark import DATASETS_DIR
from dataset_reader.ann_h5_reader import AnnH5Reader
from dataset_reader.base_reader import BaseReader
from benchmark.dataset_config import DatasetConfig

READER_TYPE = {"h5": AnnH5Reader, "jsonl": JSONReader, "tar": AnnCompoundReader}


def download_core(config_path: str, link: str):
    target_path = DATASETS_DIR / config_path
    if target_path.exists():
        print(f"❄️ {target_path} already exists")
        return
    file_name = f"{link.split('/')[-1]}"
    print(f"🥣 Downloading {link} to file {file_name}")
    file_name, _ = urllib.request.urlretrieve(link, file_name)

    if file_name.endswith(".tgz") or file_name.endswith(".tar.gz"):
        print(f"❄️ Mkdir dir if not exists: {target_path} -> Extracting: {file_name} -> {target_path}")
        os.makedirs(target_path, exist_ok=True)
        file = tarfile.open(file_name)
        file.extractall(target_path)
        file.close()
        os.remove(file_name)
    else:
        print(f"❄️ Moving: {file_name} -> {(DATASETS_DIR / config_path).parent}")
        (DATASETS_DIR / config_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_name, target_path)
        os.remove(file_name)


class Dataset:
    def __init__(self, config: dict):
        self.config = DatasetConfig(**config)
        print("experiment init dataset {}".format(self.config.name))

    def download(self):
        # download train data
        download_core(config_path=self.config.path, link=self.config.link)
        # download multi queries data
        if self.config.query_files is not None:
            for query_config in self.config.query_files:
                download_core(config_path=query_config["path"], link=query_config["link"])

    def get_reader(self, normalize: bool, dataset_config: DatasetConfig) -> BaseReader:
        reader_class = READER_TYPE[self.config.type]
        return reader_class(DATASETS_DIR, dataset_config, normalize=normalize)

# download all datasets
# dataset = read_dataset_config()
# for ds in dataset.keys():
#     dataset_config = dataset[ds]
#     print(dataset_config)
#     dataset_manager = Dataset(dataset_config)
#     dataset_manager.download()
#     print("fini-sh---")
