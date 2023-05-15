import fnmatch
import os
import socket
import tarfile
import time
import typer
from qcloud_cos import CosClientError, CosServiceError, CosConfig, CosS3Client
from benchmark import ROOT_DIR
from benchmark.config_read import read_dataset_config, read_engine_configs
from benchmark.dataset import Dataset
from engine.base_client import IncompatibilityError
from engine.clients.client_factory import ClientFactory

app = typer.Typer()

# upload benchmark results to S3
def upload_data(client, bucket, key_str, local_file_path):
    client.delete_object(Bucket=bucket, Key=key_str)
    for i in range(0, 10):
        try:
            client.upload_file(Bucket=bucket, Key=key_str, LocalFilePath=local_file_path, EnableMD5=False, progress_callback=None)
            break
        except CosClientError or CosServiceError as e:
            print(e)


def wait_socket(host_: str, port: int):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(60 * 3)
    for attempt in range(0, 1000):
        try:
            s.connect((host_, port))
            print(f"server online: {host_}:{port}")
            break
        except Exception as e:
            print(f"🐟 {e}")
            time.sleep(10)
    s.close()


@app.command()
def run(
        engines: str = "*",
        datasets: str = "*",
        host: str = "localhost",
        port: int = 8123,
        skip_upload: bool = False,

        s3_auto_upload: bool = False,  # whether to upload to S3
        s3_region: str = "beijing",
        s3_scheme: str = "https",
        s3_secret_id: str = "",        # your S3 secret id
        s3_secret_key: str = "",       # your S3 secrect key
        s3_bucket: str = "",           # your S3 bucket
        s3_key_prefix: str = "vector-db-benchmark-dash-results", # your S3 file prefix
        wait_server_online: bool = False,
):

    """
    Example:
        python3 run --engines *-m-16-* --datasets glove-*
    """
    all_engines = read_engine_configs()
    all_datasets = read_dataset_config()

    # waiting for server online
    if wait_server_online:
        wait_socket(host_=host, port=port)

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

    for engine_name, engine_config in selected_engines.items():
        for dataset_name, dataset_config in selected_datasets.items():
            datasets_need_run = engine_config.get("dataset", "")
            if datasets_need_run == "":
                raise RuntimeWarning(f"{engine_name} doesn't have dataset item, all data sets will be run 🚀 next!")
            if datasets_need_run != "" and datasets_need_run != dataset_name:
                # skip unmatched datasets
                continue
            print(
                f"try running experiment: [name ⚙️ {engine_name} ### dataset: 📚 {dataset_name}], this experiment will recreate vector index {len(selected_engines.keys())} times")
            client = ClientFactory(host).build_client(engine_config, dataset_name, dataset_config)
            # before testing, you should download dataset
            dataset = Dataset(dataset_config)
            dataset.download()
            try:
                print("trying to run experiment")
                client.run_experiment(dataset, skip_upload)
            except IncompatibilityError as e:
                print(f"Skipping {engine_name} - {dataset_name}, incompatible params")
                continue

    if s3_auto_upload:
        try:
            s3_config = CosConfig(Region=s3_region,
                                  SecretId=s3_secret_id,
                                  SecretKey=s3_secret_key,
                                  Token=None,
                                  Scheme=s3_scheme)
            s3_client = CosS3Client(s3_config)
            time_stamp = int(time.time() * 1000)
            print("🔥 tar results file...")
            with tarfile.open(f"results-{time_stamp}.tar.gz", "w:gz") as tar:
                tar.add(ROOT_DIR / "results", arcname=os.path.basename(ROOT_DIR / "results"))

            print(f"🔥 uploading results-{time_stamp}.tar.gz to S3...")
            results_file_compressed = f"results-{time_stamp}.tar.gz"
            upload_data(client=s3_client,
                        bucket=s3_bucket,
                        key_str=f"{s3_key_prefix}/{results_file_compressed}",
                        local_file_path=results_file_compressed)
            print(f"🔥 uploading results-{time_stamp}.tar.gz finished")
        except Exception:
            raise RuntimeError(f"auto upload failed!")


if __name__ == "__main__":
    app()
