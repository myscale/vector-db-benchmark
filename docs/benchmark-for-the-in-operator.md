# Benchmark for the IN Operator

This test will evaluate the performance of the IN operator of the WHERE clause on MyScale Cloud. A sample query looks like

```sql
SELECT id, distance(vector, QUERY) as dist FROM benchmark_table
WHERE group_id IN (1, 2, 3, ...) ORDER BY dist DESC LIMIT 100;
```

## Environment Preparation

To run this program, you will need a Python3 environment and the dependencies listed in `requirements.txt` must be installed.

```shell
pip3 install -r requirements.txt
```

## Modify Configuration File

To run the test, you will need to copy and modify the provided configuration file.

Step1. Copy the configuration file.

Copy [myscale_cloud_mstg_laion-768-5m-group-ip-ip.json](../experiments/needs_editing/myscale_cloud_mstg_laion-768-5m-group-ip-ip.json) to the `vector-db-benchmark/experiments/configurations` folder.

Step2. Modify the configuration file.

Please update the `connection_params` information in the copied file with your cluster information from MyScale Cloud.

## Run the Benchmark

To run the test, make sure you are in the `vector-db-benchmark` directory and execute the following command:

```shell
python3 run.py
```

For more detailed instructions, please refer to this project's [README](../README.md).

The performance test result files will be stored in the `vector-db-benchmark/results` folder.
