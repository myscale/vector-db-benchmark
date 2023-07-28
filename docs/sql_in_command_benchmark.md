# Test the performance of the 'in' command on MyScale.

## Test Introduction

This test will evaluate the performance of the `SELECT ... from ... where group_id in (1, 2, 3, ...)` type SQL command on MyScale Cloud.

## Environment Preparation
Python3 environment is required, and the dependencies recorded in requirements.txt need to be installed.
```shell
pip3 install -r requirements.txt
```

## Modify Configuration File
We provide a basic configuration file to run the test. You need to copy and modify this configuration file before you can run the program.

Step1. Copy the configuration file.

Copy [myscale_cloud_mstg_laion-768-5m-group-ip-ip.json](../experiments/needs_editing/myscale_cloud_mstg_laion-768-5m-group-ip-ip.json) to the `vector-db-benchmark/experiments/configurations` folder.

Step2. Modify the configuration file.

Please use your cluster information on MyScale Cloud to modify the connection_params information in the copied file.
## Run the Benchmark!
Ensure that you are in the vector-db-benchmark directory and execute the following command to run the test. For more detailed instructions, please refer to this project's README
```shell
python3 run.py
```
The result files of the performance test will be stored in the `vector-db-benchmark/results` folder.