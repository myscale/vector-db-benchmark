# Step-by-Step Guide for Testing Cloud Vector Database Services

[//]: # (![Benchmark Results]&#40;images/benchmark_results.png&#41;)

## Introduction
To compare the performance of vector search database cloud services, including MyScale, Pinecone, Weaviate Cloud, 
Qdrant Cloud, and Zilliz Cloud, we have developed this framework based on 
[qdrant/vector-db-benchmark](https://github.com/qdrant/vector-db-benchmark/). 
We will conduct performance tests on these cloud services using the following two datasets. 
Information about the two datasets is as follows:

| Dataset name             | Description                                                                                                                               | Number of vectors | Number of queries | Dimension | Distance | Filters                             | Payload columns | Download link                                                                                     |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|-------------------|-------------------|-----------|----------|-------------------------------------|-----------------|---------------------------------------------------------------------------------------------------|
| laion-768-5m-ip          | Provided by MyScale. Generated from [LAION 2B images](https://huggingface.co/datasets/laion/laion2b-multi-vit-h-14-embeddings/tree/main). | 5,000,000         | 10000             | 768       | IP       | N/A                                 | 0               | [link](https://myscale-datasets.s3.ap-southeast-1.amazonaws.com/laion-5m-test-ip.hdf5)            |
| arxiv-titles-384-angular | Provided by [Qdrant](https://github.com/qdrant/ann-filtering-benchmark-datasets). Generated from arXiv titles.                            | 2,138,591         | 10000             | 384       | Cosine   | Match keywords and timestamp ranges | 2               | [link](https://storage.googleapis.com/ann-filtered-benchmark/datasets/arxiv_small_payload.tar.gz) |

## Preparation
You need to install the required libraries on the client used for testing.
```shell
pip install -r requirements.txt
```

## Testing steps
For any cloud vector database, the testing process follows the flowchart below.
![](images/cloud%20test%20steps.png){width=300px}
Below are the specific testing processes for each cloud vector database.
### MyScale
#### Step1. Create Cluster
Go to the [MyScale official website](https://myscale.com/) and create a cluster. 
In the [cluster console](https://console.myscale.com/clusters), 
record the cluster connection information: `host`, `port`, `username`, and `password`.
![MyScaleConsole.jpg](images%2FMyScaleConsole.jpg)

#### Step2. Modify the configuration
We have provided two configuration files for testing MyScale:
- [myscale_cloud_mstg_laion-768-5m-ip.json](experiments/configurations/myscale_cloud_mstg_laion-768-5m-ip.json)
- [myscale_cloud_mstg_arxiv-titles-384-angular.json](experiments/configurations/myscale_cloud_mstg_arxiv-titles-384-angular.json)

You need to write the cluster connection information obtained in Step 1 into the configuration files. 
To modify the configuration files for testing, open each file and locate the `connection_params` section. 
Update the values for `host`, `port`, `user`, and `password` with the appropriate cluster connection information obtained in Step 1.

Here is an example of how the modified section may look:

```shell
"connection_params": {
  "host": "your_host.aws.dev.myscale.cloud",
  "port": 8443,
  "http_type": "http",
  "user": "your_username",
  "password": "your_password"
},
```

### Step3. Run the tests
```shell
python3 run.py --engines *myscale*
```

### Step4. View the test results
```shell
cd results
grep -E 'rps|mean_precision' $(ls -t)
```
![MyScaleResuts.jpg](images%2FMyScaleResuts.jpg)

### Pinecone
#### Step1. Create Cluster
Register with [Pinecone](https://docs.pinecone.io/docs/overview) and obtain the cluster connection information for 
`Environment` and `Value`.
![PineconeConsole.jpg](images%2FPineconeConsole.jpg)

#### Step2. Modify the configuration
We have provided two configuration files for testing Pinecone:
- [pinecone_cloud_s1_laion-768-5m-ip.json](experiments/configurations/pinecone_cloud_s1_laion-768-5m-ip.json)
- [pinecone_cloud_s1_arxiv-titles-384-angular.json](experiments/configurations/pinecone_cloud_s1_arxiv-titles-384-angular.json)

- You need to write the cluster connection information obtained in Step 1 into the configuration files. 
- Modify the `connection_params` section of the files and update the values for `environment` and `api_key`.

Here is an example of how the modified section may look:
```shell
"connection_params": {
  "api-key": "your_api_key",
  "environment": "your_environment"
},
```

### Step3. Run the tests
```shell
python3 run.py --engines *pinecone*
```

### Step4. View the test results
```shell
cd results
grep -E 'rps|mean_precision' $(ls -t)
```
![PineconeResults.jpg](images%2FPineconeResults.jpg)

### Zilliz
#### Step1. Create Cluster
You need to find the cluster connection information, including `end_point`, `user`, and `password`, 
in the [Zilliz Cloud console](https://cloud.zilliz.com/projects/MA==/databases). 
The `user` and `password` are the credentials you specified when creating the cluster.
![ZillizConsole.jpg](images%2FZillizConsole.jpg)

#### Step2. Modify the configuration
We have provided two configuration files for testing Zilliz:
- [zilliz_cloud_1cu_storage_optimized_laion-768-5m-ip.json](experiments/configurations/zilliz_cloud_1cu_storage_optimized_laion-768-5m-ip.json)
- [zilliz_cloud_1cu_storage_optimized_arxiv-titles-384-angular.json](experiments/configurations/zilliz_cloud_1cu_storage_optimized_arxiv-titles-384-angular.json)

You need to write the cluster connection information obtained in Step 1 into the configuration files. 
To modify the configuration files for testing, open each file and locate the `connection_params` section. 
Update the values for `end_point`, `cloud_user`, and `cloud_password` with the appropriate cluster connection information obtained in Step 1.

Here is an example of how the modified section may look:

```shell
"connection_params": {
  "cloud_mode": true,
  "host": "127.0.0.1",
  "port": 19530,
  "user": "",
  "password": "",
  "end_point": "https://your_host.zillizcloud.com:19538",
  "cloud_user": "your_user",
  "cloud_password": "your_password",
  "cloud_secure": true
},
```

### Step3. Run the tests
```shell
python3 run.py --engines *zilliz*
```

### Step4. View the test results
```shell
cd results
grep -E 'rps|mean_precision' $(ls -t)
```
![ZillizResults.jpg](images%2FZillizResults.jpg)

### Weaviate Cloud
#### Step1. Create Cluster
Register with [Weaviate Clou](https://console.weaviate.cloud/dashboard) and create a cluster. 
Record the cluster connection information: `cluster URL` and `Authentication`.
![WeaviateConsole.jpg](images%2FWeaviateConsole.jpg)

#### Step2. Modify the configuration
We have provided two configuration files for testing Weaviate Cloud:
- [weaviate_cloud_standard_arxiv-titles-384-angular.json](experiments/configurations/weaviate_cloud_standard_arxiv-titles-384-angular.json)
- [weaviate_cloud_standard_laion-768-5m-ip.json](experiments/configurations/weaviate_cloud_standard_laion-768-5m-ip.json)

You need to write the cluster connection information obtained in Step 1 into the configuration files. 
Modify the `connection_params` section of the files and update the values for `host` and `api_key`. 
The `host` corresponds to the `cluster URL`, and the `api_key` is the `Authentication`.

Here is an example of how the modified section may look:

```shell
"connection_params": {
  "host": "https://your_host.weaviate.cloud",
  "port": 8090,
  "timeout_config": 2000,
  "api_key": "your_api_key"
},
```

### Step3. Run the tests
```shell
python3 run.py --engines *weaviate*
```

### Step4. View the test results
```shell
cd results
grep -E 'rps|mean_precision' $(ls -t)
```
![WeaviateResults.jpg](images%2FWeaviateResults.jpg)

### Qdrant
#### Step1. Create Cluster
Register with [Qdrant Cloud](https://cloud.qdrant.io/) and create a cluster. 
Record the cluster connection information: `URL` and `API key`.
![QdrantConsole.jpg](images%2FQdrantConsole.jpg)

#### Step2. Modify the configuration
We have provided three configuration files for testing Qdrant:
- [qdrant_cloud_hnsw_2c16g_storage_optimized_laion-768-5m-ip.json](experiments/configurations/qdrant_cloud_hnsw_2c16g_storage_optimized_laion-768-5m-ip.json)
- [qdrant_cloud_hnsw_2c16g_storage_optimized_arxiv-titles-384-angular.json](experiments/configurations/qdrant_cloud_hnsw_2c16g_storage_optimized_arxiv-titles-384-angular.json)
- [qdrant_cloud_quantization_2c16g_storage_optimized_laion-768-5m-ip.json](experiments/configurations/qdrant_cloud_quantization_2c16g_storage_optimized_laion-768-5m-ip.json)

You need to write the cluster connection information obtained in Step 1 into the configuration files. 
Modify the `connection_params` section of the files and update the values for `host` and `api_key`. 
Please note that for the `connection_params` section, you need to remove the `port` from the end of the `host` string.
Here is an example of how the modified section may look:

```shell
"connection_params": {
  "host": "https://your_host.aws.cloud.qdrant.io",
  "port": 6333,
  "grpc_port": 6334,
  "prefer_grpc": false,
  "api_key": "your_api_key"
},
```

### Step3. Run the tests
```shell
python3 run.py --engines *qdrant*
```

### Step4. View the test results
```shell
cd results
grep -E 'rps|mean_precision' $(ls -t)
```
![QdrantResults.jpg](images%2FQdrantResults.jpg)