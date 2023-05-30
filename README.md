# vector-db-benchmark: a benchmark for vector databases

This benchmark evaluates typical workloads on cloud services for vector databases. It is a fork of [qdrant/vector-db-benchmark](https://github.com/qdrant/vector-db-benchmark/), and accommodates cloud services such as MyScale, Pinecone, Weaviate, Qdrant, and Zilliz.

A summary of the benchmark results can be found in our [blog post](https://blog.myscale.com/2023/05/17/myscale-outperform-special-vectordb/). For setup, datasets and detailed results, visit: <https://myscale.github.io/benchmark>.

## Run the Benchmark

First, install the necessary libraries on the client used for the benchmark.

```shell
pip install -r requirements.txt
```

Afterwards, follow the [Step-by-Step Guide for Benchmark](docs/step-by-step-guide-for-benchmark.md) to execute the benchmark for each cloud service. You can refer to [Results Visualize](docs/results-visualize.md) for visualizing the test results.
