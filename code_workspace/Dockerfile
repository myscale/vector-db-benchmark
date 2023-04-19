FROM harbor.internal.moqi.ai/mqdb/mqdb-test-base:1.1

ARG apt_archive="https://mirrors.tuna.tsinghua.edu.cn"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update --allow-releaseinfo-change
RUN apt-get install --yes ca-certificates bash python3 tar wget git python3-pip curl python3-pika

RUN python3 -m pip install typer h5py tqdm elasticsearch pymilvus==2.1.3 qdrant-client redis weaviate-cli -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com
RUN python3 -m pip install pinecone-client -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com
RUN python3 -m pip install pip install -U clickhouse-connect -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com
RUN python3 -m pip install pip install pyproximase==0.7.0 -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com
RUN python3 -m pip install pip install cos-python-sdk-v5 -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

# 加入 code with datasets
COPY vector-db-benchmark.tar.gz /home/.
WORKDIR /home/vector-db-benchmark
RUN tar -zxvf /home/vector-db-benchmark.tar.gz -C /home/.
CMD ["sh","-c","tail -f /dev/null"]
#CMD ["sh","-c","python run.py && tail -f /dev/null"]
