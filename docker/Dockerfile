FROM mochix/ubuntu_22.04:latest

ARG apt_archive="https://mirrors.tuna.tsinghua.edu.cn"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update --allow-releaseinfo-change

RUN apt-get install --yes ca-certificates bash python3 tar wget git python3-pip curl python3-pika

RUN python3 -m pip install typer h5py tqdm elasticsearch pymilvus qdrant-client redis weaviate-cli -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
RUN python3 -m pip install pinecone-client -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
RUN python3 -m pip install pip install -U clickhouse-connect -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
RUN python3 -m pip install pip install pyproximase==0.7.0 -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
RUN python3 -m pip install pip install cos-python-sdk-v5 -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn

RUN mkdir /home/vector-db-benchmark
WORKDIR /home/vector-db-benchmark
#ENTRYPOINT ["dockerd-entrypoint.sh"]
RUN pwd
RUN ls -lh
CMD ["sh","-c","tail -f /dev/null"]
