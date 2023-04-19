#!/usr/bin/env bash
# build 镜像上传到 Harbor
HARBOR_USER_NAME="yim@moqi.ai"
HARBOR_USER_PASSWORD="Zhu88jie"
HARBOR_REGISTRY="harbor.internal.moqi.ai"
HARBOR_NAMESPACE="mqdb"
echo $HARBOR_USER_PASSWORD | docker login $HARBOR_REGISTRY --username $HARBOR_USER_NAME --password-stdin
docker build . -t vector-db-benchmark-k8s:latest
docker tag vector-db-benchmark-k8s:latest $HARBOR_REGISTRY/$HARBOR_NAMESPACE/vector-db-benchmark-k8s:latest
docker push $HARBOR_REGISTRY/$HARBOR_NAMESPACE/vector-db-benchmark-k8s:latest