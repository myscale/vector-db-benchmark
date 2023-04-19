#!/usr/bin/env bash
# 准备代码
echo "rm -rf vector-db-benchmark"
rm -rf vector-db-benchmark vector-db-benchmark.tar.gz
echo "mkdir vector-db-benchmark"
mkdir -p vector-db-benchmark
echo "code is coping...."
cp -r ../benchmark vector-db-benchmark
cp -r ../dataset_reader vector-db-benchmark
cp -r ../datasets vector-db-benchmark
cp -r ../engine vector-db-benchmark
cp -r ../experiments vector-db-benchmark
cp -r ../generate_configs vector-db-benchmark
cp -r ../run.py vector-db-benchmark
echo "here is your code tree"
tree vector-db-benchmark -d
echo "after 3s, package it to tar.gz"
sleep 3
tar -czvf vector-db-benchmark.tar.gz vector-db-benchmark

mc rm cos/mqdb-release-1253802058/vector-db-benchmark-space/vector-db-benchmark.tar.gz
mc cp vector-db-benchmark.tar.gz cos/mqdb-release-1253802058/vector-db-benchmark-space