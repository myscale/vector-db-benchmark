import hashlib
import json
import os
from typing import Iterator

from common import walk_result_file_paths



def convert_string2unique_number(string_: str):
    # Create a new md5 hash object
    hash_object = hashlib.md5(string_.encode())
    # Get the hexadecimal representation of the hash
    hex_dig = hash_object.hexdigest()
    # Convert the hexadecimal to integer
    return int(hex_dig, 16)


results = []
for file_path in walk_result_file_paths(root_path='cloud_test/CloudTest_v0.0.3'):
    if file_path.find("search") == -1:
        # skip processing upload_results
        continue
    print(convert_string2unique_number(file_path))
    print(file_path)
    with open(file_path, 'r') as f:
       each_json = json.load(f)
    each_json_converted = {
        "hash_code": convert_string2unique_number(f"{each_json['result_group']}-"
                                                  f"{each_json['meta']['engine']['name']}-"
                                                  f"{each_json['meta']['engine']['version']}-"
                                                  f"{each_json['meta']['engine']['commit']}-"
                                                  f"{each_json['meta']['engine']['remark']}-"
                                                  f"{each_json['meta']['engine']['other']}-"
                                                  f"{each_json['meta']['index_type']}-"
                                                  f"{each_json['meta']['dataset']}-"
                                                  f"{each_json['meta']['dataset_group']}-"
                                                  f"{each_json['meta']['dataset_tag']}-"
                                                  f"{each_json['meta']['time_stamp']}"),
        # meta info
        "engine": each_json['meta']['engine']['name'],
        "version": each_json['meta']['engine']['version'],
        "remark": each_json['meta']['engine']['other'],
        "index_type": each_json['meta']['index_type'],
        "dataset": each_json['meta']['dataset'],
        "dataset_group": each_json['meta']['dataset_group'],
        "dataset_tag": each_json['meta']['dataset_tag'],
        "time_stamp": each_json['meta']['time_stamp'],
        "cost": each_json['meta']['monthly_cost']/(100*each_json['search_results']['rps']),
        "monthly_cost": each_json['meta']['monthly_cost'],
        # index parameter
        "index_create_parameter": each_json['index_create_parameter'],
        # search parameter
        "index_search_params": each_json['index_search_parameter'].get('params',
                                                                       each_json['index_search_parameter'].get(
                                                                           'vectorIndexConfig', '')),
        "search_parallel": each_json['index_search_parameter']['parallel'],
        "search_top": each_json['index_search_parameter']['top'],
        # upload parameter
        "upload_parallel": each_json['data_upload_parameter']['parallel'],
        "upload_batch_size": each_json['data_upload_parameter']['batch_size'],
        # search results
        "mean_precisions": each_json['search_results']['mean_precisions'],
        "rps": each_json['search_results']['rps'],
        "p95_time": each_json['search_results']['p95_time'],
        "mean_time": each_json['search_results']['mean_time'],
        # upload results
        "total_upload": each_json['upload_results']['total_time'],

    }
    # print(each_json_converted)
    results.append(each_json_converted)

# sort results
print(results)

sorted_results = sorted(results, key=lambda x: x['mean_precisions'])
print(sorted_results)
with open('benchmark.json', 'w') as f:
    json.dump(sorted_results, f, indent=2, ensure_ascii=False)
