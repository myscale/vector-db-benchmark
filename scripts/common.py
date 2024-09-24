import hashlib
import os
from typing import Iterator


def convert_string2unique_number(string_: str):
    # Create a new md5 hash object
    hash_object = hashlib.md5(string_.encode())
    # Get the hexadecimal representation of the hash
    hex_dig = hash_object.hexdigest()
    # Convert the hexadecimal to integer
    return int(hex_dig, 16)


def walk_result_file_paths(root_path: str) -> Iterator[str]:
    for cur_root_dir_path, cur_dir_names, cur_file_names in os.walk(root_path):
        if len(cur_file_names) != 0:
            for file in cur_file_names:
                if file.endswith(".json"):
                    yield "{}/{}".format(cur_root_dir_path, file)
                else:
                    print("skip invalid file: {}".format("{}/{}".format(cur_root_dir_path, file)))
