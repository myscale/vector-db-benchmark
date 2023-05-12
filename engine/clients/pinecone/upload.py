import time
from typing import List, Optional
import pinecone
import multiprocessing as mp

from engine.base_client import BaseUploader
from engine.clients.pinecone.config import *


def convert_metadata(metadata_item: dict):
    # value must be a string, number (integer or floating point, gets converted to a 64 bit floating point), boolean or list of strings
    # FixMe Convert null value to '' temporarily.
    for key in metadata_item.keys():
        if isinstance(metadata_item[key], str) \
                or isinstance(metadata_item[key], int) \
                or isinstance(metadata_item[key], float) \
                or isinstance(metadata_item[key], bool):
            continue
        elif isinstance(metadata_item[key], list):
            if len(metadata_item[key]) > 0 and not isinstance(metadata_item[key][0], str):
                # Convert all elements in the list to str.
                metadata_item[key] = [str(i) for i in metadata_item[key]]
        else:
            if metadata_item[key] is not None:
                metadata_item[key] = str(metadata_item[key])
            else:
                metadata_item[key] = ''


class PineconeUploader(BaseUploader):
    index: pinecone.Index = None
    upload_params = {}
    distance: str = None
    vector_count: int = 0

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        pinecone.init(api_key=connection_params.get("api-key", PINECONE_API_KEY),
                      environment=connection_params.get("environment", PINECONE_ENVIRONMENT))
        cls.index = pinecone.Index(index_name=PINECONE_INDEX_NAME)
        cls.upload_params = upload_params
        cls.distance = DISTANCE_MAPPING[distance]
        cls.vector_count = vector_count

    @classmethod
    def upload_batch(
            cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]
    ):
        if len(ids) != len(vectors):
            print("batch upload data is incorrect")

        vectors_multi = []
        for i in range(len(ids)):
            if metadata[0] is not None:
                # make pinecone to recognize this data.
                convert_metadata(metadata[i])
                vectors_multi.append((str(ids[i]), vectors[i], metadata[i]))  # 存储结构化字段
            else:
                vectors_multi.append((str(ids[i]), vectors[i]))
        while True:
            try:
                upsert_response = cls.index.upsert(vectors=vectors_multi)
                if upsert_response["upserted_count"] != len(ids):
                    raise RuntimeError("pinecone upload failed")
                break
            except Exception as e:
                print(f"pinecone upload exception: {e} 🐛 retrying...")
                time.sleep(0.5)

    @classmethod
    def post_upload(cls, distance):
        print(f"pinecone post upload: distance {distance}, cls.distance {cls.distance}")
        while True:
            # make sure index status is ready
            index_description = pinecone.describe_index(name=PINECONE_INDEX_NAME)
            if index_description.status["ready"] and index_description.status["state"] == "Ready":
                print("pinecone index status is Ready!")
            else:
                print("{}".format(index_description.status), end=" ", flush=True)
                time.sleep(2)
                continue
            # make sure vector index count fit datasets
            total_vector_count = cls.index.describe_index_stats().get("total_vector_count", 0)
            if total_vector_count < cls.vector_count:
                print(f"{total_vector_count}", end='🌳', flush=True)
                time.sleep(2)
            else:
                print(f"\npinecone total_vector_count: {total_vector_count}, datasets vector count: {cls.vector_count}")
                break

        return {}
