import time
import uuid
from typing import List, Optional

from weaviate import Client

from engine.base_client.upload import BaseUploader
from engine.clients.weaviate.config import WEAVIATE_CLASS_NAME, WEAVIATE_DEFAULT_PORT, generateWeaviateClient


class WeaviateUploader(BaseUploader):
    client = None
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        cls.client = generateWeaviateClient(connection_params={**connection_params}, host=host)
        cls.upload_params = upload_params

    @staticmethod
    def _update_geo_data(data_object):
        if data_object is None:
            return None
        keys = data_object.keys()
        for key in keys:
            if isinstance(data_object[key], dict):
                if lat := data_object[key].pop("lat", None):
                    data_object[key]["latitude"] = lat
                if lon := data_object[key].pop("lon", None):
                    data_object[key]["longitude"] = lon

        return data_object

    @classmethod
    def upload_batch(
            cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]
    ):
        cls.client.batch.configure(batch_size=cls.upload_params.get("batch_size", 100), timeout_retries=3, dynamic=True)

        with cls.client.batch as batch:
            for id_, vector, data_object in zip(ids, vectors, metadata):
                # Handling latitude and longitude data
                data_object = cls._update_geo_data(data_object)
                while True:
                    try:
                        batch.add_data_object(
                            data_object=data_object or {},
                            class_name=WEAVIATE_CLASS_NAME,
                            uuid=uuid.UUID(int=id_).hex,
                            vector=vector,
                        )
                        break
                    except Exception as e:
                        print(f"weaviate upload exp: {e}")
                        time.sleep(3)
            # batch.create_objects()
