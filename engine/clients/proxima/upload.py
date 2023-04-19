import time
from typing import List, Optional

from pyproximase import WriteRequest, DataType, Client, ForwardColumnParam

from engine.base_client import BaseUploader
from engine.clients.proxima.config import *


class ProximaUploader(BaseUploader):
    client: Client = None
    extra_columns_name: list = []
    extra_columns_type: list = []

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        cls.client: Client = Client(connection_params.get("host", host), PROXIMA_GRPC_PORT)
        cls.extra_columns_name = extra_columns_name
        cls.extra_columns_type = extra_columns_type

    @classmethod
    def upload_batch(
            cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]
    ):
        # Set record data format
        index_column_meta = WriteRequest.IndexColumnMeta(name='vector',
                                                         data_type=DataType.VECTOR_FP32,
                                                         dimension=len(vectors[0]))
        forward_column_metas = []
        for index in range(0, len(cls.extra_columns_name)):
            forward_op = WriteRequest.ForwardColumnMeta(name=cls.extra_columns_name[index],
                                                        data_type=convert_H52ProximaType(cls.extra_columns_type[index]))
            forward_column_metas.append(forward_op)

        row_meta = WriteRequest.RowMeta(index_column_metas=[index_column_meta],
                                        forward_column_metas=forward_column_metas)
        # Send 100 records
        rows = []
        for i in range(0, len(ids)):
            row = WriteRequest.Row(
                    primary_key=ids[i],
                    operation_type=WriteRequest.OperationType.INSERT,
                    index_column_values=[vectors[i]],
                    # FixMe Convert null value to '' temporarily.
                    forward_column_values=[metadata[i][key] if metadata[i][key] is not None else ''
                                           for key in metadata[i].keys()] if metadata[i] else None
                )
            # print(f"row is {row}")
            rows.append(row)

        # print(write_request)
        try:
            while True:
                write_request = WriteRequest(collection_name=PROXIMA_COLLECTION_NAME,
                                             rows=rows,
                                             row_meta=row_meta)
                status = cls.client.write(write_request)
                if str(status) == "success":
                    break
                else:
                    print(f"proxima upload status: {status}")
        except Exception as e:
            print(f"proxima upload exception: {e}")

    @classmethod
    def post_upload(cls, distance):
        time.sleep(60*5)
        return {}
