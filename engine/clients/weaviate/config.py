WEAVIATE_CLASS_NAME = "Benchmark"
WEAVIATE_DEFAULT_PORT = 8090

H5_COLUMN_TYPES_MAPPING = {
    "int": "int",
    "keyword": "string",
    "text": "string",
    "float": "number",
    "float64": "number",
    "float32": "number",
    "geo": "geoCoordinates",
}


# reference: https://weaviate.io/developers/weaviate/configuration/datatypes
def convert_H52WeaviateType(h5_column_type: str):
    py_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if py_type is None:
        raise RuntimeWarning(f"mqdb doesn't support h5 column type: {h5_column_type}")
    return py_type
