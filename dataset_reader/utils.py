H5_COLUMN_TYPES_MAPPING = {
    "float64": float,
    "float32": float,
    "float": float,
    "int32": int,
    "int": int,
    "integer": int,
    "text": str,
    "string": str,
    "blob": str,
}


def convert_H52py(h5_column_type: str):
    """ Convert the data type of a dataset recorded in an HDF5 file to a Python data type. """
    py_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if py_type is None:
        raise RuntimeWarning(f"Not support h5 column type: {h5_column_type}")
    return py_type
