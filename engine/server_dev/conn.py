from pymilvus import connections
from pymilvus import CollectionSchema, FieldSchema, DataType, Collection
from pymilvus import utility

connections.connect(
  alias="default",
  host='10.10.1.100',
  port='19530'
)
book_id = FieldSchema(
  name="book_id",
  dtype=DataType.INT64,
  is_primary=True,
)
book_name = FieldSchema(
  name="book_name",
  dtype=DataType.VARCHAR,
  max_length=200,
)
word_count = FieldSchema(
  name="word_count",
  dtype=DataType.INT64,
)
book_intro = FieldSchema(
  name="book_intro",
  dtype=DataType.FLOAT_VECTOR,
  dim=2
)
schema = CollectionSchema(
  fields=[book_id, book_name, word_count, book_intro],
  description="Test book search"
)
collection_name = "book"

collection = Collection(
    name=collection_name,
    schema=schema,
    using='default', # 和 alias 应该是一样的
    shards_num=2,
)

# 输出是否有 book 这个 collection
print(utility.has_collection("book"))
# 输出所有的 collection 的名称
print(utility.list_collections())
# 删除 book collection
# utility.drop_collection("book")
# 输出所有的 collection 的名称
print(utility.list_collections())

collection = Collection("book")      # Get an existing collection.
collection.load(replica_number=1)
result = collection.get_replicas()
print(result)

# 连接完表后，可以释放内存
collection.release()
collection.load(replica_number=1)
print(collection.get_replicas())