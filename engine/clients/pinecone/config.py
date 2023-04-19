from engine.base_client.distances import Distance

PINECONE_API_KEY = "c78c390e-d939-44c5-8315-e889d10ed5e0"
PINECONE_ENVIRONMENT = "us-east-1-aws"

PINECONE_INDEX_NAME = "benchmark"
# PINECONE_NAME_SPACE="vector_ns"

DISTANCE_MAPPING = {
    Distance.L2: "euclidean",
    Distance.DOT: "dotproduct",
    Distance.COSINE: "cosine"
}
