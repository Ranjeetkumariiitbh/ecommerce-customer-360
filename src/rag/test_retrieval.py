import os
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

VECTOR_PATH = os.path.join(
    ROOT, "data/processed/rag/chroma_db"
)

embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2"
)

db = Chroma(
    persist_directory=VECTOR_PATH,
    embedding_function=embeddings,
    collection_name="ecommerce_customer360"
)

queries = [
    "How many customers are in the dataset?",
    "What are the available order statuses?",
    "What product categories are available?",
]

for query in queries:
    print("\n" + "=" * 70)
    print("QUESTION:", query)

    results = db.similarity_search(query, k=2)

    for i, doc in enumerate(results, 1):
        print(f"\nRESULT {i}")
        print("Source:", doc.metadata)
        print(doc.page_content[:1000])

print("\nRAG_RETRIEVAL_TEST=PASS")
