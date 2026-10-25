import os
from langchain_core.documents import Document
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

KNOWLEDGE_PATH = os.path.join(
    ROOT, "data/processed/rag/knowledge.txt"
)

VECTOR_PATH = os.path.join(
    ROOT, "data/processed/rag/chroma_db"
)

if not os.path.exists(KNOWLEDGE_PATH):
    raise FileNotFoundError(KNOWLEDGE_PATH)

with open(KNOWLEDGE_PATH, "r", encoding="utf-8") as f:
    text = f.read()

sections = [
    section.strip()
    for section in text.split("\n\n")
    if section.strip()
]

documents = [
    Document(
        page_content=section,
        metadata={"source": "knowledge.txt", "section": str(i)}
    )
    for i, section in enumerate(sections)
]

print("Knowledge sections:", len(documents))

embeddings = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2"
)

vectorstore = Chroma.from_documents(
    documents=documents,
    embedding=embeddings,
    persist_directory=VECTOR_PATH,
    collection_name="ecommerce_customer360"
)

print("Vector store:", VECTOR_PATH)
print("Indexed documents:", vectorstore._collection.count())
print("VECTORSTORE_BUILD=PASS")
