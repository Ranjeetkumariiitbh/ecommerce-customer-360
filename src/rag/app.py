import os
import streamlit as st
from dotenv import load_dotenv
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
load_dotenv(os.path.join(ROOT, ".env"))

VECTOR_PATH = os.path.join(ROOT, "data/processed/rag/chroma_db")

st.set_page_config(
    page_title="E-Commerce Customer 360",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 E-Commerce Customer 360")
st.caption("AI-powered analytics assistant using RAG")

if not os.getenv("GROQ_API_KEY"):
    st.error("GROQ_API_KEY missing. Check your .env file.")
    st.stop()

@st.cache_resource
def load_vectorstore():
    embeddings = HuggingFaceEmbeddings(
        model_name="sentence-transformers/all-MiniLM-L6-v2"
    )

    return Chroma(
        persist_directory=VECTOR_PATH,
        embedding_function=embeddings,
        collection_name="ecommerce_customer360"
    )

@st.cache_resource
def load_llm():
    return ChatGroq(
        model="openai/gpt-oss-20b",
        temperature=0
    )

db = load_vectorstore()
llm = load_llm()

prompt = ChatPromptTemplate.from_template("""
You are an E-Commerce Customer 360 analytics assistant.

Answer the user's question using only the retrieved context.
Do not invent statistics, customer details, or business insights.
If the context does not contain the answer, clearly say that
the available knowledge base does not provide that information.

Context:
{context}

Question:
{question}

Answer:
""")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

question = st.chat_input("Ask about customers, orders, or product categories...")

if question:
    st.session_state.messages.append({
        "role": "user",
        "content": question
    })

    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Searching knowledge base..."):
            docs = db.similarity_search(question, k=3)

            context = "\n\n".join(
                doc.page_content for doc in docs
            )

            chain = prompt | llm
            response = chain.invoke({
                "context": context,
                "question": question
            })

            answer = response.content
            st.markdown(answer)

            with st.expander("📚 Retrieved Sources"):
                for i, doc in enumerate(docs, 1):
                    st.markdown(f"**Source {i}**")
                    st.caption(doc.metadata)
                    st.write(doc.page_content[:1000])

    st.session_state.messages.append({
        "role": "assistant",
        "content": answer
    })
