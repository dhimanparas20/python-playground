# QdrantUtil + DocumentUtil — Production-Ready Qdrant Toolkit

Two clean classes for vector work:

| Class | File | Job |
|-------|------|-----|
| **`QdrantUtil`** | `qdrant_util.py` | Collections, upsert/store, search, filters, scroll, delete, ingest into Qdrant |
| **`DocumentUtil`** | `qdrant_docs.py` | Load / split / embed documents (PDF, MD, HTML, CSV, TXT, DOCX, URL, directory) |

Adding points to Qdrant always goes through **`QdrantUtil`**. Loading and chunking stay on **`DocumentUtil`**.

## Installation

```bash
uv add qdrant-client
# optional for DocumentUtil + real embeddings:
uv add langchain-community langchain-text-splitters
uv add pymupdf beautifulsoup4 unstructured   # pdf / html / md / docx
uv add langchain-openai                       # openai / openrouter embeddings
uv add langchain-google-genai                 # google embeddings
```

## Docker Compose

```bash
docker compose -f composefiles/qdrant-compose.yml up -d
# Dashboard: http://localhost:6333/dashboard
# QDRANT_URL=http://localhost:6333
```

## Quick Start

```python
from modules.qdrant import QdrantUtil, DocumentUtil, get_embeddings

emb = get_embeddings("openai")
qd = QdrantUtil(collection="docs", embeddings=emb)  # default collection for all methods
docs = DocumentUtil(embeddings=emb, chunk_size=1000, chunk_overlap=100)

# Load + split + embed, then store
out = docs.pipeline("./handbook.pdf")
qd.create_collection(vector_size=len(out["vectors"][0]))
qd.bulk_upsert(out["points"])

# Or one-shot ingest
qd.ingest("./wiki", strategy="recursive")

# Search (uses default collection)
hits = qd.search(query="how does caching work?", top_k=5)
# Override per call: qd.search(query="...", collection="other")
```

Both modules call ``load_dotenv()`` on import (needs ``python-dotenv``, already in this repo).

Offline demos (no Docker / API keys):

```bash
uv run python -m modules.qdrant.qdrant_util   # QdrantUtil (+ DocumentUtil ingest)
uv run python -m modules.qdrant.qdrant_docs    # DocumentUtil load/split/embed/bulk
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `QDRANT_URL` | `http://localhost:6333` | Server URL (`:memory:` for embedded demo) |
| `QDRANT_API_KEY` | — | API key (optional for local compose) |
| `VECTOR_DIMENSION` | `1536` | Default vector size for new collections |
| `OPENAI_API_KEY` / `OPENAI_EMBEDDINGS_MODEL` | `text-embedding-3-small` | OpenAI embeddings |
| `GOOGLE_API_KEY` / `GEMINI_API_KEY` / `GOOGLE_EMBEDDINGS_MODEL` | `models/text-embedding-004` | Google embeddings |
| `OPENROUTER_API_KEY` / `OPENROUTER_EMBEDDINGS_MODEL` | `openai/text-embedding-3-small` | OpenRouter embeddings |
| `AI_GATEWAY_API_KEY` / `VERCEL_OIDC_TOKEN` | — | Vercel AI Gateway key |
| `AI_GATEWAY_EMBEDDINGS_MODEL` / `AI_GATEWAY_MODEL` | `openai/text-embedding-3-small` | Gateway embedding model |

---

# QdrantUtil

## Constructor

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str \| None` | env / localhost | Qdrant URL, `:memory:`, or local path |
| `api_key` | `str \| None` | env | API key |
| `collection` | `str \| None` | env `QDRANT_COLLECTION` | Default collection for all methods |
| `prefer_grpc` | `bool` | `False` | Prefer gRPC |
| `timeout` | `int` | `30` | Client timeout (seconds) |
| `default_vector_size` | `int \| None` | env / `1536` | Default collection vector size |
| `default_distance` | `str` | `"Cosine"` | `Cosine` / `Euclid` / `Dot` / `Manhattan` |
| `embeddings` | `Any \| None` | `None` | Object with `embed_query` / `embed_documents` |
| `check_compatibility` | `bool` | `False` | Client/server version check |

## Collections

```python
qd.list_collections()
qd.collection_exists("docs")
qd.create_collection("docs")
qd.create_collection("docs", vector_size=768, distance="Cosine", recreate=True)
qd.get_collection_info("docs")
qd.delete_collection("docs")
qd.flush_all()
```

## Store & Upsert

```python
qd.store("projects", {"name": "Alpha", "summary": "..."}, text_field="summary")
qd.upsert_points("docs", [{"id": "p1", "vector": [...], "payload": {...}}])
qd.add_texts("docs", ["chunk a", "chunk b"], metadatas=[{"src": "a"}, {"src": "b"}])
qd.add_documents("docs", chunks)   # LangChain Documents or {page_content, metadata}
```

## Search / Read / Delete

```python
qd.search("docs", query="pricing", top_k=10, score_threshold=0.7, filters={"lang": "en"})
qd.search("docs", query_vector=my_vec, top_k=5)
qd.count("docs")
qd.get("docs", "point-id")
qd.scroll("docs", limit=50, filters={"type": "faq"})
qd.scroll_all("docs", batch_size=100)
qd.find_by_filter("docs", {"session_id": "abc"})
qd.delete_points("docs", ["id1", "id2"])
qd.delete_by_filter("docs", {"session_id": "abc"})
```

## Ingest helpers

```python
# Any supported source → DocumentUtil.prepare → add_documents
qd.ingest("docs", "README.md", strategy="markdown")
qd.ingest("docs", "./papers", strategy="recursive")
qd.ingest("docs", ["a.pdf", "b.txt", "https://example.com"])

# Markdown file or directory
qd.add_markdown("docs", "guide.md")
qd.add_markdown("docs", "./wiki", is_directory=True)
```

## Bulk actions (QdrantUtil)

```python
# Many dict records → embed + upsert
qd.bulk_store("docs", [{"body": "a"}, {"body": "b"}], text_field="body", batch_size=64)

# Chunked raw vector upsert
qd.bulk_upsert("docs", points, batch_size=64)

# Many IDs
qd.bulk_get("docs", ["id1", "id2", "id3"])
qd.bulk_delete("docs", point_ids=["id1", "id2"])
qd.bulk_delete("docs", filters={"source": "old.md"})

# Many queries
results = qd.bulk_search("docs", ["q1", "q2", "q3"], top_k=5)
# → [hits_q1, hits_q2, hits_q3]

# Many collections
qd.bulk_create_collections(["a", "b", "c"], vector_size=1536)
qd.bulk_count(["a", "b", "c"])
qd.bulk_delete_collections(["a", "b"])

# Many files/URLs into one collection
qd.bulk_ingest("docs", ["a.pdf", "b.md", "./wiki"])
```

---

# DocumentUtil

Load → split → embed. **Does not** write to Qdrant.

## Constructor

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `embeddings` | `Any \| None` | `None` | Pre-built embeddings object |
| `embeddings_provider` | `str \| None` | `None` | `openai` / `google` / `openrouter` (builds embeddings) |
| `chunk_size` | `int` | `1000` | Default splitter size |
| `chunk_overlap` | `int` | `100` | Default overlap |

## Loaders

| Method | Input |
|--------|--------|
| `load(source)` | Auto-detect file ext or URL |
| `load_many([sources])` | Many paths/URLs (skips failures) |
| `load_directory(path, glob="**/*.*")` | Mixed files under a folder |
| `load_text` / `load_markdown` / `load_pdf` | Single file |
| `load_html` / `load_csv` / `load_docx` | Single file |
| `load_url(url)` | Web page |

Supported extensions: `.txt`, `.md`, `.markdown`, `.pdf`, `.html`, `.htm`, `.csv`, `.docx`, plus `http(s)://` URLs.

```python
docs = DocumentUtil()
docs.load("notes.txt")
docs.load_pdf("paper.pdf")
docs.load_csv("rows.csv")
docs.load_url("https://example.com/page")
docs.load_directory("./knowledge", glob="**/*.{md,pdf,txt}")
docs.load_many(["a.md", "b.pdf", "https://example.com"])
```

## Split

```python
chunks = docs.split(raw_docs, chunk_size=800, chunk_overlap=100, strategy="recursive")
# strategy: "recursive" | "markdown" | "character"
```

## Embed

```python
docs = DocumentUtil(embeddings_provider="openai")
vec = docs.embed_query("hello")
vecs = docs.embed_documents(["a", "b"])
vecs = docs.embed_chunks(chunks)
```

## Pipeline

```python
from pathlib import Path

# load + split only
chunks = docs.prepare("handbook.pdf")
chunks = docs.prepare(Path("./wiki"), strategy="markdown")
chunks = docs.prepare(["a.txt", "b.md"], split=True)

# load + split + embed (one shot) — any type / Path / URL / dir
out = docs.pipeline(Path("./samples/book.pdf"))
out = docs.pipeline("https://example.com/page")
out = docs.pipeline(["a.pdf", Path("b.md"), "./notes"])
# out["chunks"], out["texts"], out["vectors"], out["metadatas"], out["points"], out["count"]

# Upsert without re-embedding
qd = QdrantUtil(embeddings=docs.embeddings)
qd.bulk_upsert("docs", out["points"])

# Or let QdrantUtil do the whole thing
qd.ingest("docs", Path("./samples/book.pdf"))
```

## Bulk actions (DocumentUtil)

```python
# Many paths/URLs
docs.bulk_load(["a.pdf", "b.md", "https://example.com"])

# Flat chunks from many sources
chunks = docs.bulk_prepare(["a.pdf", "b.md", "./wiki"])

# Per-source map
by_src = docs.bulk_prepare(["a.pdf", "b.md"], separate=True)
# → {"a.pdf": [...], "b.md": [...]}

# Split many groups independently
groups = docs.bulk_split([docs_a, docs_b], strategy="recursive")

# Embed in batches
vectors = docs.bulk_embed(["t1", "t2", "t3"], batch_size=64)
group_vecs = docs.bulk_embed_chunks([chunks_a, chunks_b])
```

---

## Embeddings factory

```python
from modules.qdrant import get_embeddings

emb = get_embeddings("openai")
emb = get_embeddings("google")
emb = get_embeddings("openrouter", model="openai/text-embedding-3-small")

# Vercel AI Gateway (base URL hardcoded: https://ai-gateway.vercel.sh/v1)
emb = get_embeddings("vercel_ai_gateway")
emb = get_embeddings(
    "vercel_ai_gateway",
    model="openai/text-embedding-3-small",
    # api_key from AI_GATEWAY_API_KEY or VERCEL_OIDC_TOKEN
)
```

Providers: `openai` | `google` | `openrouter` | `vercel_ai_gateway`.

Any object with `embed_query` / `embed_documents` works.

---

## Typical flow

```text
files / URLs
    ↓
DocumentUtil.load / prepare     →  chunks (Documents)
    ↓
QdrantUtil.add_documents        →  embed + upsert
    ↓
QdrantUtil.search               →  hits
```

```python
from modules.qdrant import QdrantUtil, DocumentUtil, get_embeddings

emb = get_embeddings("openai")
with QdrantUtil(embeddings=emb) as qd:
    prep = DocumentUtil(embeddings=emb)
    chunks = prep.prepare("./docs", strategy="recursive")
    qd.create_collection("kb")
    qd.add_documents("kb", chunks)
    print(qd.search("kb", query="install guide", top_k=3))
```

---

## Error Handling

```python
from modules.qdrant import QdrantUtil, QdrantException, DocumentUtil, DocumentException

try:
    qd = QdrantUtil()
except QdrantException as e:
    print("Qdrant:", e)

try:
    DocumentUtil().load("missing.pdf")
except DocumentException as e:
    print("Docs:", e)
```

---

## Production Notes

- Core Qdrant path uses **raw `qdrant-client`** only
- LangChain loaders/splitters/embeddings are **lazy-imported** and optional
- `DocumentUtil` never opens a Qdrant connection
- `create_collection` is idempotent (unless `recreate=True`)
- Filters are simple payload equality dicts (AND)
- Context manager closes the Qdrant client
- Run `python -m modules.qdrant.qdrant_util` for an offline demo

## Related

- [Qdrant docs](https://qdrant.tech/documentation/)
- Compose: `composefiles/qdrant-compose.yml`
