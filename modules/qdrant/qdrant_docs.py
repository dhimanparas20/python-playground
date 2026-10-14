"""
DocumentUtil — load, split/chunk, and embed documents for vector ingest.

Supports text, markdown, PDF, HTML, CSV, DOCX, URLs, and directories.
LangChain loaders/splitters are imported only when a method is called.
Adding points to Qdrant stays on QdrantUtil.
"""

from __future__ import annotations

import os
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Union

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from modules.logger import get_logger

logger = get_logger(__name__)

EmbeddingProvider = Literal["openai", "google", "openrouter", "vercel_ai_gateway"]
SplitStrategy = Literal["recursive", "markdown", "character"]
SourceLike = Union[str, Path]
SourcesLike = Union[SourceLike, Sequence[SourceLike]]

# Extension → logical type key used by the loader map
_EXT_MAP = {
    ".txt": "text",
    ".md": "markdown",
    ".markdown": "markdown",
    ".pdf": "pdf",
    ".html": "html",
    ".htm": "html",
    ".csv": "csv",
    ".docx": "docx",
}


class DocumentException(Exception):
    """Raised when document load / split / embed fails."""


class SimpleDocument:
    """Minimal Document stand-in (page_content + metadata) when LangChain is absent."""

    def __init__(self, page_content: str, metadata: Optional[dict] = None) -> None:
        self.page_content = page_content
        self.metadata: dict = metadata or {}


def _normalize_source(source: SourceLike) -> str:
    """Accept ``str`` or ``pathlib.Path``; return a clean string path/URL."""
    if isinstance(source, Path):
        return str(source)
    return str(source).strip()


def _normalize_sources(sources: Sequence[SourceLike]) -> List[str]:
    return [_normalize_source(s) for s in sources]


def _file_extension(path: str) -> str:
    """Return lowercase file extension including the dot ('' if none)."""
    return Path(path).suffix.lower()


def _is_url(source: str) -> bool:
    s = source.strip().lower()
    return s.startswith(("http://", "https://", "www."))


def _as_documents(raw: Any) -> List[Any]:
    """Normalize loader output to a list of Document-like objects."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    return [raw]


def _doc_metadata(doc: Any) -> Dict[str, Any]:
    if hasattr(doc, "metadata") and isinstance(doc.metadata, dict):
        return dict(doc.metadata)
    if isinstance(doc, dict):
        return dict(doc.get("metadata") or {})
    return {}



class DocumentUtil:
    """
    Load documents from files/URLs/directories, split into chunks, and embed text.

    Does **not** talk to Qdrant — pass chunks to ``QdrantUtil.add_documents``.

    Attributes:
        chunk_size: Default chunk size for splitters.
        chunk_overlap: Default overlap between chunks.
        embeddings: Optional embeddings object (``embed_query`` / ``embed_documents``).
    """

    def __init__(
        self,
        embeddings: Optional[Any] = None,
        embeddings_provider: Optional[EmbeddingProvider] = None,
        chunk_size: int = 1000,
        chunk_overlap: int = 100,
    ) -> None:
        """
        Args:
            embeddings: Pre-built embeddings object.
            embeddings_provider: If set and ``embeddings`` is None, build via
                                 ``get_embeddings(provider)``.
            chunk_size: Default splitter chunk size.
            chunk_overlap: Default splitter overlap.
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

        if embeddings is not None:
            self.embeddings = embeddings
        elif embeddings_provider is not None:
            from .qdrant_util import get_embeddings

            self.embeddings = get_embeddings(embeddings_provider)
        else:
            self.embeddings = None

    # ──────────────────────────────────────────────
    # LOAD — single type
    # ──────────────────────────────────────────────

    def load_text(self, path: SourceLike, encoding: str = "utf-8") -> List[Any]:
        """Load a plain text file (no LangChain required). Accepts ``str`` or ``Path``."""
        path = _normalize_source(path)
        try:
            try:
                from langchain_community.document_loaders import TextLoader

                docs = TextLoader(path, encoding=encoding).load()
            except ImportError:
                text = Path(path).read_text(encoding=encoding)
                docs = [SimpleDocument(text, {"source": path})]
            self._tag_source(docs, path)
            return docs
        except DocumentException:
            raise
        except Exception as exc:
            raise DocumentException(f"Failed to load text '{path}': {exc}") from exc

    def load_markdown(self, path: SourceLike) -> List[Any]:
        """Load a markdown file. Accepts ``str`` or ``Path``."""
        path = _normalize_source(path)
        try:
            from langchain_community.document_loaders import UnstructuredMarkdownLoader
        except ImportError as exc:
            raise ImportError(
                "Install langchain-community (+ unstructured) to load markdown"
            ) from exc

        try:
            docs = UnstructuredMarkdownLoader(path, mode="multi", strategy="fast").load()
            self._tag_source(docs, path)
            return docs
        except Exception as exc:
            raise DocumentException(f"Failed to load markdown '{path}': {exc}") from exc

    def load_pdf(self, path: SourceLike) -> List[Any]:
        """Load a PDF (PyMuPDF). Accepts ``str`` or ``Path``."""
        path = _normalize_source(path)
        try:
            from langchain_community.document_loaders import PyMuPDFLoader
        except ImportError as exc:
            raise ImportError(
                "Install langchain-community and pymupdf to load PDFs"
            ) from exc

        try:
            docs = PyMuPDFLoader(path).load()
            self._tag_source(docs, path)
            return docs
        except Exception as exc:
            raise DocumentException(f"Failed to load PDF '{path}': {exc}") from exc

    def load_html(self, path: SourceLike) -> List[Any]:
        """Load a local HTML file. Accepts ``str`` or ``Path``."""
        path = _normalize_source(path)
        try:
            from langchain_community.document_loaders import BSHTMLLoader
        except ImportError as exc:
            raise ImportError(
                "Install langchain-community and beautifulsoup4 to load HTML"
            ) from exc

        try:
            docs = BSHTMLLoader(path).load()
            self._tag_source(docs, path)
            return docs
        except Exception as exc:
            raise DocumentException(f"Failed to load HTML '{path}': {exc}") from exc

    def load_csv(self, path: SourceLike) -> List[Any]:
        """Load a CSV file (one Document per row by default). Accepts ``str`` or ``Path``."""
        path = _normalize_source(path)
        try:
            from langchain_community.document_loaders import CSVLoader
        except ImportError as exc:
            raise ImportError("Install langchain-community to load CSV") from exc

        try:
            docs = CSVLoader(file_path=path).load()
            self._tag_source(docs, path)
            return docs
        except Exception as exc:
            raise DocumentException(f"Failed to load CSV '{path}': {exc}") from exc

    def load_docx(self, path: SourceLike) -> List[Any]:
        """Load a Word ``.docx`` file. Accepts ``str`` or ``Path``."""
        path = _normalize_source(path)
        try:
            from langchain_community.document_loaders import UnstructuredWordDocumentLoader
        except ImportError as exc:
            raise ImportError(
                "Install langchain-community (+ unstructured) to load DOCX"
            ) from exc

        try:
            docs = UnstructuredWordDocumentLoader(path).load()
            self._tag_source(docs, path)
            return docs
        except Exception as exc:
            raise DocumentException(f"Failed to load DOCX '{path}': {exc}") from exc

    def load_url(self, url: SourceLike) -> List[Any]:
        """Load a web page by URL."""
        url = _normalize_source(url)
        try:
            from langchain_community.document_loaders import WebBaseLoader
        except ImportError as exc:
            raise ImportError("Install langchain-community to load URLs") from exc

        try:
            docs = WebBaseLoader(url).load()
            self._tag_source(docs, url)
            return docs
        except Exception as exc:
            raise DocumentException(f"Failed to load URL '{url}': {exc}") from exc

    def load_directory(
        self,
        directory: SourceLike,
        glob: str = "**/*.*",
        show_progress: bool = True,
        max_concurrency: int = 10,
    ) -> List[Any]:
        """
        Load all matching files under a directory.

        Uses per-file ``load()`` so mixed types (pdf + md + txt) work.
        Unsupported extensions are skipped with a warning.
        Accepts ``str`` or ``Path``.
        """
        directory = _normalize_source(directory)
        root = Path(directory)
        if not root.is_dir():
            raise DocumentException(f"Not a directory: {directory}")

        paths = sorted(str(p) for p in root.glob(glob) if p.is_file())
        if show_progress:
            logger.info("Loading %d file(s) from %s (glob=%s)", len(paths), directory, glob)
        return self.load_many(paths)

    # ──────────────────────────────────────────────
    # LOAD — auto-detect
    # ──────────────────────────────────────────────

    def load(self, source: SourceLike) -> List[Any]:
        """
        Load one source: URL or local file (type from extension).

        Supported: ``.txt``, ``.md``, ``.pdf``, ``.html``, ``.csv``, ``.docx``,
        and ``http(s)://`` URLs. Accepts ``str`` or ``pathlib.Path``.
        """
        source = _normalize_source(source)
        if _is_url(source):
            return self.load_url(source)

        if not os.path.isfile(source):
            raise DocumentException(f"File not found: {source}")

        kind = _EXT_MAP.get(_file_extension(source))
        if kind is None:
            logger.warning("Unsupported file type, skipped: %s", source)
            return []

        loaders = {
            "text": self.load_text,
            "markdown": self.load_markdown,
            "pdf": self.load_pdf,
            "html": self.load_html,
            "csv": self.load_csv,
            "docx": self.load_docx,
        }
        return loaders[kind](source)

    def load_many(self, sources: Sequence[SourceLike]) -> List[Any]:
        """Load many paths/URLs (``str`` or ``Path``). Failures on one source are skipped."""

        def _one(src: str) -> List[Any]:
            try:
                return self.load(src)
            except DocumentException as exc:
                logger.error("%s", exc)
                return []
            except Exception as exc:
                logger.error("Failed to load '%s': %s", src, exc)
                return []

        return list(chain.from_iterable(map(_one, _normalize_sources(sources))))

    # ──────────────────────────────────────────────
    # SPLIT
    # ──────────────────────────────────────────────

    def split(
        self,
        documents: Sequence[Any],
        chunk_size: Optional[int] = None,
        chunk_overlap: Optional[int] = None,
        strategy: SplitStrategy = "recursive",
    ) -> List[Any]:
        """
        Split documents into chunks.

        Args:
            documents: LangChain Documents (or dicts with page_content).
            chunk_size: Override instance default.
            chunk_overlap: Override instance default.
            strategy: ``recursive`` (default), ``markdown``, or ``character``.
        """
        size = chunk_size if chunk_size is not None else self.chunk_size
        overlap = chunk_overlap if chunk_overlap is not None else self.chunk_overlap
        docs = list(documents)
        if not docs:
            return []

        try:
            try:
                if strategy == "markdown":
                    from langchain_text_splitters import MarkdownTextSplitter

                    splitter = MarkdownTextSplitter(chunk_size=size, chunk_overlap=overlap)
                elif strategy == "character":
                    from langchain_text_splitters import CharacterTextSplitter

                    splitter = CharacterTextSplitter(chunk_size=size, chunk_overlap=overlap)
                else:
                    from langchain_text_splitters import RecursiveCharacterTextSplitter

                    splitter = RecursiveCharacterTextSplitter(
                        chunk_size=size,
                        chunk_overlap=overlap,
                    )
                chunks = splitter.split_documents(docs)
            except ImportError:
                chunks = self._simple_split(docs, size=size, overlap=overlap)

            logger.debug(
                "Split %d doc(s) → %d chunk(s) (strategy=%s, size=%d, overlap=%d)",
                len(docs),
                len(chunks),
                strategy,
                size,
                overlap,
            )
            return chunks
        except DocumentException:
            raise
        except Exception as exc:
            raise DocumentException(f"Failed to split documents: {exc}") from exc

    @staticmethod
    def _simple_split(documents: Sequence[Any], size: int, overlap: int) -> List[Any]:
        """Naive character chunking used when langchain-text-splitters is missing."""
        step = max(size - overlap, 1)
        chunks: List[Any] = []
        for doc in documents:
            if hasattr(doc, "page_content"):
                text = str(doc.page_content)
                meta = dict(getattr(doc, "metadata", {}) or {})
            elif isinstance(doc, dict):
                text = str(doc.get("page_content", doc.get("text", "")))
                meta = dict(doc.get("metadata") or {})
            else:
                text = str(doc)
                meta = {}

            if not text:
                continue
            if len(text) <= size:
                chunks.append(SimpleDocument(text, meta))
                continue
            for start in range(0, len(text), step):
                piece = text[start : start + size]
                if piece:
                    chunks.append(SimpleDocument(piece, dict(meta)))
        return chunks

    # ──────────────────────────────────────────────
    # EMBED
    # ──────────────────────────────────────────────

    def _require_embeddings(self) -> Any:
        if self.embeddings is None:
            raise DocumentException(
                "No embeddings configured. Pass embeddings=... or "
                "embeddings_provider=... to DocumentUtil."
            )
        return self.embeddings

    def embed_query(self, text: str) -> List[float]:
        """Embed a single query/string."""
        try:
            return list(self._require_embeddings().embed_query(text))
        except DocumentException:
            raise
        except Exception as exc:
            raise DocumentException(f"Failed to embed query: {exc}") from exc

    def embed_documents(self, texts: Sequence[str]) -> List[List[float]]:
        """Embed a list of texts."""
        emb = self._require_embeddings()
        try:
            if hasattr(emb, "embed_documents"):
                return [list(v) for v in emb.embed_documents(list(texts))]
            return [list(emb.embed_query(t)) for t in texts]
        except Exception as exc:
            raise DocumentException(f"Failed to embed documents: {exc}") from exc

    def embed_chunks(self, documents: Sequence[Any]) -> List[List[float]]:
        """Embed ``page_content`` (or ``text``) from each document/chunk."""
        texts: List[str] = []
        for doc in documents:
            if hasattr(doc, "page_content"):
                texts.append(str(doc.page_content))
            elif isinstance(doc, dict):
                texts.append(str(doc.get("page_content", doc.get("text", ""))))
            else:
                texts.append(str(doc))
        return self.embed_documents(texts)

    # ──────────────────────────────────────────────
    # PIPELINE — load + split (+ embed)
    # ──────────────────────────────────────────────

    def prepare(
        self,
        source: SourcesLike,
        chunk_size: Optional[int] = None,
        chunk_overlap: Optional[int] = None,
        strategy: SplitStrategy = "recursive",
        split: bool = True,
    ) -> List[Any]:
        """
        Load source(s) and optionally split into chunks.

        Args:
            source: File path, URL, directory, ``Path``, or a list of those.
            chunk_size / chunk_overlap / strategy: Passed to ``split``.
            split: If False, return raw loaded docs without chunking.

        Returns:
            List of Documents ready for ``QdrantUtil.add_documents``.
        """
        if isinstance(source, (str, Path)):
            src = _normalize_source(source)
            if os.path.isdir(src):
                docs = self.load_directory(src)
            else:
                docs = self.load(src)
        else:
            docs = self.load_many(source)

        if not split:
            return docs
        return self.split(
            docs,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            strategy=strategy,
        )

    def pipeline(
        self,
        source: SourcesLike,
        chunk_size: Optional[int] = None,
        chunk_overlap: Optional[int] = None,
        strategy: SplitStrategy = "recursive",
        split: bool = True,
        batch_size: int = 64,
    ) -> Dict[str, Any]:
        """
        Full pipeline: **load → split → embed** in one call.

        Works with any supported source: PDF, markdown, HTML, CSV, TXT, DOCX,
        URL, directory, ``pathlib.Path``, or a list of those.

        Args:
            source: Same as ``prepare``.
            chunk_size / chunk_overlap / strategy / split: Passed to ``prepare``.
            batch_size: Embedding batch size.

        Returns:
            Dict with:
              - ``chunks`` — Document objects
              - ``texts`` — page_content strings
              - ``vectors`` — embedding vectors (aligned with texts)
              - ``metadatas`` — payload dicts per chunk
              - ``points`` — ready for ``QdrantUtil.bulk_upsert`` /
                ``upsert_points`` (``{vector, payload}``, no ids)
              - ``count`` — number of chunks

        Example:
            >>> out = doc.pipeline(Path("./samples/book.pdf"))
            >>> qd.bulk_upsert("books", out["points"])
            >>> # or re-embed via util: qd.add_documents("books", out["chunks"])
        """
        chunks = self.prepare(
            source,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            strategy=strategy,
            split=split,
        )
        texts = self.texts_from_documents(chunks)
        vectors = self.bulk_embed(texts, batch_size=batch_size) if texts else []
        metadatas = [_doc_metadata(c) for c in chunks]

        points: List[Dict[str, Any]] = []
        for text, vector, meta in zip(texts, vectors, metadatas):
            payload = {**meta, "text": text}
            points.append({"vector": vector, "payload": payload})

        result = {
            "chunks": chunks,
            "texts": texts,
            "vectors": vectors,
            "metadatas": metadatas,
            "points": points,
            "count": len(chunks),
        }
        logger.info(
            "pipeline: source=%s → %d chunk(s), %d vector(s)",
            source if isinstance(source, (str, Path)) else f"{len(list(source))} sources",
            result["count"],
            len(vectors),
        )
        return result

    # ──────────────────────────────────────────────
    # BULK ACTIONS
    # ──────────────────────────────────────────────

    def bulk_load(self, sources: Sequence[SourceLike]) -> List[Any]:
        """Load many paths/URLs (alias of ``load_many``). Accepts ``str`` or ``Path``."""
        return self.load_many(sources)

    def bulk_prepare(
        self,
        sources: Sequence[SourceLike],
        chunk_size: Optional[int] = None,
        chunk_overlap: Optional[int] = None,
        strategy: SplitStrategy = "recursive",
        split: bool = True,
        separate: bool = False,
    ) -> Union[List[Any], Dict[str, List[Any]]]:
        """
        Prepare many sources (load + optional split).

        Args:
            sources: File paths, URLs, directories (``str`` or ``Path``).
            separate: If True, return ``{source: chunks}`` per source.
                      If False, return one flat chunk list.

        Returns:
            Flat list of chunks, or a dict keyed by source when ``separate=True``.
        """
        normalized = _normalize_sources(sources)
        if not separate:
            return self.prepare(
                normalized,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                strategy=strategy,
                split=split,
            )

        out: Dict[str, List[Any]] = {}
        for src in normalized:
            out[src] = self.prepare(
                src,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                strategy=strategy,
                split=split,
            )
        return out

    def bulk_pipeline(
        self,
        sources: Sequence[SourceLike],
        chunk_size: Optional[int] = None,
        chunk_overlap: Optional[int] = None,
        strategy: SplitStrategy = "recursive",
        split: bool = True,
        batch_size: int = 64,
        separate: bool = False,
    ) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        """
        Run ``pipeline`` on many sources.

        Args:
            separate: If True, return ``{source: pipeline_result}``.
                      If False, one combined pipeline result (all sources).
        """
        normalized = _normalize_sources(sources)
        if not separate:
            return self.pipeline(
                normalized,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                strategy=strategy,
                split=split,
                batch_size=batch_size,
            )
        return {
            src: self.pipeline(
                src,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                strategy=strategy,
                split=split,
                batch_size=batch_size,
            )
            for src in normalized
        }

    def bulk_split(
        self,
        document_groups: Sequence[Sequence[Any]],
        chunk_size: Optional[int] = None,
        chunk_overlap: Optional[int] = None,
        strategy: SplitStrategy = "recursive",
    ) -> List[List[Any]]:
        """
        Split many document groups independently.

        Returns:
            List of chunk-lists (one per input group, same order).
        """
        return [
            self.split(
                group,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                strategy=strategy,
            )
            for group in document_groups
        ]

    def bulk_embed(
        self,
        texts: Sequence[str],
        batch_size: int = 64,
    ) -> List[List[float]]:
        """
        Embed many texts in batches.

        Args:
            texts: Strings to embed.
            batch_size: Chunk size for provider calls.
        """
        if not texts:
            return []
        if batch_size <= 0:
            return self.embed_documents(texts)

        vectors: List[List[float]] = []
        for start in range(0, len(texts), batch_size):
            chunk = list(texts[start : start + batch_size])
            vectors.extend(self.embed_documents(chunk))
        return vectors

    def bulk_embed_chunks(
        self,
        document_groups: Sequence[Sequence[Any]],
        batch_size: int = 64,
    ) -> List[List[List[float]]]:
        """
        Embed many chunk groups.

        Returns:
            List of vector-lists (one per document group).
        """
        results: List[List[List[float]]] = []
        for group in document_groups:
            texts = self.texts_from_documents(group)
            results.append(self.bulk_embed(texts, batch_size=batch_size))
        return results

    # ──────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────

    @staticmethod
    def _tag_source(docs: Sequence[Any], source: str) -> None:
        for doc in docs:
            if hasattr(doc, "metadata") and isinstance(doc.metadata, dict):
                doc.metadata.setdefault("source", source)

    @staticmethod
    def texts_from_documents(documents: Sequence[Any]) -> List[str]:
        """Extract page_content/text strings from documents."""
        out: List[str] = []
        for doc in documents:
            if hasattr(doc, "page_content"):
                out.append(str(doc.page_content))
            elif isinstance(doc, dict):
                out.append(str(doc.get("page_content", doc.get("text", ""))))
            else:
                out.append(str(doc))
        return out


# ──────────────────────────────────────────────
# USAGE EXAMPLES
# ──────────────────────────────────────────────


def example_document_util() -> None:
    """Demonstrate DocumentUtil load / split / embed / bulk (offline)."""
    import tempfile
    from pathlib import Path

    print("=" * 60)
    print("DocumentUtil — Usage Examples")
    print("=" * 60)

    class _FakeEmb:
        def embed_query(self, text: str) -> List[float]:
            n = len(text) or 1
            return [((n + i) % 7) / 7.0 for i in range(4)]

        def embed_documents(self, texts: List[str]) -> List[List[float]]:
            return [self.embed_query(t) for t in texts]

    docs = DocumentUtil(
        embeddings=_FakeEmb(),
        chunk_size=80,
        chunk_overlap=10,
    )

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        a = root / "alpha.txt"
        b = root / "beta.txt"
        a.write_text(
            "DocumentUtil loads text, markdown, PDF, HTML, CSV, DOCX, and URLs.\n"
            "It can also split and embed chunks before Qdrant ingest.\n",
            encoding="utf-8",
        )
        b.write_text(
            "Bulk helpers cover load, prepare, split, and embed in batches.\n"
            "Use QdrantUtil.add_documents to write vectors.\n",
            encoding="utf-8",
        )

        print("\n--- load() ---")
        loaded = docs.load(str(a))
        print(f"Loaded {len(loaded)} doc(s) from alpha.txt")
        print(f"Preview: {docs.texts_from_documents(loaded)[0][:70]}...")

        print("\n--- split() ---")
        chunks = docs.split(loaded, strategy="character", chunk_size=60, chunk_overlap=5)
        print(f"Split into {len(chunks)} chunk(s)")

        print("\n--- prepare() ---")
        prepared = docs.prepare(str(b), strategy="character", chunk_size=50, chunk_overlap=5)
        print(f"prepare(beta.txt) → {len(prepared)} chunk(s)")

        print("\n--- pipeline() Path + load/split/embed ---")
        piped = docs.pipeline(a, strategy="character", chunk_size=50, chunk_overlap=5)
        print(
            f"pipeline: count={piped['count']} "
            f"vectors={len(piped['vectors'])} points={len(piped['points'])}"
        )

        print("\n--- bulk_load / bulk_prepare ---")
        many = docs.bulk_load([str(a), str(b)])
        print(f"bulk_load: {len(many)} doc(s)")
        flat = docs.bulk_prepare([str(a), str(b)], strategy="character", chunk_size=50)
        print(f"bulk_prepare (flat): {len(flat)} chunk(s)")
        separate = docs.bulk_prepare(
            [str(a), str(b)],
            strategy="character",
            chunk_size=50,
            separate=True,
        )
        assert isinstance(separate, dict)
        print(f"bulk_prepare (separate): { {k: len(v) for k, v in separate.items()} }")

        print("\n--- bulk_split ---")
        groups = docs.bulk_split([loaded, many], strategy="character", chunk_size=40)
        print(f"bulk_split groups: {[len(g) for g in groups]}")

        print("\n--- embed / bulk_embed ---")
        texts = docs.texts_from_documents(chunks)
        one = docs.embed_query(texts[0] if texts else "hello")
        print(f"embed_query dim: {len(one)}")
        vectors = docs.bulk_embed(texts, batch_size=8)
        print(f"bulk_embed: {len(vectors)} vector(s)")
        group_vecs = docs.bulk_embed_chunks(groups, batch_size=8)
        print(f"bulk_embed_chunks: {[len(v) for v in group_vecs]}")

        print("\n--- load_directory ---")
        dir_docs = docs.load_directory(str(root), glob="*.txt")
        print(f"directory load: {len(dir_docs)} doc(s)")

    print("\nDone!")
    print("Tip: pass embeddings_provider='openai'|'google'|'openrouter'|'vercel_ai_gateway'")
    print("     or embeddings=get_embeddings(...) — .env is loaded via load_dotenv().")


if __name__ == "__main__":
    example_document_util()
