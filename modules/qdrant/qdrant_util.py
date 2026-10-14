"""
QdrantUtil — Production-ready Qdrant vector database utility.

Covers collections, point CRUD, semantic search, metadata filters,
batch upsert, scroll, and optional embeddings (LangChain or any
object with embed_query / embed_documents).
"""

from __future__ import annotations

import json
import os
import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Union

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http.models import (
        Distance,
        FieldCondition,
        Filter,
        MatchValue,
        PointStruct,
        VectorParams,
    )
except ImportError as exc:
    raise ImportError("Install qdrant-client: pip install qdrant-client") from exc

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from modules.logger import get_logger

logger = get_logger(__name__)

DistanceName = Literal["Cosine", "Euclid", "Dot", "Manhattan"]
EmbeddingProvider = Literal["openai", "google", "openrouter", "vercel_ai_gateway"]

DEFAULT_URL = "http://localhost:6333"
DEFAULT_VECTOR_SIZE = 1536
_MEMORY_URLS = {":memory:", "memory", "inmemory", "in-memory"}

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
VERCEL_AI_GATEWAY_BASE_URL = "https://ai-gateway.vercel.sh/v1"


class QdrantException(Exception):
    """Raised when a Qdrant operation fails."""


# ──────────────────────────────────────────────
# Optional embeddings factory (lazy imports)
# ──────────────────────────────────────────────


def get_embeddings(
    provider: EmbeddingProvider = "openai",
    model: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Any:
    """
    Build a LangChain embeddings object.

    Requires the matching langchain package for the provider.
    Pass the result into QdrantUtil(embeddings=...).

    Args:
        provider: ``openai``, ``google``, ``openrouter``, or ``vercel_ai_gateway``.
        model: Model name. Falls back to env / provider default.
        api_key: API key. Falls back to provider env vars.

    Returns:
        Embeddings instance with ``embed_query`` / ``embed_documents``.

    Env (per provider):
        openai             — OPENAI_API_KEY, OPENAI_EMBEDDINGS_MODEL
        google             — GOOGLE_API_KEY / GEMINI_API_KEY, GOOGLE_EMBEDDINGS_MODEL
        openrouter         — OPENROUTER_API_KEY, OPENROUTER_EMBEDDINGS_MODEL
        vercel_ai_gateway  — AI_GATEWAY_API_KEY / VERCEL_OIDC_TOKEN,
                             AI_GATEWAY_EMBEDDINGS_MODEL / AI_GATEWAY_MODEL
    """
    provider = provider.lower()  # type: ignore[assignment]

    try:
        if provider == "openai":
            from langchain_openai import OpenAIEmbeddings

            return OpenAIEmbeddings(
                model=model or os.getenv("OPENAI_EMBEDDINGS_MODEL", "text-embedding-3-small"),
                api_key=api_key or os.getenv("OPENAI_API_KEY"),
            )

        if provider == "google":
            from langchain_google_genai import GoogleGenerativeAIEmbeddings

            return GoogleGenerativeAIEmbeddings(
                model=model or os.getenv("GOOGLE_EMBEDDINGS_MODEL", "models/text-embedding-004"),
                google_api_key=api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY"),
            )

        if provider == "openrouter":
            from langchain_openai import OpenAIEmbeddings

            return OpenAIEmbeddings(
                model=model or os.getenv("OPENROUTER_EMBEDDINGS_MODEL", "openai/text-embedding-3-small"),
                api_key=api_key or os.getenv("OPENROUTER_API_KEY"),
                base_url=OPENROUTER_BASE_URL,
                check_embedding_ctx_length=False,
            )

        if provider == "vercel_ai_gateway":
            from langchain_openai import OpenAIEmbeddings

            gateway_key = (
                api_key
                or os.getenv("AI_GATEWAY_API_KEY")
                or os.getenv("VERCEL_OIDC_TOKEN")
            )
            gateway_model = (
                model
                or os.getenv("AI_GATEWAY_EMBEDDINGS_MODEL")
                or os.getenv("AI_GATEWAY_MODEL")
                or "openai/text-embedding-3-small"
            )
            return OpenAIEmbeddings(
                model=gateway_model,
                api_key=gateway_key,
                base_url=VERCEL_AI_GATEWAY_BASE_URL,
                check_embedding_ctx_length=False,
            )

        raise ValueError(
            f"Unknown embeddings provider: {provider}. "
            "Use openai, google, openrouter, or vercel_ai_gateway."
        )
    except ImportError as exc:
        raise ImportError(
            f"Could not load {provider} embeddings. "
            f"Install the matching langchain package. Detail: {exc}"
        ) from exc


class QdrantUtil:
    """
    Production-ready Qdrant utility for collections, vectors, and search.

    Attributes:
        url: Qdrant server URL.
        api_key: Optional API key.
        collection: Optional default collection used when methods omit ``collection=``.
        default_vector_size: Default vector dimension for new collections.
        default_distance: Default distance metric (Cosine / Euclid / Dot / Manhattan).
    """

    def __init__(
        self,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        collection: Optional[str] = None,
        prefer_grpc: bool = False,
        timeout: int = 30,
        default_vector_size: Optional[int] = None,
        default_distance: DistanceName = "Cosine",
        embeddings: Optional[Any] = None,
        check_compatibility: bool = False,
    ) -> None:
        """
        Connect to Qdrant.

        Args:
            url: Server URL. Env ``QDRANT_URL`` or ``http://localhost:6333``.
            api_key: API key. Env ``QDRANT_API_KEY``.
            collection: Default collection name for methods that take ``collection``.
                        Env ``QDRANT_COLLECTION``. Pass ``collection=...`` on a
                        call to override for that call only.
            prefer_grpc: Prefer gRPC when available.
            timeout: Client timeout in seconds.
            default_vector_size: Default size for ``create_collection``.
                                 Env ``VECTOR_DIMENSION`` or 1536.
            default_distance: Default distance metric.
            embeddings: Optional object with ``embed_query(text)`` and
                        ``embed_documents(texts)``. Required for text search/store.
            check_compatibility: Qdrant client version check (off by default).

        Example:
            >>> qd = QdrantUtil()
            >>> qd = QdrantUtil(collection="books", embeddings=get_embeddings("openai"))
        """
        raw_url = (url or os.getenv("QDRANT_URL") or DEFAULT_URL).strip()
        self.url: str = raw_url.rstrip("/")
        self.api_key: Optional[str] = api_key if api_key is not None else os.getenv("QDRANT_API_KEY")
        self.collection: Optional[str] = collection or os.getenv("QDRANT_COLLECTION")
        self.prefer_grpc: bool = prefer_grpc
        self.timeout: int = timeout
        self.default_distance: str = default_distance

        size_env = os.getenv("VECTOR_DIMENSION")
        if default_vector_size is not None:
            self.default_vector_size: int = default_vector_size
        elif size_env:
            self.default_vector_size = int(size_env)
        else:
            self.default_vector_size = DEFAULT_VECTOR_SIZE

        self.embeddings = embeddings

        try:
            if self.url.lower() in _MEMORY_URLS:
                self.client = QdrantClient(location=":memory:")
                logger.info("Connected to in-memory Qdrant")
            elif self.url.startswith("/") or self.url.startswith("./") or self.url.endswith(".db"):
                # Local on-disk path (embedded mode)
                self.client = QdrantClient(path=self.url)
                logger.info("Connected to local Qdrant path %s", self.url)
            else:
                use_https = self.url.startswith("https://")
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message="Api key is used with an insecure connection",
                    )
                    self.client = QdrantClient(
                        url=self.url,
                        api_key=self.api_key,
                        https=use_https,
                        prefer_grpc=prefer_grpc,
                        timeout=timeout,
                        check_compatibility=check_compatibility,
                    )
                self.client.get_collections()
                logger.info("Connected to Qdrant at %s", self.url)
        except QdrantException:
            raise
        except Exception as exc:
            raise QdrantException(f"Could not connect to Qdrant at {self.url}: {exc}") from exc

    # ──────────────────────────────────────────────
    # INTERNAL HELPERS
    # ──────────────────────────────────────────────

    def _require_embeddings(self) -> Any:
        if self.embeddings is None:
            raise QdrantException(
                "No embeddings configured. Pass embeddings=... to QdrantUtil "
                "or use get_embeddings(provider)."
            )
        return self.embeddings

    def _collection(self, collection: Optional[str] = None) -> str:
        """Resolve collection: method arg → instance default → error."""
        name = collection if collection is not None else self.collection
        if not name:
            raise QdrantException(
                "No collection specified. Pass collection=... to the method "
                "or set collection=... when creating QdrantUtil "
                "(or env QDRANT_COLLECTION)."
            )
        return name

    def _embed_query(self, text: str) -> List[float]:
        emb = self._require_embeddings()
        return list(emb.embed_query(text))

    def _embed_documents(self, texts: Sequence[str]) -> List[List[float]]:
        emb = self._require_embeddings()
        if hasattr(emb, "embed_documents"):
            return [list(v) for v in emb.embed_documents(list(texts))]
        return [list(emb.embed_query(t)) for t in texts]

    @staticmethod
    def _distance(name: str) -> Distance:
        key = name.strip().upper()
        try:
            return Distance[key]
        except KeyError as exc:
            raise QdrantException(
                f"Unknown distance '{name}'. Use Cosine, Euclid, Dot, or Manhattan."
            ) from exc

    @staticmethod
    def _new_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def build_filter(conditions: Optional[Dict[str, Any]] = None) -> Optional[Filter]:
        """
        Build a Qdrant Filter from simple key → value matches (AND).

        Args:
            conditions: Payload field equals value, e.g. ``{"session_id": "abc"}``.

        Returns:
            Filter or None if conditions is empty/None.
        """
        if not conditions:
            return None
        return Filter(
            must=[
                FieldCondition(key=key, match=MatchValue(value=value))
                for key, value in conditions.items()
            ]
        )

    # ──────────────────────────────────────────────
    # COLLECTIONS
    # ──────────────────────────────────────────────

    def list_collections(self) -> List[str]:
        """Return all collection names."""
        try:
            return [c.name for c in self.client.get_collections().collections]
        except Exception as exc:
            raise QdrantException(f"Failed to list collections: {exc}") from exc

    def collection_exists(self, collection: Optional[str] = None) -> bool:
        """Return True if the collection exists."""
        collection = self._collection(collection)
        try:
            return self.client.collection_exists(collection_name=collection)
        except Exception as exc:
            # Older servers may lack collection_exists — fall back to list.
            try:
                return collection in self.list_collections()
            except Exception:
                raise QdrantException(f"Failed to check collection '{collection}': {exc}") from exc

    def create_collection(
        self,
        vector_size: Optional[int] = None,
        distance: Optional[str] = None,
        recreate: bool = False,
        collection: Optional[str] = None,
    ) -> bool:
        """
        Create a collection if it does not exist.

        Args:
            collection: Collection name (or instance default).
            vector_size: Vector dimension (default: instance default).
            distance: Distance metric name (default: instance default).
            recreate: If True, delete existing collection first.

        Returns:
            True if created (or recreated), False if it already existed.
        """
        collection = self._collection(collection)
        try:
            if recreate and self.collection_exists(collection):
                self.delete_collection(collection)

            if self.collection_exists(collection):
                return False

            size = vector_size if vector_size is not None else self.default_vector_size
            dist = self._distance(distance or self.default_distance)
            self.client.create_collection(
                collection_name=collection,
                vectors_config=VectorParams(size=size, distance=dist),
            )
            logger.info("Created collection '%s' (size=%s, distance=%s)", collection, size, dist)
            return True
        except Exception as exc:
            raise QdrantException(f"Failed to create collection '{collection}': {exc}") from exc

    def delete_collection(self, collection: Optional[str] = None) -> bool:
        """
        Delete a collection. No-op if missing.

        Returns:
            True if deleted, False if it did not exist.
        """
        collection = self._collection(collection)
        try:
            if not self.collection_exists(collection):
                return False
            self.client.delete_collection(collection_name=collection)
            logger.warning("Deleted collection '%s'", collection)
            return True
        except Exception as exc:
            raise QdrantException(f"Failed to delete collection '{collection}': {exc}") from exc

    def get_collection_info(self, collection: Optional[str] = None) -> Dict[str, Any]:
        """Return collection info as a plain dict."""
        collection = self._collection(collection)
        try:
            if not self.collection_exists(collection):
                raise QdrantException(f"Collection '{collection}' does not exist")
            info = self.client.get_collection(collection_name=collection)
            return info.model_dump() if hasattr(info, "model_dump") else dict(info)
        except QdrantException:
            raise
        except Exception as exc:
            raise QdrantException(f"Failed to get info for '{collection}': {exc}") from exc

    def flush_all(self) -> List[str]:
        """
        Delete every collection.

        Returns:
            Names of deleted collections.
        """
        try:
            names = self.list_collections()
            for name in names:
                self.client.delete_collection(collection_name=name)
            logger.warning("Flushed %d collection(s)", len(names))
            return names
        except Exception as exc:
            raise QdrantException(f"Failed to flush database: {exc}") from exc

    # ──────────────────────────────────────────────
    # POINTS — WRITE
    # ──────────────────────────────────────────────

    def upsert_points(
        self,
        points: List[Dict[str, Any]],
        ensure_collection: bool = True,
        vector_size: Optional[int] = None,
        batch_size: int = 0,
        collection: Optional[str] = None,
    ) -> List[Union[str, int]]:
        """
        Upsert raw points.

        Each point dict needs:
            - ``id`` (optional — auto UUID if missing)
            - ``vector`` (list[float])
            - ``payload`` (optional dict)

        Args:
            collection: Target collection.
            points: List of point dicts.
            ensure_collection: Create collection from first vector size if missing.
            vector_size: Override size when creating the collection.
            batch_size: If > 0, upsert in chunks of this size (safer for large lists).

        Returns:
            List of point IDs written.
        """
        collection = self._collection(collection)
        if not points:
            return []

        try:
            structs: List[PointStruct] = []
            ids: List[Union[str, int]] = []
            for item in points:
                pid = item.get("id") or self._new_id()
                vector = item["vector"]
                payload = item.get("payload") or {}
                structs.append(PointStruct(id=pid, vector=vector, payload=payload))
                ids.append(pid)

            if ensure_collection and not self.collection_exists(collection):
                size = vector_size or len(points[0]["vector"])
                self.create_collection(vector_size=size, collection=collection)

            if batch_size and batch_size > 0:
                for start in range(0, len(structs), batch_size):
                    self.client.upsert(
                        collection_name=collection,
                        points=structs[start : start + batch_size],
                    )
            else:
                self.client.upsert(collection_name=collection, points=structs)
            return ids
        except QdrantException:
            raise
        except Exception as exc:
            raise QdrantException(f"Failed to upsert points into '{collection}': {exc}") from exc


    def store(
        self,
        data: Dict[str, Any],
        text_field: Optional[str] = None,
        text: Optional[str] = None,
        point_id: Optional[str] = None,
        extra_payload: Optional[Dict[str, Any]] = None,
        ensure_collection: bool = True,
        collection: Optional[str] = None,
    ) -> str:
        """
        Embed text and upsert one point with payload.

        Embedding text resolution order:
            1. ``text`` argument
            2. ``data[text_field]`` if ``text_field`` is set
            3. JSON string of entire ``data``

        Args:
            collection: Target collection.
            data: Payload body (stored as-is under payload).
            text_field: Field in ``data`` to embed.
            text: Explicit text to embed (wins over text_field).
            point_id: Custom point ID (UUID if omitted).
            extra_payload: Merged into payload.
            ensure_collection: Create collection if missing.

        Returns:
            Point ID.
        """
        collection = self._collection(collection)
        try:
            if text is not None:
                text_to_embed = text
            elif text_field and text_field in data:
                text_to_embed = str(data[text_field])
            else:
                text_to_embed = json.dumps(data, default=str)

            vector = self._embed_query(text_to_embed)
            pid = point_id or self._new_id()

            payload: Dict[str, Any] = {
                **data,
                "_stored_at": self._utc_now(),
                "_point_id": pid,
                "_embedded_text": text_to_embed[:500],
            }
            if extra_payload:
                payload.update(extra_payload)

            self.upsert_points(
                collection=collection,
                points=[{"id": pid, "vector": vector, "payload": payload}],
                ensure_collection=ensure_collection,
                vector_size=len(vector),
            )
            logger.info("Stored point %s in '%s'", pid, collection)
            return pid
        except QdrantException:
            raise
        except Exception as exc:
            raise QdrantException(f"Failed to store data in '{collection}': {exc}") from exc

    def add_texts(
        self,
        texts: Sequence[str],
        metadatas: Optional[Sequence[Dict[str, Any]]] = None,
        ids: Optional[Sequence[str]] = None,
        batch_size: int = 64,
        ensure_collection: bool = True,
        collection: Optional[str] = None,
    ) -> List[str]:
        """
        Embed and upsert a list of texts (optionally with metadata).

        Args:
            collection: Target collection.
            texts: Texts to embed.
            metadatas: Optional payload per text (same length as texts).
            ids: Optional IDs per text.
            batch_size: Embed/upsert batch size.
            ensure_collection: Create collection if missing.

        Returns:
            List of point IDs.
        """
        collection = self._collection(collection)
        if not texts:
            return []

        if metadatas is not None and len(metadatas) != len(texts):
            raise QdrantException("metadatas length must match texts length")
        if ids is not None and len(ids) != len(texts):
            raise QdrantException("ids length must match texts length")

        try:
            all_ids: List[str] = []
            total = len(texts)

            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                batch_texts = list(texts[start:end])
                vectors = self._embed_documents(batch_texts)

                if ensure_collection and start == 0 and not self.collection_exists(collection):
                    self.create_collection(vector_size=len(vectors[0]), collection=collection)

                points: List[Dict[str, Any]] = []
                for i, (text, vector) in enumerate(zip(batch_texts, vectors)):
                    idx = start + i
                    pid = str(ids[idx]) if ids is not None else self._new_id()
                    payload: Dict[str, Any] = {
                        "text": text,
                        "_stored_at": self._utc_now(),
                        "_point_id": pid,
                    }
                    if metadatas is not None:
                        payload.update(metadatas[idx])
                    points.append({"id": pid, "vector": vector, "payload": payload})
                    all_ids.append(pid)

                self.upsert_points(
                    collection=collection,
                    points=points,
                    ensure_collection=False,
                )

            logger.info("Added %d text(s) to '%s'", len(all_ids), collection)
            return all_ids
        except QdrantException:
            raise
        except Exception as exc:
            raise QdrantException(f"Failed to add texts to '{collection}': {exc}") from exc

    def add_documents(
        self,
        documents: Sequence[Any],
        batch_size: int = 64,
        ensure_collection: bool = True,
        collection: Optional[str] = None,
    ) -> List[str]:
        """
        Upsert LangChain-style documents (``page_content`` + ``metadata``).

        Also accepts plain dicts with ``page_content`` / ``text`` and optional ``metadata``.
        """
        collection = self._collection(collection)
        texts: List[str] = []
        metadatas: List[Dict[str, Any]] = []
        ids: List[str] = []

        for doc in documents:
            if hasattr(doc, "page_content"):
                texts.append(str(doc.page_content))
                meta = dict(getattr(doc, "metadata", {}) or {})
                metadatas.append(meta)
                if "id" in meta:
                    ids.append(str(meta["id"]))
            elif isinstance(doc, dict):
                content = doc.get("page_content", doc.get("text", ""))
                texts.append(str(content))
                meta = dict(doc.get("metadata") or {})
                metadatas.append(meta)
                if "id" in doc:
                    ids.append(str(doc["id"]))
                elif "id" in meta:
                    ids.append(str(meta["id"]))
            else:
                raise QdrantException(
                    "Each document must be a LangChain Document or a dict "
                    "with page_content/text"
                )

        return self.add_texts(
            collection=collection,
            texts=texts,
            metadatas=metadatas,
            ids=ids if len(ids) == len(texts) else None,
            batch_size=batch_size,
            ensure_collection=ensure_collection,
        )

    # ──────────────────────────────────────────────
    # POINTS — READ / SEARCH
    # ──────────────────────────────────────────────

    def count(self, exact: bool = True, collection: Optional[str] = None) -> int:
        """Return number of points in a collection (0 if missing)."""
        collection = self._collection(collection)
        try:
            if not self.collection_exists(collection):
                return 0
            result = self.client.count(collection_name=collection, exact=exact)
            return int(result.count)
        except Exception as exc:
            raise QdrantException(f"Failed to count '{collection}': {exc}") from exc

    def get(
        self,
        point_ids: Union[str, int, Sequence[Union[str, int]]],
        with_payload: bool = True,
        with_vectors: bool = False,
        collection: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch points by ID.

        Returns:
            List of ``{id, payload, vector?}`` dicts.
        """
        collection = self._collection(collection)
        try:
            if isinstance(point_ids, (str, int)):
                ids: List[Union[str, int]] = [point_ids]
            else:
                ids = list(point_ids)

            if not ids or not self.collection_exists(collection):
                return []

            records = self.client.retrieve(
                collection_name=collection,
                ids=ids,
                with_payload=with_payload,
                with_vectors=with_vectors,
            )
            results: List[Dict[str, Any]] = []
            for rec in records:
                item: Dict[str, Any] = {"id": rec.id, "payload": rec.payload}
                if with_vectors:
                    item["vector"] = rec.vector
                results.append(item)
            return results
        except Exception as exc:
            raise QdrantException(f"Failed to get points from '{collection}': {exc}") from exc

    def search(
        self,
        query: Optional[str] = None,
        query_vector: Optional[Sequence[float]] = None,
        top_k: int = 5,
        score_threshold: Optional[float] = None,
        filters: Optional[Dict[str, Any]] = None,
        with_payload: bool = True,
        with_vectors: bool = False,
        collection: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Semantic search. Pass ``query`` (embedded) or ``query_vector``.

        Args:
            collection: Collection name.
            query: Text query (needs embeddings).
            query_vector: Precomputed vector (skips embeddings).
            top_k: Max results.
            score_threshold: Minimum score (optional).
            filters: Simple payload equality filters (AND).
            with_payload: Include payload.
            with_vectors: Include vectors.

        Returns:
            List of ``{id, score, payload, vector?}``.
        """
        collection = self._collection(collection)
        try:
            if not self.collection_exists(collection):
                return []

            if query_vector is not None:
                vector = list(query_vector)
            elif query is not None:
                vector = self._embed_query(query)
            else:
                raise QdrantException("Provide query= or query_vector=")

            response = self.client.query_points(
                collection_name=collection,
                query=vector,
                query_filter=self.build_filter(filters),
                limit=top_k,
                score_threshold=score_threshold,
                with_payload=with_payload,
                with_vectors=with_vectors,
            )

            results: List[Dict[str, Any]] = []
            for point in response.points:
                item: Dict[str, Any] = {
                    "id": point.id,
                    "score": point.score,
                    "payload": point.payload,
                }
                if with_vectors:
                    item["vector"] = point.vector
                results.append(item)
            return results
        except QdrantException:
            raise
        except Exception as exc:
            raise QdrantException(f"Failed to search '{collection}': {exc}") from exc

    def scroll(
        self,
        limit: int = 100,
        offset: Optional[Any] = None,
        filters: Optional[Dict[str, Any]] = None,
        with_payload: bool = True,
        with_vectors: bool = False,
        collection: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Scroll a page of points.

        Returns:
            ``{"points": [...], "next_offset": ...}``
        """
        collection = self._collection(collection)
        try:
            if not self.collection_exists(collection):
                return {"points": [], "next_offset": None}

            records, next_offset = self.client.scroll(
                collection_name=collection,
                scroll_filter=self.build_filter(filters),
                limit=limit,
                offset=offset,
                with_payload=with_payload,
                with_vectors=with_vectors,
            )
            points: List[Dict[str, Any]] = []
            for rec in records:
                item: Dict[str, Any] = {"id": rec.id, "payload": rec.payload}
                if with_vectors:
                    item["vector"] = rec.vector
                points.append(item)
            return {"points": points, "next_offset": next_offset}
        except Exception as exc:
            raise QdrantException(f"Failed to scroll '{collection}': {exc}") from exc

    def scroll_all(
        self,
        batch_size: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        with_payload: bool = True,
        with_vectors: bool = False,
        max_points: Optional[int] = None,
        collection: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Scroll through all matching points (paginated).

        Args:
            max_points: Soft cap on total points returned (None = no cap).
        """
        collection = self._collection(collection)
        all_points: List[Dict[str, Any]] = []
        offset: Any = None

        while True:
            page = self.scroll(
                collection=collection,
                limit=batch_size,
                offset=offset,
                filters=filters,
                with_payload=with_payload,
                with_vectors=with_vectors,
            )
            all_points.extend(page["points"])
            offset = page["next_offset"]

            if max_points is not None and len(all_points) >= max_points:
                return all_points[:max_points]
            if offset is None:
                break

        return all_points

    def find_by_filter(
        self,
        filters: Dict[str, Any],
        limit: int = 100,
        with_payload: bool = True,
        collection: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return points matching payload filters (scroll, no vectors)."""
        collection = self._collection(collection)
        page = self.scroll(
            collection=collection,
            limit=limit,
            filters=filters,
            with_payload=with_payload,
            with_vectors=False,
        )
        return page["points"]

    # ──────────────────────────────────────────────
    # POINTS — DELETE
    # ──────────────────────────────────────────────

    def delete_points(
        self,
        point_ids: Sequence[Union[str, int]],
        collection: Optional[str] = None,
    ) -> int:
        """
        Delete points by ID.

        Returns:
            Number of IDs requested for deletion (0 if collection missing).
        """
        collection = self._collection(collection)
        try:
            ids = list(point_ids)
            if not ids or not self.collection_exists(collection):
                return 0
            self.client.delete(collection_name=collection, points_selector=ids)
            return len(ids)
        except Exception as exc:
            raise QdrantException(f"Failed to delete points from '{collection}': {exc}") from exc

    def delete_by_filter(
        self,
        filters: Dict[str, Any],
        collection: Optional[str] = None,
    ) -> bool:
        """
        Delete all points matching payload filters.

        Returns:
            True if delete was issued, False if collection missing / empty filter.
        """
        collection = self._collection(collection)
        try:
            if not filters or not self.collection_exists(collection):
                return False
            qfilter = self.build_filter(filters)
            self.client.delete(collection_name=collection, points_selector=qfilter)
            return True
        except Exception as exc:
            raise QdrantException(
                f"Failed to delete by filter from '{collection}': {exc}"
            ) from exc

    # ──────────────────────────────────────────────
    # BULK ACTIONS
    # ──────────────────────────────────────────────

    def bulk_store(
        self,
        records: Sequence[Dict[str, Any]],
        text_field: Optional[str] = None,
        texts: Optional[Sequence[str]] = None,
        ids: Optional[Sequence[str]] = None,
        batch_size: int = 64,
        ensure_collection: bool = True,
        collection: Optional[str] = None,
    ) -> List[str]:
        """
        Embed and upsert many dict records in batches.

        Args:
            collection: Target collection.
            records: Payload dicts (one per point).
            text_field: Field to embed from each record.
            texts: Optional explicit embed texts (same length as records).
            ids: Optional point IDs (same length as records).
            batch_size: Embed + upsert chunk size.
            ensure_collection: Create collection if missing.

        Returns:
            List of point IDs.
        """
        collection = self._collection(collection)
        if not records:
            return []
        if texts is not None and len(texts) != len(records):
            raise QdrantException("texts length must match records length")
        if ids is not None and len(ids) != len(records):
            raise QdrantException("ids length must match records length")

        try:
            all_ids: List[str] = []
            total = len(records)
            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                batch_records = list(records[start:end])

                embed_texts: List[str] = []
                for i, data in enumerate(batch_records):
                    idx = start + i
                    if texts is not None:
                        embed_texts.append(str(texts[idx]))
                    elif text_field and text_field in data:
                        embed_texts.append(str(data[text_field]))
                    else:
                        embed_texts.append(json.dumps(data, default=str))

                vectors = self._embed_documents(embed_texts)
                if ensure_collection and start == 0 and not self.collection_exists(collection):
                    self.create_collection(vector_size=len(vectors[0]), collection=collection)

                points: List[Dict[str, Any]] = []
                batch_ids: List[str] = []
                for i, (data, vector, text_to_embed) in enumerate(
                    zip(batch_records, vectors, embed_texts)
                ):
                    idx = start + i
                    pid = str(ids[idx]) if ids is not None else self._new_id()
                    payload = {
                        **data,
                        "_stored_at": self._utc_now(),
                        "_point_id": pid,
                        "_embedded_text": text_to_embed[:500],
                    }
                    points.append({"id": pid, "vector": vector, "payload": payload})
                    batch_ids.append(pid)

                self.upsert_points(
                    collection=collection,
                    points=points,
                    ensure_collection=False,
                )
                all_ids.extend(batch_ids)

            logger.info("bulk_store wrote %d point(s) to '%s'", len(all_ids), collection)
            return all_ids
        except QdrantException:
            raise
        except Exception as exc:
            raise QdrantException(f"bulk_store failed on '{collection}': {exc}") from exc

    def bulk_upsert(
        self,
        points: List[Dict[str, Any]],
        batch_size: int = 64,
        ensure_collection: bool = True,
        vector_size: Optional[int] = None,
        collection: Optional[str] = None,
    ) -> List[Union[str, int]]:
        """Chunked upsert of raw points (same shape as ``upsert_points``)."""
        collection = self._collection(collection)
        return self.upsert_points(
            collection=collection,
            points=points,
            ensure_collection=ensure_collection,
            vector_size=vector_size,
            batch_size=batch_size,
        )

    def bulk_get(
        self,
        point_ids: Sequence[Union[str, int]],
        with_payload: bool = True,
        with_vectors: bool = False,
        batch_size: int = 256,
        collection: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch many points by ID in batches."""
        collection = self._collection(collection)
        ids = list(point_ids)
        if not ids:
            return []
        results: List[Dict[str, Any]] = []
        for start in range(0, len(ids), batch_size):
            chunk = ids[start : start + batch_size]
            results.extend(
                self.get(
                    chunk,
                    with_payload=with_payload,
                    with_vectors=with_vectors,
                    collection=collection,
                )
            )
        return results

    def bulk_delete(
        self,
        point_ids: Optional[Sequence[Union[str, int]]] = None,
        filters: Optional[Dict[str, Any]] = None,
        collection: Optional[str] = None,
    ) -> int:
        """
        Delete many points by IDs and/or a payload filter.

        Returns:
            Number of IDs deleted when ``point_ids`` is set; ``1`` if only a
            filter delete was issued; ``0`` if nothing to do.
        """
        collection = self._collection(collection)
        deleted = 0
        if point_ids:
            deleted = self.delete_points(point_ids, collection=collection)
        if filters:
            if self.delete_by_filter(filters, collection=collection):
                deleted = deleted or 1
        return deleted

    def bulk_search(
        self,
        queries: Sequence[str],
        top_k: int = 5,
        score_threshold: Optional[float] = None,
        filters: Optional[Dict[str, Any]] = None,
        with_payload: bool = True,
        with_vectors: bool = False,
        collection: Optional[str] = None,
    ) -> List[List[Dict[str, Any]]]:
        """
        Run semantic search for many queries.

        Returns:
            List of hit-lists (one per query, same order).
        """
        collection = self._collection(collection)
        return [
            self.search(
                query=q,
                top_k=top_k,
                score_threshold=score_threshold,
                filters=filters,
                with_payload=with_payload,
                with_vectors=with_vectors,
                collection=collection,
            )
            for q in queries
        ]

    def bulk_create_collections(
        self,
        names: Sequence[str],
        vector_size: Optional[int] = None,
        distance: Optional[str] = None,
        recreate: bool = False,
    ) -> Dict[str, bool]:
        """
        Create many collections.

        Returns:
            ``{name: True if created, False if already existed}``.
        """
        return {
            name: self.create_collection(
                vector_size=vector_size,
                distance=distance,
                recreate=recreate,
                collection=name,
            )
            for name in names
        }

    def bulk_delete_collections(self, names: Sequence[str]) -> Dict[str, bool]:
        """
        Delete many collections.

        Returns:
            ``{name: True if deleted, False if missing}``.
        """
        return {name: self.delete_collection(collection=name) for name in names}

    def bulk_count(self, collections: Sequence[str], exact: bool = True) -> Dict[str, int]:
        """Return ``{collection_name: count}`` for each name."""
        return {name: self.count(exact=exact, collection=name) for name in collections}

    def bulk_ingest(
        self,
        sources: Sequence[str],
        chunk_size: int = 1000,
        chunk_overlap: int = 100,
        strategy: str = "recursive",
        batch_size: int = 64,
        collection: Optional[str] = None,
    ) -> List[str]:
        """
        Ingest many files/URLs/dirs into one collection.

        Returns:
            All point IDs written across sources.
        """
        collection = self._collection(collection)
        all_ids: List[str] = []
        for source in sources:
            ids = self.ingest(
                collection=collection,
                source=source,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                strategy=strategy,
                batch_size=batch_size,
            )
            all_ids.extend(ids)
        logger.info(
            "bulk_ingest wrote %d point(s) from %d source(s) into '%s'",
            len(all_ids),
            len(sources),
            collection,
        )
        return all_ids

    # ──────────────────────────────────────────────
    # INGEST PIPELINE (uses DocumentUtil for load/split)
    # ──────────────────────────────────────────────

    def ingest(
        self,
        source: Union[str, Path, Sequence[Union[str, Path]]],
        chunk_size: int = 1000,
        chunk_overlap: int = 100,
        strategy: str = "recursive",
        batch_size: int = 64,
        use_pipeline: bool = True,
        collection: Optional[str] = None,
    ) -> List[str]:
        """
        Load → split → embed → upsert via ``DocumentUtil``.

        Args:
            collection: Target collection.
            source: File path, URL, directory, ``Path``, or list of those.
            chunk_size / chunk_overlap / strategy: Passed to DocumentUtil.
            batch_size: Embed/upsert batch size.
            use_pipeline: If True (default), use ``DocumentUtil.pipeline`` and
                          ``bulk_upsert`` (embed once). If False, ``prepare`` +
                          ``add_documents`` (util embeds again).

        Returns:
            Point IDs written.
        """
        collection = self._collection(collection)
        from .qdrant_docs import DocumentUtil

        prep = DocumentUtil(
            embeddings=self.embeddings,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
        if use_pipeline:
            if self.embeddings is None:
                raise QdrantException(
                    "ingest(use_pipeline=True) needs embeddings on QdrantUtil"
                )
            out = prep.pipeline(
                source,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                strategy=strategy,  # type: ignore[arg-type]
                batch_size=batch_size,
            )
            return [
                str(i)
                for i in self.bulk_upsert(
                    out["points"],
                    batch_size=batch_size,
                    collection=collection,
                )
            ]

        docs = prep.prepare(
            source,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            strategy=strategy,  # type: ignore[arg-type]
        )
        return self.add_documents(
            documents=docs,
            batch_size=batch_size,
            collection=collection,
        )

    def add_markdown(
        self,
        path: str,
        chunk_size: int = 1000,
        chunk_overlap: int = 100,
        batch_size: int = 64,
        is_directory: bool = False,
        collection: Optional[str] = None,
    ) -> List[str]:
        """
        Load → split (markdown) → upsert a markdown file or directory.

        Convenience wrapper around ``ingest`` with ``strategy="markdown"``.
        """
        collection = self._collection(collection)
        source: Union[str, Sequence[str]] = path
        if is_directory:
            from .qdrant_docs import DocumentUtil

            docs = DocumentUtil().load_directory(path, glob="**/*.md")
            chunks = DocumentUtil(
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
            ).split(docs, strategy="markdown")
            return self.add_documents(
                documents=chunks,
                batch_size=batch_size,
                collection=collection,
            )
        return self.ingest(
            source=source,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            strategy="markdown",
            batch_size=batch_size,
            collection=collection,
        )

    # ──────────────────────────────────────────────
    # LIFECYCLE
    # ──────────────────────────────────────────────

    def close(self) -> None:
        """Close the underlying Qdrant client."""
        try:
            self.client.close()
        except Exception:
            pass

    def __enter__(self) -> "QdrantUtil":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# ──────────────────────────────────────────────
# USAGE EXAMPLES
# ──────────────────────────────────────────────


def example_qdrant_util() -> None:
    """Demonstrate QdrantUtil + DocumentUtil with in-memory Qdrant."""
    from .qdrant_docs import DocumentUtil

    print("=" * 60)
    print("QdrantUtil + DocumentUtil — Usage Examples")
    print("=" * 60)

    class _FakeEmb:
        """Tiny deterministic embeddings for offline demo (dim=4)."""

        def embed_query(self, text: str) -> List[float]:
            n = len(text) or 1
            return [((n + i) % 7) / 7.0 for i in range(4)]

        def embed_documents(self, texts: List[str]) -> List[List[float]]:
            return [self.embed_query(t) for t in texts]

    emb = _FakeEmb()
    qd = QdrantUtil(
        url=":memory:",
        collection="demo_docs",
        default_vector_size=4,
        embeddings=emb,
    )
    docs = DocumentUtil(embeddings=emb, chunk_size=80, chunk_overlap=10)

    print(f"\n--- Collections (before) ---")
    print(qd.list_collections())

    qd.create_collection(vector_size=4)
    print(f"Created: {qd.collection}")
    print(f"Exists:  {qd.collection_exists()}")

    # Store a dict
    print("\n--- store() ---")
    pid = qd.store(
        {"title": "Caching", "body": "Redis and Qdrant speed up retrieval."},
        text_field="body",
    )
    print(f"Stored point: {pid}")

    # bulk_store
    print("\n--- bulk_store() ---")
    bulk_ids = qd.bulk_store(
        records=[
            {"title": "A", "body": "First bulk record about vectors."},
            {"title": "B", "body": "Second bulk record about embeddings."},
        ],
        text_field="body",
        batch_size=16,
    )
    print(f"bulk_store ids: {len(bulk_ids)}")

    # add_texts
    print("\n--- add_texts() ---")
    ids = qd.add_texts(
        texts=[
            "Qdrant is a vector database for semantic search.",
            "DocumentUtil loads PDF, markdown, CSV, HTML, and URLs.",
        ],
        metadatas=[{"kind": "note"}, {"kind": "note"}],
    )
    print(f"Added {len(ids)} texts")

    print("\n--- bulk_search() ---")
    multi = qd.bulk_search(["vector database", "embeddings"], top_k=2)
    print(f"Queries: {len(multi)}, hits/query: {[len(h) for h in multi]}")

    print("\n--- bulk_get / bulk_count ---")
    got = qd.bulk_get(bulk_ids)
    print(f"bulk_get: {len(got)}")
    print(f"bulk_count: {qd.bulk_count([qd.collection])}")

    # DocumentUtil prepare from plain text file
    print("\n--- DocumentUtil.prepare (temp .txt) ---")
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        sample = Path(tmp) / "sample.txt"
        sample.write_text(
            "Python playground ships reusable utils.\n"
            "QdrantUtil handles collections and search.\n"
            "DocumentUtil handles loading and chunking.\n",
            encoding="utf-8",
        )
        chunks = docs.prepare(str(sample), strategy="character", chunk_size=60, chunk_overlap=5)
        print(f"Loaded+split into {len(chunks)} chunk(s)")
        written = qd.add_documents(chunks)
        print(f"Upserted {len(written)} chunk(s)")

    print(f"\nCount: {qd.count()}")

    print("\n--- search() ---")
    hits = qd.search(query="vector database semantic search", top_k=3)
    for h in hits:
        payload = h.get("payload") or {}
        preview = payload.get("text") or payload.get("body") or payload.get("_embedded_text") or ""
        print(f"  score={h['score']:.4f}  {str(preview)[:70]}")

    print("\n--- find_by_filter / scroll ---")
    notes = qd.find_by_filter({"kind": "note"})
    print(f"Filter kind=note: {len(notes)} hit(s)")
    page = qd.scroll(limit=10)
    print(f"Scroll page size: {len(page['points'])}")

    print("\n--- delete ---")
    qd.delete_points([pid])
    print(f"Count after delete: {qd.count()}")
    qd.delete_collection()
    print(f"Collections after drop: {qd.list_collections()}")

    qd.close()
    print("\nDone!")


if __name__ == "__main__":
    example_qdrant_util()
