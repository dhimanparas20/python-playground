import os
from pathlib import Path

os.environ.setdefault("LOG_FORMAT", "text")
os.environ.setdefault("LOG_LEVEL", "INFO")

from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from rich import print

from modules.llm_utils import (
    DEFAULT_INPUT_PRICE_PER_MILLION,
    DEFAULT_OUTPUT_PRICE_PER_MILLION,
    DEFAULT_PROVIDER,
    collect_tools,
    create_llm,
    estimate_cost,
    extract_usage,
    get_agent,
    run_stream,
)
from modules.logger import get_logger
from modules.qdrant import DocumentUtil, QdrantUtil, get_embeddings

log = get_logger(__name__, json_output=False)

COLLECTION = "books"
PDF_PATH = Path("./samples/book.pdf")
TOP_K = 5
EXIT_CMDS = {"/q", "/quit", "/exit"}
THREAD_ID = "book-chat"

SYSTEM_PROMPT = (
    "You are a book assistant that helps readers by answering questions from the book. "
    "Use the retrieve_from_book tool to look up relevant passages before answering. "
    "Base your answers on the retrieved text; quote or paraphrase when useful. "
    "If the book does not cover the question, say so clearly. "
    "Keep answers clear and helpful."
)

embeddings = get_embeddings(provider="openai", model="text-embedding-3-small")
qd = QdrantUtil(collection=COLLECTION, embeddings=embeddings)
doc = DocumentUtil(embeddings=embeddings)

# --- optional ingest (already done once; set True to re-embed / re-upload) ---
INGEST = False
if INGEST:
    out = doc.pipeline(PDF_PATH)
    qd.create_collection(vector_size=len(out["vectors"][0]))
    ids = qd.bulk_upsert(out["points"])
    log.success("Ingest complete: %d points → %s", len(ids), qd.collection)


@tool
def retrieve_from_book(query: str, top_k: int = TOP_K) -> str:
    """Search the indexed book for passages relevant to the query.

    Call this whenever you need facts, quotes, or context from the book
    before answering the user.
    """
    hits = qd.search(query=query, top_k=top_k)
    if not hits:
        return "No relevant passages found in the book."

    parts: list[str] = []
    for i, h in enumerate(hits, 1):
        payload = h.get("payload") or {}
        text = (payload.get("text") or payload.get("_embedded_text") or "").strip()
        score = h.get("score")
        score_s = f"{score:.4f}" if isinstance(score, (int, float)) else str(score)
        source = payload.get("source", "")
        parts.append(f"[{i}] score={score_s} source={source}\n{text}")
    return "\n\n---\n\n".join(parts)


def build_agent():
    tools = collect_tools(extra=[retrieve_from_book], mcp=False)
    llm = create_llm(
        provider=DEFAULT_PROVIDER,
        input_price_per_million=DEFAULT_INPUT_PRICE_PER_MILLION,
        output_price_per_million=DEFAULT_OUTPUT_PRICE_PER_MILLION,
    )
    return get_agent(
        llm=llm,
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
        checkpointer=InMemorySaver(),
        input_price_per_million=DEFAULT_INPUT_PRICE_PER_MILLION,
        output_price_per_million=DEFAULT_OUTPUT_PRICE_PER_MILLION,
    )


def _print_usage(label: str, usage: dict) -> None:
    print(
        f"[dim]{label}: input={usage['input_tokens']} "
        f"output={usage['output_tokens']} "
        f"total={usage['total_tokens']}[/dim]"
    )
    cost = usage.get("cost")
    if cost:
        print(
            f"[dim]{label} cost_usd: input={cost['input_cost_usd']:.6f} "
            f"output={cost['output_cost_usd']:.6f} "
            f"total={cost['total_cost_usd']:.6f} "
            f"($/1M in={cost['input_price_per_million']} "
            f"out={cost['output_price_per_million']})[/dim]"
        )
    else:
        print(
            f"[dim]{label} cost_usd: (set DEFAULT_INPUT_PRICE_PER_MILLION / "
            "DEFAULT_OUTPUT_PRICE_PER_MILLION in .env)[/dim]"
        )


def chat_loop(agent) -> None:
    config = {"configurable": {"thread_id": THREAD_ID}}
    session_in = session_out = session_total = turns = 0

    print("Book assistant ready. Ask about the book. Type /q, /quit, or /exit to leave.")
    print(
        f"[dim]pricing $/1M: in={DEFAULT_INPUT_PRICE_PER_MILLION} "
        f"out={DEFAULT_OUTPUT_PRICE_PER_MILLION}[/dim]\n"
    )

    while True:
        try:
            user = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nbye")
            break

        if not user:
            continue
        if user.lower() in EXIT_CMDS:
            print("bye")
            break

        try:
            result = run_stream(agent, user, config=config, print_tokens=True)
            usage = extract_usage(result.get("messages") or [], pricing_from=agent)
            session_in += usage["input_tokens"]
            session_out += usage["output_tokens"]
            session_total += usage["total_tokens"]
            turns += 1
            print()
            _print_usage("turn", usage)
            print()
        except Exception as exc:
            log.exception("Agent error")
            print(f"[red]error:[/red] {exc}")

    print()
    print(f"[bold]session total[/bold]  turns={turns}")
    _print_usage(
        "session",
        {
            "input_tokens": session_in,
            "output_tokens": session_out,
            "total_tokens": session_total,
            "cost": estimate_cost(
                session_in,
                session_out,
                DEFAULT_INPUT_PRICE_PER_MILLION,
                DEFAULT_OUTPUT_PRICE_PER_MILLION,
            ),
        },
    )


if __name__ == "__main__":
    print(f"collection={qd.collection}  points={qd.count()}  url={qd.url}")
    chat_loop(build_agent())
