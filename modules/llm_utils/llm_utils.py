"""
Reusable LangChain LLM + agent + tools helpers.

Required packages (install with uv as needed — not installed by this module):

    uv add langchain
    uv add langchain-openai              # openai, openrouter, vercel_ai_gateway
    uv add langchain-google-genai        # google
    uv add langchain-groq                # groq
    uv add python-dotenv                 # optional .env loading
    uv add langchain-mcp-adapters        # external MCP tools
    uv add langchain-community           # FileManagementToolkit and other kits

Environment variables (used when args are omitted):

    Shared:
        MODEL_TEMPERATURE, MAX_TOKENS
        MCP_ENABLED=true|1|yes          # gate MCP loading (default false)

    openai:            OPENAI_API_KEY, OPENAI_MODEL
    google:            GOOGLE_API_KEY (or GEMINI_API_KEY), GOOGLE_MODEL
    openrouter:        OPENROUTER_API_KEY, OPENROUTER_MODEL
    groq:              GROQ_API_KEY, GROQ_MODEL
    vercel_ai_gateway: AI_GATEWAY_API_KEY (or VERCEL_OIDC_TOKEN), AI_GATEWAY_MODEL
                       # base_url is hardcoded — not read from .env

Quick start:
    from modules.llm_utils import create_llm, get_agent, collect_tools, human_msg
    # or: from llm_utils import ...  (when cwd / PYTHONPATH includes this package)

    tools = collect_tools(extra=[my_tool], mcp=True, files=True)
    agent = get_agent(tools=tools, system_prompt="Be helpful.")
    # or configure the model first:
    # llm = create_llm(model_name="gpt-4o", model_provider="openai")
    # agent = get_agent(llm=llm, tools=tools)
    result = agent.invoke({"messages": [human_msg("hi")]})
"""

from __future__ import annotations

import asyncio
import importlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional, Sequence

def _load_dotenv_skip_empty(path: str | Path = ".env") -> None:
    """
    Load .env into os.environ without letting blank ``KEY=`` lines wipe values.

    python-dotenv's last-assignment-wins means a later empty ``AI_GATEWAY_API_KEY=``
    can replace an earlier real key. We keep the last *non-empty* assignment per
    key, and only fill env slots that are missing/blank (do not override a
    non-empty process env).
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv(dotenv_path=path)

    env_path = Path(path)
    if not env_path.is_file():
        return

    nonempty: dict[str, str] = {}
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and value:
            nonempty[key] = value

    for key, value in nonempty.items():
        if not (os.getenv(key) or "").strip():
            os.environ[key] = value


_load_dotenv_skip_empty()

from modules.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Tunables / defaults (edit these — or override via function args / .env)
# ---------------------------------------------------------------------------

DEFAULT_PROVIDER: str = os.getenv("DEFAULT_PROVIDER", "openai")
DEFAULT_SYSTEM_PROMPT: str = os.getenv(
    "DEFAULT_SYSTEM_PROMPT",
    "You are a helpful assistant.",
)
DEFAULT_DEMO_QUERY: str = os.getenv(
    "DEFAULT_DEMO_QUERY",
    "Reply with exactly one short sentence confirming you work.",
)

def _env_float(name: str) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return None
    try:
        return float(str(raw).strip())
    except ValueError:
        return None


# Optional manual USD pricing ($ per 1M tokens). From .env or set here.
# Example: gpt-4o-ish → input 2.50, output 10.00
DEFAULT_INPUT_PRICE_PER_MILLION: Optional[float] = _env_float(
    "DEFAULT_INPUT_PRICE_PER_MILLION"
)
DEFAULT_OUTPUT_PRICE_PER_MILLION: Optional[float] = _env_float(
    "DEFAULT_OUTPUT_PRICE_PER_MILLION"
)

# Attr name used to stash pricing on llm / agent objects
_PRICING_ATTR = "_llm_utils_pricing"

# Root dir for FileManagementToolkit sandbox (None = skip unless passed)
FILE_TOOLS_ROOT: Optional[str] = os.getenv("FILE_TOOLS_ROOT") or None

# Gate MCP so the module works with no MCP servers installed
MCP_ENABLED_DEFAULT = os.getenv("MCP_ENABLED", "false").lower() in ("true", "1", "yes")

# External MCP servers — enable via MCP_ENABLED=true
MCP_TOOLS: dict[str, dict[str, Any]] = {
    # web search via stdio (needs uv + internet)
    "ddg-search": {
        "command": "uvx",
        "transport": "stdio",
        "args": ["duckduckgo-mcp-server"],
        "env": {
            "DDG_SAFE_SEARCH": "MODERATE",
            "DDG_REGION": "in-en",
        },
    },
    # local HTTP MCP (e.g. FastMCP)
    # "real_estate_search": {
    #     "url": os.getenv("DEFAULT_MCP_SERVER_URL", "http://localhost:8000/mcp/"),
    #     "transport": "streamable_http",
    # },
    # remote authenticated MCP
    # "pageindex": {
    #     "transport": "http",
    #     "url": "https://api.pageindex.ai/mcp",
    #     "headers": {"Authorization": "Bearer " + os.getenv("PAGE_INDEX_API_KEY", "")},
    # },
    # "fetch": {"command": "uv", "transport": "stdio", "args": ["run", "mcp-server-fetch"]},
    # "time": {"command": "uv", "transport": "stdio", "args": ["run", "mcp-server-time"]},
    # "firecrawl-mcp": {
    #     "command": "npx",
    #     "transport": "stdio",
    #     "args": ["-y", "firecrawl-mcp"],
    #     "env": {"FIRECRAWL_API_KEY": os.getenv("FIRECRAWL_API_KEY", "")},
    # },
}

Provider = Literal["openai", "google", "openrouter", "groq", "vercel_ai_gateway"]

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
VERCEL_AI_GATEWAY_BASE_URL = "https://ai-gateway.vercel.sh/v1"


@dataclass(frozen=True)
class ProviderConfig:
    module_name: str
    class_name: str
    api_key_env: str
    model_env: str
    api_key_env_fallbacks: tuple[str, ...] = ()
    base_url: Optional[str] = None


MODEL_REGISTRY: dict[str, ProviderConfig] = {
    "openai": ProviderConfig(
        module_name="langchain_openai",
        class_name="ChatOpenAI",
        api_key_env="OPENAI_API_KEY",
        model_env="OPENAI_MODEL",
    ),
    "google": ProviderConfig(
        module_name="langchain_google_genai",
        class_name="ChatGoogleGenerativeAI",
        api_key_env="GOOGLE_API_KEY",
        model_env="GOOGLE_MODEL",
        api_key_env_fallbacks=("GEMINI_API_KEY",),
    ),
    "openrouter": ProviderConfig(
        module_name="langchain_openai",
        class_name="ChatOpenAI",
        api_key_env="OPENROUTER_API_KEY",
        model_env="OPENROUTER_MODEL",
        base_url=OPENROUTER_BASE_URL,
    ),
    "groq": ProviderConfig(
        module_name="langchain_groq",
        class_name="ChatGroq",
        api_key_env="GROQ_API_KEY",
        model_env="GROQ_MODEL",
    ),
    "vercel_ai_gateway": ProviderConfig(
        module_name="langchain_openai",
        class_name="ChatOpenAI",
        api_key_env="AI_GATEWAY_API_KEY",
        model_env="AI_GATEWAY_MODEL",
        api_key_env_fallbacks=("VERCEL_OIDC_TOKEN",),
        base_url=VERCEL_AI_GATEWAY_BASE_URL,
    ),
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _clean_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.replace("\r", "").strip()
        return cleaned or None
    return value


def _env(name: str) -> Optional[str]:
    return _clean_str(os.getenv(name))


def _resolve_api_key(config: ProviderConfig, api_key: Optional[str]) -> Optional[str]:
    if api_key is not None:
        return _clean_str(api_key)
    for env_name in (config.api_key_env, *config.api_key_env_fallbacks):
        value = _env(env_name)
        if value:
            return value
    return None


def _resolve_temperature(model_temperature: Optional[float]) -> Optional[float]:
    if model_temperature is not None:
        return model_temperature
    raw = _env("MODEL_TEMPERATURE")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError as e:
        raise ValueError(f"MODEL_TEMPERATURE must be a float, got: {raw!r}") from e


def _resolve_max_tokens(max_tokens: Optional[int]) -> Optional[int]:
    if max_tokens is not None:
        return max_tokens
    raw = _env("MAX_TOKENS")
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"MAX_TOKENS must be an int, got: {raw!r}") from e


def mcp_enabled(override: Optional[bool] = None) -> bool:
    if override is not None:
        return override
    return MCP_ENABLED_DEFAULT


def _stash_pricing(
    obj: Any,
    input_price_per_million: Optional[float],
    output_price_per_million: Optional[float],
) -> None:
    """Attach optional pricing onto an llm/agent for later cost estimates."""
    if input_price_per_million is None and output_price_per_million is None:
        return
    try:
        setattr(
            obj,
            _PRICING_ATTR,
            {
                "input_price_per_million": input_price_per_million,
                "output_price_per_million": output_price_per_million,
            },
        )
    except Exception:
        pass


def _read_pricing(
    obj: Any = None,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
) -> tuple[Optional[float], Optional[float]]:
    """Resolve prices: explicit args → stashed on obj → module defaults."""
    if input_price_per_million is None or output_price_per_million is None:
        stashed = getattr(obj, _PRICING_ATTR, None) if obj is not None else None
        if isinstance(stashed, dict):
            if input_price_per_million is None:
                input_price_per_million = stashed.get("input_price_per_million")
            if output_price_per_million is None:
                output_price_per_million = stashed.get("output_price_per_million")
    if input_price_per_million is None:
        input_price_per_million = DEFAULT_INPUT_PRICE_PER_MILLION
    if output_price_per_million is None:
        output_price_per_million = DEFAULT_OUTPUT_PRICE_PER_MILLION
    return input_price_per_million, output_price_per_million


def estimate_cost(
    input_tokens: int,
    output_tokens: int,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
) -> Optional[dict[str, float]]:
    """
    Manual USD cost from token counts + $/1M rates.

    Returns None when neither price is set.
    Missing side defaults to 0.0 for that side only.
    """
    input_price_per_million, output_price_per_million = _read_pricing(
        None, input_price_per_million, output_price_per_million
    )
    if input_price_per_million is None and output_price_per_million is None:
        return None

    in_rate = float(input_price_per_million or 0.0)
    out_rate = float(output_price_per_million or 0.0)
    input_cost = (input_tokens / 1_000_000) * in_rate
    output_cost = (output_tokens / 1_000_000) * out_rate
    return {
        "input_cost_usd": input_cost,
        "output_cost_usd": output_cost,
        "total_cost_usd": input_cost + output_cost,
        "input_price_per_million": in_rate,
        "output_price_per_million": out_rate,
    }


# ---------------------------------------------------------------------------
# Message helpers — prefer typed messages over raw dicts
# ---------------------------------------------------------------------------

def system_msg(content: str):
    from langchain_core.messages import SystemMessage

    return SystemMessage(content=content)


def human_msg(content: str):
    from langchain_core.messages import HumanMessage

    return HumanMessage(content=content)


def ai_msg(content: str):
    from langchain_core.messages import AIMessage

    return AIMessage(content=content)


def tool_msg(content: str, tool_call_id: str, name: Optional[str] = None):
    from langchain_core.messages import ToolMessage

    kwargs: dict[str, Any] = {"content": content, "tool_call_id": tool_call_id}
    if name is not None:
        kwargs["name"] = name
    return ToolMessage(**kwargs)


def agent_input(text: str, *, system: Optional[str] = None) -> dict[str, Any]:
    """Build the standard {"messages": [...]} payload for invoke/stream."""
    messages: list[Any] = []
    if system:
        messages.append(system_msg(system))
    messages.append(human_msg(text))
    return {"messages": messages}


# ---------------------------------------------------------------------------
# Usage / response helpers
# ---------------------------------------------------------------------------

def extract_usage(
    messages: Sequence[Any],
    *,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
    pricing_from: Any = None,
) -> dict[str, Any]:
    """
    Sum usage_metadata across AI messages.

    Optionally estimates USD cost when you pass $/1M rates (or stashed
    pricing on pricing_from / module defaults).
    """
    input_tokens = 0
    output_tokens = 0
    total_tokens = 0
    per_message: list[dict[str, Any]] = []
    extras: list[Any] = []

    for msg in messages:
        usage = getattr(msg, "usage_metadata", None)
        if usage:
            per_message.append(dict(usage))
            input_tokens += int(usage.get("input_tokens") or 0)
            output_tokens += int(usage.get("output_tokens") or 0)
            total_tokens += int(usage.get("total_tokens") or 0)

        meta = getattr(msg, "response_metadata", None)
        if meta:
            keep = {
                k: v
                for k, v in dict(meta).items()
                if k in ("model_name", "model", "finish_reason", "system_fingerprint")
                or "cost" in k.lower()
                or "credit" in k.lower()
                or "token" in k.lower()
            }
            if keep:
                extras.append(keep)

    out: dict[str, Any] = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens or (input_tokens + output_tokens),
        "per_message": per_message,
        "response_metadata": extras,
        "cost": None,
    }
    cost = estimate_cost(
        input_tokens,
        output_tokens,
        *_read_pricing(pricing_from, input_price_per_million, output_price_per_million),
    )
    out["cost"] = cost
    return out


def last_ai_text(messages: Sequence[Any]) -> str:
    for msg in reversed(messages):
        if getattr(msg, "type", None) == "ai" or msg.__class__.__name__ == "AIMessage":
            content = getattr(msg, "content", "")
            if isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, dict) and "text" in block:
                        parts.append(str(block["text"]))
                    else:
                        parts.append(str(block))
                return "\n".join(parts)
            return str(content)
    return ""


def print_run_summary(
    label: str,
    result: dict[str, Any],
    *,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
    pricing_from: Any = None,
) -> None:
    messages = result.get("messages") or []
    text = last_ai_text(messages)
    usage = extract_usage(
        messages,
        input_price_per_million=input_price_per_million,
        output_price_per_million=output_price_per_million,
        pricing_from=pricing_from,
    )
    print(f"\n=== {label} ===")
    print("response:", text)
    print(
        "tokens:",
        f"input={usage['input_tokens']} "
        f"output={usage['output_tokens']} "
        f"total={usage['total_tokens']}",
    )
    if usage["cost"]:
        c = usage["cost"]
        print(
            "cost_usd:",
            f"input={c['input_cost_usd']:.6f} "
            f"output={c['output_cost_usd']:.6f} "
            f"total={c['total_cost_usd']:.6f} "
            f"(rates $/1M in={c['input_price_per_million']} "
            f"out={c['output_price_per_million']})",
        )
    if usage["per_message"]:
        print("usage_metadata:", usage["per_message"])
    if usage["response_metadata"]:
        print("response_metadata:", usage["response_metadata"])
    if "structured_response" in result:
        print("structured_response:", result["structured_response"])


# ---------------------------------------------------------------------------
# Tool loaders — @tool / toolkits / MCP / anything BaseTool-like
# ---------------------------------------------------------------------------

def load_file_tools(root_dir: Optional[str] = None) -> list[Any]:
    """Load LangChain FileManagementToolkit tools for a sandbox root."""
    root = root_dir or FILE_TOOLS_ROOT
    if not root:
        return []
    try:
        from langchain_community.agent_toolkits import FileManagementToolkit
    except ImportError as e:
        raise ImportError(
            "langchain-community required for file tools. "
            "uv add langchain-community"
        ) from e

    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    toolkit = FileManagementToolkit(root_dir=str(path))
    tools = toolkit.get_tools()
    print(f"[files] sandbox at {path} -> {[t.name for t in tools]}")
    return list(tools)


def _stringify_mcp_result(result: Any) -> str:
    if isinstance(result, list):
        parts = []
        for item in result:
            if isinstance(item, dict) and "text" in item:
                parts.append(str(item["text"]))
            else:
                parts.append(str(item))
        return "\n".join(parts)
    return str(result)


def _to_sync_tool(async_tool: Any) -> Any:
    """Bridge async MCP tools to sync StructuredTool for sync graphs."""
    from langchain_core.tools import StructuredTool

    def run_sync(**kwargs):
        return _stringify_mcp_result(asyncio.run(async_tool.ainvoke(kwargs)))

    return StructuredTool.from_function(
        func=run_sync,
        name=async_tool.name,
        description=async_tool.description,
        args_schema=async_tool.args_schema,
    )


async def _load_from_mcp_server(server_name: str, server_config: dict) -> list[Any]:
    from langchain_mcp_adapters.client import MultiServerMCPClient

    client = MultiServerMCPClient({server_name: server_config})
    return await client.get_tools()


def load_mcp_tools(
    config: Optional[dict[str, dict[str, Any]]] = None,
    *,
    enabled: Optional[bool] = None,
    as_sync: bool = True,
) -> list[Any]:
    """
    Load tools from every configured MCP server.

    Dead/missing servers are skipped with a one-line notice.
    """
    if not mcp_enabled(enabled):
        return []

    try:
        from langchain_mcp_adapters.client import MultiServerMCPClient  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "langchain-mcp-adapters required for MCP tools. "
            "uv add langchain-mcp-adapters"
        ) from e

    servers = config if config is not None else MCP_TOOLS
    all_tools: list[Any] = []

    for server_name, server_config in servers.items():
        try:
            tools = asyncio.run(_load_from_mcp_server(server_name, server_config))
            print(
                f"[mcp] {server_name}: {len(tools)} tools -> "
                f"{[t.name for t in tools]}"
            )
            if as_sync:
                all_tools.extend(_to_sync_tool(t) for t in tools)
            else:
                all_tools.extend(tools)
        except Exception as error:
            msg = str(error).split("\n")[0][:80]
            print(f"[mcp] {server_name}: unavailable ({msg})")

    return all_tools


def collect_tools(
    *,
    extra: Optional[Sequence[Any]] = None,
    mcp: Optional[bool] = None,
    mcp_config: Optional[dict[str, dict[str, Any]]] = None,
    files: bool = False,
    file_root: Optional[str] = None,
    as_sync_mcp: bool = True,
) -> list[Any]:
    """
    One-stop tool bag for agents.

    - extra: your @tool functions / BaseTool instances / toolkit tools
    - mcp: load MCP_TOOLS (defaults to MCP_ENABLED env)
    - files: load FileManagementToolkit from FILE_TOOLS_ROOT / file_root
    """
    tools: list[Any] = []
    if extra:
        tools.extend(list(extra))
    if files or file_root:
        tools.extend(load_file_tools(file_root))
    tools.extend(load_mcp_tools(mcp_config, enabled=mcp, as_sync=as_sync_mcp))
    return tools


# ---------------------------------------------------------------------------
# LLM + agent factories
# ---------------------------------------------------------------------------

def create_llm(
    model_name: Optional[str] = None,
    api_key: Optional[str] = None,
    model_provider: Provider | str = DEFAULT_PROVIDER,
    model_temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
    **extra: Any,
):
    """
    Factory to create a LangChain chat model instance from ``MODEL_REGISTRY``.

    Resolves model name / API key from args or provider env vars, then
    instantiates the provider class (e.g. ``ChatOpenAI``). Pricing args are
    stashed on the model for later cost estimates and are not sent to the API.

    Args:
        model_name: Chat model id (e.g. ``"gpt-4o"``). When omitted, read from
            the provider's ``model_env`` (e.g. ``OPENAI_MODEL``).
        api_key: Provider API key. When omitted, read from the provider's
            ``api_key_env`` / fallbacks (e.g. ``OPENAI_API_KEY``).
        model_provider: Registry key: ``"openai"``, ``"google"``,
            ``"openrouter"``, ``"groq"``, or ``"vercel_ai_gateway"``. Defaults
            to ``DEFAULT_PROVIDER`` (from the ``DEFAULT_PROVIDER`` env).
        model_temperature: Sampling temperature. When omitted, uses
            ``MODEL_TEMPERATURE`` env (if set); otherwise provider default.
        max_tokens: Max completion tokens. When omitted, uses ``MAX_TOKENS``
            env (if set); otherwise provider default.
        input_price_per_million: Optional USD cost per 1M input tokens. Stored
            on the model for ``estimate_cost`` / ``print_run_summary``; not
            forwarded to the provider.
        output_price_per_million: Optional USD cost per 1M output tokens.
            Same stash behavior as ``input_price_per_million``.
        **extra: Extra kwargs passed through to the chat model constructor
            (e.g. ``timeout``, ``max_retries``, provider-specific options).
            Do not pass ``model`` / ``api_key`` / ``temperature`` /
            ``max_tokens`` / ``base_url`` here — those are set by this factory.

    Returns:
        A LangChain chat model instance for the chosen provider.

    Raises:
        ValueError: Unknown ``model_provider``, or missing model name / API key
            after env resolution.
        ImportError: Provider package / class cannot be imported.
    """
    model_provider = _clean_str(model_provider) or DEFAULT_PROVIDER  # type: ignore[assignment]
    model_name = _clean_str(model_name)

    config = MODEL_REGISTRY.get(model_provider)
    if not config:
        raise ValueError(
            f"Unknown model provider: {model_provider!r}. "
            f"Supported: {list(MODEL_REGISTRY)}"
        )

    resolved_model = model_name or _env(config.model_env)
    resolved_api_key = _resolve_api_key(config, api_key)
    temperature = _resolve_temperature(model_temperature)
    resolved_max_tokens = _resolve_max_tokens(max_tokens)

    if not resolved_model:
        raise ValueError(
            f"No model name provided and env var '{config.model_env}' is not set."
        )
    if not resolved_api_key:
        tried = (config.api_key_env, *config.api_key_env_fallbacks)
        raise ValueError(
            f"No API key provided and none of {list(tried)} are set."
        )

    logger.info(
        "Loading %s model: %s || temp: %s || max_tokens: %s",
        model_provider,
        resolved_model,
        temperature,
        resolved_max_tokens,
    )

    try:
        module = importlib.import_module(config.module_name)
        ModelClass = getattr(module, config.class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(
            f"Could not load '{config.class_name}' from '{config.module_name}'. "
            f"Is the package installed? Error: {e}"
        ) from e

    model_kwargs: dict[str, Any] = {
        "model": resolved_model,
        "api_key": resolved_api_key,
        **extra,
    }
    if temperature is not None:
        model_kwargs["temperature"] = temperature
    if resolved_max_tokens is not None:
        model_kwargs["max_tokens"] = resolved_max_tokens
    if config.base_url:
        model_kwargs["base_url"] = config.base_url

    llm = ModelClass(**model_kwargs)
    _stash_pricing(llm, input_price_per_million, output_price_per_million)
    return llm


def get_agent(
    model_name: Optional[str] = None,
    api_key: Optional[str] = None,
    model_provider: Provider | str = DEFAULT_PROVIDER,
    model_temperature: Optional[float] = None,
    *,
    llm: Optional[Any] = None,
    tools: Optional[Sequence[Any]] = None,
    system_prompt: Optional[Any] = DEFAULT_SYSTEM_PROMPT,
    response_format: Optional[Any] = None,
    middleware: Sequence[Any] = (),
    state_schema: Optional[Any] = None,
    context_schema: Optional[Any] = None,
    checkpointer: Optional[Any] = None,
    store: Optional[Any] = None,
    interrupt_before: Optional[list[str]] = None,
    interrupt_after: Optional[list[str]] = None,
    debug: bool = False,
    name: Optional[str] = None,
    cache: Optional[Any] = None,
    transformers: Optional[Sequence[Any]] = None,
    max_tokens: Optional[int] = None,
    llm_extra: Optional[dict[str, Any]] = None,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
):
    """
    Build a LangChain agent via ``create_llm`` + ``langchain.agents.create_agent``.

    If ``llm`` is passed, that model is used as-is (skip ``create_llm``).
    Otherwise ``create_llm`` is called with the model / pricing / ``llm_extra``
    args. Pricing is stashed on the agent for ``print_run_summary`` /
    ``extract_usage``. When ``llm`` is provided and pricing args are omitted,
    any pricing already stashed on that llm is reused.

    Args:
        model_name: Chat model id when building via ``create_llm``. Ignored if
            ``llm`` is provided. Falls back to the provider ``model_env``.
        api_key: Provider API key when building via ``create_llm``. Ignored if
            ``llm`` is provided. Falls back to the provider ``api_key_env``.
        model_provider: Registry key for ``create_llm`` (``"openai"``,
            ``"google"``, ``"openrouter"``, ``"groq"``,
            ``"vercel_ai_gateway"``). Ignored if ``llm`` is provided. Defaults
            to ``DEFAULT_PROVIDER``.
        model_temperature: Sampling temperature for ``create_llm``. Ignored if
            ``llm`` is provided.
        llm: Pre-built LangChain chat model. When set, skips ``create_llm`` and
            uses this instance as the agent model.
        tools: Tools for the agent — ``@tool`` callables, ``BaseTool``
            instances, dicts, or the list from ``collect_tools``. ``None`` /
            empty means a model-only agent (no tool loop).
        system_prompt: System instructions as ``str`` or ``SystemMessage``.
            Defaults to ``DEFAULT_SYSTEM_PROMPT``. Added at the start of the
            message list when calling the model.
        response_format: Structured-output config (e.g. Pydantic model,
            ``ToolStrategy``, ``ProviderStrategy``, or raw schema). When set,
            the agent produces structured responses.
        middleware: Sequence of LangChain agent middleware instances that
            intercept / modify behavior at various stages.
        state_schema: Optional ``TypedDict`` extending ``AgentState``. Used as
            the base state schema (merged with middleware state) so you can
            add custom fields.
        context_schema: Optional schema type for immutable run-scoped context
            (passed via invoke ``context``), separate from mutable agent state.
        checkpointer: LangGraph checkpointer (e.g. ``InMemorySaver``) for
            short-term / thread memory across turns. Use with
            ``config={"configurable": {"thread_id": "..."}}``.
        store: Optional LangGraph ``BaseStore`` for longer-term cross-thread
            memory.
        interrupt_before: Node names to interrupt before (human-in-the-loop).
            Common values include ``"tools"``.
        interrupt_after: Node names to interrupt after (human-in-the-loop).
        debug: Enable verbose LangGraph execution logging (node runs, state
            updates, transitions).
        name: Optional name for the compiled graph (useful when nesting as a
            subgraph in multi-agent setups).
        cache: Optional LangGraph ``BaseCache`` for caching graph execution.
        transformers: Optional sequence of stream-transformer factories
            registered on the compiled graph (in addition to agent defaults /
            middleware transformers).
        max_tokens: Max completion tokens for ``create_llm``. Ignored if
            ``llm`` is provided.
        llm_extra: Extra kwargs forwarded to ``create_llm`` (``**extra``) when
            building the model. Ignored if ``llm`` is provided.
        input_price_per_million: Optional USD cost per 1M input tokens. Stashed
            on the agent (and on a newly built llm). Overrides / refreshes
            pricing already on a passed-in ``llm`` when set.
        output_price_per_million: Optional USD cost per 1M output tokens. Same
            stash behavior as ``input_price_per_million``.

    Returns:
        A compiled LangGraph ``CompiledStateGraph`` agent (invoke / stream).

    Raises:
        ImportError: ``langchain`` is not installed, or provider package
            missing when ``create_llm`` runs.
        ValueError: Invalid / incomplete model config from ``create_llm``.
    """
    try:
        from langchain.agents import create_agent
    except ImportError as e:
        raise ImportError(
            "langchain is required for get_agent. Install with: uv add langchain"
        ) from e

    if llm is None:
        llm = create_llm(
            model_name=model_name,
            api_key=api_key,
            model_provider=model_provider,
            model_temperature=model_temperature,
            max_tokens=max_tokens,
            input_price_per_million=input_price_per_million,
            output_price_per_million=output_price_per_million,
            **(llm_extra or {}),
        )
        in_price, out_price = input_price_per_million, output_price_per_million
    else:
        # Reuse stashed pricing on the given llm unless caller overrides
        in_price, out_price = _read_pricing(
            llm, input_price_per_million, output_price_per_million
        )
        # If caller passed explicit prices, refresh stash on llm too
        if (
            input_price_per_million is not None
            or output_price_per_million is not None
        ):
            _stash_pricing(llm, input_price_per_million, output_price_per_million)
            in_price, out_price = input_price_per_million, output_price_per_million

    agent_kwargs: dict[str, Any] = {
        "model": llm,
        "tools": list(tools) if tools is not None else None,
        "system_prompt": system_prompt,
        "middleware": middleware,
        "response_format": response_format,
        "state_schema": state_schema,
        "context_schema": context_schema,
        "checkpointer": checkpointer,
        "store": store,
        "interrupt_before": interrupt_before,
        "interrupt_after": interrupt_after,
        "debug": debug,
        "name": name,
        "cache": cache,
    }
    if transformers is not None:
        agent_kwargs["transformers"] = transformers

    agent = create_agent(**agent_kwargs)
    _stash_pricing(agent, in_price, out_price)
    return agent


def run_invoke(agent: Any, text: str, *, config: Optional[dict] = None) -> dict[str, Any]:
    """Invoke agent with a HumanMessage and return full state."""
    return agent.invoke(agent_input(text), config=config)


def _is_model_text_chunk(token: Any, meta: Any) -> bool:
    """True only for assistant text tokens (skip tool node / ToolMessage)."""
    if isinstance(meta, dict):
        node = str(meta.get("langgraph_node") or "")
        if node == "tools" or node.startswith("tools"):
            return False

    name = token.__class__.__name__
    msg_type = getattr(token, "type", None)
    if msg_type == "tool" or "ToolMessage" in name:
        return False
    return msg_type == "ai" or "AIMessage" in name


def run_stream(
    agent: Any,
    text: str,
    *,
    config: Optional[dict] = None,
    print_tokens: bool = True,
) -> dict[str, Any]:
    """
    Stream agent with messages+updates, print model tokens live, return final state.

    Only streams assistant text to the console — tool call args / tool results
    are never printed. Messages from every update step are still accumulated
    for ``extract_usage`` / ``print_run_summary``.
    """
    messages: list[Any] = []
    final: dict[str, Any] = {"messages": messages}
    stream_kwargs: dict[str, Any] = {
        "stream_mode": ["messages", "updates"],
        "version": "v2",
    }
    if config is not None:
        stream_kwargs["config"] = config

    if print_tokens:
        print()
    for chunk in agent.stream(agent_input(text), **stream_kwargs):
        if isinstance(chunk, dict) and "type" in chunk:
            kind = chunk.get("type")
            data = chunk.get("data")
            if kind == "messages" and data:
                token, meta = data
                if not print_tokens or not _is_model_text_chunk(token, meta):
                    continue
                piece = getattr(token, "content", None) or ""
                if isinstance(piece, list):
                    # content blocks — only join text parts
                    piece = "".join(
                        str(b.get("text", "")) if isinstance(b, dict) else str(b)
                        for b in piece
                        if not (isinstance(b, dict) and b.get("type") == "tool_use")
                    )
                if piece:
                    print(piece, end="", flush=True)
            elif kind == "updates" and isinstance(data, dict):
                for _node, update in data.items():
                    if isinstance(update, dict) and "messages" in update:
                        step_msgs = update.get("messages") or []
                        messages.extend(step_msgs)
                        final = {**update, "messages": messages}

    if print_tokens:
        print()
    return final

# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from langchain_core.tools import tool

    @tool
    def ping() -> str:
        """Simple health-check tool. Returns 'pong'."""
        return "pong"

    # Mix: local @tool + optional MCP + optional file toolkit
    tools = collect_tools(
        extra=[ping],
        mcp=None,  # follows MCP_ENABLED env
        files=bool(FILE_TOOLS_ROOT),
    )
    print("[tools]", [t.name if hasattr(t, "name") else t for t in tools])

    agent = get_agent(
        model_provider=DEFAULT_PROVIDER,
        tools=tools,
        system_prompt=DEFAULT_SYSTEM_PROMPT,
        # fill these manually for the model you use (USD per 1M tokens)
        input_price_per_million=DEFAULT_INPUT_PRICE_PER_MILLION,
        output_price_per_million=DEFAULT_OUTPUT_PRICE_PER_MILLION,
        # e.g. input_price_per_million=2.50, output_price_per_million=10.00,
    )

    # 1) invoke
    invoked = run_invoke(agent, DEFAULT_DEMO_QUERY)
    print_run_summary("invoke", invoked, pricing_from=agent)

    # 2) stream
    streamed = run_stream(agent, DEFAULT_DEMO_QUERY)
    if streamed.get("messages"):
        print_run_summary("stream (final update)", streamed, pricing_from=agent)
    else:
        print("\n=== stream ===\n(no final state captured; see tokens above)")
