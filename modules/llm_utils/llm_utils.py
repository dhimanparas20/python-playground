"""
llm_utils — build a chat model or an agent on any provider. That is all.

    uv add langchain langchain-openai python-dotenv
    uv add langchain-anthropic       # anthropic, and Claude models via opencode
    uv add langchain-google-genai    # google
    uv add langchain-groq            # groq
    uv add langchain-mcp-adapters    # optional: MCP tools
    uv add langchain-community       # optional: file tools

    from modules.llm_utils import create_llm, get_agent, collect_tools, run_stream

    llm = create_llm()                                    # DEFAULT_PROVIDER + .env
    llm = create_llm("deepseek-v4.1-flash", provider="opencode-go")
    llm = create_llm("kimi-k2", provider="acme",
                     base_url="https://acme.example/v1", api_key="...")

    agent = get_agent(llm, tools=[my_tool], system_prompt="Be helpful.")
    run_stream(agent, "hello")

Providers: openai, anthropic, google, groq, openrouter, vercel, opencode, opencode-go.
Any other name works too — set <NAME>_API_KEY, <NAME>_MODEL, <NAME>_BASE_URL and it is
treated as an OpenAI-compatible endpoint. Args beat env everywhere.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import socket
import uuid
from pathlib import Path
from typing import Any, NamedTuple, Optional, Sequence


# ---------------------------------------------------------------------------
# .env — fill os.environ, but never let a blank `KEY=` wipe a real value
# ---------------------------------------------------------------------------

def _load_env(path: str = ".env") -> None:
    try:
        from dotenv import dotenv_values
    except ImportError:
        return
    for key, value in dotenv_values(path).items():
        if value and value.strip() and not (os.getenv(key) or "").strip():
            os.environ[key] = value.strip()


_load_env()


def _prefer_ipv4() -> None:
    """Connect over IPv4 first. Opt out with LLM_PREFER_IPV4=0.

    On hosts with broken IPv6 routing (common on WSL) every model call stalls
    ~30-60s on the IPv6 attempt before failing, which looks like a hung agent.
    """
    if os.getenv("LLM_PREFER_IPV4", "1").strip().lower() in ("0", "false", "no", "off"):
        return
    if getattr(socket.getaddrinfo, "_ipv4_first", False):
        return
    original = socket.getaddrinfo

    def getaddrinfo(*args, **kwargs):
        infos = original(*args, **kwargs)
        return sorted(infos, key=lambda info: 0 if info[0] == socket.AF_INET else 1)

    getaddrinfo._ipv4_first = True  # type: ignore[attr-defined]
    socket.getaddrinfo = getaddrinfo


_prefer_ipv4()

try:
    from modules.logger import get_logger
except ImportError:  # running from inside this folder
    import logging

    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)


logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Defaults (override per call, or in .env)
# ---------------------------------------------------------------------------

DEFAULT_PROVIDER: str = os.getenv("DEFAULT_PROVIDER", "openai")
DEFAULT_SYSTEM_PROMPT: str = os.getenv(
    "DEFAULT_SYSTEM_PROMPT", "You are a helpful assistant."
)
DEFAULT_DEMO_QUERY: str = os.getenv(
    "DEFAULT_DEMO_QUERY", "Reply with exactly one short sentence confirming you work."
)


def _env_float(name: str) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw.strip())
    except ValueError as e:
        raise ValueError(f"{name} must be a float, got {raw!r}") from e


DEFAULT_INPUT_PRICE_PER_MILLION: Optional[float] = _env_float(
    "DEFAULT_INPUT_PRICE_PER_MILLION"
)
DEFAULT_OUTPUT_PRICE_PER_MILLION: Optional[float] = _env_float(
    "DEFAULT_OUTPUT_PRICE_PER_MILLION"
)

FILE_TOOLS_ROOT: Optional[str] = os.getenv("FILE_TOOLS_ROOT") or None
MCP_ENABLED_DEFAULT = os.getenv("MCP_ENABLED", "false").lower() in ("true", "1", "yes")

# External MCP servers, loaded by collect_tools(mcp=True). Dead servers are skipped.
MCP_TOOLS: dict[str, dict[str, Any]] = {
    "ddg-search": {
        "command": "uvx",
        "transport": "stdio",
        "args": ["duckduckgo-mcp-server"],
        "env": {"DDG_SAFE_SEARCH": "MODERATE", "DDG_REGION": "in-en"},
    },
    # "local": {"url": "http://localhost:8000/mcp/", "transport": "streamable_http"},
}

# ---------------------------------------------------------------------------
# Providers
# ---------------------------------------------------------------------------

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
VERCEL_BASE_URL = "https://ai-gateway.vercel.sh/v1"
OPENCODE_BASE_URL = "https://opencode.ai/inference"
OPENCODE_GO_BASE_URL = "https://opencode.ai/inference/go"

_CLIENTS = {
    "openai": ("langchain_openai", "ChatOpenAI"),
    "anthropic": ("langchain_anthropic", "ChatAnthropic"),
    "google": ("langchain_google_genai", "ChatGoogleGenerativeAI"),
    "groq": ("langchain_groq", "ChatGroq"),
}


class Provider(NamedTuple):
    """How to reach a provider: which client to use and where the config lives."""

    kind: str                       # key into _CLIENTS
    api_key_env: str
    model_env: str
    base_url: Optional[str] = None
    key_env_fallbacks: tuple[str, ...] = ()


PROVIDERS: dict[str, Provider] = {
    "openai": Provider("openai", "OPENAI_API_KEY", "OPENAI_MODEL"),
    "anthropic": Provider("anthropic", "ANTHROPIC_API_KEY", "ANTHROPIC_MODEL"),
    "google": Provider(
        "google", "GOOGLE_API_KEY", "GOOGLE_MODEL", key_env_fallbacks=("GEMINI_API_KEY",)
    ),
    "groq": Provider("groq", "GROQ_API_KEY", "GROQ_MODEL"),
    "openrouter": Provider(
        "openai", "OPENROUTER_API_KEY", "OPENROUTER_MODEL", OPENROUTER_BASE_URL
    ),
    "vercel": Provider(
        "openai",
        "AI_GATEWAY_API_KEY",
        "AI_GATEWAY_MODEL",
        VERCEL_BASE_URL,
        ("VERCEL_OIDC_TOKEN",),
    ),
    "vercel_ai_gateway": Provider(
        "openai",
        "AI_GATEWAY_API_KEY",
        "AI_GATEWAY_MODEL",
        VERCEL_BASE_URL,
        ("VERCEL_OIDC_TOKEN",),
    ),
    "opencode": Provider(
        "openai", "OPENCODE_API_KEY", "OPENCODE_MODEL", f"{OPENCODE_BASE_URL}/openai/v1"
    ),
    "opencode-go": Provider(
        "openai",
        "OPENCODE_GO_API_KEY",
        "OPENCODE_GO_MODEL",
        f"{OPENCODE_GO_BASE_URL}/openai/v1",
    ),
}

# OpenCode serves a few model families over the Anthropic API and GPT/Grok/Muse over
# OpenAI's Responses API; everything else is plain chat completions.
_ANTHROPIC_PREFIXES = ("claude-", "qwen3.5-plus", "qwen3.6-plus", "qwen3.7-", "qwen3.8-flash")
_ANTHROPIC_GO_MODELS = ("minimax-m3", "qwen3.8-max")
_RESPONSES_PREFIXES = ("gpt-", "grok-", "muse-")


def _provider(name: str) -> Provider:
    """Built-in provider, or any other name as an OpenAI-compatible endpoint."""
    if name in PROVIDERS:
        return PROVIDERS[name]
    stem = name.upper().replace("-", "_")
    return Provider("openai", f"{stem}_API_KEY", f"{stem}_MODEL")


def _opencode_target(model: str, go: bool) -> tuple[str, Optional[str], dict[str, Any]]:
    """(client kind, base_url, extra kwargs) for one OpenCode model id."""
    root = OPENCODE_GO_BASE_URL if go else OPENCODE_BASE_URL
    if model.startswith(_ANTHROPIC_PREFIXES) or (go and model in _ANTHROPIC_GO_MODELS):
        return "anthropic", f"{root}/anthropic", {}
    if model.startswith(_RESPONSES_PREFIXES):
        return "openai", f"{root}/openai/v1", {"use_responses_api": True}
    return "openai", f"{root}/openai/v1", {}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.replace("\r", "").strip()
    return cleaned or None


def _env(name: str) -> Optional[str]:
    return _clean(os.getenv(name))


def _env_number(name: str, cast) -> Optional[float]:
    raw = _env(name)
    if raw is None:
        return None
    try:
        return cast(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be a {cast.__name__}, got {raw!r}") from e


_PRICING_ATTR = "_llm_utils_pricing"


def _stash_pricing(obj: Any, in_price: Optional[float], out_price: Optional[float]) -> None:
    """Remember $/1M rates on an llm/agent so later cost estimates can find them."""
    if in_price is None and out_price is None:
        return
    try:
        setattr(obj, _PRICING_ATTR, {"input": in_price, "output": out_price})
    except Exception:
        pass


def _read_pricing(
    obj: Any = None,
    in_price: Optional[float] = None,
    out_price: Optional[float] = None,
) -> tuple[Optional[float], Optional[float]]:
    """explicit args → rates stashed on obj → module defaults."""
    stashed = getattr(obj, _PRICING_ATTR, None) if obj is not None else None
    if isinstance(stashed, dict):
        in_price = in_price if in_price is not None else stashed.get("input")
        out_price = out_price if out_price is not None else stashed.get("output")
    return (
        in_price if in_price is not None else DEFAULT_INPUT_PRICE_PER_MILLION,
        out_price if out_price is not None else DEFAULT_OUTPUT_PRICE_PER_MILLION,
    )


# ---------------------------------------------------------------------------
# LLM + agent
# ---------------------------------------------------------------------------

def create_llm(
    model: Optional[str] = None,
    *,
    provider: str = DEFAULT_PROVIDER,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
    **extra: Any,
):
    """Build a chat model.

    ``model`` / ``api_key`` / ``base_url`` fall back to the provider's env vars
    (<NAME>_MODEL, <NAME>_API_KEY, <NAME>_BASE_URL). ``temperature`` and
    ``max_tokens`` fall back to MODEL_TEMPERATURE / MAX_TOKENS. Prices are kept
    on the model for later cost estimates — they are never sent to the provider.
    Anything else in ``**extra`` goes to the model constructor (timeout, …).
    """
    provider = (_clean(provider) or DEFAULT_PROVIDER).lower()
    config = _provider(provider)

    model = _clean(model) or _env(config.model_env)
    if not model:
        raise ValueError(f"No model given and {config.model_env} is not set.")

    key_envs = (config.api_key_env, *config.key_env_fallbacks)
    api_key = _clean(api_key)
    for env_name in key_envs:
        api_key = api_key or _env(env_name)
    if not api_key:
        raise ValueError(f"No API key given and none of {list(key_envs)} are set.")

    kind, url, route_extra = config.kind, config.base_url, {}
    if provider.startswith("opencode"):
        kind, url, route_extra = _opencode_target(model, provider == "opencode-go")
    url = _clean(base_url) or _env(f"{provider.upper().replace('-', '_')}_BASE_URL") or url
    if url is None and provider not in PROVIDERS:
        raise ValueError(
            f"Provider {provider!r} has no base URL. Set {provider.upper().replace('-', '_')}_BASE_URL "
            f"or pass base_url= (providers without one: {sorted(PROVIDERS)})"
        )

    module_name, class_name = _CLIENTS[kind]
    try:
        model_class = getattr(importlib.import_module(module_name), class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(
            f"{class_name} needs its package: uv add {module_name.replace('_', '-')}"
        ) from e

    temperature = temperature if temperature is not None else _env_number("MODEL_TEMPERATURE", float)
    max_tokens = max_tokens if max_tokens is not None else _env_number("MAX_TOKENS", int)

    kwargs: dict[str, Any] = {
        "model": model,
        "api_key": api_key,
        **route_extra,
        **extra,
    }
    if temperature is not None:
        kwargs["temperature"] = temperature
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    if url:
        kwargs["base_url"] = url
    if provider.startswith("opencode"):
        # The Go gateway rejects calls without a session id (400 MissingSessionID)
        # and asks clients to identify themselves.
        kwargs["default_headers"] = {
            "User-Agent": "llm_utils/1.0",
            "x-opencode-session": _env("OPENCODE_SESSION_ID") or uuid.uuid4().hex,
        }

    logger.info("llm: %s / %s (%s) temp=%s max_tokens=%s", provider, model, class_name, temperature, max_tokens)
    llm = model_class(**kwargs)
    _stash_pricing(llm, input_price_per_million, output_price_per_million)
    return llm


def get_agent(
    llm: Any = None,
    *,
    tools: Optional[Sequence[Any]] = None,
    system_prompt: Optional[Any] = DEFAULT_SYSTEM_PROMPT,
    provider: str = DEFAULT_PROVIDER,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
    **agent_options: Any,
):
    """Build a LangChain agent.

    Pass ``llm=`` to reuse a model, or pass the model args (provider, model,
    api_key, base_url, temperature, max_tokens) and one is built for you.
    Everything else — checkpointer, store, middleware, response_format,
    interrupt_before, debug, … — is forwarded to ``langchain.agents.create_agent``.
    """
    try:
        from langchain.agents import create_agent
    except ImportError as e:
        raise ImportError("get_agent needs langchain: uv add langchain") from e

    if llm is None:
        llm = create_llm(
            model,
            provider=provider,
            api_key=api_key,
            base_url=base_url,
            temperature=temperature,
            max_tokens=max_tokens,
            input_price_per_million=input_price_per_million,
            output_price_per_million=output_price_per_million,
        )
        prices = (input_price_per_million, output_price_per_million)
    else:
        # Reuse prices stashed on the given model unless the caller overrides them
        prices = _read_pricing(llm, input_price_per_million, output_price_per_million)
        if input_price_per_million is not None or output_price_per_million is not None:
            _stash_pricing(llm, input_price_per_million, output_price_per_million)
            prices = (input_price_per_million, output_price_per_million)

    agent = create_agent(
        model=llm,
        tools=list(tools) if tools else [],
        system_prompt=system_prompt,
        **agent_options,
    )
    _stash_pricing(agent, *prices)
    return agent


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

def collect_tools(
    extra: Optional[Sequence[Any]] = None,
    *,
    mcp: Optional[bool] = None,
    files: bool = False,
    file_root: Optional[str] = None,
    as_sync: bool = True,
) -> list[Any]:
    """One bag of tools for an agent.

    ``extra``: your @tool functions. ``mcp=True`` adds the servers in MCP_TOOLS
    (defaults to the MCP_ENABLED env). ``files=True`` adds the file toolkit
    sandboxed at FILE_TOOLS_ROOT / ``file_root``.
    """
    tools: list[Any] = list(extra or [])
    if files or file_root:
        tools.extend(_file_tools(file_root))
    if MCP_ENABLED_DEFAULT if mcp is None else mcp:
        tools.extend(_mcp_tools(as_sync=as_sync))
    return tools


def _file_tools(root: Optional[str]) -> list[Any]:
    root = root or FILE_TOOLS_ROOT
    if not root:
        return []
    try:
        from langchain_community.agent_toolkits import FileManagementToolkit
    except ImportError as e:
        raise ImportError("file tools need: uv add langchain-community") from e
    path = Path(root)
    path.mkdir(parents=True, exist_ok=True)
    return list(FileManagementToolkit(root_dir=str(path)).get_tools())


def _mcp_tools(as_sync: bool = True) -> list[Any]:
    try:
        from langchain_mcp_adapters.client import MultiServerMCPClient
    except ImportError as e:
        raise ImportError("MCP tools need: uv add langchain-mcp-adapters") from e

    tools: list[Any] = []
    for name, server in MCP_TOOLS.items():
        try:
            loaded = asyncio.run(MultiServerMCPClient({name: server}).get_tools())
        except Exception as error:
            first_line = str(error).splitlines()[0][:80]
            print(f"[mcp] {name}: unavailable ({first_line})")
            continue
        print(f"[mcp] {name}: {len(loaded)} tools")
        if as_sync:
            tools.extend(_sync_tool(t) for t in loaded)
        else:
            tools.extend(loaded)
    return tools


def _sync_tool(tool: Any) -> Any:
    """Wrap an async MCP tool so it works in sync agent runs."""
    from langchain_core.tools import StructuredTool

    def run(**kwargs: Any) -> str:
        result = asyncio.run(tool.ainvoke(kwargs))
        if isinstance(result, list):
            return "\n".join(
                str(item.get("text", item)) if isinstance(item, dict) else str(item)
                for item in result
            )
        return str(result)

    return StructuredTool.from_function(
        func=run,
        name=tool.name,
        description=tool.description,
        args_schema=tool.args_schema,
    )


# ---------------------------------------------------------------------------
# Messages, runs, usage
# ---------------------------------------------------------------------------

def system_msg(content: str):
    from langchain_core.messages import SystemMessage

    return SystemMessage(content=content)


def human_msg(content: str):
    from langchain_core.messages import HumanMessage

    return HumanMessage(content=content)


def agent_input(text: str, *, system: Optional[str] = None) -> dict[str, Any]:
    """The {"messages": [...]} payload used by agent.invoke / agent.stream."""
    messages: list[Any] = [system_msg(system)] if system else []
    messages.append(human_msg(text))
    return {"messages": messages}


def run_invoke(agent: Any, text: str, *, config: Optional[dict] = None) -> dict[str, Any]:
    """Run the agent once and return its final state."""
    return agent.invoke(agent_input(text), config=config)


def run_stream(
    agent: Any,
    text: str,
    *,
    config: Optional[dict] = None,
    print_tokens: bool = True,
) -> dict[str, Any]:
    """Run the agent, printing assistant text live. Returns the final state.

    Tool calls and tool results are never printed; their messages are still
    collected so extract_usage can count the tokens.
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
        if not isinstance(chunk, dict) or "type" not in chunk:
            continue
        kind, data = chunk.get("type"), chunk.get("data")
        if kind == "messages" and data:
            token, meta = data
            if not print_tokens or not _is_assistant_text(token, meta):
                continue
            piece = _text(token)
            if piece:
                print(piece, end="", flush=True)
        elif kind == "updates" and isinstance(data, dict):
            for update in data.values():
                if isinstance(update, dict) and "messages" in update:
                    messages.extend(update.get("messages") or [])
                    final = {**update, "messages": messages}

    if print_tokens:
        print()
    return final


def _is_assistant_text(token: Any, meta: Any) -> bool:
    """True for assistant text only — skip the tool node and tool messages.

    Streaming tokens are ``AIMessageChunk`` instances, so match by substring.
    """
    if isinstance(meta, dict) and str(meta.get("langgraph_node") or "").startswith("tools"):
        return False
    name = token.__class__.__name__
    if "ToolMessage" in name or getattr(token, "type", None) == "tool":
        return False
    return "AIMessage" in name or getattr(token, "type", None) == "ai"


def _text(message: Any) -> str:
    content = getattr(message, "content", "") or ""
    if not isinstance(content, list):
        return str(content)
    return "".join(
        str(block.get("text", "")) if isinstance(block, dict) else str(block)
        for block in content
        if not (isinstance(block, dict) and block.get("type") == "tool_use")
    )


def estimate_cost(
    input_tokens: int,
    output_tokens: int,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
) -> Optional[dict[str, float]]:
    """USD cost from token counts and $/1M rates. None when no rates are set."""
    in_rate, out_rate = _read_pricing(None, input_price_per_million, output_price_per_million)
    if in_rate is None and out_rate is None:
        return None
    in_rate, out_rate = float(in_rate or 0.0), float(out_rate or 0.0)
    input_cost = (input_tokens / 1_000_000) * in_rate
    output_cost = (output_tokens / 1_000_000) * out_rate
    return {
        "input_cost_usd": input_cost,
        "output_cost_usd": output_cost,
        "total_cost_usd": input_cost + output_cost,
        "input_price_per_million": in_rate,
        "output_price_per_million": out_rate,
    }


def extract_usage(
    messages: Sequence[Any],
    *,
    pricing_from: Any = None,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
) -> dict[str, Any]:
    """Token totals across messages, plus a cost estimate when rates are known."""
    input_tokens = output_tokens = total_tokens = 0
    for message in messages:
        usage = getattr(message, "usage_metadata", None) or {}
        input_tokens += int(usage.get("input_tokens") or 0)
        output_tokens += int(usage.get("output_tokens") or 0)
        total_tokens += int(usage.get("total_tokens") or 0)
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens or (input_tokens + output_tokens),
        "cost": estimate_cost(
            input_tokens,
            output_tokens,
            *_read_pricing(pricing_from, input_price_per_million, output_price_per_million),
        ),
    }


def print_run_summary(
    label: str,
    result: dict[str, Any],
    *,
    pricing_from: Any = None,
    input_price_per_million: Optional[float] = None,
    output_price_per_million: Optional[float] = None,
) -> None:
    """Print the reply, token counts and (when known) the cost of one run."""
    messages = result.get("messages") or []
    usage = extract_usage(
        messages,
        pricing_from=pricing_from,
        input_price_per_million=input_price_per_million,
        output_price_per_million=output_price_per_million,
    )
    print(f"\n=== {label} ===")
    for message in reversed(messages):
        if getattr(message, "type", None) == "ai":
            print("response:", _text(message))
            break
    print(
        "tokens:",
        f"input={usage['input_tokens']} output={usage['output_tokens']} total={usage['total_tokens']}",
    )
    if usage["cost"]:
        c = usage["cost"]
        print(f"cost_usd: total={c['total_cost_usd']:.6f} ($/1M in={c['input_price_per_million']} out={c['output_price_per_million']})")
    if "structured_response" in result:
        print("structured_response:", result["structured_response"])


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from langchain_core.tools import tool

    @tool
    def ping() -> str:
        """Health check tool. Returns 'pong'."""
        return "pong"

    agent = get_agent(
        tools=collect_tools(extra=[ping], mcp=False),
        system_prompt=DEFAULT_SYSTEM_PROMPT,
        input_price_per_million=DEFAULT_INPUT_PRICE_PER_MILLION,
        output_price_per_million=DEFAULT_OUTPUT_PRICE_PER_MILLION,
    )
    print_run_summary("invoke", run_invoke(agent, DEFAULT_DEMO_QUERY), pricing_from=agent)
    print_run_summary("stream", run_stream(agent, DEFAULT_DEMO_QUERY), pricing_from=agent)
