# `llm_utils` — drop-in LLM + agent toolkit

One file so you stop rewriting LangChain setup every project.

**What it gives you**

- Load chat models from **OpenAI, Google, OpenRouter, Groq, Vercel AI Gateway**
- Spin up a **LangChain v1 agent** (`create_agent`) in one call
- Attach tools from **`@tool`**, **toolkits**, or **external MCP servers**
- Run with **`.invoke`** or **`.stream`**, then print **response + tokens + optional $ cost**
- Fall back to **`.env`** when args are missing; raise clear errors when they are not

Location: `python-playground/modules/llm_utils/`

| File | Role |
|------|------|
| `llm_utils.py` | The module |
| `.env.llmutil.sample` | All env vars you might need |
| `LLM-UTIL-README.md` | This guide |

---

## Why this exists

Every LangGraph / LangChain project needs the same boilerplate:

1. Pick a provider and model  
2. Load API keys from env  
3. Wire tools  
4. Call `create_agent`  
5. Invoke / stream and inspect usage  

`llm_utils` is that boilerplate, done once, reused forever.

---

## Install (packages are not auto-installed)

Core:

```bash
uv add langchain langchain-openai python-dotenv
```

Providers you actually use:

```bash
uv add langchain-google-genai   # google
uv add langchain-groq           # groq
# openrouter + vercel_ai_gateway reuse langchain-openai (ChatOpenAI + base_url)
```

Optional tools:

```bash
uv add langchain-mcp-adapters   # MCP servers
uv add langchain-community      # FileManagementToolkit, etc.
```

Env:

```bash
cp .env.llmutil.sample .env
# fill keys for the providers you use
```

Quick demo (from the llm_utils package folder):

```bash
cd /mnt/d/0Coding/python-playground/modules/llm_utils
python llm_utils.py
```

Import from another project (add modules to `PYTHONPATH`, or use a relative/package import):

```python
from modules.llm_utils import create_llm, get_agent
```

---

## Mental model (30 seconds)

```text
.env / args
    ↓
create_llm()          → Chat model (ChatOpenAI, ChatGroq, …)
    ↓                      ↘
collect_tools()            get_agent(llm=...)   ← pass a ready model
    ↓                      ↗
get_agent()           → (or create_llm internally if llm omitted)
    ↓
run_invoke / run_stream / agent.invoke / agent.stream
    ↓
print_run_summary()   → text + tokens + optional cost
```

---

## What to import

Most projects only need:

```python
from modules.llm_utils import (
    create_llm,        # raw chat model
    get_agent,         # full agent
    collect_tools,     # mix tool sources
    human_msg,         # typed user message
    agent_input,       # {"messages": [...]}
    run_invoke,        # convenience invoke
    run_stream,        # convenience stream
    print_run_summary, # pretty print + usage
    estimate_cost,     # manual $ from tokens
)
```

Also useful:

```python
from modules.llm_utils import (
    system_msg, ai_msg, tool_msg,
    extract_usage, last_ai_text,
    load_mcp_tools, load_file_tools,
    mcp_enabled,
    # tunables
    DEFAULT_PROVIDER, DEFAULT_SYSTEM_PROMPT,
    MCP_TOOLS, FILE_TOOLS_ROOT,
    DEFAULT_INPUT_PRICE_PER_MILLION,
    DEFAULT_OUTPUT_PRICE_PER_MILLION,
)
```

---

## Supported providers

| `model_provider` | Package | Class | API key env | Model env | Base URL |
|------------------|---------|-------|-------------|-----------|----------|
| `openai` | `langchain-openai` | `ChatOpenAI` | `OPENAI_API_KEY` | `OPENAI_MODEL` | provider default |
| `google` | `langchain-google-genai` | `ChatGoogleGenerativeAI` | `GOOGLE_API_KEY` (or `GEMINI_API_KEY`) | `GOOGLE_MODEL` | provider default |
| `openrouter` | `langchain-openai` | `ChatOpenAI` | `OPENROUTER_API_KEY` | `OPENROUTER_MODEL` | hardcoded `https://openrouter.ai/api/v1` |
| `groq` | `langchain-groq` | `ChatGroq` | `GROQ_API_KEY` | `GROQ_MODEL` | provider default |
| `vercel_ai_gateway` | `langchain-openai` | `ChatOpenAI` | `AI_GATEWAY_API_KEY` (or `VERCEL_OIDC_TOKEN`) | `AI_GATEWAY_MODEL` | hardcoded `https://ai-gateway.vercel.sh/v1` |

OpenRouter and Vercel base URLs are **hardcoded in the module** (not read from `.env`), so they stay consistent everywhere.

---

## Environment variables

Copy from `.env.llmutil.sample`. Fill only what you need.

### Shared

| Variable | Purpose | Example |
|----------|---------|---------|
| `DEFAULT_PROVIDER` | Default for `create_llm` / `get_agent` | `openai` |
| `DEFAULT_SYSTEM_PROMPT` | Default agent system prompt | `You are a helpful assistant.` |
| `DEFAULT_DEMO_QUERY` | Used by `python llm_utils.py` (inside `modules/llm_utils/`) | short test string |
| `MODEL_TEMPERATURE` | Fallback temperature | `0.2` |
| `MAX_TOKENS` | Fallback max tokens | `4096` |
| `FILE_TOOLS_ROOT` | Sandbox for file toolkit | `./sandbox` |
| `MCP_ENABLED` | Load MCP tools when `true`/`1`/`yes` | `false` |

### Per provider

| Provider | Key | Model |
|----------|-----|-------|
| openai | `OPENAI_API_KEY` | `OPENAI_MODEL` |
| google | `GOOGLE_API_KEY` or `GEMINI_API_KEY` | `GOOGLE_MODEL` |
| openrouter | `OPENROUTER_API_KEY` | `OPENROUTER_MODEL` |
| groq | `GROQ_API_KEY` | `GROQ_MODEL` |
| vercel | `AI_GATEWAY_API_KEY` or `VERCEL_OIDC_TOKEN` | `AI_GATEWAY_MODEL` |

### Optional MCP helpers (if you uncomment those servers in `MCP_TOOLS`)

- `DEFAULT_MCP_SERVER_URL`
- `PAGE_INDEX_API_KEY`
- `FIRECRAWL_API_KEY`

### Pricing is **not** env-based

Set USD rates in code (or module globals):

```python
# in llm_utils.py
DEFAULT_INPUT_PRICE_PER_MILLION = 2.50
DEFAULT_OUTPUT_PRICE_PER_MILLION = 10.00
```

or pass into `create_llm` / `get_agent`.

**Resolution order for model / key / temp:** function argument → env → error if required and still missing.

---

## Module globals you can edit

At the top of `llm_utils.py`:

| Name | Meaning |
|------|---------|
| `DEFAULT_PROVIDER` | Default provider string |
| `DEFAULT_SYSTEM_PROMPT` | Default system prompt |
| `DEFAULT_DEMO_QUERY` | Demo query for `__main__` |
| `DEFAULT_INPUT_PRICE_PER_MILLION` | Optional $/1M input |
| `DEFAULT_OUTPUT_PRICE_PER_MILLION` | Optional $/1M output |
| `FILE_TOOLS_ROOT` | File toolkit root (or `None`) |
| `MCP_ENABLED_DEFAULT` | From `MCP_ENABLED` env |
| `MCP_TOOLS` | Dict of MCP server configs |
| `MODEL_REGISTRY` | Provider → package/class/env mapping |
| `OPENROUTER_BASE_URL` | Hardcoded OpenRouter URL |
| `VERCEL_AI_GATEWAY_BASE_URL` | Hardcoded Vercel gateway URL |

---

## Functions reference

### `create_llm(...)` → chat model

Creates a LangChain chat model for any supported provider.

| Arg | Type | Default | Notes |
|-----|------|---------|-------|
| `model_name` | `str \| None` | `None` | Falls back to provider `*_MODEL` env |
| `api_key` | `str \| None` | `None` | Falls back to provider key env(s) |
| `model_provider` | provider string | `DEFAULT_PROVIDER` | See table above |
| `model_temperature` | `float \| None` | `None` | `0.0` is valid; env `MODEL_TEMPERATURE` |
| `max_tokens` | `int \| None` | `None` | Env `MAX_TOKENS` |
| `input_price_per_million` | `float \| None` | `None` | Manual $/1M input (stashed, not sent to API) |
| `output_price_per_million` | `float \| None` | `None` | Manual $/1M output |
| `**extra` | any | — | Forwarded to the model ctor (`timeout`, `max_retries`, …) |

```python
from modules.llm_utils import create_llm

llm = create_llm(
    model_name="gpt-4o",
    model_provider="openai",
    model_temperature=0.2,
    input_price_per_million=2.50,
    output_price_per_million=10.00,
)
print(llm.invoke("Say hi in one word").content)
```

Vercel example:

```python
llm = create_llm(
    model_name="anthropic/claude-sonnet-4",
    model_provider="vercel_ai_gateway",
)
```

---

### `get_agent(...)` → compiled agent graph

Builds a LangChain v1 agent via `langchain.agents.create_agent`.

**LLM resolution**

1. If you pass `llm=...`, that model is used as-is (`create_llm` is skipped).
2. Otherwise `create_llm` runs with the model_* / pricing / `llm_extra` args.

This lets you configure a model once and reuse it:

```python
from modules.llm_utils import create_llm, get_agent, collect_tools

llm = create_llm(
    model_name="gpt-4o",
    model_provider="openai",
    model_temperature=0.2,
    input_price_per_million=2.50,
    output_price_per_million=10.00,
)
agent = get_agent(llm=llm, tools=collect_tools(extra=[add]), system_prompt="Be concise.")
# same llm can also be used directly in chains / nodes:
# llm.invoke("hi")
```

**Model args** (used only when `llm` is omitted — same as `create_llm`):  
`model_name`, `api_key`, `model_provider`, `model_temperature`, `max_tokens`,  
`input_price_per_million`, `output_price_per_million`, `llm_extra`

| Arg | Default | Notes |
|-----|---------|-------|
| `llm` | `None` | Pre-built chat model; if set, skips `create_llm` |
| `tools` | `None` | List of tools / callables / dicts |
| `system_prompt` | `DEFAULT_SYSTEM_PROMPT` | `str` or `SystemMessage` |
| `response_format` | `None` | `ToolStrategy`, `ProviderStrategy`, or Pydantic model |
| `middleware` | `()` | Agent middleware sequence |
| `state_schema` | `None` | Custom agent state |
| `context_schema` | `None` | Runtime context schema |
| `checkpointer` | `None` | e.g. `InMemorySaver()` for memory |
| `store` | `None` | Cross-thread store |
| `interrupt_before` / `interrupt_after` | `None` | Human-in-the-loop node names |
| `debug` | `False` | Verbose graph logging |
| `name` | `None` | Graph name (useful as subgraph) |
| `cache` | `None` | Execution cache |
| `transformers` | `None` | Extra stream transformers |
| `llm_extra` | `None` | Dict forwarded into `create_llm(**extra)` (ignored if `llm=` set) |

```python
from modules.llm_utils import get_agent, collect_tools, run_invoke, print_run_summary
from langchain_core.tools import tool

@tool
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b

tools = collect_tools(extra=[add])

# Option A — let get_agent build the model
agent = get_agent(
    model_provider="openai",
    model_name="gpt-4o",
    tools=tools,
    system_prompt="Be concise. Use tools when helpful.",
    input_price_per_million=2.50,
    output_price_per_million=10.00,
)

# Option B — configure llm yourself, then pass it in
# llm = create_llm(model_name="gpt-4o", model_provider="openai", ...)
# agent = get_agent(llm=llm, tools=tools, system_prompt="Be concise.")

result = run_invoke(agent, "What is 2 + 3?")
print_run_summary("demo", result, pricing_from=agent)
```

With memory:

```python
from langgraph.checkpoint.memory import InMemorySaver

agent = get_agent(tools=tools, checkpointer=InMemorySaver())
cfg = {"configurable": {"thread_id": "user-1"}}
agent.invoke({"messages": [{"role": "user", "content": "I'm Blu"}]}, config=cfg)
```

---

### Tools

#### `collect_tools(...)` — one bag for everything

| Arg | Default | Notes |
|-----|---------|-------|
| `extra` | `None` | Your `@tool`s / BaseTools |
| `mcp` | `None` | `True`/`False`, or `None` → follow `MCP_ENABLED` |
| `mcp_config` | `None` | Override `MCP_TOOLS` dict |
| `files` | `False` | Load file toolkit |
| `file_root` | `None` | Sandbox dir (else `FILE_TOOLS_ROOT`) |
| `as_sync_mcp` | `True` | Bridge async MCP tools to sync |

```python
tools = collect_tools(
    extra=[add],
    mcp=True,
    files=True,
    file_root="./sandbox",
)
```

#### `load_file_tools(root_dir=None)`

Uses `FileManagementToolkit` under a sandbox root. Needs `langchain-community`.

#### `load_mcp_tools(config=None, *, enabled=None, as_sync=True)`

Loads every entry in `MCP_TOOLS` (or your `config`). Dead servers are skipped with a one-line notice. Needs `langchain-mcp-adapters`.

#### `mcp_enabled(override=None)`

Returns whether MCP should load (`MCP_ENABLED` env, or explicit override).

#### Configuring `MCP_TOOLS`

Edit the dict at the top of `llm_utils.py`:

```python
MCP_TOOLS = {
    "ddg-search": {
        "command": "uvx",
        "transport": "stdio",
        "args": ["duckduckgo-mcp-server"],
        "env": {"DDG_SAFE_SEARCH": "MODERATE", "DDG_REGION": "in-en"},
    },
    # "my-http": {
    #     "url": "http://localhost:8000/mcp/",
    #     "transport": "streamable_http",
    # },
}
```

Set `MCP_ENABLED=true` in `.env`, or pass `mcp=True` to `collect_tools`.

---

### Messages (prefer these over raw dicts)

| Function | Returns |
|----------|---------|
| `system_msg(content)` | `SystemMessage` |
| `human_msg(content)` | `HumanMessage` |
| `ai_msg(content)` | `AIMessage` |
| `tool_msg(content, tool_call_id, name=None)` | `ToolMessage` |
| `agent_input(text, system=None)` | `{"messages": [...]}` ready for invoke/stream |

```python
from modules.llm_utils import agent_input, human_msg

agent.invoke(agent_input("Hello"))
# same idea:
agent.invoke({"messages": [human_msg("Hello")]})
```

Typed messages are clearer for tool calls, ids, and multimodal content than plain `{"role": "user", ...}` dicts.

---

### Running agents

#### `run_invoke(agent, text, *, config=None)`

Wraps `agent.invoke(agent_input(text), ...)`.

#### `run_stream(agent, text, *, config=None, print_tokens=True)`

Streams with `stream_mode=["messages", "updates"]` and `version="v2"`. Prints tokens live; returns the last state update that had messages (when available).

You can always call the graph directly:

```python
result = agent.invoke(agent_input("hi"))
for chunk in agent.stream(agent_input("hi"), stream_mode="messages", version="v2"):
    ...
```

---

### Usage, cost, printing

#### Tokens from the provider

Providers often attach `usage_metadata` on `AIMessage`s (`input_tokens`, `output_tokens`, `total_tokens`).  
**Credits / $ from the provider are usually not returned.** This module estimates $ only if *you* supply rates.

#### `extract_usage(messages, *, input_price_per_million=None, output_price_per_million=None, pricing_from=None)`

Returns:

```python
{
  "input_tokens": int,
  "output_tokens": int,
  "total_tokens": int,
  "per_message": [...],
  "response_metadata": [...],
  "cost": None | {
      "input_cost_usd": float,
      "output_cost_usd": float,
      "total_cost_usd": float,
      "input_price_per_million": float,
      "output_price_per_million": float,
  },
}
```

#### `estimate_cost(input_tokens, output_tokens, input_price_per_million=None, output_price_per_million=None)`

Formula: `(tokens / 1_000_000) * price_per_million`. Returns `None` if no rates are set.

#### `last_ai_text(messages)` → final AI reply string

#### `print_run_summary(label, result, *, input_price_per_million=None, output_price_per_million=None, pricing_from=None)`

Prints response, token totals, optional cost, and any useful metadata.

```python
print_run_summary("invoke", result, pricing_from=agent)
```

Pricing resolution: **explicit args → stashed on `pricing_from` (llm/agent) → module defaults**.

---

## End-to-end recipes

### 1) Bare LLM (no agent)

```python
from modules.llm_utils import create_llm

llm = create_llm(model_provider="groq", model_name="llama-3.1-8b-instant")
print(llm.invoke("One fun fact about WSL.").content)
```

### 1b) Configure LLM once, then build agent

```python
from modules.llm_utils import create_llm, get_agent, collect_tools

llm = create_llm(
    model_name="gpt-4o",
    model_provider="openai",
    model_temperature=0.1,
    input_price_per_million=2.50,
    output_price_per_million=10.00,
)
agent = get_agent(llm=llm, tools=collect_tools(extra=[]), system_prompt="Be brief.")
# llm is still usable on its own for non-agent calls
```

### 2) Agent + local `@tool`

```python
from langchain_core.tools import tool
from modules.llm_utils import get_agent, run_invoke, print_run_summary, collect_tools

@tool
def ping() -> str:
    """Health check. Returns pong."""
    return "pong"

agent = get_agent(tools=collect_tools(extra=[ping]))
print_run_summary("ping", run_invoke(agent, "Call ping."), pricing_from=agent)
```

### 3) Agent + files + MCP

```python
# .env: MCP_ENABLED=true, FILE_TOOLS_ROOT=./sandbox
from modules.llm_utils import get_agent, collect_tools, run_stream, print_run_summary

tools = collect_tools(mcp=True, files=True)
agent = get_agent(tools=tools, system_prompt="Use tools when needed.")
final = run_stream(agent, "Search for LangChain create_agent docs.")
print_run_summary("stream", final, pricing_from=agent)
```

### 4) Structured output

```python
from pydantic import BaseModel, Field
from langchain.agents.structured_output import ToolStrategy
from modules.llm_utils import get_agent, run_invoke

class Answer(BaseModel):
    summary: str = Field(description="One-sentence summary")
    confidence: float

agent = get_agent(
    tools=[],
    response_format=ToolStrategy(Answer),
    system_prompt="Always return structured Answer.",
)
result = run_invoke(agent, "Summarize what an MCP server is.")
print(result.get("structured_response"))
```

### 5) Cost tracking

```python
agent = get_agent(
    model_name="gpt-4o",
    model_provider="openai",
    input_price_per_million=2.50,
    output_price_per_million=10.00,
)
result = run_invoke(agent, "Hi")
print_run_summary("costed", result, pricing_from=agent)
```

---

## Features checklist

- [x] Multi-provider LLM factory with dynamic imports  
- [x] Env fallbacks + clear `ValueError` / `ImportError`  
- [x] Hardcoded OpenRouter + Vercel gateway URLs  
- [x] LangChain v1 `create_agent` wrapper (prompt, tools, structured output, middleware, memory, …)  
- [x] Typed message helpers  
- [x] `@tool` + toolkit + MCP tool loading  
- [x] Sync bridge for async MCP tools  
- [x] Invoke + stream helpers  
- [x] Token aggregation from `usage_metadata`  
- [x] Optional manual USD cost from $/1M rates  
- [x] `__main__` demo (`python llm_utils.py` in this folder)  
- [x] Sample env file (`.env.llmutil.sample`)

---

## Common errors

| Error | Fix |
|-------|-----|
| `Unknown model provider` | Use one of: `openai`, `google`, `openrouter`, `groq`, `vercel_ai_gateway` |
| `No API key provided…` | Set the provider key in `.env` or pass `api_key=` |
| `No model name provided…` | Pass `model_name=` or set `*_MODEL` env |
| `Could not load 'Chat…'` | `uv add` the matching integration package |
| `langchain is required for get_agent` | `uv add langchain` |
| MCP tools empty / unavailable | `MCP_ENABLED=true`, install server deps (`uvx`, …), check `MCP_TOOLS` |
| Cost always `None` | Pass `input_price_per_million` / `output_price_per_million` (providers rarely return $) |

---

## Design notes

- **Args win over env.** Env is the fallback so scripts stay short.  
- **Temperature `0.0` works** — only `None` triggers the env default.  
- **Pricing is manual on purpose** — rates change; you set them when you know the model.  
- **MCP is off by default** — so the module runs without extra servers installed.  
- Prefer **`get_agent` + `collect_tools`** for agents; use **`create_llm`** when you only need a raw model for chains / LangGraph nodes.
- Or do both: `llm = create_llm(...)` then `get_agent(llm=llm, ...)` so one configured model is shared.

That’s the whole toolkit — import what you need, set env once, ship agents without redoing setup.
