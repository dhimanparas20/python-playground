# `llm_utils` — LLMs and agents on any provider

One file so you stop rewriting LangChain setup every project.

```python
from modules.llm_utils import create_llm, get_agent, collect_tools, run_stream

llm = create_llm()                                    # DEFAULT_PROVIDER + .env
llm = create_llm("deepseek-v4.1-flash", provider="opencode-go")
llm = create_llm("kimi-k2", provider="acme",
                 base_url="https://acme.example/v1", api_key="...")

agent = get_agent(llm, tools=[my_tool], system_prompt="Be helpful.")
run_stream(agent, "hello")
```

| File | Role |
|------|------|
| `llm_utils.py` | The module (one file) |
| `.env.llmutil.sample` | Every env var you might need |
| `LLM-UTIL-README.md` | This guide |

---

## Install

Only what you use:

```bash
uv add langchain langchain-openai python-dotenv
uv add langchain-anthropic       # anthropic, and Claude models via opencode
uv add langchain-google-genai    # google
uv add langchain-groq            # groq
uv add langchain-mcp-adapters    # optional: MCP tools
uv add langchain-community       # optional: file tools
```

Env:

```bash
cp modules/llm_utils/.env.llmutil.sample .env   # .env is read from the folder you run in
```

Smoke test:

```bash
uv run python modules/llm_utils/llm_utils.py    # invoke + stream + usage
```

---

## Providers

Built in:

| `provider` | Client | Key env(s) | Model env | Base URL |
|------------|--------|------------|-----------|----------|
| `openai` | `ChatOpenAI` | `OPENAI_API_KEY` | `OPENAI_MODEL` | official |
| `anthropic` | `ChatAnthropic` | `ANTHROPIC_API_KEY` | `ANTHROPIC_MODEL` | official |
| `google` | `ChatGoogleGenerativeAI` | `GOOGLE_API_KEY` (or `GEMINI_API_KEY`) | `GOOGLE_MODEL` | official |
| `groq` | `ChatGroq` | `GROQ_API_KEY` | `GROQ_MODEL` | official |
| `openrouter` | `ChatOpenAI` | `OPENROUTER_API_KEY` | `OPENROUTER_MODEL` | `https://openrouter.ai/api/v1` |
| `vercel` | `ChatOpenAI` | `AI_GATEWAY_API_KEY` (or `VERCEL_OIDC_TOKEN`) | `AI_GATEWAY_MODEL` | `https://ai-gateway.vercel.sh/v1` |
| `opencode` | `ChatOpenAI` / `ChatAnthropic` | `OPENCODE_API_KEY` | `OPENCODE_MODEL` | `https://opencode.ai/inference/…` |
| `opencode-go` | `ChatOpenAI` / `ChatAnthropic` | `OPENCODE_GO_API_KEY` | `OPENCODE_GO_MODEL` | `https://opencode.ai/inference/go/…` |

### Any other provider

No registration, no code changes. Pick any name and set three env vars — it becomes an
OpenAI-compatible provider:

```bash
ACME_API_KEY=...
ACME_MODEL=kimi-k2
ACME_BASE_URL=https://acme.example/v1
```

```python
llm = create_llm(provider="acme")     # or pass base_url= / api_key= / model= as args
```

The rule: `<NAME>_API_KEY`, `<NAME>_MODEL`, `<NAME>_BASE_URL`. Built-in names are just
pre-filled shortcuts of the same rule (plus the right LangChain client class).
An unknown provider with no base URL is an error — it never silently falls back.

Args always beat env. Every `create_llm` argument has an env fallback:

| Arg | Env fallback |
|-----|--------------|
| `model` | `<NAME>_MODEL` |
| `api_key` | `<NAME>_API_KEY` |
| `base_url` | `<NAME>_BASE_URL` |
| `temperature` | `MODEL_TEMPERATURE` |
| `max_tokens` | `MAX_TOKENS` |

Extra kwargs (`timeout=`, `max_retries=`, …) go straight to the model constructor.

### OpenCode notes

- Model ids are bare (`deepseek-v4.1-flash`, `glm-5.3-flash`, `claude-haiku-5-5`).
  Cheap default: `deepseek-v4.1-flash`. (`jev-1.13` is not a chat model.)
- OpenCode serves different model families over different protocols. The module routes
  per model id: `claude-*` / some Qwen → Anthropic API, `gpt-*` / `grok-*` / `muse-*` →
  OpenAI Responses API, everything else → chat completions. `gemini-*` is not wired yet.
- The Go gateway requires a session header; each built model sends a fresh
  `x-opencode-session` (set `OPENCODE_SESSION_ID` to keep one stable).
- On hosts with broken IPv6 (common on WSL) calls hang ~30-60s. IPv4 is preferred by
  default; opt out with `LLM_PREFER_IPV4=0`.

---

## API

### `create_llm(model=None, *, provider=..., api_key=..., base_url=..., temperature=..., max_tokens=..., input_price_per_million=..., output_price_per_million=..., **extra)`

Builds and returns a LangChain chat model. Prices are stored on the model for cost
estimates and are never sent to the provider.

### `get_agent(llm=None, *, tools=..., system_prompt=..., [model args...], **agent_options)`

Builds a `langchain.agents.create_agent` agent. Pass `llm=`, or pass the model args and
one is built for you. **Everything else is forwarded to `create_agent`** — `checkpointer`,
`store`, `middleware`, `response_format`, `state_schema`, `interrupt_before`, `debug`, …

```python
from langgraph.checkpoint.memory import InMemorySaver

agent = get_agent(llm, tools=tools, system_prompt="Be brief.", checkpointer=InMemorySaver())
agent.invoke({"messages": [{"role": "user", "content": "hi"}]},
             config={"configurable": {"thread_id": "u1"}})
```

### `collect_tools(extra=None, *, mcp=None, files=False, file_root=None, as_sync=True)`

One bag of tools: your `@tool` functions, plus (optionally) MCP servers from `MCP_TOOLS`
and the `FileManagementToolkit` sandbox. Dead MCP servers are skipped with a notice.

### `run_invoke(agent, text, *, config=None)` / `run_stream(agent, text, *, config=None, print_tokens=True)`

Run the agent with a user message. `run_stream` prints assistant text live (never tool
calls or tool results) and returns the final state.

### Usage and cost

```python
usage = extract_usage(result["messages"], pricing_from=agent)
# {'input_tokens': 1747, 'output_tokens': 364, 'total_tokens': 2111,
#  'cost': {'input_cost_usd': ..., 'total_cost_usd': ..., ...} | None}

estimate_cost(1000, 1000, 0.30, 1.20)   # manual $ from token counts
print_run_summary("run", result, pricing_from=agent)   # reply + tokens + cost
```

Providers report tokens but rarely dollars. Cost is estimated from `$ per 1M tokens`:
explicit args → rates stashed on the llm/agent → `DEFAULT_INPUT_PRICE_PER_MILLION` /
`DEFAULT_OUTPUT_PRICE_PER_MILLION` env. No rates → `cost` is `None`.

### Messages

`system_msg(content)`, `human_msg(content)`, `agent_input(text, system=None)` build the
`{"messages": [...]}` payload. For `AIMessage` / `ToolMessage`, import from
`langchain_core.messages` directly.

---

## Recipes

**Plain model**

```python
llm = create_llm("llama-3.1-8b-instant", provider="groq")
print(llm.invoke("One fun fact about WSL.").content)
```

**Agent + tool, with cost**

```python
from langchain_core.tools import tool

@tool
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b

agent = get_agent(tools=[add], system_prompt="Be concise.",
                  input_price_per_million=0.30, output_price_per_million=1.20)
print_run_summary("add", run_invoke(agent, "What is 2 + 3?"), pricing_from=agent)
```

**Structured output**

```python
from pydantic import BaseModel
from langchain.agents.structured_output import ToolStrategy

class Answer(BaseModel):
    summary: str
    confidence: float

agent = get_agent(tools=[], response_format=ToolStrategy(Answer), system_prompt="Always return Answer.")
print(run_invoke(agent, "Summarize what an MCP server is.")["structured_response"])
```

**Book assistant over Qdrant** — see `main.py` in the repo root for a full app.

---

## Errors

| Error | Fix |
|-------|-----|
| `No model given and X_MODEL is not set` | Pass `model=` or set the env var |
| `No API key given and none of [...]` | Set `<NAME>_API_KEY` or pass `api_key=` |
| `Provider 'x' has no base URL` | Set `<NAME>_BASE_URL` or pass `base_url=` (unknown providers need one) |
| `ChatAnthropic needs its package: uv add langchain-anthropic` | Install the named package |
| `get_agent needs langchain: uv add langchain` | Install langchain |
| OpenCode `400 MissingSessionID` | Handled automatically; on custom gateways set `OPENCODE_SESSION_ID` |
| Call hangs ~40s (WSL) | IPv6 stall — this module prefers IPv4; `LLM_PREFER_IPV4=0` opts out |
| `Model is unavailable` on OpenCode | The model id is not served on that plan's chat endpoint |
| `gemini-*` on OpenCode fails | Google's native protocol is not wired; use `provider="google"` instead |

---

## Design notes

- **One rule for providers**: `<NAME>_API_KEY` / `<NAME>_MODEL` / `<NAME>_BASE_URL`.
  Built-ins are pre-filled shortcuts of that rule.
- **Args beat env**, env beats defaults. Blank `.env` values never wipe real keys.
- **`get_agent(**agent_options)` passes through to `create_agent`**, so new LangGraph
  features work without touching this module.
- **Lazy imports** — each LangChain integration is imported only when used, with the
  install command in the error.
- **Prices are manual on purpose** — rates change; you set them when you know the model.
- Need a client class we don't know? Build the model yourself and pass `llm=`.
