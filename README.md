# NobleBlocks MCP Server

Expose the NobleBlocks paper search corpus (290M+ academic papers) to any AI tool that speaks the **Model Context Protocol** — Claude Desktop, Cursor, Cline, ChatGPT (via MCP bridges), etc.

## What this gives your AI tool

Three new tools become available in your AI assistant:

| Tool | What it does |
|---|---|
| `search_papers` | Full-text search across 290M+ papers from PubMed, OpenAlex, SemanticScholar, arXiv, EuropePMC, Scopus. Filter by year, citations, source. |
| `get_paper` | Fetch full metadata for a single paper by DOI / PMID / arXiv ID / OpenAlex ID. |
| `find_similar` | Semantic similarity search using vector embeddings (pgvector) — discover related work beyond keyword matches. |

## Install

```bash
cd nobleblocks-mcp
pip install -e .
```

This installs the `nobleblocks-mcp` console script.

## Configure (Claude Desktop)

Add to your `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "nobleblocks": {
      "command": "nobleblocks-mcp",
      "env": {
        "NOBLEBLOCKS_API_BASE": "https://www.nobleblocks.com",
        "NOBLEBLOCKS_API_KEY": ""
      }
    }
  }
}
```

Restart Claude Desktop. You should see the three NobleBlocks tools available in the tools picker.

## Configure (Cursor / Cline / Continue)

Refer to your editor's MCP settings docs. The command is `nobleblocks-mcp` and it speaks MCP over stdio.

## Free vs Pro

| Tier | Quota | API key required? |
|---|---|---|
| **Free** | 100 queries/day per IP | No |
| **Pro** | Higher quotas, credits-based | Yes — get one at https://www.nobleblocks.com/settings/api-keys |

Set `NOBLEBLOCKS_API_KEY` in the config above to authenticate as Pro.

## Try it

Once installed, ask Claude (or your AI tool):

> "Find me the top 5 most-cited papers on CRISPR base editing from the last 3 years."

Claude will call `search_papers` with the right filters and synthesize the results.

> "Show me papers similar to this one: Doudna & Charpentier 2014 on CRISPR-Cas9 mechanism."

Claude will call `find_similar`.

## Development

```bash
# run locally for testing (stdio)
NOBLEBLOCKS_API_BASE=https://www.dev.nobleblocks.com nobleblocks-mcp
```

Use the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) for interactive testing:

```bash
npx @modelcontextprotocol/inspector nobleblocks-mcp
```

## Roadmap

- [ ] Streaming responses (large result sets)
- [ ] `summarize_papers` tool — invokes the AI synthesis pipeline directly
- [ ] `slr_create` — start a Systematic Literature Review from MCP
- [ ] Resource subscriptions (live updates when new papers match a saved query)
