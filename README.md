# NobleBlocks MCP Server

<!-- mcp-name: io.github.nobleblocks/nobleblocks-mcp -->

<p align="center">
  <img src="https://www.nobleblocks.com/favicon.png" width="80" alt="NobleBlocks" />
</p>

> Search **290M+ academic papers** from your AI assistant — Claude, ChatGPT, Cursor, VS Code Copilot, and more.

---

## What you get

### Tools

| Tool | Description |
|------|-------------|
| `search_papers` | Full-text search across PubMed, OpenAlex, Semantic Scholar, arXiv, EuropePMC, and Scopus |
| `get_paper` | Fetch metadata for a paper by DOI, PMID, arXiv ID, or OpenAlex ID |
| `find_similar` | Semantic similarity search using vector embeddings |
| `get_citation_graph` | Explore who cites a paper and what it references |
| `multi_search` | Batch search: run up to 10 queries with deduplication (systematic reviews, multi-faceted research) |
| `get_field_overview` | Comprehensive field overview: top papers, recent work, reviews, gaps, key authors |
| `find_research_gaps` | Identify gaps by analyzing review papers for limitation/future-direction language |
| `create_literature_review` | Generate a structured lit review with citations (Pro only) |

### Pre-built Research Workflows (Prompts/Skills)

| Skill | What It Does |
|-------|-------------|
| `literature_review` | Multi-step lit review: recon → framework → sub-area searches → synthesis |
| `grant_research` | NIH grant positioning: gap analysis → RePORTER mapping → mechanism matching |
| `curriculum_reading_list` | Course reading list with discussion questions mapped to learning objectives |
| `systematic_review_protocol` | PRISMA-compliant protocol with search strings per database |
| `research_gaps` | Systematic gap identification: population, methodological, mechanism, application |
| `evidence_synthesis` | Evidence-based answer: meta-analyses → RCTs → observational → quality assessment |
| `paper_deep_dive` | Paper context: intellectual ancestry, impact, neighborhood, field positioning |
| `methodology_scout` | Find exemplary papers using a specific method, with best practices & pitfalls |
| `thesis_planner` | Literature landscape mapping for thesis/dissertation planning |
| `journal_targeting` | Find best journals for your manuscript with fit analysis |
| `prior_art_search` | Exhaustive prior art for patents, grants, or startup novelty claims |
| `trend_tracker` | Publication trend analysis with era-gated searches |
| `clinical_evidence` | Clinical evidence hierarchy (meta-analyses → RCTs → guidelines → safety) |
| `collaboration_finder` | Identify active research groups and potential collaborators |
| `proposal_strengthener` | Validate claims, find better citations, anticipate reviewer concerns |

## Quick Start

### 1. Install

```bash
pip install nobleblocks-mcp
```

Or from source:
```bash
git clone https://github.com/nobleblocks/nobleblocks-mcp.git
cd nobleblocks-mcp
pip install -e .
```

### 2. Configure Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "nobleblocks": {
      "command": "nobleblocks-mcp",
      "env": {
        "NOBLEBLOCKS_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

Restart Claude Desktop. The NobleBlocks tools appear in the tools picker (🔧).

### 3. Get an API Key (optional)

- **Free tier** (no key): 100 searches/day — great for trying it out
- **Pro tier**: Higher limits + literature review generation. Get a key at [nobleblocks.com/settings/api-keys](https://www.nobleblocks.com/settings/api-keys)

## Usage Examples

Once installed, just ask naturally:

> "Find the top 5 most-cited papers on CRISPR base editing from 2022-2024"

> "Help me with a literature review on gut microbiome and mental health"

> "Find NIH grants for my research on AI-assisted drug discovery"

> "What are the research gaps in immunotherapy for solid tumors?"

> "Build me a reading list for my graduate neuroscience course"

> "Is my idea novel? Using LLMs for automated systematic review screening"

> "What's trending in single-cell RNA-seq research since 2020?"

> "Show me papers similar to 'Attention Is All You Need' by Vaswani et al."

> "Find the best journal for my paper on climate adaptation strategies"

> "Evaluate the clinical evidence for psilocybin in treatment-resistant depression"

## Configure for Other AI Tools

### Cursor / Cline / Continue

Refer to your editor's MCP settings. The command is `nobleblocks-mcp` (stdio transport).

### VS Code Copilot

Add to `.vscode/mcp.json`:
```json
{
  "servers": {
    "nobleblocks": {
      "command": "nobleblocks-mcp",
      "env": {
        "NOBLEBLOCKS_API_KEY": ""
      }
    }
  }
}
```

### ChatGPT (Custom GPT)

We also provide a ChatGPT Custom GPT that uses the same API. See [chatgpt/GPT_CONFIG.md](chatgpt/GPT_CONFIG.md) for setup instructions.

## Security

| Protection | How |
|-----------|-----|
| **Input sanitization** | All inputs validated, length-capped, checked for injection patterns |
| **Rate limiting** | Per-minute (60/min) + daily quotas (100/day free, 5000/day Pro) |
| **Audit logging** | Every call logged (tool, args, timing) to JSON-L file |
| **No full text** | Only abstracts (max 600 chars) returned — full text stays behind paywall |
| **Bearer auth** | API key required for Pro features; validated server-side |
| **Prompt injection defense** | Paper content treated as untrusted; dangerous patterns rejected |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NOBLEBLOCKS_API_BASE` | `https://www.nobleblocks.com` | API endpoint |
| `NOBLEBLOCKS_API_KEY` | *(empty)* | Your API key for Pro tier |
| `RATE_LIMIT_FREE` | `100` | Daily limit without key |
| `RATE_LIMIT_PRO` | `5000` | Daily limit with key |
| `RATE_LIMIT_PER_MINUTE` | `60` | Per-minute throttle |
| `AUDIT_LOG_FILE` | `/tmp/nobleblocks-mcp-audit.jsonl` | Audit log path |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

## Development

```bash
# Test with MCP Inspector
npx @modelcontextprotocol/inspector nobleblocks-mcp
```

## Architecture

```
User's AI tool (Claude, Cursor, etc.)
        ↓ MCP (stdio)
  nobleblocks-mcp server (this package)
        ↓ HTTPS (Bearer auth)
  NobleBlocks API (nobleblocks.com)
        ↓ Internal VPC
  Paper Search DB (290M papers, pgvector)
```

The MCP server is a thin authenticated proxy — all search logic lives on the NobleBlocks backend. This means:
- **You don't need to update the MCP when we fix bugs or add features** — it just proxies to the API
- Search quality improves automatically as we improve the backend
- New paper sources appear without MCP changes

## License

MIT — see [LICENSE](LICENSE)

## Links

- **Website**: [nobleblocks.com](https://www.nobleblocks.com)
- **API Docs**: [nobleblocks.com/docs/api](https://www.nobleblocks.com/docs/api)
- **Support**: info@nobleblocks.com
- **Twitter**: [@nobleblocks](https://x.com/nobleblocks)
