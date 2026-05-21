# NobleBlocks MCP Server

<!-- mcp-name: io.github.nobleblocks/nobleblocks-mcp -->

<p align="center">
  <img src="https://www.nobleblocks.com/favicon.png" width="80" alt="NobleBlocks" />
</p>

<p align="center">
  <a href="https://pypi.org/project/nobleblocks-mcp/"><img src="https://img.shields.io/pypi/v/nobleblocks-mcp.svg" alt="PyPI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
  <a href="https://www.nobleblocks.com"><img src="https://img.shields.io/badge/papers-300M%2B-orange" alt="Papers"></a>
  <a href="https://github.com/nobleblocks/nobleblocks-mcp/stargazers"><img src="https://img.shields.io/github/stars/nobleblocks/nobleblocks-mcp?style=social" alt="Stars"></a>
</p>

> The largest pre-indexed academic search MCP — **300M+ deduplicated papers**, a biomedical knowledge graph, and vector embeddings. Works with Claude, ChatGPT, Cursor, VS Code Copilot, and any MCP-compatible AI tool.

**Unlike API-relay MCPs that query one source at a time, NobleBlocks searches a unified, pre-deduplicated database of 300M+ papers with sub-second latency.**

---

## Why NobleBlocks?

| Feature | NobleBlocks MCP | Typical Paper-Search MCPs |
|---------|----------------|--------------------------|
| **Papers indexed** | 300M+ deduplicated | Live API relay (no local data) |
| **Deduplication** | Cross-source dedup (DOI/PMID/arXiv) | Per-query, client-side |
| **Vector search** | HNSW embeddings on all papers | Not available |
| **Knowledge graph** | 1.3M+ entities (genes, drugs, diseases, institutions) | None |
| **Search latency** | 50–500ms (pre-indexed) | 2–10s (live API calls) |
| **Citation graph** | Pre-computed, depth-2 BFS | Not available |
| **Full-text extraction** | 8M+ OA papers with parsed sections | Not available |

---

## Data Sources (deduplicated into one unified index)

| Source | Papers | Update Cycle |
|--------|--------|--------------|
| OpenAlex | ~250M works | Daily incremental |
| Semantic Scholar | ~220M papers | Weekly snapshot |
| PubMed / MEDLINE | ~41M articles | Daily |
| Europe PMC | ~43M articles | Daily |
| DOAJ | ~43M articles | Complete |
| Crossref | Incremental | Daily cursor |
| arXiv | ~2.5M preprints | Daily |
| Scopus | ~90M (via API) | Weekly |
| bioRxiv / medRxiv | ~1K/day | Daily |
| ClinicalTrials.gov | ~582K trials | Daily |
| SciELO | ~166K articles | Daily |

**Total after deduplication: 300M+ unique papers** (as of May 2026)

---

## Knowledge Graph & Enrichment

Beyond papers, every search query has access to a **biomedical knowledge graph** with 16+ entity types:

| Entity Type | Source | Coverage |
|-------------|--------|----------|
| Genes | PubTator Central NER | ~100K+ unique genes |
| Diseases | PubTator + DisGeNET | ~30K+ diseases |
| Chemicals / Drugs | ChEMBL + DrugBank + PubTator | ~500K+ molecules |
| Proteins | UniProt | ~500K+ reviewed entries |
| Institutions | Research Organization Registry (ROR) | ~125K normalized |
| Researchers | ORCID Public Data | ~18M profiles |
| Topics & Concepts | OpenAlex | ~65K+ topics |
| Genetic Variants | GWAS Catalog | 1M+ associations |
| Drug→Target→Disease | Open Targets Platform | Full therapeutic graph |
| Code & Datasets | Papers with Code | ~200K papers linked |
| Retractions | Retraction Watch + OpenAlex | ~47K flagged papers |
| Funding Links | Europe PMC Grants | NIH, ERC, Wellcome, NSF, MRC, + more |

**Total KG entities: 1.3M+** | **Paper→Entity links: 109M+** | **Citation edges: 1.46M+**

---

## Tools

| Tool | Description |
|------|-------------|
| `search_papers` | Hybrid full-text + semantic search across all 300M+ papers |
| `get_paper` | Fetch metadata by DOI, PMID, arXiv ID, or OpenAlex ID |
| `find_similar` | Vector-embedding similarity search (HNSW, 768-dim) |
| `get_citation_graph` | Pre-computed citation network — who cites what, depth-2 |
| `search_by_entity` | Find papers linked to a gene, drug, disease, or institution |
| `create_literature_review` | AI-generated structured lit review with citations (Pro) |

## Quick Start

### Option A: pip (recommended)

```bash
pip install nobleblocks-mcp
```

### Option B: uvx (no install needed)

```bash
uvx nobleblocks-mcp
```

### Option C: Docker

```bash
docker run -e NOBLEBLOCKS_API_KEY=your-key ghcr.io/nobleblocks/nobleblocks-mcp
```

### Option D: From source

```bash
git clone https://github.com/nobleblocks/nobleblocks-mcp.git
cd nobleblocks-mcp
pip install -e .
```

### Configure Claude Desktop

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

### Get an API Key (optional)

- **Free tier** (no key): 100 searches/day — great for trying it out
- **Pro tier**: Higher limits + literature review generation. Get a key at [nobleblocks.com/settings/api-keys](https://www.nobleblocks.com/settings/api-keys)

## Usage Examples

Once installed, just ask naturally:

> "Find the top 5 most-cited papers on CRISPR base editing from 2022-2024"

> "Show me papers similar to 'Attention Is All You Need' by Vaswani et al."

> "What's the citation network for DOI 10.1038/s41586-020-2649-2?"

> "Generate a literature review on stem cell treatments for Parkinson's disease"

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
        ↓ MCP (stdio or Streamable HTTP)
  nobleblocks-mcp server (this package)
        ↓ HTTPS (Bearer auth)
  NobleBlocks API (nobleblocks.com)
        ↓ Internal VPC
  ┌─────────────────────────────────────────┐
  │  Paper Search DB (300M+ papers)         │
  │  • PostgreSQL + pgvector (HNSW)         │
  │  • GIN full-text index (12GB)           │
  │  • 768-dim embeddings (all papers)      │
  │  • Citation graph (1.46M+ edges)        │
  │  • Knowledge graph (109M+ links)        │
  └─────────────────────────────────────────┘
```

The MCP server is a thin authenticated proxy — all search logic lives on the NobleBlocks backend. This means:
- **Zero maintenance** — you don't need to update the MCP when we fix bugs or add features
- **Search quality improves automatically** as we improve ranking, embeddings, and the KG
- **New paper sources appear without MCP changes** — papers indexed daily

## Contributing

We welcome contributions! This is the MCP client layer — feel free to:
- Add new tool wrappers for research workflows
- Improve error handling or retry logic
- Add support for new AI editors / MCP clients
- Fix bugs or improve documentation

```bash
git clone https://github.com/nobleblocks/nobleblocks-mcp.git
cd nobleblocks-mcp
pip install -e ".[dev]"
```

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=nobleblocks/nobleblocks-mcp&type=Date)](https://star-history.com/#nobleblocks/nobleblocks-mcp&Date)

## License

MIT — see [LICENSE](LICENSE)

## Links

- **Website**: [nobleblocks.com](https://www.nobleblocks.com)
- **API Docs**: [nobleblocks.com/docs/api](https://www.nobleblocks.com/docs/api)
- **Platform**: [Search 300M+ papers](https://www.nobleblocks.com/search)
- **Support**: info@nobleblocks.com
- **Twitter**: [@nobleblocks](https://x.com/nobleblocks)
