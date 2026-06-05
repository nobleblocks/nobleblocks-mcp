# NobleBlocks MCP Server

<!-- mcp-name: com.nobleblocks/nobleblocks-mcp -->

<p align="center">
  <a href="https://registry.modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP_Registry-listed-brightgreen" alt="MCP Registry"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
  <a href="https://www.nobleblocks.com"><img src="https://img.shields.io/badge/papers-340M%2B-orange" alt="Papers"></a>
</p>

Search 340 million deduplicated academic papers from Claude Desktop, Claude Web, ChatGPT, Cursor, VS Code Copilot, or any MCP-compatible tool. Covers 15+ academic databases — all cross-linked through a biomedical knowledge graph with 1.3M+ entities and 109M+ paper connections.

This is a thin authenticated client — all indexing, ranking, vector search, and knowledge graph traversal runs on the NobleBlocks backend. You get fast results without managing any infrastructure.

---

## What's in the index

| Source | Papers | Updates |
|--------|--------|---------|
| OpenAlex | ~250M works | Daily |
| Semantic Scholar | ~220M papers | Weekly |
| PubMed / MEDLINE | ~41M articles | Daily |
| Europe PMC | ~43M articles | Daily |
| Crossref | Incremental | Daily |
| arXiv | ~2.5M preprints | Daily |
| bioRxiv / medRxiv | ~1K/day | Daily |
| ClinicalTrials.gov | ~582K trials | Daily |
| Unpaywall | OA link resolution | Daily |
| DBLP | Computer science | Via OpenAlex |
| CORE | ~37M open access outputs | Coming soon |
| BASE (Bielefeld) | Discovery metadata | Via OpenAlex |
| DOAJ | Open access journals | Via Crossref |
| Papers with Code | ML/AI benchmarks | Weekly |
| Retraction Watch | Retraction status | Weekly |
| USPTO / EPO patents | ~9.3M patent-paper links | Daily |
| …and others | | |

All sources are cross-deduplicated on DOI, PMID, and arXiv ID. Total unique records after dedup: **340M+** (June 2026).

## Knowledge graph

The search backend also maintains a biomedical knowledge graph (1.3M+ entities, 109M+ paper links) with genes, diseases, drugs, proteins, institutions, researchers, topics, genetic variants, and drug-target-disease relationships sourced from PubTator Central, DisGeNET, ChEMBL, UniProt, ROR, ORCID, Open Targets, and others.

## Tools

| Tool | What it does |
|------|-------------|
| `search_papers` | Full-text + semantic hybrid search with year/citation/source filters |
| `get_paper` | Fetch metadata by DOI, PMID, arXiv ID, or OpenAlex ID |
| `find_similar` | Find papers related by meaning, not just keyword matching |
| `get_citation_graph` | Citation network — references and citing papers |
| `search_by_entity` | Find papers linked to a gene, drug, disease, or institution via the KG |
| `create_literature_review` | AI-generated structured lit review with citations (Pro) |

---

## Get an API key

All usage requires a free NobleBlocks account. Sign up takes 30 seconds:

1. Go to [nobleblocks.com/signup](https://www.nobleblocks.com/auth/signup)
2. Create an account (email or Google)
3. Navigate to [Settings > API Keys](https://www.nobleblocks.com/settings/api-keys) and generate a key

**Free plan**: 100 searches/day, 5 similar/day, citation graph access.
**Pro plan**: 5,000 searches/day, literature review generation, priority support.

---

## Quick start (Remote — no install needed)

The easiest way to get started is the **remote connector** — no API keys, no Python install:

1. Open [claude.ai](https://claude.ai) → Settings → Connectors (or Integrations)
2. Click "Add" and paste: `https://mcp.nobleblocks.com/mcp`
3. Sign in with your NobleBlocks account
4. Ask Claude: *"Search for papers on CRISPR gene therapy"*

---

## Install (Local)

```bash
uvx nobleblocks-mcp
```

---

## Configure Claude Desktop

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "nobleblocks": {
      "command": "nobleblocks-mcp",
      "env": {
        "NOBLEBLOCKS_API_KEY": "nb_pk_your_key_here"
      }
    }
  }
}
```

Restart Claude Desktop. The NobleBlocks tools will appear in the tools list.

## Configure VS Code Copilot

Add to `.vscode/mcp.json` in your project:

```json
{
  "servers": {
    "nobleblocks": {
      "command": "nobleblocks-mcp",
      "env": {
        "NOBLEBLOCKS_API_KEY": "nb_pk_your_key_here"
      }
    }
  }
}
```

## Configure Cursor / Cline / Continue

Point your editor's MCP settings at the `nobleblocks-mcp` command with stdio transport. Set `NOBLEBLOCKS_API_KEY` in the environment.

---

## CLI usage

You can also search directly from the terminal:

```bash
# Search papers
nobleblocks search "CRISPR base editing" --limit 5 --min-year 2022

# Get a specific paper
nobleblocks get 10.1038/s41586-020-2649-2

# Find similar papers
nobleblocks similar "Attention Is All You Need"

# Citation graph
nobleblocks citations 10.1038/s41586-020-2649-2 --direction both
```

All CLI commands require `NOBLEBLOCKS_API_KEY` set in your environment (or pass `--key`).

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NOBLEBLOCKS_API_KEY` | *(required)* | Your API key from nobleblocks.com |
| `NOBLEBLOCKS_API_BASE` | `https://www.nobleblocks.com` | API endpoint |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

---

## Examples

Once configured, ask naturally in Claude/Cursor:

> "Find the most-cited papers on CRISPR base editing from 2022-2024"

> "Show me papers similar to 'Attention Is All You Need' by Vaswani"

> "What papers cite DOI 10.1038/s41586-020-2649-2?"

> "Find papers linking BRCA1 to drug resistance"

> "Generate a literature review on CAR-T cell therapy in solid tumors"

---

## Development

```bash
git clone https://github.com/nobleblocks/nobleblocks-mcp.git
cd nobleblocks-mcp
pip install -e .

# Test with MCP Inspector
npx @modelcontextprotocol/inspector nobleblocks-mcp
```

## License

MIT — see [LICENSE](LICENSE).

---

Built by [NobleBlocks](https://www.nobleblocks.com) — the academic research platform for the AI era.
