"""
NobleBlocks CLI — search academic papers from the terminal.

Usage:
  nobleblocks search "CRISPR base editing" --limit 5 --min-year 2022
  nobleblocks get 10.1038/s41586-020-2649-2
  nobleblocks similar "Attention Is All You Need"
  nobleblocks citations 10.1038/s41586-020-2649-2

Requires NOBLEBLOCKS_API_KEY (or pass --key).
Get a free key: https://www.nobleblocks.com/settings/api-keys
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()
logging.getLogger("httpx").setLevel(logging.WARNING)

API_BASE = os.environ.get("NOBLEBLOCKS_API_BASE", "https://www.nobleblocks.com").rstrip("/")
HTTP_TIMEOUT = 30.0

SIGNUP_URL = "https://www.nobleblocks.com/auth/signup"
KEY_URL = "https://www.nobleblocks.com/settings/api-keys"


def _get_key(args) -> str:
    key = getattr(args, "key", None) or os.environ.get("NOBLEBLOCKS_API_KEY", "")
    if not key:
        print(
            "Error: No API key provided.\n"
            f"  1. Sign up free at {SIGNUP_URL}\n"
            f"  2. Generate a key at {KEY_URL}\n"
            "  3. Set NOBLEBLOCKS_API_KEY in your environment or pass --key",
            file=sys.stderr,
        )
        sys.exit(1)
    return key


def _headers(key: str) -> dict:
    return {
        "User-Agent": "nobleblocks-cli/2.0.0",
        "Accept": "application/json",
        "Authorization": f"Bearer {key}",
    }


async def _get(path: str, params: dict, key: str) -> dict:
    url = f"{API_BASE}{path}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=_headers(key)) as client:
        resp = await client.get(url, params={k: v for k, v in params.items() if v is not None})
        if resp.status_code == 401:
            print("Error: Invalid API key. Check your key or generate a new one at", KEY_URL, file=sys.stderr)
            sys.exit(1)
        if resp.status_code == 403:
            print("Error: Access denied. Your API key may not have permission for this.", file=sys.stderr)
            sys.exit(1)
        if resp.status_code == 429:
            print("Error: Rate limit exceeded. Try again later or upgrade at https://www.nobleblocks.com/pricing", file=sys.stderr)
            sys.exit(1)
        resp.raise_for_status()
        return resp.json()


def _print_papers(papers: list[dict], verbose: bool = False):
    for i, p in enumerate(papers, 1):
        title = p.get("title", "Untitled")
        authors = p.get("authors") or []
        if isinstance(authors, list) and authors:
            if isinstance(authors[0], dict):
                author_str = ", ".join(a.get("name", "") for a in authors[:3])
            else:
                author_str = ", ".join(str(a) for a in authors[:3])
            if len(authors) > 3:
                author_str += f" +{len(authors)-3} more"
        else:
            author_str = ""

        year = p.get("year") or ""
        citations = p.get("citationCount") or p.get("citation_count") or 0
        doi = p.get("doi") or p.get("DOI") or ""

        print(f"\n[{i}] {title}")
        if author_str:
            print(f"    {author_str} ({year})")
        print(f"    Citations: {citations}", end="")
        if doi:
            print(f"  |  DOI: {doi}", end="")
        print()

        if verbose:
            abstract = p.get("abstract") or ""
            if abstract:
                print(f"    {abstract[:300]}{'...' if len(abstract) > 300 else ''}")


async def cmd_search(args):
    key = _get_key(args)
    params = {
        "query": args.query,
        "limit": args.limit,
        "min_year": getattr(args, "min_year", None),
        "max_year": getattr(args, "max_year", None),
        "min_citations": getattr(args, "min_citations", None),
        "sort": getattr(args, "sort", "relevance"),
    }
    data = await _get("/api/v1/papers/search", params, key)
    papers = data.get("papers") or data.get("results") or []
    total = data.get("total", len(papers))
    print(f"Found {total:,} results for '{args.query}'")
    _print_papers(papers[:args.limit], verbose=args.verbose)


async def cmd_get(args):
    key = _get_key(args)
    data = await _get("/api/v1/papers/lookup", {"id": args.paper_id}, key)
    paper = data.get("paper") or data
    if args.json:
        print(json.dumps(paper, indent=2, default=str))
    else:
        _print_papers([paper], verbose=True)


async def cmd_similar(args):
    key = _get_key(args)
    data = await _get("/api/v1/papers/similar", {"query": args.query, "limit": args.limit}, key)
    papers = data.get("papers") or data.get("results") or []
    print(f"Papers similar to: '{args.query}'")
    _print_papers(papers[:args.limit], verbose=args.verbose)


async def cmd_citations(args):
    key = _get_key(args)
    data = await _get("/api/v1/papers/citation-graph", {"paperId": args.paper_id, "limit": args.limit}, key)

    refs = data.get("references") or []
    cites = data.get("citations") or []

    if args.direction in ("references", "both") and refs:
        print(f"\nReferences ({len(refs)}):")
        _print_papers(refs[:args.limit])

    if args.direction in ("citations", "both") and cites:
        print(f"\nCited by ({len(cites)}):")
        _print_papers(cites[:args.limit])

    if not refs and not cites:
        print("No citation data found for this paper.")


def main():
    parser = argparse.ArgumentParser(
        prog="nobleblocks",
        description="Search 300M+ academic papers from the terminal.",
    )
    parser.add_argument("--key", help="NobleBlocks API key (or set NOBLEBLOCKS_API_KEY)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # search
    sp = subparsers.add_parser("search", help="Search papers by keyword or question")
    sp.add_argument("query", help="Search query")
    sp.add_argument("--limit", type=int, default=10, help="Max results (default 10)")
    sp.add_argument("--min-year", type=int, help="Earliest publication year")
    sp.add_argument("--max-year", type=int, help="Latest publication year")
    sp.add_argument("--min-citations", type=int, help="Minimum citation count")
    sp.add_argument("--sort", choices=["relevance", "date", "citations"], default="relevance")
    sp.add_argument("-v", "--verbose", action="store_true", help="Show abstracts")
    sp.add_argument("--key", help=argparse.SUPPRESS)

    # get
    sp = subparsers.add_parser("get", help="Get paper by DOI, PMID, or arXiv ID")
    sp.add_argument("paper_id", help="DOI, PMID, arXiv ID, or OpenAlex ID")
    sp.add_argument("--json", action="store_true", help="Output raw JSON")
    sp.add_argument("--key", help=argparse.SUPPRESS)

    # similar
    sp = subparsers.add_parser("similar", help="Find semantically similar papers")
    sp.add_argument("query", help="Paper title or text to find similar papers for")
    sp.add_argument("--limit", type=int, default=10, help="Max results (default 10)")
    sp.add_argument("-v", "--verbose", action="store_true", help="Show abstracts")
    sp.add_argument("--key", help=argparse.SUPPRESS)

    # citations
    sp = subparsers.add_parser("citations", help="Get citation graph for a paper")
    sp.add_argument("paper_id", help="DOI, PMID, or paper ID")
    sp.add_argument("--direction", choices=["references", "citations", "both"], default="both")
    sp.add_argument("--limit", type=int, default=20, help="Max per direction (default 20)")
    sp.add_argument("--key", help=argparse.SUPPRESS)

    args = parser.parse_args()

    if args.command == "search":
        asyncio.run(cmd_search(args))
    elif args.command == "get":
        asyncio.run(cmd_get(args))
    elif args.command == "similar":
        asyncio.run(cmd_similar(args))
    elif args.command == "citations":
        asyncio.run(cmd_citations(args))


if __name__ == "__main__":
    main()
