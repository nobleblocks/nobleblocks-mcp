"""
DynamoDB-backed token store for MCP OAuth.

Replaces the in-memory dicts in NobleBlocksOAuthProvider with durable
storage that survives ECS restarts and works across multiple containers.

Table schema:
  PK  = "token_type#token_value"   (e.g. "access#Abc123...")
  SK  = "meta"
  TTL = expires_at (epoch seconds, DynamoDB auto-deletes expired items)
  data = JSON blob of the token/client/mapping

Uses a single table (OAUTH_TABLE_NAME env var, default "nobleblocks-mcp-oauth").
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("nobleblocks-mcp.dynamo")

TABLE_NAME = os.environ.get("OAUTH_TABLE_NAME", "nobleblocks-mcp-oauth")
AWS_REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1"))


def _get_table():
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    return dynamodb.Table(TABLE_NAME)


def _pk(token_type: str, key: str) -> str:
    return f"{token_type}#{key}"


def _epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def _serialize_datetime(obj):
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _deserialize_datetime(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s)


class DynamoTokenStore:
    """Key-value store backed by DynamoDB with automatic TTL expiry."""

    def __init__(self):
        self._table = _get_table()

    # ── Core ops ─────────────────────────────────────────────────────────────

    def put(self, token_type: str, key: str, data: dict, ttl_epoch: int | None = None):
        """Store a token/record. TTL is optional epoch seconds for auto-deletion."""
        item = {
            "PK": _pk(token_type, key),
            "SK": "meta",
            "data": json.dumps(data, default=_serialize_datetime),
            "token_type": token_type,
            "created_at": int(time.time()),
        }
        if ttl_epoch:
            item["TTL"] = ttl_epoch
        self._table.put_item(Item=item)

    def get(self, token_type: str, key: str) -> dict | None:
        """Retrieve a record. Returns None if not found or expired."""
        try:
            resp = self._table.get_item(
                Key={"PK": _pk(token_type, key), "SK": "meta"}
            )
            item = resp.get("Item")
            if not item:
                return None
            # Check manual expiry (DynamoDB TTL can lag up to 48h)
            ttl = item.get("TTL")
            if ttl and int(ttl) < int(time.time()):
                return None
            return json.loads(item["data"])
        except ClientError as e:
            logger.error("DynamoDB get error: %s", e)
            return None

    def delete(self, token_type: str, key: str):
        """Delete a record."""
        try:
            self._table.delete_item(
                Key={"PK": _pk(token_type, key), "SK": "meta"}
            )
        except ClientError as e:
            logger.error("DynamoDB delete error: %s", e)

    def scan_by_type(self, token_type: str) -> list[dict]:
        """Scan all items of a given type. Use sparingly (not efficient at scale)."""
        try:
            resp = self._table.scan(
                FilterExpression="token_type = :tt",
                ExpressionAttributeValues={":tt": token_type},
            )
            items = []
            for item in resp.get("Items", []):
                ttl = item.get("TTL")
                if ttl and int(ttl) < int(time.time()):
                    continue
                data = json.loads(item["data"])
                data["_key"] = item["PK"].split("#", 1)[1] if "#" in item["PK"] else item["PK"]
                items.append(data)
            return items
        except ClientError as e:
            logger.error("DynamoDB scan error: %s", e)
            return []


def ensure_table_exists():
    """Create the DynamoDB table if it doesn't exist. Idempotent."""
    client = boto3.client("dynamodb", region_name=AWS_REGION)
    try:
        client.describe_table(TableName=TABLE_NAME)
        logger.info("DynamoDB table '%s' already exists", TABLE_NAME)
    except client.exceptions.ResourceNotFoundException:
        logger.info("Creating DynamoDB table '%s'...", TABLE_NAME)
        client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        # Wait for table to become active
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=TABLE_NAME)
        # Enable TTL
        client.update_time_to_live(
            TableName=TABLE_NAME,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "TTL",
            },
        )
        logger.info("DynamoDB table '%s' created with TTL enabled", TABLE_NAME)
