#!/usr/bin/env python3
"""
Deploy NobleBlocks MCP Remote Server to ECS (Fargate)
=====================================================

Creates/updates:
  - ECR repository: nobleblocks-mcp
  - ECS Task Definition: nobleblocks-mcp
  - ECS Service: nobleblocks-mcp-service on nobleblocks-cluster

Usage:
  AWS_PROFILE=admin-delroy python3 scripts/deploy_mcp_ecs.py
  AWS_PROFILE=admin-delroy python3 scripts/deploy_mcp_ecs.py --skip-tests   # emergency hotfix
  AWS_PROFILE=admin-delroy python3 scripts/deploy_mcp_ecs.py --bump minor   # minor version bump
"""

import subprocess
import sys
import json
import os
import re
import time
from pathlib import Path

REGION = "ap-southeast-1"
ACCOUNT_ID = "891377173693"
CLUSTER = "nobleblocks-cluster"
ECR_REPO = "nobleblocks-mcp"
SERVICE_NAME = "nobleblocks-mcp-service"
TASK_FAMILY = "nobleblocks-mcp"
TASK_ROLE = "nobleblocks-mcp-task-role"  # grants DynamoDB access for OAuth token store
IMAGE_TAG = "latest"
CONTAINER_PORT = 8080
CPU = "256"
MEMORY = "512"
HEALTH_URL = "https://mcp.nobleblocks.com/health"

# Paths
MCP_DIR = Path(__file__).resolve().parent.parent
PYPROJECT = MCP_DIR / "pyproject.toml"
SERVER_JSON = MCP_DIR / "server.json"
REGRESSION_SCRIPT = MCP_DIR / "scripts" / "regression_test.py"


def run(cmd, check=True, cwd=None):
    print(f"  → {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if check and result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        sys.exit(1)
    return result


def get_version() -> str:
    """Read version from pyproject.toml."""
    text = PYPROJECT.read_text()
    match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    if not match:
        print("  ERROR: Could not parse version from pyproject.toml")
        sys.exit(1)
    return match.group(1)


def bump_version(bump_type: str = "patch") -> str:
    """Bump version in pyproject.toml and server.json. Returns new version."""
    current = get_version()
    parts = current.split(".")
    if len(parts) != 3:
        print(f"  ERROR: Version '{current}' is not semver")
        sys.exit(1)

    major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])
    if bump_type == "major":
        major, minor, patch = major + 1, 0, 0
    elif bump_type == "minor":
        major, minor, patch = major, minor + 1, 0
    else:  # patch
        patch += 1
    new_version = f"{major}.{minor}.{patch}"

    # Update pyproject.toml
    text = PYPROJECT.read_text()
    text = re.sub(r'^(version\s*=\s*)"[^"]+"', f'\\1"{new_version}"', text, count=1, flags=re.MULTILINE)
    PYPROJECT.write_text(text)

    # Update server.json
    if SERVER_JSON.exists():
        sj = json.loads(SERVER_JSON.read_text())
        sj["version"] = new_version
        SERVER_JSON.write_text(json.dumps(sj, indent=4) + "\n")

    print(f"  Version bumped: {current} → {new_version}")
    return new_version


def run_regression_tests() -> bool:
    """Run static regression tests. Returns True if passed."""
    print("\n[0/6] Running regression tests...")
    result = run(f"python3 {REGRESSION_SCRIPT} --check-code", check=False, cwd=str(MCP_DIR.parent))
    if result.returncode != 0:
        print("\n  ✗ REGRESSION TESTS FAILED — deploy aborted.")
        print("    Fix the issues above before deploying.")
        print("    Use --skip-tests for emergency hotfixes only.\n")
        return False
    print("  ✓ All regression tests passed")
    return True


def verify_deployment(expected_version: str, max_wait: int = 120) -> bool:
    """Poll health endpoint until new version appears or timeout."""
    print(f"\n[6/6] Verifying deployment (waiting for v{expected_version})...")
    import urllib.request
    import urllib.error

    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            req = urllib.request.Request(HEALTH_URL, headers={"User-Agent": "deploy-verify"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                live_version = data.get("version", "")
                if live_version == expected_version:
                    print(f"  ✓ Live version confirmed: {live_version}")
                    return True
                print(f"  … waiting (current: {live_version}, want: {expected_version})")
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            print("  … waiting (health check not responding yet)")
        time.sleep(10)

    print(f"  ✗ Timeout: version {expected_version} not live after {max_wait}s")
    return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Deploy NobleBlocks MCP to ECS")
    parser.add_argument("--skip-tests", action="store_true", help="Skip regression tests (emergency only)")
    parser.add_argument("--bump", choices=["patch", "minor", "major"], default="patch",
                        help="Version bump type (default: patch)")
    parser.add_argument("--no-verify", action="store_true", help="Skip post-deploy verification")
    args = parser.parse_args()

    # Step 0: Regression tests (unless skipped)
    if not args.skip_tests:
        if not run_regression_tests():
            sys.exit(1)
    else:
        print("\n  ⚠ Regression tests SKIPPED (--skip-tests)")

    # Auto-bump version
    new_version = bump_version(args.bump)

    ecr_uri = f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}"

    # Environment variables for the container
    container_env = {
        "NOBLEBLOCKS_API_BASE": "https://www.nobleblocks.com",
        "MCP_BASE_URL": "https://mcp.nobleblocks.com",
        "NB_INTERNAL_TOKEN": "6vdNeoQJ0Dbi2-NDhRiNljKZoKCzxxg2vxI-i1oy7u56mdr_YP6K1H9RDu1amXLu",
        "MCP_HOST": "0.0.0.0",
        "MCP_PORT": "8080",
        "MCP_VERSION": new_version,
        "LOG_LEVEL": "INFO",
    }

    print("\n" + "=" * 60)
    print(f"  NobleBlocks MCP Server — ECS Deployment v{new_version}")
    print("=" * 60)

    # Step 1: Ensure ECR repository exists
    print("\n[1/6] Ensuring ECR repository exists...")
    result = run(
        f"aws ecr describe-repositories --repository-names {ECR_REPO} --region {REGION} 2>/dev/null",
        check=False,
    )
    if result.returncode != 0:
        run(f"aws ecr create-repository --repository-name {ECR_REPO} --region {REGION}")
        print("  Created ECR repository")
    else:
        print("  ECR repository exists")

    # Step 2: Build and push Docker image
    print("\n[2/6] Building Docker image...")
    os.chdir(str(MCP_DIR))
    run(f"docker build -f Dockerfile.remote -t {ECR_REPO}:{IMAGE_TAG} .")

    print("\n[3/6] Pushing to ECR...")
    run(f"aws ecr get-login-password --region {REGION} | docker login --username AWS --password-stdin {ecr_uri.split('/')[0]}")
    run(f"docker tag {ECR_REPO}:{IMAGE_TAG} {ecr_uri}:{IMAGE_TAG}")
    run(f"docker push {ecr_uri}:{IMAGE_TAG}")

    # Step 3: Register task definition
    print("\n[4/6] Registering task definition...")
    env_list = [{"name": k, "value": v} for k, v in container_env.items()]

    task_def = {
        "family": TASK_FAMILY,
        "networkMode": "awsvpc",
        "requiresCompatibilities": ["FARGATE"],
        "cpu": CPU,
        "memory": MEMORY,
        "executionRoleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/ecsTaskExecutionRole",
        "taskRoleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/{TASK_ROLE}",
        "containerDefinitions": [
            {
                "name": ECR_REPO,
                "image": f"{ecr_uri}:{IMAGE_TAG}",
                "portMappings": [{"containerPort": CONTAINER_PORT, "protocol": "tcp"}],
                "environment": env_list,
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": f"/ecs/{TASK_FAMILY}",
                        "awslogs-region": REGION,
                        "awslogs-stream-prefix": "ecs",
                        "awslogs-create-group": "true",
                    },
                },
                "essential": True,
                "healthCheck": {
                    "command": ["CMD-SHELL", f"python -c \"import urllib.request; urllib.request.urlopen('http://localhost:{CONTAINER_PORT}/health')\""],
                    "interval": 30,
                    "timeout": 5,
                    "retries": 3,
                },
            }
        ],
    }

    task_def_path = "/tmp/nobleblocks-mcp-taskdef.json"
    with open(task_def_path, "w") as f:
        json.dump(task_def, f)
    result = run(
        f"aws ecs register-task-definition --cli-input-json file://{task_def_path} --region {REGION}"
    )
    td = json.loads(result.stdout)
    td_arn = td["taskDefinition"]["taskDefinitionArn"]
    print(f"  Task definition: {td_arn}")

    # Step 4: Update service (force new deployment)
    print("\n[5/6] Updating ECS service...")
    result = run(
        f"aws ecs describe-services --cluster {CLUSTER} --services {SERVICE_NAME} --region {REGION}",
        check=False,
    )

    if result.returncode == 0:
        services = json.loads(result.stdout).get("services", [])
        active = [s for s in services if s.get("status") == "ACTIVE"]
        if active:
            run(
                f"aws ecs update-service --cluster {CLUSTER} --service {SERVICE_NAME} "
                f"--task-definition {td_arn} --force-new-deployment --region {REGION}"
            )
            print("  Service updated — rolling deployment started")
        else:
            print("  No active service found, need to create one.")
            print(f"  Task definition ARN: {td_arn}")
    else:
        print("  ⚠ Service doesn't exist yet. Manual step required.")

    # Step 5: Verify deployment
    if not args.no_verify:
        success = verify_deployment(new_version)
        if not success:
            print("  ⚠ Deployment may still be rolling out. Check ECS console.")
    else:
        print("\n[6/6] Verification skipped (--no-verify)")

    # Git commit the version bump
    print("\n  Committing version bump...")
    run(f"git add pyproject.toml server.json", cwd=str(MCP_DIR))
    run(f'git commit -m "release: v{new_version} — deployed to ECS"', cwd=str(MCP_DIR), check=False)

    print("\n" + "=" * 60)
    print(f"  ✓ DEPLOYED: v{new_version}")
    print(f"  Image:  {ecr_uri}:{IMAGE_TAG}")
    print(f"  Task:   {td_arn}")
    print(f"  Live:   https://mcp.nobleblocks.com/mcp")
    print(f"  Health: {HEALTH_URL}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
