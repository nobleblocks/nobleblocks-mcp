#!/usr/bin/env python3
"""
Deploy NobleBlocks MCP Remote Server to ECS (Fargate)
=====================================================

Creates/updates:
  - ECR repository: nobleblocks-mcp
  - ECS Task Definition: nobleblocks-mcp
  - ECS Service: nobleblocks-mcp-service on nobleblocks-cluster

Usage:
  AWS_PROFILE=admin-delroy python3 deploy_mcp_ecs.py
"""

import subprocess
import sys
import json
import os

REGION = "ap-southeast-1"
ACCOUNT_ID = "891377173693"
CLUSTER = "nobleblocks-cluster"
ECR_REPO = "nobleblocks-mcp"
SERVICE_NAME = "nobleblocks-mcp-service"
TASK_FAMILY = "nobleblocks-mcp"
IMAGE_TAG = "latest"
CONTAINER_PORT = 8080
CPU = "256"
MEMORY = "512"

# Environment variables for the container
CONTAINER_ENV = {
    "NOBLEBLOCKS_API_BASE": "https://www.nobleblocks.com",
    "MCP_BASE_URL": "https://mcp.nobleblocks.com",
    "MCP_HOST": "0.0.0.0",
    "MCP_PORT": "8080",
    "LOG_LEVEL": "INFO",
}


def run(cmd, check=True):
    print(f"  → {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        sys.exit(1)
    return result


def main():
    mcp_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ecr_uri = f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}"

    print("=" * 60)
    print("NobleBlocks MCP Server — ECS Deployment")
    print("=" * 60)

    # Step 1: Ensure ECR repository exists
    print("\n[1/5] Ensuring ECR repository exists...")
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
    print("\n[2/5] Building Docker image...")
    os.chdir(mcp_dir)
    run(f"docker build -f Dockerfile.remote -t {ECR_REPO}:{IMAGE_TAG} .")

    print("\n[3/5] Pushing to ECR...")
    run(f"aws ecr get-login-password --region {REGION} | docker login --username AWS --password-stdin {ecr_uri.split('/')[0]}")
    run(f"docker tag {ECR_REPO}:{IMAGE_TAG} {ecr_uri}:{IMAGE_TAG}")
    run(f"docker push {ecr_uri}:{IMAGE_TAG}")

    # Step 3: Register task definition
    print("\n[4/5] Registering task definition...")
    env_list = [{"name": k, "value": v} for k, v in CONTAINER_ENV.items()]

    task_def = {
        "family": TASK_FAMILY,
        "networkMode": "awsvpc",
        "requiresCompatibilities": ["FARGATE"],
        "cpu": CPU,
        "memory": MEMORY,
        "executionRoleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/ecsTaskExecutionRole",
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

    task_def_json = json.dumps(task_def)
    result = run(
        f"aws ecs register-task-definition --cli-input-json '{task_def_json}' --region {REGION}"
    )
    td = json.loads(result.stdout)
    td_arn = td["taskDefinition"]["taskDefinitionArn"]
    print(f"  Task definition: {td_arn}")

    # Step 4: Create or update service
    print("\n[5/5] Creating/updating ECS service...")
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
            print("  Service updated")
        else:
            print("  No active service found, need to create one.")
            print("  ⚠ Manual step: Create the service with an ALB target group first.")
            print(f"  Task definition ARN: {td_arn}")
    else:
        print("  ⚠ Service doesn't exist yet. Manual step required:")
        print(f"  1. Create an ALB target group for port {CONTAINER_PORT}")
        print(f"  2. Create the ECS service with:")
        print(f"     - Cluster: {CLUSTER}")
        print(f"     - Task definition: {td_arn}")
        print(f"     - Launch type: FARGATE")
        print(f"     - Desired count: 1")

    print("\n" + "=" * 60)
    print("DONE")
    print(f"  Image:  {ecr_uri}:{IMAGE_TAG}")
    print(f"  Task:   {td_arn}")
    print(f"  Target: https://mcp.nobleblocks.com/mcp")
    print("=" * 60)


if __name__ == "__main__":
    main()
