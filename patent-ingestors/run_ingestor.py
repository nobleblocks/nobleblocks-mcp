#!/usr/bin/env python3
"""Launcher that applies SSL fix then runs the specified ingestor."""
import sys
import os

# Apply SSL bypass
exec(open(os.path.join(os.path.dirname(__file__), "ssl_wrapper.py")).read())

# Run the target script
if len(sys.argv) < 2:
    print("Usage: python3 run_ingestor.py <script_name.py>")
    sys.exit(1)

script = os.path.join(os.path.dirname(os.path.abspath(__file__)), sys.argv[1])
sys.argv = sys.argv[1:]  # Shift argv so the target script sees its own args
exec(open(script).read())
