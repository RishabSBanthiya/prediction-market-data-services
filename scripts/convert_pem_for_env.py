#!/usr/bin/env python3
"""
Convert a PEM file to a single-line format for environment variables.

Usage:
    python scripts/convert_pem_for_env.py kalshi.pem

This will output the PEM content with newlines replaced by \\n,
ready to paste into Render's environment variable field.
"""
import sys


def convert_pem(filepath: str) -> str:
    with open(filepath, "r") as f:
        content = f.read()
    # Replace actual newlines with literal \n
    return content.replace("\n", "\\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/convert_pem_for_env.py <path-to-pem>")
        sys.exit(1)

    pem_path = sys.argv[1]
    converted = convert_pem(pem_path)

    print("=" * 60)
    print("Copy the following value for KALSHI_PRIVATE_KEY:")
    print("=" * 60)
    print(converted)
    print("=" * 60)
