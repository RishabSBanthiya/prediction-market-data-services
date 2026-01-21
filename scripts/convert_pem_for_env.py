#!/usr/bin/env python3
"""
Convert a PEM file to formats suitable for environment variables.

Usage:
    python scripts/convert_pem_for_env.py kalshi.pem

Outputs two formats:
1. Escaped newlines (traditional): -----BEGIN...\\n...\\n-----END...
2. Base64 encoded (recommended): base64:LS0tLS1CRUdJTi...

The base64 format is more reliable as it avoids newline handling issues
across different platforms and deployment systems.
"""
import base64
import sys


def convert_pem(filepath: str) -> tuple[str, str]:
    with open(filepath, "rb") as f:
        content = f.read()

    # Format 1: Escaped newlines
    text_content = content.decode("utf-8")
    escaped = text_content.replace("\n", "\\n")

    # Format 2: Base64 encoded (prefix with "base64:")
    b64 = "base64:" + base64.b64encode(content).decode("utf-8")

    return escaped, b64


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/convert_pem_for_env.py <path-to-pem>")
        sys.exit(1)

    pem_path = sys.argv[1]
    escaped, b64 = convert_pem(pem_path)

    print("=" * 70)
    print("OPTION 1: Escaped newlines (traditional)")
    print("=" * 70)
    print(escaped)
    print()
    print("=" * 70)
    print("OPTION 2: Base64 encoded (RECOMMENDED - more reliable)")
    print("=" * 70)
    print(b64)
    print()
    print("=" * 70)
    print("Set KALSHI_PRIVATE_KEY in Render to one of the values above.")
    print("The base64 format (Option 2) is recommended for reliability.")
    print("=" * 70)
