#!/usr/bin/env python3
"""Emit only the backup fields consumed by Beacon health diagnostics."""

import json
import re
import sys
from datetime import datetime


RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$"
)


def require_rfc3339(source: dict[str, object], key: str) -> str:
    value = source.get(key)
    if not isinstance(value, str) or RFC3339.fullmatch(value) is None:
        raise ValueError(f"{key} must be an RFC3339 string")
    datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    return value


def require_bool(source: dict[str, object], key: str) -> bool:
    value = source.get(key)
    if type(value) is not bool:
        raise ValueError(f"{key} must be a JSON boolean")
    return value


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} MANIFEST")
    with open(sys.argv[1], encoding="utf-8") as manifest_file:
        source = json.load(manifest_file)
    if not isinstance(source, dict):
        raise ValueError("manifest must be a JSON object")
    payload = {
        "createdAt": require_rfc3339(source, "createdAt"),
        "listVerified": require_bool(source, "listVerified"),
        "scratchRestoreVerified": require_bool(source, "scratchRestoreVerified"),
    }
    if "scratchRestoreVerifiedAt" in source:
        payload["scratchRestoreVerifiedAt"] = require_rfc3339(
            source, "scratchRestoreVerifiedAt"
        )
    json.dump(payload, sys.stdout, sort_keys=True, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
