from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def valid_asin(value: Any) -> bool:
    return isinstance(value, str) and bool(ASIN_RE.fullmatch(value.strip().upper()))


def normalize_asin(value: str) -> str:
    return value.strip().upper()


def compact_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        text = "\n".join(str(v) for v in value if v is not None)
    else:
        text = str(value)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text or None


def listify_bullets(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value).splitlines()
    out: list[str] = []
    for item in raw:
        text = compact_text(item)
        if text and text not in out:
            out.append(text)
    return out


def unique_strings(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        value = value.strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def append_jsonl(path: Path, record: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())


def read_jsonl(path: Path) -> list[Any]:
    if not path.exists():
        return []
    out: list[Any] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def write_json_array_from_jsonl(jsonl_path: Path, json_path: Path) -> None:
    records = read_jsonl(jsonl_path)
    atomic_write_json(json_path, records)
