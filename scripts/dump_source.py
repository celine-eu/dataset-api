#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import yaml
from pathlib import Path
from typing import Iterable, List, Dict

ROOT = Path.cwd().resolve()
CONFIG_FILE = ROOT / "dump.yaml"

# ---------------------------------------------------------------------------
# File handling
# ---------------------------------------------------------------------------

TEXT_EXTENSIONS = {
    ".py",
    ".pyi",
    ".md",
    ".yaml",
    ".yml",
    ".toml",
    ".json",
    ".txt",
    ".rst",
    ".ini",
}


def is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTENSIONS


def should_skip(path: Path) -> bool:
    if "__pycache__" in path.parts:
        return True
    if path.is_dir():
        return True
    if not is_text_file(path):
        return True
    return False


def expand_content_entry(entry: str) -> List[Path]:
    """
    Expansion rules:
      - directory → recursive include (dir/**)
      - glob pattern → glob expansion
      - file → include file
    """
    path = (ROOT / entry).resolve()

    # Explicit glob pattern
    if any(ch in entry for ch in ["*", "?", "["]):
        return sorted(ROOT.glob(entry))

    # Directory → recursive include
    if path.is_dir():
        return sorted(path.rglob("*"))

    # Single file
    if path.is_file():
        return [path]

    return []


def iter_content_files(entries: Iterable[str]) -> List[Path]:
    seen = set()
    result: List[Path] = []

    for entry in entries:
        for path in expand_content_entry(entry):
            if should_skip(path):
                continue
            if path in seen:
                continue
            seen.add(path)
            result.append(path)

    return sorted(result)


# ---------------------------------------------------------------------------
# Git metadata
# ---------------------------------------------------------------------------


def git(cmd: List[str]) -> str:
    try:
        return subprocess.check_output(
            ["git"] + cmd,
            cwd=ROOT,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return ""


def get_git_metadata() -> Dict[str, str]:
    return {
        "repository_root": git(["rev-parse", "--show-toplevel"]),
        "branch": git(["rev-parse", "--abbrev-ref", "HEAD"]),
        "commit": git(["rev-parse", "HEAD"]),
        "commit_time": git(["show", "-s", "--format=%ci"]),
        "author": git(["show", "-s", "--format=%an <%ae>"]),
        "message": git(["show", "-s", "--format=%s"]),
        "dirty": "yes" if git(["status", "--porcelain"]) else "no",
    }


def render_git_metadata(meta: Dict[str, str]) -> str:
    lines = ["# Git metadata"]
    for k, v in meta.items():
        lines.append(f"# {k}: {v}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_FILE}")

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    prompt: str = config.get("prompt", "").strip()
    header: str = config.get("header", "").strip()
    footer: str = config.get("footer", "").strip()
    contents: List[str] = config.get("contents", [])

    files = iter_content_files(contents)
    git_meta = get_git_metadata()

    output_path = config.get("output", "sources.txt")
    OUTFILE = (ROOT / output_path).resolve()

    with open(OUTFILE, "w", encoding="utf-8") as out:
        if prompt:
            out.write(prompt + "\n\n")

        out.write(render_git_metadata(git_meta) + "\n\n")

        if header:
            out.write(header + "\n\n")

        for file in files:
            rel = file.relative_to(ROOT)
            out.write(f"\n# file: {rel}\n")
            out.write(file.read_text(encoding="utf-8", errors="ignore"))
            out.write("\n")

        if footer:
            out.write("\n" + footer + "\n")

    print(f"Wrote LLM context: {OUTFILE}")
    print(f"Files included: {len(files)}")


if __name__ == "__main__":
    main()
