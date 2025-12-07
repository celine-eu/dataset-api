#!/usr/bin/env python3
from pathlib import Path

OUTFILE = Path(__file__).resolve().parent.parent / "data/source.py.txt"

DIRS = [
    Path(__file__).resolve().parent.parent / "dataset",
    Path(__file__).resolve().parent.parent / "tests",
]

README = Path(__file__).resolve().parent.parent / "README.md"


def should_skip(path: Path) -> bool:
    """Return True if file or directory should be excluded."""
    parts = path.parts
    if "__pycache__" in parts:
        return True
    if path.suffix not in (".py", ".pyi"):
        return True
    return False


def main():
    with open(OUTFILE, "w", encoding="utf-8") as out:

        with open(README, "r") as readme:
            out.write(readme.read() + "\n\n")

        for source_dir in DIRS:
            for file in sorted(source_dir.rglob("*")):
                if file.is_file() and not should_skip(file):
                    rel = file.relative_to(source_dir.parent)
                    out.write(f"\n# file: {rel} \n")
                    out.write(file.read_text(encoding="utf-8"))
                    out.write("\n")

    print(f"Wrote: {OUTFILE}")


if __name__ == "__main__":
    main()
