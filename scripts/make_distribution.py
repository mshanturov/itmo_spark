from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

DIST_PATH = Path("dist/lab5_distribution.zip")
INCLUDE_PATHS = [
    "src",
    "scripts",
    "configs",
    "notebooks",
    "requirements.txt",
    "README.md",
    "REPORT_LAB5.md",
]


def iter_files(base: Path):
    if base.is_file():
        yield base
    else:
        for path in base.rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts and not path.name.endswith(".pyc"):
                yield path


def main() -> None:
    DIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(DIST_PATH, "w", compression=ZIP_DEFLATED) as archive:
        for item in INCLUDE_PATHS:
            root = Path(item)
            if not root.exists():
                continue
            for file_path in iter_files(root):
                archive.write(file_path, arcname=file_path.as_posix())

    print(f"Distribution created: {DIST_PATH}")


if __name__ == "__main__":
    main()
