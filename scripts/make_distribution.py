from __future__ import annotations

import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

INCLUDE_PATHS = [
    "src",
    "scripts",
    "configs",
    "datamart",
    "study_guides",
    "helm",
    "k8s",
    "docker",
    "notebooks",
    "Dockerfile",
    "docker-compose.yml",
    "docker-compose.lab7.yml",
    "requirements.txt",
    "README.md",
]


def iter_files(base: Path):
    if base.is_file():
        yield base
    else:
        for path in base.rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts and not path.name.endswith(".pyc"):
                yield path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build zip distribution for selected lab")
    parser.add_argument("--lab", default="8", help="Lab number for report and output file naming")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dist_path = Path(f"dist/lab{args.lab}_distribution.zip")
    report_name = f"REPORT_LAB{args.lab}.md"
    protocol_name = f"PROTOCOL_LAB{args.lab}.md"
    dynamic_paths = INCLUDE_PATHS + [report_name, protocol_name]

    dist_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(dist_path, "w", compression=ZIP_DEFLATED) as archive:
        for item in dynamic_paths:
            root = Path(item)
            if not root.exists():
                continue
            for file_path in iter_files(root):
                archive.write(file_path, arcname=file_path.as_posix())

    print(f"Distribution created: {dist_path}")


if __name__ == "__main__":
    main()
