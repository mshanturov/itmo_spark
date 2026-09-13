from __future__ import annotations

import argparse
import gzip
from pathlib import Path

import requests

try:
    from src.common import load_config
except ModuleNotFoundError:
    from common import load_config


USER_AGENT = "Mozilla/5.0 (compatible; ITMO-Lab5/1.0)"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download a manageable sample from OpenFoodFacts")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--max-lines", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    source_url = config["data"]["source_url"]
    output_path = Path(config["data"]["raw_sample_jsonl"])
    max_lines = args.max_lines if args.max_lines is not None else int(config["sampling"]["max_lines"])

    output_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(source_url, stream=True, timeout=120, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()

    lines_written = 0
    with output_path.open("w", encoding="utf-8") as output_file:
        with gzip.GzipFile(fileobj=response.raw) as gz_file:
            for raw_line in gz_file:
                line = raw_line.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                output_file.write(line + "\n")
                lines_written += 1
                if lines_written >= max_lines:
                    break

    print(f"Downloaded sample with {lines_written} lines to {output_path}")


if __name__ == "__main__":
    main()
