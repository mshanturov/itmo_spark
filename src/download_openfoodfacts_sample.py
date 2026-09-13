from __future__ import annotations

import argparse
import gzip
from pathlib import Path
import sys

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.settings import AppSettings, load_settings

USER_AGENT = "Mozilla/5.0 (compatible; ITMO-Lab5/1.0)"


class OpenFoodFactsSampler:
    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings

    def run(self, max_lines_override: int | None = None) -> None:
        source_url = self._settings.data.source_url
        output_path = Path(self._settings.data.raw_sample_jsonl)
        max_lines = max_lines_override if max_lines_override is not None else self._settings.sampling.max_lines

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download a manageable sample from OpenFoodFacts")
    parser.add_argument("--app-config", default="configs/app_config.yaml")
    parser.add_argument("--spark-config", default="configs/spark_config.yaml")
    parser.add_argument("--max-lines", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(args.app_config, args.spark_config)
    sampler = OpenFoodFactsSampler(settings)
    sampler.run(args.max_lines)


if __name__ == "__main__":
    main()
