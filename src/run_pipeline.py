from __future__ import annotations

import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineStep:
    name: str
    command: list[str]


class Lab5Pipeline:
    def __init__(self) -> None:
        self._steps = [
            PipelineStep("WordCount", ["python3", "-m", "src.wordcount"]),
            PipelineStep("Download sample", ["python3", "-m", "src.download_openfoodfacts_sample"]),
            PipelineStep("Preprocess", ["python3", "-m", "src.preprocess_openfoodfacts"]),
            PipelineStep("KMeans clustering", ["python3", "-m", "src.kmeans_clustering"]),
        ]

    def run(self) -> None:
        for step in self._steps:
            print(f"Running step: {step.name}", flush=True)
            subprocess.run(step.command, check=True)


def main() -> None:
    pipeline = Lab5Pipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
