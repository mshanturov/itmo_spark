from __future__ import annotations

import subprocess

STEPS = [
    ["python3", "src/wordcount.py"],
    ["python3", "src/download_openfoodfacts_sample.py"],
    ["python3", "src/preprocess_openfoodfacts.py"],
    ["python3", "src/kmeans_clustering.py"],
]


def main() -> None:
    for step in STEPS:
        print(f"Running: {' '.join(step)}")
        subprocess.run(step, check=True)


if __name__ == "__main__":
    main()
