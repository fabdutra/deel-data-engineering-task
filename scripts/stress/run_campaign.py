"""Explicit fresh-environment launcher for the archived assessment workload.

Run from repository root. It refuses to reuse existing campaign resources.
"""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]


def run(*args):
    subprocess.run(list(args), cwd=REPO, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check prerequisites without starting Docker containers",
    )
    args = parser.parse_args()
    assert Path.cwd().resolve() == REPO, "Run this launcher from repository root."
    assert (
        REPO / ".env"
    ).exists(), "Generate ignored credentials first: python3 scripts/setup.py"
    for command in (
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            "label=com.docker.compose.project=deel-stress",
        ],
        [
            "docker",
            "volume",
            "ls",
            "-q",
            "--filter",
            "label=com.docker.compose.project=deel-stress",
        ],
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            "label=com.docker.compose.project=deel-assessment",
        ],
    ):
        r = subprocess.run(command, capture_output=True, text=True, check=True)
        assert (
            not r.stdout.strip()
        ), "Existing campaign resources or running assessment stack found. Use an isolated fresh Docker environment; no resources were deleted."
    print("Fresh-environment checks passed.", flush=True)
    if args.check:
        return
    for directory in ("results", "logs"):
        (ROOT / directory).mkdir(exist_ok=True)
    for program in (
        "controller.py",
        "bronze_controller.py",
        "online_controller.py",
        "finish.py",
        "postprocess.py",
    ):
        print("Campaign phase:", program, flush=True)
        run(sys.executable, str(ROOT / program))
    print(
        "Campaign completed; source/Gold checks passed and stress services stopped. Raw results remain under scripts/stress/results/.",
        flush=True,
    )


if __name__ == "__main__":
    main()
