"""Create a local ZIP from committed deliverables; no private context is read."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--git", default=shutil.which("git"))
    args = parser.parse_args()

    def git(*values):
        return subprocess.check_output([args.git, *values], cwd=ROOT)

    assert not git(
        "status", "--porcelain"
    ).strip(), "Commit the reviewed deliverables before packaging."
    names = git("ls-files", "-z").decode().split("\0")
    names = [n for n in names if n]
    for name in names:
        parts = PurePosixPath(name).parts
        assert not any(
            p in parts for p in ("context", ".git", ".venv", "__pycache__")
        ), name
        assert PurePosixPath(name).name != ".env", name
        assert not (ROOT / name).is_symlink(), name
    prefix = "deel-data-engineering-task/"
    manifest = {
        "repository": "https://github.com/fabdutra/deel-data-engineering-task",
        "commit": git("rev-parse", "HEAD").decode().strip(),
        "files": [
            {"path": n, "sha256": hashlib.sha256((ROOT / n).read_bytes()).hexdigest()}
            for n in names
        ],
        "excluded": [
            "Git internals/history (available in repository)",
            "ignored credentials and personal context",
            "large raw fixtures and logs",
        ],
    }
    out = ROOT / "artifacts/deel-assessment-submission.zip"
    out.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(
        out, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
    ) as archive:
        for name in names:
            archive.write(ROOT / name, prefix + name)
        archive.writestr(
            prefix + "SUBMISSION.json", json.dumps(manifest, indent=2) + "\n"
        )
    with zipfile.ZipFile(out) as archive:
        assert archive.testzip() is None
    print("Created", out, "with", len(names), "tracked files and a source manifest.")


if __name__ == "__main__":
    main()
