#!/usr/bin/env python3
"""Validate the component package's exact archive boundary.

The positive case invokes the real package script. The negative cases exercise
the same archive validator against duplicate, directory, symlink, and extra
members so a source-text-only check cannot pass.
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path
import subprocess
import tarfile
import tempfile


EXPECTED_MEMBERS = {
    "bin/mining-pool",
    "bin/poolworker",
    "SOURCE_COMMIT",
    "COMPONENT_VERSION",
}


def validate_archive(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if len(names) != len(set(names)):
            raise AssertionError(f"duplicate archive member: {names}")
        if set(names) != EXPECTED_MEMBERS:
            raise AssertionError(f"archive members={names}, want={sorted(EXPECTED_MEMBERS)}")
        if any(not member.isreg() for member in members):
            raise AssertionError("archive contains a non-regular member")


def synthetic_archive(path: Path, members: list[tuple[str, tarfile.TarInfo, bytes]]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for _, info, payload in members:
            archive.addfile(info, io.BytesIO(payload))


def regular(name: str) -> tuple[str, tarfile.TarInfo, bytes]:
    payload = b"fixture"
    info = tarfile.TarInfo(name)
    info.size = len(payload)
    return name, info, payload


def negative_members(kind: str) -> list[tuple[str, tarfile.TarInfo, bytes]]:
    members = [regular(name) for name in sorted(EXPECTED_MEMBERS)]
    if kind == "duplicate":
        members.append(regular("SOURCE_COMMIT"))
    elif kind == "directory":
        info = tarfile.TarInfo("bin/")
        info.type = tarfile.DIRTYPE
        members.append(("bin/", info, b""))
    elif kind == "symlink":
        info = tarfile.TarInfo("bin/extra")
        info.type = tarfile.SYMTYPE
        info.linkname = "mining-pool"
        members.append(("bin/extra", info, b""))
    elif kind == "extra":
        members.append(regular("README"))
    else:
        raise ValueError(kind)
    return members


def run_positive(repo: Path, output: Path) -> Path:
    version = (repo / "VERSION").read_text(encoding="utf-8").strip()
    arch = subprocess.check_output(["go", "env", "GOARCH"], text=True).strip()
    subprocess.run(
        ["bash", str(repo / "scripts/package-component.sh"), version, arch, str(output)],
        cwd=repo,
        check=True,
    )
    return output / f"pool-{version}-linux-{arch}.tar.gz"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    repo = args.repo.resolve()
    with tempfile.TemporaryDirectory(prefix="pool-package-test-") as directory:
        output = Path(directory)
        archive = run_positive(repo, output)
        validate_archive(archive)
        for kind in ("duplicate", "directory", "symlink", "extra"):
            candidate = output / f"{kind}.tar.gz"
            synthetic_archive(candidate, negative_members(kind))
            try:
                validate_archive(candidate)
            except AssertionError:
                pass
            else:
                raise AssertionError(f"{kind} archive was accepted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
