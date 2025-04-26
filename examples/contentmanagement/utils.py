from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory


def create_diff(old: str, new: str) -> str:
    return run("diff %s %s > %s", old, new)


def apply_diff(old: str, diff: str) -> str:
    return run("patch -s %s %s -o %s", old, diff)


def run(cmd: str, a: str, b: str) -> str:
    with TemporaryDirectory() as td:
        a_path = Path(td) / "a"
        b_path = Path(td) / "b"
        c_path = Path(td) / "c"
        with a_path.open("w") as a_file:
            a_file.write(a)
        with b_path.open("w") as b_file:
            b_file.write(b)
        os.system(cmd % (a_path, b_path, c_path))  # noqa: S605
        with c_path.open() as c_file:
            return c_file.read()
