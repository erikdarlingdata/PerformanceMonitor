#!/usr/bin/env python3
"""Put [Trait("Reads", "Lite")] on the Darling.Tests classes the census names (#5459 cut 3a).

LiteReadsTraitGuardTests derives, from the test sources, which classes read a Lite file, and fails with one
`<file>: <Class>` line per class that lacks the trait. This script reads that list (the failure text of the
census run, or any file of such lines) and writes the attribute line above each named class declaration, so the
tags are generated from the census's own answer and the two cannot disagree about the detector.

    python .github/scripts/tag-reads-lite.py census-output.txt

Idempotent: a class that already carries the trait is left alone. Paths are relative to Darling/Darling.Tests;
a `Linked/<name>.cs` path is a Lite.Tests file the project compiles in, found under Lite.Tests.
Standard library only.
"""
from __future__ import annotations

import os
import re
import sys

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
TESTS = os.path.join(ROOT, "Darling", "Darling.Tests")
TRAIT = '[Trait("Reads", "Lite")]'
LINE = re.compile(r"^\s*(?P<file>[\w./\\-]+\.cs): (?P<cls>\w+)\s*$")


def resolve(rel: str) -> str:
    path = os.path.join(TESTS, rel.replace("\\", os.sep).replace("/", os.sep))
    if os.path.exists(path):
        return path
    if rel.replace("\\", "/").startswith("Linked/"):
        return os.path.join(ROOT, "Lite.Tests", os.path.basename(rel))
    return path


def tag(path: str, classes: set[str]) -> int:
    with open(path, "rb") as fh:
        raw = fh.read()
    bom = raw.startswith(b"\xef\xbb\xbf")
    text = raw.decode("utf-8-sig")
    eol = "\r\n" if "\r\n" in text else "\n"
    lines = text.replace("\r\n", "\n").split("\n")
    decl = re.compile(r"^(?P<indent>[ \t]*)(?:(?:public|internal|private|protected|sealed|static|abstract|partial)\s+)*class\s+(?P<name>\w+)")
    done = 0
    i = 0
    while i < len(lines):
        m = decl.match(lines[i])
        if m and m.group("name") in classes:
            j = i - 1
            has = False
            while j >= 0 and lines[j].strip().startswith("["):
                if TRAIT.replace(" ", "") in lines[j].replace(" ", ""):
                    has = True
                j -= 1
            if not has:
                lines.insert(i, m.group("indent") + TRAIT)
                done += 1
                i += 1
        i += 1
    if done:
        out = eol.join(lines).encode("utf-8")
        with open(path, "wb") as fh:
            fh.write((b"\xef\xbb\xbf" if bom else b"") + out)
    return done


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__)
        return 2
    wanted: dict[str, set[str]] = {}
    with open(argv[0], encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = LINE.match(line.rstrip("\n"))
            if m:
                wanted.setdefault(resolve(m.group("file")), set()).add(m.group("cls"))
    total = 0
    for path, classes in sorted(wanted.items()):
        if not os.path.exists(path):
            print(f"missing file {path}")
            return 1
        total += tag(path, classes)
    print(f"tagged {total} classes in {len(wanted)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
