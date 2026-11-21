#!/usr/bin/env python3
"""Assert that every native PE file under a directory was built for one CPU (#9256).

A win-arm64 publish is self-contained, so it carries the .NET runtime, WPF's native pieces and the native
libraries of every package (DuckDB, SkiaSharp, HarfBuzz, SqlClient's SNI). One x64-only library in that tree
is a file that fails to load on a native ARM64 process, and nothing in `dotnet publish` says so. This reads
each .dll, .exe and .node file's PE header and fails on any native file whose machine type is not the
expected one.

Managed assemblies are skipped. They carry a CLR header, they are CPU-neutral IL, and their machine field
says nothing about where they load (a managed dll built AnyCPU reports x86 or x64 there). The apphost
exe has no CLR header: it is a native exe that starts the runtime, so it is checked.

Usage:
  assert_pe_machine.py --machine arm64 DIR [--list]

Exit status: 0 when every native file matches, 1 when one does not or when DIR holds no native file at all
(an empty census passes nothing), 2 on a usage error.
"""

from __future__ import annotations

import argparse
import os
import struct
import sys

MACHINES = {
    "x64": 0x8664,
    "arm64": 0xAA64,
}
NAMES = {
    0x014C: "x86",
    0x8664: "x64",
    0xAA64: "arm64",
    0xA641: "arm64ec",
    0x01C4: "arm",
    0x0200: "ia64",
}
EXTENSIONS = (".dll", ".exe", ".node")
CLR_DIRECTORY_INDEX = 14


def read_pe(path: str) -> tuple[int, bool] | None:
    """Return (machine, is_managed) for a PE file, or None when the file is not a PE."""
    with open(path, "rb") as handle:
        head = handle.read(64)
        if len(head) < 64 or head[:2] != b"MZ":
            return None
        (pe_offset,) = struct.unpack_from("<I", head, 0x3C)
        handle.seek(pe_offset)
        header = handle.read(24)
        if len(header) < 24 or header[:4] != b"PE\0\0":
            return None
        machine, _sections, _stamp, _symtab, _symcount, optional_size, _chars = struct.unpack_from(
            "<HHIIIHH", header, 4
        )
        optional = handle.read(optional_size)
        if len(optional) < 2:
            return (machine, False)
        (magic,) = struct.unpack_from("<H", optional, 0)
        # Data directories start at 96 (PE32) or 112 (PE32+); each entry is 8 bytes.
        directories_at = 96 if magic == 0x10B else 112
        clr_at = directories_at + CLR_DIRECTORY_INDEX * 8
        if len(optional) < clr_at + 8:
            return (machine, False)
        (clr_rva, clr_size) = struct.unpack_from("<II", optional, clr_at)
        return (machine, clr_rva != 0 and clr_size != 0)


def scan(directory: str, expected: int) -> tuple[list[str], list[str], int]:
    """Return (native lines, failure lines, managed count) for every PE file under directory."""
    native: list[str] = []
    failures: list[str] = []
    managed = 0
    for root, _dirs, files in os.walk(directory):
        for name in sorted(files):
            if not name.lower().endswith(EXTENSIONS):
                continue
            path = os.path.join(root, name)
            relative = os.path.relpath(path, directory).replace(os.sep, "/")
            try:
                found = read_pe(path)
            except OSError as exc:
                failures.append(f"{relative}: cannot be read ({exc})")
                continue
            if found is None:
                continue
            machine, is_managed = found
            if is_managed:
                managed += 1
                continue
            label = NAMES.get(machine, hex(machine))
            native.append(f"{relative}: {label}")
            if machine != expected:
                failures.append(f"{relative}: {label}")
    return native, failures, managed


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--machine", required=True, choices=sorted(MACHINES))
    parser.add_argument("--list", action="store_true", help="print every native file and its machine type")
    parser.add_argument("directory")
    args = parser.parse_args(argv)

    if not os.path.isdir(args.directory):
        print(f"assert_pe_machine: {args.directory} is not a directory", file=sys.stderr)
        return 2

    expected = MACHINES[args.machine]
    native, failures, managed = scan(args.directory, expected)
    if args.list:
        for line in native:
            print(line)
    print(f"{len(native)} native and {managed} managed PE files under {args.directory}; {len(failures)} not {args.machine}.")
    if not native:
        print(f"::error::No native PE file under {args.directory}: an empty census proves nothing.")
        return 1
    if failures:
        for line in failures:
            print(f"::error::Not {args.machine}: {line}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
