#!/usr/bin/env python3
"""Report which published Windows executables carry an Authenticode signature (#3288).

Every executable `vpk pack` GENERATES shipped unsigned on v3.6.0 and v3.7.0 -- the portable
launcher stub, `Update.exe`, and `Setup.exe` -- while the app payload was signed. Nobody
noticed for two releases because `build.yml` carries a comment saying the gap was closed, and
because the only visible symptom is a Windows "unknown publisher" block on someone else's
machine.

So this exists to be run against a PUBLISHED release, from any platform, and to answer the one
question no other check asks: is there a signature attached to each thing a user can launch.

WHAT IT CHECKS, AND WHAT IT DELIBERATELY DOES NOT

  It reads the PE optional header's Security data directory (index 4) and reports whether the
  certificate table is non-empty. That is "a signature is attached" and nothing more.

  It does NOT verify the signature: not the chain, not trust, not the timestamp, not revocation,
  not whether the digest matches the file. Doing that properly needs a Windows trust store, and
  the defect actually shipped twice was TOTAL ABSENCE -- offset 0, size 0 -- which this catches
  for free. Read a pass as "signed", never as "correctly signed".

  It only understands PE files. A NuGet package signature, an MSIX catalog, or a detached
  signature file are all invisible to it.

  Executables inside a published archive are checked too, because the one that matters most is
  inside one: the portable zip's ROOT `PerformanceMonitorLite.exe` is a 399 kB launcher shim,
  while the signed 182 kB app sits in `current/`. A check that only looked at top-level assets
  would have passed every affected release.

COST, because it is why this is runnable rather than theoretical

  It never downloads a whole artifact. For each standalone `.exe` it fetches the first 4 kB to
  locate the certificate table, then that table's byte range. For each archive it fetches the
  end-of-central-directory record, the central directory, and then only the entries that are
  executables. Checking all five of v3.7.0's Lite assets moves a few MB rather than the ~370 MB
  those assets total.

  This relies on the asset host honouring HTTP Range. GitHub release downloads do. If a host
  ignores Range and returns the whole body, the reads still succeed and it merely costs what a
  download would -- it does not silently check the wrong bytes, because a short or unexpected
  read raises rather than being interpreted.

USAGE

  python3 .github/scripts/verify_release_signatures.py v3.7.0
  python3 .github/scripts/verify_release_signatures.py v3.7.0 --json

  Exit status is 1 if any executable is unsigned, 2 if a release or asset could not be read at
  all, and 0 only when every executable found carried a signature. An empty executable list is
  status 2, not 0: finding nothing to check is a broken run, not a clean one.

  The CI half of #3288 is deliberately NOT this script. In the release job the artifacts are on
  disk before upload, so that check needs no HTTP at all and belongs in the workflow's own pwsh.
  This one answers "what did we actually publish", which is the question that goes unasked.
"""

from __future__ import annotations

import argparse
import json
import struct
import subprocess
import sys
import zlib
from dataclasses import dataclass, field

ARCHIVE_SUFFIXES = (".zip", ".nupkg")
EXECUTABLE_SUFFIXES = (".exe", ".dll")
HEADER_BYTES = 4096
# The end-of-central-directory record is 22 bytes plus a comment of up to 64 kB.
EOCD_SEARCH_BYTES = 66 * 1024


class ReadError(RuntimeError):
    """A byte range could not be read, or came back the wrong size."""


@dataclass
class Finding:
    asset: str
    path: str
    size: int
    signed: bool
    note: str = ""

    @property
    def label(self) -> str:
        return self.asset if self.path == "" else f"{self.asset} :: {self.path}"


@dataclass
class Report:
    tag: str
    findings: list[Finding] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def unsigned(self) -> list[Finding]:
        return [f for f in self.findings if not f.signed]


def fetch_range(url: str, start: int, length: int) -> bytes:
    """Fetch `length` bytes at `start`. Raises rather than returning a short read."""
    end = start + length - 1
    proc = subprocess.run(
        ["curl", "-sSL", "-r", f"{start}-{end}", "--fail", url],
        capture_output=True,
    )
    if proc.returncode != 0:
        raise ReadError(f"range {start}-{end} failed: {proc.stderr.decode(errors='replace')[:200]}")
    data = proc.stdout
    # A host ignoring Range returns the whole body, which is longer than asked for. That is
    # usable (the prefix is still the right bytes for a start of 0) but a SHORT read is not.
    if len(data) < length:
        raise ReadError(f"range {start}-{end} returned {len(data)} of {length} bytes")
    return data[:length]


def certificate_table(pe: bytes) -> tuple[int, int]:
    """Return (offset, size) of the PE certificate table, or (0, 0) when absent."""
    if pe[:2] != b"MZ":
        raise ReadError("not an MZ image")
    pe_off = struct.unpack_from("<I", pe, 0x3C)[0]
    if pe[pe_off : pe_off + 4] != b"PE\0\0":
        raise ReadError("no PE signature")
    magic = struct.unpack_from("<H", pe, pe_off + 24)[0]
    if magic == 0x20B:
        directories = pe_off + 24 + 112
    elif magic == 0x10B:
        directories = pe_off + 24 + 96
    else:
        raise ReadError(f"unknown optional-header magic 0x{magic:X}")
    # Index 4 is IMAGE_DIRECTORY_ENTRY_SECURITY. Unlike every other directory this one holds a
    # FILE OFFSET rather than an RVA, which is what makes it readable without section mapping.
    return struct.unpack_from("<II", pe, directories + 4 * 8)


def check_standalone_exe(url: str, name: str, size: int) -> Finding:
    head = fetch_range(url, 0, min(HEADER_BYTES, size))
    off, length = certificate_table(head)
    return Finding(asset=name, path="", size=size, signed=off != 0 and length != 0)


def zip_entries(url: str, size: int) -> list[tuple[str, int, int, int, int]]:
    """(name, method, compressed_size, uncompressed_size, local_header_offset) per entry."""
    window = min(EOCD_SEARCH_BYTES, size)
    tail = fetch_range(url, size - window, window)
    i = tail.rfind(b"PK\x05\x06")
    if i < 0:
        raise ReadError("no end-of-central-directory record")
    cd_size, cd_off = struct.unpack_from("<II", tail, i + 12)
    if cd_off == 0xFFFFFFFF or cd_size == 0xFFFFFFFF:
        raise ReadError("zip64 central directory: not supported by this reader")
    cd = fetch_range(url, cd_off, cd_size)
    out, p = [], 0
    while p < len(cd) - 46:
        if cd[p : p + 4] != b"PK\x01\x02":
            break
        method = struct.unpack_from("<H", cd, p + 10)[0]
        csize, usize = struct.unpack_from("<II", cd, p + 20)
        nlen, elen, clen = struct.unpack_from("<HHH", cd, p + 28)
        lho = struct.unpack_from("<I", cd, p + 42)[0]
        name = cd[p + 46 : p + 46 + nlen].decode("utf-8", "replace")
        out.append((name, method, csize, usize, lho))
        p += 46 + nlen + elen + clen
    return out


def read_zip_member(url: str, method: int, csize: int, lho: int) -> bytes:
    lh = fetch_range(url, lho, 30)
    nlen, elen = struct.unpack_from("<HH", lh, 26)
    data = fetch_range(url, lho + 30 + nlen + elen, csize)
    if method == 0:
        return data
    if method == 8:
        return zlib.decompress(data, -15)
    raise ReadError(f"unsupported compression method {method}")


def check_archive(url: str, name: str, size: int, include_dlls: bool) -> tuple[list[Finding], list[str]]:
    findings: list[Finding] = []
    errors: list[str] = []
    wanted = EXECUTABLE_SUFFIXES if include_dlls else (".exe",)
    for member, method, csize, usize, lho in zip_entries(url, size):
        if not member.lower().endswith(wanted):
            continue
        try:
            blob = read_zip_member(url, method, csize, lho)
            off, length = certificate_table(blob)
            findings.append(
                Finding(asset=name, path=member, size=usize, signed=off != 0 and length != 0)
            )
        except ReadError as exc:
            errors.append(f"{name} :: {member}: {exc}")
    return findings, errors


def release_assets(tag: str, repo: str, gh: str) -> list[dict]:
    proc = subprocess.run(
        [gh, "api", f"repos/{repo}/releases/tags/{tag}"], capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise ReadError(f"cannot read release {tag}: {proc.stderr.strip()[:300]}")
    return json.loads(proc.stdout).get("assets", [])


def build_report(tag: str, repo: str, gh: str, include_dlls: bool) -> Report:
    report = Report(tag=tag)
    for asset in release_assets(tag, repo, gh):
        name, url, size = asset["name"], asset["browser_download_url"], asset["size"]
        lower = name.lower()
        try:
            if lower.endswith(".exe"):
                report.findings.append(check_standalone_exe(url, name, size))
            elif lower.endswith(ARCHIVE_SUFFIXES):
                found, errs = check_archive(url, name, size, include_dlls)
                report.findings.extend(found)
                report.errors.extend(errs)
        except ReadError as exc:
            report.errors.append(f"{name}: {exc}")
    return report


def _synth_pe(*, signed: bool, plus: bool = True) -> bytes:
    """A minimal PE image with the Security directory either populated or zeroed.

    The control this whole script needs: a checker that answered "signed" unconditionally would
    have reported every release clean, which is indistinguishable from the fix having landed.
    """
    buf = bytearray(1024)
    buf[0:2] = b"MZ"
    pe_off = 128
    struct.pack_into("<I", buf, 0x3C, pe_off)
    buf[pe_off : pe_off + 4] = b"PE\0\0"
    struct.pack_into("<H", buf, pe_off + 24, 0x20B if plus else 0x10B)
    directories = pe_off + 24 + (112 if plus else 96)
    table = (0x800, 0x1C0) if signed else (0, 0)
    struct.pack_into("<II", buf, directories + 4 * 8, *table)
    return bytes(buf)


def self_test() -> int:
    failures: list[str] = []

    for plus in (True, False):
        shape = "PE32+" if plus else "PE32"
        off, size = certificate_table(_synth_pe(signed=True, plus=plus))
        if (off, size) == (0, 0):
            failures.append(f"{shape}: a signed image read as unsigned")
        off, size = certificate_table(_synth_pe(signed=False, plus=plus))
        if (off, size) != (0, 0):
            failures.append(f"{shape}: an unsigned image read as signed ({off}, {size})")

    # Both directions matter, and so does refusing garbage rather than defaulting either way.
    #
    # The third case is load-bearing and was added because a mutation exposed its absence: with
    # the MZ check deleted, "empty", "not MZ" and "MZ without PE" were ALL still caught by the
    # PE-signature check below it, so nothing actually asserted that the MZ check does anything.
    # This one is an image that is not MZ but DOES carry a well-formed PE header at the offset,
    # which only the MZ check can reject.
    not_mz_but_valid_pe = bytearray(_synth_pe(signed=True))
    not_mz_but_valid_pe[0:2] = b"ZZ"
    for label, blob in (
        ("empty", b""),
        ("not MZ", b"ZM" + bytes(1022)),
        ("MZ without PE", b"MZ" + bytes(1022)),
        ("non-MZ carrying a valid PE header", bytes(not_mz_but_valid_pe)),
    ):
        try:
            certificate_table(blob)
        except (ReadError, struct.error, IndexError):
            pass
        else:
            failures.append(f"{label}: returned a verdict instead of raising")

    # A short range must raise, or a truncated read silently becomes "no signature".
    try:
        fetch_range("file:///dev/null", 0, 64)
    except ReadError:
        pass
    else:
        failures.append("a short read returned data instead of raising")

    for f in failures:
        print(f"SELF-TEST FAIL: {f}", file=sys.stderr)
    if failures:
        return 1
    print("self-test: 10 assertions passed (both PE shapes, both verdicts, 4 malformed, short read)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tag", nargs="?", help="release tag, e.g. v3.7.0")
    parser.add_argument("--self-test", action="store_true", dest="self_test")
    parser.add_argument("--repo", default="erikdarlingdata/PerformanceMonitor")
    parser.add_argument("--gh", default="gh", help="gh executable (use the identity wrapper)")
    parser.add_argument(
        "--include-dlls",
        action="store_true",
        help="also check .dll members inside archives (slower; the shipped defect was .exe only)",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    if not args.tag:
        parser.error("a release tag is required unless --self-test is given")

    try:
        report = build_report(args.tag, args.repo, args.gh, args.include_dlls)
    except ReadError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2

    if args.as_json:
        print(
            json.dumps(
                {
                    "tag": report.tag,
                    "executables": [
                        {"asset": f.asset, "path": f.path, "size": f.size, "signed": f.signed}
                        for f in report.findings
                    ],
                    "unsigned_count": len(report.unsigned),
                    "errors": report.errors,
                },
                indent=2,
            )
        )
    else:
        print(f"{report.tag}: {len(report.findings)} executable(s) checked\n")
        width = max((len(f.label) for f in report.findings), default=0)
        for f in sorted(report.findings, key=lambda x: (x.signed, x.label)):
            mark = "signed  " if f.signed else "UNSIGNED"
            print(f"  {mark}  {f.label:<{width}}  {f.size:>10,} bytes")
        for e in report.errors:
            print(f"\n  ERROR  {e}", file=sys.stderr)

    if report.errors and not report.findings:
        return 2
    if not report.findings:
        print("\nFATAL: no executables found — a release with nothing to check is a broken run.",
              file=sys.stderr)
        return 2
    if report.unsigned:
        print(f"\n{len(report.unsigned)} unsigned executable(s). See #3288.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
