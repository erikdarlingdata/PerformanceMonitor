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

  Two executables are allowed to be unsigned, and only in one place each: `lib/app/*_ExecutionStub.exe`
  and `lib/app/Squirrel.exe` INSIDE a `.nupkg`. `releases.<channel>.json` records a SHA256 and a
  Size for each `.nupkg`, the delta package patches against those bytes, and `vpk pack` is what
  writes that manifest -- so re-signing a member of a `.nupkg` invalidates it. The copies of the
  same two files that ship in `Portable.zip` carry no recorded hash and ARE signed, and the
  allowance is keyed on the container as well as the member path so those copies, and a
  same-named file anywhere else, still fail. See ALLOWED_UNSIGNED.

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
import re
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


@dataclass(frozen=True)
class AllowedUnsigned:
    """One executable that is allowed to be unsigned, in one kind of container only.

    `container_suffix` is matched against the containing archive's name and `member` against the
    path inside it. A standalone file has no container and therefore no allowance: that is what
    keeps an unsigned `Setup.exe`, or an unsigned copy of either of these two files sitting
    beside it, a failure.
    """

    container_suffix: str
    member: re.Pattern[str]
    reason: str


ALLOWED_UNSIGNED: tuple[AllowedUnsigned, ...] = (
    AllowedUnsigned(
        container_suffix=".nupkg",
        member=re.compile(r"\Alib/app/[^/]+_ExecutionStub\.exe\Z", re.IGNORECASE),
        reason=(
            "Velopack generates the launcher stub during `vpk pack`, so no pre-pack round reaches "
            "it, and `releases.<channel>.json` records this package's SHA256 and Size for the "
            "updater and the delta to patch against, so no post-pack round may rewrite it. The "
            "copy in Portable.zip is signed."
        ),
    ),
    AllowedUnsigned(
        container_suffix=".nupkg",
        member=re.compile(r"\Alib/app/Squirrel\.exe\Z", re.IGNORECASE),
        reason=(
            "Velopack's updater, generated during `vpk pack` and deployed as Update.exe. Same "
            "recorded-hash constraint as the launcher stub. The copy in Portable.zip is signed."
        ),
    ),
)


def allowance_for(container: str, member: str) -> AllowedUnsigned | None:
    """The allowance covering an unsigned `member` of `container`, or None.

    `member` is empty for a standalone file, which never has an allowance.
    """
    if not member:
        return None
    normalised = member.replace("\\", "/")
    for allowed in ALLOWED_UNSIGNED:
        if not container.lower().endswith(allowed.container_suffix):
            continue
        if allowed.member.search(normalised):
            return allowed
    return None


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


def verify_local_files(paths: list[str]) -> int:
    """Assert every named local file carries a signature. Used as the post-condition of signing.

    This exists because `vpk pack` does NOT verify that its `--signTemplate` actually signed
    anything: a template that exits 0 without signing leaves packing to complete normally and
    the release to publish unsigned binaries, which is exactly how #3288 shipped twice. So the
    signing script asserts the outcome here rather than trusting its own success.
    """
    failures, checked = [], 0
    for path in paths:
        try:
            with open(path, "rb") as handle:
                head = handle.read(HEADER_BYTES)
            off, length = certificate_table(head)
        except (OSError, ReadError, struct.error) as exc:
            failures.append(f"{path}: unreadable as a PE image ({exc})")
            continue
        checked += 1
        if off == 0 or length == 0:
            failures.append(f"{path}: NO SIGNATURE after signing")
    for f in failures:
        print(f"VERIFY FAIL: {f}", file=sys.stderr)
    if not checked:
        print("VERIFY FAIL: nothing was checked", file=sys.stderr)
        return 2
    if failures:
        return 1
    print(f"verified: {checked} file(s) carry a signature")
    return 0


def verify_local_dir(directory: str) -> int:
    """Assert every executable under `directory`, including inside zips and nupkgs, is signed.

    This is the release guard. It runs on the artifacts as they sit on disk BEFORE upload, so it
    needs no network and no published release -- the difference between this and the tag mode is
    only where the bytes come from.

    ALLOWED_UNSIGNED exempts exactly two members of a `.nupkg`. Everything else fails, including
    those same two names in any other container and as standalone files.
    """
    import glob
    import os
    import zipfile

    findings: list[Finding] = []
    for path in sorted(glob.glob(os.path.join(directory, "**", "*"), recursive=True)):
        if not os.path.isfile(path):
            continue
        lower = path.lower()
        rel = os.path.relpath(path, directory)
        if lower.endswith(".exe"):
            try:
                with open(path, "rb") as handle:
                    off, length = certificate_table(handle.read(HEADER_BYTES))
                findings.append(Finding(rel, "", os.path.getsize(path), off != 0 and length != 0))
            except (OSError, ReadError, struct.error) as exc:
                print(f"GUARD: cannot read {rel}: {exc}", file=sys.stderr)
                return 2
        elif lower.endswith(ARCHIVE_SUFFIXES):
            try:
                with zipfile.ZipFile(path) as zf:
                    for info in zf.infolist():
                        if not info.filename.lower().endswith(".exe"):
                            continue
                        blob = zf.read(info)
                        off, length = certificate_table(blob)
                        findings.append(
                            Finding(rel, info.filename, info.file_size, off != 0 and length != 0)
                        )
            except (OSError, zipfile.BadZipFile, ReadError, struct.error) as exc:
                print(f"GUARD: cannot read {rel}: {exc}", file=sys.stderr)
                return 2

    if not findings:
        print(f"GUARD FAIL: no executables found under {directory}", file=sys.stderr)
        return 2

    allowed: list[tuple[Finding, AllowedUnsigned]] = []
    unsigned: list[Finding] = []
    for finding in findings:
        if finding.signed:
            continue
        allowance = allowance_for(os.path.basename(finding.asset), finding.path)
        if allowance is None:
            unsigned.append(finding)
        else:
            allowed.append((finding, allowance))

    allowed_ids = {id(entry) for entry, _ in allowed}
    for finding in sorted(findings, key=lambda x: (x.signed, x.label)):
        if finding.signed:
            mark = "signed  "
        elif id(finding) in allowed_ids:
            mark = "allowed "
        else:
            mark = "UNSIGNED"
        print(f"  {mark}  {finding.label}")

    for finding, allowance in allowed:
        print(f"\nallowed unsigned: {finding.label}\n  {allowance.reason}")

    # An allowance that matches nothing has outlived the constraint that justifies it. It is
    # reported rather than fatal: the guard's job is to refuse unsigned executables, and an
    # allowance covering none of them refuses nothing.
    used = {id(allowance) for _, allowance in allowed}
    for allowance in ALLOWED_UNSIGNED:
        if id(allowance) not in used:
            print(
                f"ALLOWANCE UNUSED: {allowance.container_suffix} :: "
                f"{allowance.member.pattern} matched no unsigned executable"
            )

    print(
        f"\n{len(findings)} executable(s) checked, {len(unsigned)} unsigned, "
        f"{len(allowed)} allowed unsigned"
    )
    if unsigned:
        print(f"GUARD FAIL: {len(unsigned)} unsigned executable(s). See #3288.", file=sys.stderr)
        return 1
    return 0


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


def _release_dir(root: str, layout: dict[str, object]) -> str:
    """Materialise a release directory. A str key is a standalone file, a dict is an archive.

    Used by the self-test so the guard is exercised through its real entry point -- a directory
    of real zip and nupkg files -- rather than through a stubbed finding list.
    """
    import os
    import zipfile

    os.makedirs(root, exist_ok=True)
    for name, content in layout.items():
        target = os.path.join(root, name)
        os.makedirs(os.path.dirname(target) or root, exist_ok=True)
        if isinstance(content, dict):
            with zipfile.ZipFile(target, "w") as zf:
                for member, signed in content.items():
                    zf.writestr(member, _synth_pe(signed=bool(signed)))
        else:
            with open(target, "wb") as handle:
                handle.write(_synth_pe(signed=bool(content)))
    return root


def self_test() -> int:
    import contextlib
    import io
    import os
    import tempfile

    failures: list[str] = []
    checks = 0

    def guard(label: str, layout: dict[str, object], want: int) -> None:
        """Run the release guard over a synthesised release directory."""
        nonlocal checks
        checks += 1
        with tempfile.TemporaryDirectory() as tmp:
            directory = _release_dir(os.path.join(tmp, "releases"), layout)
            sink = io.StringIO()
            with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                got = verify_local_dir(directory)
        if got != want:
            failures.append(f"{label}: guard returned {got}, expected {want}")

    def expect(label: str, condition: bool) -> None:
        nonlocal checks
        checks += 1
        if not condition:
            failures.append(label)

    for plus in (True, False):
        shape = "PE32+" if plus else "PE32"
        checks += 2
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
        checks += 1
        try:
            certificate_table(blob)
        except (ReadError, struct.error, IndexError):
            pass
        else:
            failures.append(f"{label}: returned a verdict instead of raising")

    # A short range must raise, or a truncated read silently becomes "no signature".
    checks += 1
    try:
        fetch_range("file:///dev/null", 0, 64)
    except ReadError:
        pass
    else:
        failures.append("a short read returned data instead of raising")

    # ------------------------------------------------------------------------------------------
    # The release guard and its allowlist.
    #
    # A blanket skip by filename is the failure mode these cases exist to refuse: the two allowed
    # names also appear as the portable zip's root launcher and as Update.exe, and `Setup.exe`
    # shares nothing with them but would pass any allowlist loose enough to be written by name
    # alone. So every case below pairs a member path with a container.
    # ------------------------------------------------------------------------------------------
    stub = "lib/app/PerformanceMonitorLite_ExecutionStub.exe"
    squirrel = "lib/app/Squirrel.exe"
    app = "lib/app/PerformanceMonitorLite.exe"
    nupkg = "PerformanceMonitorLite-3.7.2-lite-full.nupkg"
    portable = "PerformanceMonitorLite-lite-Portable.zip"
    setup = "PerformanceMonitorLite-lite-Setup.exe"

    def correct_release() -> dict[str, object]:
        """What a release looks like once post-pack signing has run."""
        return {
            nupkg: {app: True, stub: False, squirrel: False},
            portable: {
                "PerformanceMonitorLite.exe": True,
                "Update.exe": True,
                "current/PerformanceMonitorLite.exe": True,
            },
            setup: True,
        }

    guard("a correct release", correct_release(), 0)

    # The allowance is the whole point, so it has to actually apply.
    only_allowed = {nupkg: {app: True, stub: False, squirrel: False}}
    guard("the two allowed nupkg members", only_allowed, 0)
    guard("the allowed stub alone", {nupkg: {app: True, stub: False}}, 0)
    guard("the allowed Squirrel.exe alone", {nupkg: {app: True, squirrel: False}}, 0)

    # And it has to stop applying the moment anything about the location changes.
    broken = correct_release()
    broken[setup] = False
    guard("an unsigned Setup.exe beside allowed nupkg members", broken, 1)

    broken = correct_release()
    broken[portable] = dict(broken[portable])  # type: ignore[arg-type]
    broken[portable]["PerformanceMonitorLite.exe"] = False  # type: ignore[index]
    guard("an unsigned portable-root launcher", broken, 1)

    broken = correct_release()
    broken[portable] = dict(broken[portable])  # type: ignore[arg-type]
    broken[portable]["Update.exe"] = False  # type: ignore[index]
    guard("an unsigned portable Update.exe", broken, 1)

    guard(
        "the allowed member paths inside a .zip rather than a .nupkg",
        {portable.replace("Portable", "Other"): {stub: False, squirrel: False}},
        1,
    )
    guard("an unsigned standalone Squirrel.exe", {"Squirrel.exe": False}, 1)
    guard(
        "an unsigned standalone launcher stub",
        {"PerformanceMonitorLite_ExecutionStub.exe": False},
        1,
    )
    guard(
        "an unsigned nupkg member that is not on the allowlist",
        {nupkg: {app: True, "lib/app/Update.exe": False}},
        1,
    )
    guard(
        "an allowed name in a different directory inside the nupkg",
        {nupkg: {app: True, "lib/other/Squirrel.exe": False}},
        1,
    )
    guard("an unsigned app payload inside the nupkg", {nupkg: {app: False}}, 1)
    guard("a nupkg whose stub is signed and app is not", {nupkg: {app: False, stub: False}}, 1)

    # Finding nothing to check is a broken run, not a clean one.
    guard("a directory with no executables", {}, 2)

    # The allowance predicate itself, at the boundary the container check defends.
    expect(
        "a standalone file was granted an allowance",
        allowance_for("PerformanceMonitorLite-lite-Setup.exe", "") is None,
    )
    expect(
        "the stub inside a .nupkg was not allowed",
        allowance_for(nupkg, stub) is not None,
    )
    expect(
        "the stub inside a .zip was allowed",
        allowance_for(portable, stub) is None,
    )
    expect(
        "a backslash-separated nupkg member path was not normalised",
        allowance_for(nupkg, squirrel.replace("/", "\\")) is not None,
    )
    expect(
        "a nested path under lib/app was allowed",
        allowance_for(nupkg, "lib/app/sub/Squirrel.exe") is None,
    )
    expect(
        "a name that merely ends with the allowed name was allowed",
        allowance_for(nupkg, "lib/app/NotSquirrel.exe") is None,
    )
    expect(
        "every allowance carries a reason",
        all(a.reason.strip() for a in ALLOWED_UNSIGNED),
    )
    expect(
        f"the allowlist holds {len(ALLOWED_UNSIGNED)} entries rather than the two nupkg members",
        len(ALLOWED_UNSIGNED) == 2,
    )

    for f in failures:
        print(f"SELF-TEST FAIL: {f}", file=sys.stderr)
    if failures:
        return 1
    print(f"self-test: {checks} assertions passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tag", nargs="?", help="release tag, e.g. v3.7.0")
    parser.add_argument("--self-test", action="store_true", dest="self_test")
    parser.add_argument(
        "--verify-files",
        nargs="+",
        metavar="PATH",
        help="verify local files carry a signature (the signing script's post-condition)",
    )
    parser.add_argument(
        "--verify-dir",
        metavar="DIR",
        help="release guard: every .exe under DIR, including inside zips/nupkgs, must be signed",
    )
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
    if args.verify_files:
        return verify_local_files(args.verify_files)
    if args.verify_dir:
        return verify_local_dir(args.verify_dir)
    if not args.tag:
        parser.error("a release tag is required unless --self-test or --verify-files is given")

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
