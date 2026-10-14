#!/usr/bin/env python3
"""Report which published Windows executables carry an Authenticode signature (#3288).

Every executable `vpk pack` GENERATES shipped unsigned on v3.6.0 and v3.7.0 -- the portable
launcher stub, `Update.exe`, and `Setup.exe` -- while the app payload was signed. It went
unnoticed for two releases because the only visible symptom is a Windows "unknown publisher"
block on someone else's machine, and because nothing asked the question.

So this asks it, from any platform: is there a signature attached to each thing a user can
launch. `--verify-dir` asks it of artifacts still on disk in the release job, which is the step
that refuses to publish. The tag form asks it of what a release actually shipped.

WHAT IT CHECKS, AND WHAT IT DELIBERATELY DOES NOT

  It reads the PE optional header's Security data directory (index 4) and reports whether the
  certificate table is non-empty. That is "a signature is attached" and nothing more.

  PowerShell scripts (#5796) carry their signature as a trailing comment block instead, so a
  script counts as signed when it ENDS with that block: a `# SIG # Begin signature block` line,
  one or more `# <base64>` lines, and the `# SIG # End signature block` line, each ending in CRLF,
  with nothing at all after the End line. The Darling zip must hold its three scripts at its root,
  and the release guard needs a Darling zip to be there at all. The release job then asks Windows
  itself (Get-AuthenticodeSignature) whether the three scripts' signatures are Valid and made with
  the SignPath Foundation certificate, which is the check this reader cannot make.

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
  python3 .github/scripts/verify_release_signatures.py --verify-dir releases
  python3 .github/scripts/verify_release_signatures.py --self-test

  Exit status is 1 if any executable is unsigned and not covered by ALLOWED_UNSIGNED, 2 if a
  release, asset or directory could not be read at all, and 0 otherwise. An empty executable
  list is status 2, not 0: finding nothing to check is a broken run, not a clean one.

  `--verify-dir` is the release guard: the same reader over artifacts that are still on disk
  before upload, so it needs no HTTP and no published release. The tag form answers "what did we
  actually publish", which is the question that goes unasked. Both apply ALLOWED_UNSIGNED.
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

# PowerShell scripts carry their Authenticode signature as a trailing comment block, not as a PE
# certificate table (#5796). A signed script ENDS with the block, exactly as Set-AuthenticodeSignature
# writes it:
#
#     # SIG # Begin signature block
#     # MIIoIgYJKoZIhvcNAQcCoIIoEzCCKA8CAQExDzANBglghkgBZQMEAgEFADB5Bgor
#     # BgEEAYI3AgEEoGswaTA0BgorBgEEAYI3AgEeMCYCAwEAAAQQH8w7YFlLCE63JNLG
#     # SIG # End signature block
#
# Only the Begin and End lines carry `# SIG #`; each body line is `# ` and base64, nothing else.
# Each line is matched whole, so a marker at the end of a code line, a block inside a here-string
# or a comment with code after it, or a Begin/End pair with no body, does not read as signed.
# The rest follows what Windows itself reads (pwsh 7.6 and PowerShell 5.1 both report NotSigned
# otherwise): every line of the block ends in CRLF, the End line's CRLF is the last byte of the file
# (no blank, whitespace or control-character line after it), and the block holds one Begin line.
# .psm1, .psd1 and .ps1xml are Authenticode-signed too. None ships today; they are listed so one
# that starts shipping is checked rather than skipped. A .ps1xml writes its block as XML comments,
# which this reader does not parse, so a signed one would read as unsigned: a failure, not a pass.
SCRIPT_SUFFIXES = (".ps1", ".psm1", ".psd1", ".ps1xml")
SIG_BEGIN = re.compile(r"# SIG # Begin signature block")
SIG_BODY = re.compile(r"# [A-Za-z0-9+/=]+")
SIG_END = re.compile(r"# SIG # End signature block")
# The three scripts the Darling service zip ships at its root (the service project links them
# in). A Darling zip without all three AT ITS ROOT fails the census: a script that is not there
# cannot be checked, and that is not the same as one that is signed.
DARLING_SCRIPTS = ("install-darling.ps1", "uninstall-darling.ps1", "upgrade-darling.ps1")


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
            "copy in Portable.zip is signed after packing."
        ),
    ),
    AllowedUnsigned(
        container_suffix=".nupkg",
        member=re.compile(r"\Alib/app/Squirrel\.exe\Z", re.IGNORECASE),
        reason=(
            "Velopack's updater, generated during `vpk pack` and deployed as Update.exe. Same "
            "recorded-hash constraint as the launcher stub. The copy in Portable.zip is signed "
            "after packing."
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


def is_script(name: str) -> bool:
    return name.lower().endswith(SCRIPT_SUFFIXES)


def script_is_signed(blob: bytes) -> bool:
    """True when the script ENDS with a well-formed signature block.

    The file must end with the End line and its CRLF, every line above it up to the Begin line must
    be a `# <base64>` body line, and there must be at least one; each of those lines ends in CRLF.
    Anything else reads as unsigned: a Begin with no End, code after the End line (a block inside a
    here-string or a comment), a Begin/End pair with no body, a marker at the end of a code line,
    anything after the End line's CRLF (a blank, whitespace or control-character line, or no final
    CRLF at all), a block with LF-only line endings, or a second Begin line inside the block.
    Windows reports NotSigned for each of those, so a pass here never outruns what it would read.
    PowerShell writes the block in the file's own encoding, so a UTF-16 file (BOM FF FE or FE FF) is
    decoded as UTF-16, and a UTF-8 BOM is dropped so it cannot hide a marker on the first line.
    """
    if blob.startswith((b"\xff\xfe", b"\xfe\xff")):
        text = blob.decode("utf-16", "replace")
    else:
        text = blob.decode("utf-8-sig", "replace")
    # Split on CRLF only: a line of the block that ends in a bare LF stays joined to its neighbour,
    # and that joined line matches none of the three patterns.
    if not text.endswith("\r\n"):
        return False
    lines = text[:-2].split("\r\n")
    if not SIG_END.fullmatch(lines.pop()):
        return False
    body = 0
    while lines and SIG_BODY.fullmatch(lines[-1]):
        lines.pop()
        body += 1
    if body == 0 or not lines or not SIG_BEGIN.fullmatch(lines.pop()):
        return False
    # A second Begin line inside the block, which Windows reports as NotSigned: a run of body-shaped
    # lines topped by another Begin is one block holding two. A comment line in the code above the
    # block is body-shaped too, so the walk looks past those to the first line that is not.
    while lines and SIG_BODY.fullmatch(lines[-1]):
        lines.pop()
    return not (lines and SIG_BEGIN.fullmatch(lines[-1]))


def is_darling_zip(name: str) -> bool:
    """True for the Darling service zip (PerformanceMonitorDarling-<version>.zip).

    "darling" anywhere in a .zip name, except the Darling Viewer's: the Viewer's Velopack
    Portable.zip (PerformanceMonitorDarlingViewer-*) sits in the same release directory and
    carries no scripts, so a plain "contains Darling" rule would fail every release.
    """
    base = name.replace("\\", "/").rsplit("/", 1)[-1].lower()
    return base.endswith(".zip") and "darling" in base and "darlingviewer" not in base


def darling_census(archive: str, members: list[str]) -> str | None:
    """Why `archive` fails the script census, or None. Only a Darling zip has a census (#5796)."""
    if not is_darling_zip(archive):
        return None
    # Full member paths, not file names: the scripts ship at the zip root (#5796), and a copy under
    # a subfolder is not the one an operator runs.
    present = {m.replace("\\", "/").lower() for m in members}
    missing = [script for script in DARLING_SCRIPTS if script not in present]
    if not missing:
        return None
    return f"{archive}: the Darling zip must hold three scripts; missing {', '.join(missing)}"


class ReadError(RuntimeError):
    """A byte range could not be read, or came back the wrong size."""


# What reading one archive member can raise: our own ReadError, a corrupt deflate stream, a
# truncated header, or (zipfile) a compression method this Python cannot decode. Each is a read
# failure, reported as one, and never a reason to skip the member.
UNREADABLE = (ReadError, zlib.error, struct.error, NotImplementedError)


def no_darling_zip(where: str) -> str:
    return (
        f"{where}: no Darling zip (a .zip whose name holds 'darling', not the Viewer's), so its "
        "three scripts were never checked"
    )


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
    census: list[str] = field(default_factory=list)

    @property
    def allowed(self) -> list[tuple[Finding, AllowedUnsigned]]:
        pairs = ((f, allowance_for(f.asset, f.path)) for f in self.findings if not f.signed)
        return [(f, a) for f, a in pairs if a is not None]

    @property
    def unsigned(self) -> list[Finding]:
        """Unsigned and NOT allowed. The same allowlist the release guard applies.

        Both modes read the same allowlist so that a correct release audits clean here. A mode
        that reported the two allowed `.nupkg` members as failures would be red on every release
        from now on, which is the state in which nobody reads it.
        """
        return [
            f
            for f in self.findings
            if not f.signed and allowance_for(f.asset, f.path) is None
        ]


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
    total = struct.unpack_from("<H", tail, i + 10)[0]
    cd_size, cd_off = struct.unpack_from("<II", tail, i + 12)
    if cd_off == 0xFFFFFFFF or cd_size == 0xFFFFFFFF or total == 0xFFFF:
        raise ReadError("zip64 central directory: not supported by this reader")
    cd = fetch_range(url, cd_off, cd_size)
    out, p = [], 0
    # Every entry the zip says it holds is walked, or the walk is a read error (#5796): a damaged
    # entry that ended the walk early would leave the entries after it unchecked.
    while p + 46 <= len(cd):
        if cd[p : p + 4] != b"PK\x01\x02":
            raise ReadError(f"central directory entry {len(out) + 1} has no entry signature")
        method = struct.unpack_from("<H", cd, p + 10)[0]
        csize, usize = struct.unpack_from("<II", cd, p + 20)
        nlen, elen, clen = struct.unpack_from("<HHH", cd, p + 28)
        lho = struct.unpack_from("<I", cd, p + 42)[0]
        name = cd[p + 46 : p + 46 + nlen].decode("utf-8", "replace")
        out.append((name, method, csize, usize, lho))
        p += 46 + nlen + elen + clen
    if len(out) != total:
        raise ReadError(f"central directory holds {len(out)} entries, the zip says {total}")
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


def check_archive(
    url: str, name: str, size: int, include_dlls: bool
) -> tuple[list[Finding], list[str], str | None]:
    """(findings, read errors, census failure or None) for one archive asset."""
    findings: list[Finding] = []
    errors: list[str] = []
    wanted = (EXECUTABLE_SUFFIXES if include_dlls else (".exe",)) + SCRIPT_SUFFIXES
    entries = zip_entries(url, size)
    for member, method, csize, usize, lho in entries:
        if not member.lower().endswith(wanted):
            continue
        try:
            # Members are read whole. A script's signature is at its end, not in a header.
            blob = read_zip_member(url, method, csize, lho)
            if is_script(member):
                signed = script_is_signed(blob)
            else:
                off, length = certificate_table(blob)
                signed = off != 0 and length != 0
            findings.append(Finding(asset=name, path=member, size=usize, signed=signed))
        except UNREADABLE as exc:
            # A member that cannot be read is an error, never a skip: main exits 2 on any error.
            errors.append(f"{name} :: {member}: {type(exc).__name__}: {exc}")
    return findings, errors, darling_census(name, [entry[0] for entry in entries])


def verify_local_dir(directory: str) -> int:
    """Assert every executable and script under `directory`, inside zips and nupkgs too, is signed.

    This is the release guard. It runs on the artifacts as they sit on disk BEFORE upload, so it
    needs no network and no published release -- the difference between this and the tag mode is
    only where the bytes come from.

    ALLOWED_UNSIGNED exempts exactly two members of a `.nupkg`. Everything else fails, including
    those same two names in any other container and as standalone files. No allowance covers a
    script, a Darling zip must be there, and it must hold all three of its scripts at its root
    (#5796). A file or member that cannot be read is status 2, never a skip.
    """
    import glob
    import os
    import zipfile

    findings: list[Finding] = []
    census: list[str] = []
    darling_zips = 0
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
        elif is_script(lower):
            try:
                with open(path, "rb") as handle:
                    signed = script_is_signed(handle.read())
                findings.append(Finding(rel, "", os.path.getsize(path), signed))
            except OSError as exc:
                print(f"GUARD: cannot read {rel}: {exc}", file=sys.stderr)
                return 2
        elif lower.endswith(ARCHIVE_SUFFIXES):
            if is_darling_zip(rel):
                darling_zips += 1
            try:
                with zipfile.ZipFile(path) as zf:
                    members = zf.infolist()
                    failure = darling_census(rel, [info.filename for info in members])
                    if failure:
                        census.append(failure)
                    for info in members:
                        if is_script(info.filename):
                            signed = script_is_signed(zf.read(info))
                            findings.append(Finding(rel, info.filename, info.file_size, signed))
                            continue
                        if not info.filename.lower().endswith(".exe"):
                            continue
                        blob = zf.read(info)
                        off, length = certificate_table(blob)
                        findings.append(
                            Finding(rel, info.filename, info.file_size, off != 0 and length != 0)
                        )
            # zipfile raises RuntimeError for an encrypted member and EOFError for one whose data
            # ends before its recorded size (#5796): each is a read error, status 2.
            except (OSError, zipfile.BadZipFile, RuntimeError, EOFError, *UNREADABLE) as exc:
                print(f"GUARD: cannot read {rel}: {type(exc).__name__}: {exc}", file=sys.stderr)
                return 2

    if not darling_zips:
        census.append(no_darling_zip(directory))
    if not findings:
        print(f"GUARD FAIL: no executables or scripts found under {directory}", file=sys.stderr)
        return 2
    scripts = sum(1 for f in findings if is_script(f.path or f.asset))

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
        f"\n{len(findings) - scripts} executable(s) and {scripts} script(s) checked, "
        f"{len(unsigned)} unsigned, {len(allowed)} allowed unsigned"
    )
    if unsigned:
        print(
            f"GUARD FAIL: {len(unsigned)} unsigned executable(s) or script(s). See #3288 and #5796.",
            file=sys.stderr,
        )
    for failure in census:
        print(f"GUARD FAIL: {failure}", file=sys.stderr)
    if census:
        return 2
    return 1 if unsigned else 0


def release_assets(tag: str, repo: str, gh: str) -> list[dict]:
    proc = subprocess.run(
        [gh, "api", f"repos/{repo}/releases/tags/{tag}"], capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise ReadError(f"cannot read release {tag}: {proc.stderr.strip()[:300]}")
    return json.loads(proc.stdout).get("assets", [])


def build_report(tag: str, repo: str, gh: str, include_dlls: bool) -> Report:
    report = Report(tag=tag)
    darling_zips = 0
    for asset in release_assets(tag, repo, gh):
        name, url, size = asset["name"], asset["browser_download_url"], asset["size"]
        lower = name.lower()
        try:
            if lower.endswith(".exe"):
                report.findings.append(check_standalone_exe(url, name, size))
            elif is_script(lower):
                blob = fetch_range(url, 0, size) if size else b""
                report.findings.append(Finding(name, "", size, script_is_signed(blob)))
            elif lower.endswith(ARCHIVE_SUFFIXES):
                if is_darling_zip(name):
                    darling_zips += 1
                found, errs, census = check_archive(url, name, size, include_dlls)
                report.findings.extend(found)
                report.errors.extend(errs)
                if census:
                    report.census.append(census)
        except UNREADABLE as exc:
            report.errors.append(f"{name}: {type(exc).__name__}: {exc}")
    if not darling_zips:
        report.census.append(no_darling_zip(tag))
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


SIG_BLOCK = (
    "# SIG # Begin signature block\r\n# MIIFAKEAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\r\n"
    "# SIG # End signature block\r\n"
)
# Scripts that hold the markers but do not END with a well-formed block. Each must read unsigned.
FORGED_SCRIPTS: dict[str, bytes] = {
    "a marker at the end of a code line": (
        "Write-Output 1\r\n# SIG # Begin signature block\r\n# MIIF\r\n"
        "Write-Output 2 # SIG # End signature block\r\n"
    ).encode("ascii"),
    "a block inside a here-string": (
        "$block = @'\r\n" + SIG_BLOCK + "'@\r\nWrite-Output $block\r\n"
    ).encode("ascii"),
    "a block inside a comment that closes after it": (
        "Write-Output 1\r\n<#\r\n" + SIG_BLOCK + "#>\r\n"
    ).encode("ascii"),
    "code after the End line": ("Write-Output 1\r\n" + SIG_BLOCK + "Write-Output 2\r\n").encode("ascii"),
    "only the two markers": b"# SIG # Begin signature block\r\n# SIG # End signature block\r\n",
    "a block with a non-base64 body line": (
        "Write-Output 1\r\n# SIG # Begin signature block\r\n# not base64!\r\n"
        "# SIG # End signature block\r\n"
    ).encode("ascii"),
    "a block with a code line in its body": (
        "Write-Output 1\r\n# SIG # Begin signature block\r\n# MIIF\r\nWrite-Output 2\r\n"
        "# MIIF\r\n# SIG # End signature block\r\n"
    ).encode("ascii"),
    "an End line with no Begin line": (
        "Write-Output 1\r\n# MIIF\r\n# SIG # End signature block\r\n"
    ).encode("ascii"),
    "a Begin marker after code on its line": (
        "Write-Output 1 # SIG # Begin signature block\r\n# MIIF\r\n"
        "# SIG # End signature block\r\n"
    ).encode("ascii"),
    # The shapes below each END with the block yet Windows reports NotSigned for them (pwsh 7.6 and
    # PowerShell 5.1 agree), so the reader must too.
    "a blank and a whitespace-only line after the End line": (
        "Write-Output 1\r\n" + SIG_BLOCK + "\r\n   \r\n\t\r\n"
    ).encode("ascii"),
    "a form-feed line after the End line": ("Write-Output 1\r\n" + SIG_BLOCK + "\x0c\r\n").encode("ascii"),
    "a 0x1C line after the End line": ("Write-Output 1\r\n" + SIG_BLOCK + "\x1c\r\n").encode("ascii"),
    "spaces after the End line": ("Write-Output 1\r\n" + SIG_BLOCK[:-2] + "  \r\n").encode("ascii"),
    "no CRLF after the End line": ("Write-Output 1\r\n" + SIG_BLOCK[:-2]).encode("ascii"),
    "LF-only line endings": ("Write-Output 1\r\n" + SIG_BLOCK).replace("\r\n", "\n").encode("ascii"),
    "a block with LF-only line endings": (
        "Write-Output 1\r\n" + SIG_BLOCK.replace("\r\n", "\n")
    ).encode("ascii"),
    "a second Begin line at the top of the block": (
        "Write-Output 1\r\n# SIG # Begin signature block\r\n" + SIG_BLOCK
    ).encode("ascii"),
    "a second Begin line inside the block's body": (
        "Write-Output 1\r\n# SIG # Begin signature block\r\n# MIIF\r\n" + SIG_BLOCK
    ).encode("ascii"),
}
# Shapes that Windows reads as a signature (Valid, or HashMismatch when the content changed), so
# the reader must read them as signed: a mismatch is Get-AuthenticodeSignature's to report.
SIGNED_SCRIPTS: dict[str, bytes] = {
    "a block after code that ends in comment lines": (
        "Write-Output 1\r\n# done\r\n# MIIF\r\n" + SIG_BLOCK
    ).encode("ascii"),
    "LF-only code with the CRLF block Set-AuthenticodeSignature appends": (
        "param()\nWrite-Output 1\n\r\n" + SIG_BLOCK
    ).encode("ascii"),
    "a stale block at the top of the file and a fresh one at its end": (
        SIG_BLOCK + "Write-Output 1\r\n" + SIG_BLOCK
    ).encode("ascii"),
}
# A layout member with this name marks its zip for a zip64 end-of-central-directory record.
ZIP64 = "<zip64>"
# Layout members that damage a zip's central directory after the last member is written: CD_SHORT
# shrinks the end-of-central-directory record's size field so the last entry falls outside it, and
# CD_BAD_SIG overwrites the last entry's signature and lowers the record's entry count to match
# (#5796).
CD_SHORT = "<cd-short>"
CD_BAD_SIG = "<cd-bad-sig>"
ZIP_MARKERS = (ZIP64, CD_SHORT, CD_BAD_SIG)
# The fixture Set-AuthenticodeSignature signed with a throwaway self-signed certificate (#5796).
GOLDEN_SCRIPT = "fixtures/signed-golden.ps1"


def _synth_ps1(*, signed: bool, end: bool = True) -> bytes:
    """A small script, with a signature block when `signed` (its End line only when `end`).

    The block's body is filler: the guard reads the markers, not the certificate.
    """
    text = "param()\r\nWrite-Output 'hello'\r\n"
    if signed:
        text += "\r\n# SIG # Begin signature block\r\n# MIIFAKEAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\r\n"
        if end:
            text += "# SIG # End signature block\r\n"
    return text.encode("ascii")


def _release_dir(root: str, layout: dict[str, object]) -> str:
    """Materialise a release directory. A str key is a standalone file, a dict is an archive.

    Used by the self-test so the guard is exercised through its real entry point -- a directory
    of real zip and nupkg files -- rather than through a stubbed finding list. A `.ps1` name gets a
    synthetic script, every other name a synthetic PE image; the value `"no-end"` is a script whose
    signature block has a Begin line and no End line.
    """
    import os
    import zipfile

    os.makedirs(root, exist_ok=True)
    for name, content in layout.items():
        target = os.path.join(root, name)
        os.makedirs(os.path.dirname(target) or root, exist_ok=True)
        if isinstance(content, dict):
            with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
                for member, signed in content.items():
                    if member in ZIP_MARKERS:
                        continue
                    zf.writestr(member, _synth_file(member, signed))
            damaged = {"corrupt", "deflate64", "encrypted"}
            if set(ZIP_MARKERS) & set(content) or damaged & set(map(str, content.values())):
                _damage_zip(target, content)
        else:
            with open(target, "wb") as handle:
                handle.write(_synth_file(name, content))
    return root


def _damage_zip(path: str, content: dict[str, object]) -> None:
    """Damage a synthetic zip after it is written, as its layout asks.

    ZIP64 marks it zip64; CD_SHORT and CD_BAD_SIG damage its central directory (see their
    definitions). A member's value damages that member: "corrupt" overwrites its deflate stream
    with junk, "deflate64" relabels it as method 9, which neither reader decodes, and "encrypted"
    sets its encryption flag.
    """
    import zipfile

    with open(path, "rb") as handle:
        data = bytearray(handle.read())
    with zipfile.ZipFile(path) as zf:
        infos = zf.infolist()
        cd_offset = zf.start_dir
    eocd = data.rfind(b"PK\x05\x06")
    # Each member's central-directory entry, walked in order from the start of the directory.
    entries: dict[str, int] = {}
    p = cd_offset
    while data[p : p + 4] == b"PK\x01\x02":
        nlen, elen, clen = struct.unpack_from("<HHH", data, p + 28)
        entries[data[p + 46 : p + 46 + nlen].decode("utf-8")] = p
        p += 46 + nlen + elen + clen
    for info in infos:
        lho, cde = info.header_offset, entries[info.filename]
        kind = content.get(info.filename)
        if kind == "deflate64":
            struct.pack_into("<H", data, lho + 8, 9)
            struct.pack_into("<H", data, cde + 10, 9)
        elif kind == "encrypted":
            struct.pack_into("<H", data, lho + 6, info.flag_bits | 1)
            struct.pack_into("<H", data, cde + 8, info.flag_bits | 1)
        elif kind == "corrupt":
            nlen, elen = struct.unpack_from("<HH", data, lho + 26)
            start = lho + 30 + nlen + elen
            data[start : start + info.compress_size] = b"\xff" * info.compress_size
    last = entries[infos[-1].filename]
    if CD_SHORT in content:
        struct.pack_into("<I", data, eocd + 12, last - cd_offset)
    if CD_BAD_SIG in content:
        # The record's entry count agrees with the entries before the damaged one, so only the
        # signature check can catch it.
        data[last : last + 4] = b"PK\x09\x09"
        struct.pack_into("<HH", data, eocd + 8, len(infos) - 1, len(infos) - 1)
    if ZIP64 in content:
        struct.pack_into("<II", data, eocd + 12, 0xFFFFFFFF, 0xFFFFFFFF)
    with open(path, "wb") as handle:
        handle.write(bytes(data))


def _synth_file(name: str, signed: object) -> bytes:
    if is_script(name):
        if signed == "no-end":
            return _synth_ps1(signed=True, end=False)
        if isinstance(signed, str) and signed in FORGED_SCRIPTS:
            return FORGED_SCRIPTS[signed]
        # "deflate64", "corrupt" and "encrypted" are a signed script stored in a way the readers
        # cannot decode.
        return _synth_ps1(signed=bool(signed))
    return _synth_pe(signed=bool(signed))


def self_test() -> int:
    import base64
    import contextlib
    import io
    import os
    import pathlib
    import tempfile
    import zipfile

    failures: list[str] = []
    checks = 0

    DARLING_ZIP = "PerformanceMonitorDarling-3.10.0.zip"

    def darling_zip(**scripts: object) -> dict[str, object]:
        """The Darling zip's members: the service exe and the three scripts at the root.

        A keyword names a script by its first word (install=False) and sets its content.
        """
        content: dict[str, object] = {"PerformanceMonitor.Darling.Service.exe": True}
        for script in DARLING_SCRIPTS:
            content[script] = scripts.get(script.split("-")[0], True)
        return content

    def guard(label: str, layout: dict[str, object], want: int, *, darling: bool = True) -> None:
        """Run the release guard over a synthesised release directory.

        Every release must carry the Darling zip (#5796), so a layout with none gets a correct one
        (three signed scripts at its root) unless `darling` is False.
        """
        nonlocal checks
        checks += 1
        if darling and not any(is_darling_zip(name) for name in layout):
            layout = {**layout, DARLING_ZIP: darling_zip()}
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
    VIEWER = "PerformanceMonitorDarlingViewer-darlingviewer-Portable.zip"
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
    guard("a directory with no executables", {}, 2, darling=False)

    # ------------------------------------------------------------------------------------------
    # The Darling zip's PowerShell scripts (#5796). Each one is signed like an executable, no
    # allowance covers them, and the Darling zip must hold all three.
    # ------------------------------------------------------------------------------------------

    guard("a Darling zip with three signed scripts", {**correct_release(), DARLING_ZIP: darling_zip()}, 0)
    guard(
        "a Darling zip with one unsigned script",
        {**correct_release(), DARLING_ZIP: darling_zip(upgrade=False)},
        1,
    )
    guard(
        "a Darling zip with no scripts",
        {**correct_release(), DARLING_ZIP: {"PerformanceMonitor.Darling.Service.exe": True}},
        2,
    )
    missing_one = darling_zip()
    del missing_one["uninstall-darling.ps1"]
    guard("a Darling zip missing one script", {**correct_release(), DARLING_ZIP: missing_one}, 2)
    guard(
        "a Darling zip whose script has a Begin line and no End line",
        {**correct_release(), DARLING_ZIP: darling_zip(install="no-end")},
        1,
    )
    guard(
        "a non-Darling zip with no scripts and signed executables",
        {portable.replace("Portable", "Other"): {"Other.exe": True, "Another.exe": True}},
        0,
    )
    guard(
        "the Darling Viewer's Velopack zip, which carries no scripts",
        {VIEWER: {"Viewer.exe": True}},
        0,
    )
    guard("an unsigned standalone script", {**correct_release(), "install-darling.ps1": False}, 1)
    guard(
        "a directory holding only signed scripts and the Darling zip",
        {"install-darling.ps1": True},
        0,
    )

    # The census reads full member paths, and a release with no Darling zip has no census at all.
    viewer_only = {"PerformanceMonitor.Darling.Service.exe": True}
    viewer_only.update({f"viewer/{script}": True for script in DARLING_SCRIPTS})
    guard("a Darling zip with its scripts only under a subfolder", {DARLING_ZIP: viewer_only}, 2)
    guard("a release with no Darling zip", correct_release(), 2, darling=False)
    guard(
        "a release whose Darling zip was renamed",
        {**correct_release(), "PerformanceMonitorService-3.10.0.zip": darling_zip()},
        2,
        darling=False,
    )
    # Members zipfile cannot read, and a central directory that ends early: each is status 2, never a
    # traceback and never a pass (#5796).
    guard("a Darling zip with an encrypted script", {DARLING_ZIP: darling_zip(upgrade="encrypted")}, 2)
    # zipfile raises EOFError when a member's data ends before its recorded size. Its own overlap
    # check (Python 3.12 and later) turns a file of that shape into BadZipFile first, so the read is
    # made to raise it here.
    real_read = zipfile.ZipFile.read

    def short_read(self: zipfile.ZipFile, name: object, pwd: bytes | None = None) -> bytes:
        if getattr(name, "filename", name) == "upgrade-darling.ps1":
            raise EOFError("simulated: the member's data ended early")
        return real_read(self, name, pwd)

    zipfile.ZipFile.read = short_read
    try:
        guard("a Darling zip whose script's data ends early", {DARLING_ZIP: darling_zip()}, 2)
    finally:
        zipfile.ZipFile.read = real_read
    late_unsigned = {**darling_zip(), "late-unsigned.exe": False}
    guard("a Darling zip whose central directory is short", {DARLING_ZIP: {**late_unsigned, CD_SHORT: True}}, 2)
    guard(
        "a Darling zip whose last central-directory entry has a bad signature",
        {DARLING_ZIP: {**late_unsigned, CD_BAD_SIG: True}},
        2,
    )
    guard("a release with only the Viewer's zip", {VIEWER: {"Viewer.exe": True}}, 2, darling=False)
    guard(
        "a Darling zip with a member in a compression method it cannot decode",
        {DARLING_ZIP: darling_zip(upgrade="deflate64")},
        2,
    )
    guard(
        "a Darling zip with a corrupt deflate stream",
        {DARLING_ZIP: darling_zip(upgrade="corrupt")},
        2,
    )

    # Each forged or damaged block, inside the Darling zip, through the release guard.
    for kind in FORGED_SCRIPTS:
        guard(f"a Darling zip whose script is {kind}", {DARLING_ZIP: darling_zip(install=kind)}, 1)

    # The marker reader itself.
    expect("a signed script read as unsigned", script_is_signed(_synth_ps1(signed=True)))
    expect("an unsigned script read as signed", not script_is_signed(_synth_ps1(signed=False)))
    expect(
        "a Begin line with no End line read as signed",
        not script_is_signed(_synth_ps1(signed=True, end=False)),
    )
    expect(
        "an End line before the Begin line read as signed",
        not script_is_signed(
            b"# SIG # End signature block\r\nWrite-Output 1\r\n# SIG # Begin signature block\r\n"
        ),
    )
    expect(
        "a marker inside a string read as signed",
        not script_is_signed(
            b"$a = '# SIG # Begin signature block'\r\n$b = '# SIG # End signature block'\r\n"
        ),
    )
    for kind, blob in FORGED_SCRIPTS.items():
        expect(f"a script that is {kind} read as signed", not script_is_signed(blob))
    for kind, blob in SIGNED_SCRIPTS.items():
        expect(f"a script that is {kind} read as unsigned", script_is_signed(blob))
    # Windows reports NotSigned for a block with blank lines after it, so the reader must too.
    expect(
        "a signed script with trailing blank lines read as signed",
        not script_is_signed(_synth_ps1(signed=True) + b"\r\n  \r\n"),
    )
    expect(
        "a UTF-8 BOM hid a first-line Begin marker",
        script_is_signed(b"\xef\xbb\xbf" + SIG_BLOCK.encode("ascii")),
    )
    expect(
        "a signed script with a UTF-8 BOM read as unsigned",
        script_is_signed(b"\xef\xbb\xbf" + _synth_ps1(signed=True)),
    )
    expect(
        "a .psm1, .psd1 or .ps1xml was not treated as a script",
        all(is_script(f"m{suffix}") for suffix in (".psm1", ".psd1", ".ps1xml", ".PS1")),
    )
    expect(
        "the release-mode census passed scripts that are only under a subfolder",
        darling_census(DARLING_ZIP, [f"viewer/{script}" for script in DARLING_SCRIPTS]) is not None,
    )
    expect(
        "a signed UTF-16 script read as unsigned",
        script_is_signed(b"\xff\xfe" + _synth_ps1(signed=True).decode("ascii").encode("utf-16-le")),
    )
    expect(
        "an LF-only signed script read as signed",
        not script_is_signed(_synth_ps1(signed=True).replace(b"\r\n", b"\n")),
    )

    # A real signature (#5796): Set-AuthenticodeSignature's own output, made with a throwaway
    # self-signed certificate. Fixtures written from a description of the block once used the
    # wrong body-line shape, and the reader built to match them read every real one as unsigned.
    try:
        golden = pathlib.Path(__file__).resolve().parent.joinpath(GOLDEN_SCRIPT).read_bytes()
    except OSError as exc:
        golden = b""
        failures.append(f"the signed fixture {GOLDEN_SCRIPT} could not be read: {exc}")
    begin = golden.rfind(b"# SIG # Begin signature block\r\n")
    code, block = golden[:begin], golden[begin:]
    body = block.split(b"\r\n")[1:-2]
    der = base64.b64decode(b"".join(line[2:] for line in body)) if begin >= 0 else b""
    expect(
        "the signed fixture's block is not a PKCS #7 SignedData blob",
        der[:1] == b"\x30" and b"\x06\x09\x2a\x86\x48\x86\xf7\x0d\x01\x07\x02" in der[:32],
    )
    # What pwsh 7.6.6 and PowerShell 5.1 both report for each shape of the fixture: True where they
    # read a signature (Valid, or HashMismatch once the content changed), False for NotSigned.
    windows_reads = {
        "no change": (golden, True),
        "a blank and a whitespace-only line after the block": (golden + b"\r\n   \r\n\t\r\n", False),
        "code after the block": (golden + b"Write-Output 1\r\n", False),
        "a 0x1C line after the block": (golden + b"\x1c\r\n", False),
        "a form-feed line after the block": (golden + b"\x0c\r\n", False),
        "no CRLF after the End line": (golden[:-2], False),
        "spaces after the End line": (golden[:-2] + b"  \r\n", False),
        "LF-only line endings": (golden.replace(b"\r\n", b"\n"), False),
        "a block with LF-only line endings": (code + block.replace(b"\r\n", b"\n"), False),
        "a second Begin line in the block": (
            code + block.replace(b"\r\n# ", b"\r\n# SIG # Begin signature block\r\n# ", 1),
            False,
        ),
        "a UTF-8 BOM": (b"\xef\xbb\xbf" + golden, True),
        "UTF-16": (b"\xff\xfe" + golden.decode("utf-8-sig").encode("utf-16-le"), True),
        "changed code": (code.replace(b"Write-Output", b"Write-Output ", 1) + block, True),
        "a stale copy of the block before the code": (block + code + block, True),
    }
    for kind, (blob, windows) in windows_reads.items():
        expect(
            f"the signed fixture with {kind} read differently from Windows",
            script_is_signed(blob) == windows,
        )

    # A Microsoft-signed script, on Windows: a signer and a block this repo did not make.
    modules = pathlib.Path(r"C:\Program Files\WindowsPowerShell\Modules")
    sample = None
    if os.name == "nt" and modules.is_dir():
        for candidate in sorted(modules.rglob("*")):
            if candidate.suffix.lower() in (".ps1", ".psm1", ".psd1") and candidate.is_file():
                blob = candidate.read_bytes()
                if b"# SIG # Begin signature block" in blob:
                    sample = (candidate, blob)
                    break
    if sample is None:
        print(f"self-test: skipped the Microsoft-signed script case (not Windows, or none under {modules})")
    else:
        expect(f"the Microsoft-signed {sample[0]} read as unsigned", script_is_signed(sample[1]))
    expect(
        "the Darling Viewer's Portable.zip was taken for the Darling zip",
        not is_darling_zip("PerformanceMonitorDarlingViewer-darlingviewer-Portable.zip"),
    )
    expect("Darling-test.zip was not taken for a Darling zip", is_darling_zip("Darling-test.zip"))
    expect(
        "the release-mode census passed a Darling zip with no scripts",
        darling_census(DARLING_ZIP, ["PerformanceMonitor.Darling.Service.exe"]) is not None,
    )
    expect(
        "the release-mode census failed a Darling zip with its three scripts",
        darling_census(DARLING_ZIP, ["x.exe", *DARLING_SCRIPTS]) is None,
    )

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

    # The published-release mode and the release guard have to agree, or the audit is red on
    # every correct release and stops being read.
    def as_findings(layout: dict[str, object]) -> Report:
        out = Report(tag="synthetic")
        for asset, content in layout.items():
            if isinstance(content, dict):
                for member, signed in content.items():
                    out.findings.append(Finding(asset, member, 1024, bool(signed)))
            else:
                out.findings.append(Finding(asset, "", 1024, bool(content)))
        return out

    fixed = as_findings(correct_release())
    expect(
        f"the published-release mode reports {len(fixed.unsigned)} unsigned on a correct release",
        not fixed.unsigned,
    )
    expect(
        f"the published-release mode reports {len(fixed.allowed)} allowances on a correct release",
        len(fixed.allowed) == 2,
    )
    regressed = as_findings({**correct_release(), setup: False})
    expect(
        "the published-release mode passed an unsigned Setup.exe",
        [f.label for f in regressed.unsigned] == [setup],
    )

    # ------------------------------------------------------------------------------------------
    # The published-release mode, end to end: main() over real archives served as file:// URLs,
    # which curl reads with the same ranged requests it makes against GitHub. Only the asset
    # listing is replaced, because that is the one call that needs a published release.
    # ------------------------------------------------------------------------------------------
    def by_tag(label: str, layout: dict[str, object], want: int, **extra: str) -> str:
        """Run main() in release mode over `layout` and return its output.

        `extra` maps an asset name to a URL that cannot be read.
        """
        nonlocal checks
        checks += 1
        module = sys.modules[__name__]
        listing = module.release_assets
        with tempfile.TemporaryDirectory() as tmp:
            directory = _release_dir(os.path.join(tmp, "releases"), layout)
            assets = [
                {
                    "name": name,
                    "browser_download_url": pathlib.Path(directory, name).resolve().as_uri(),
                    "size": os.path.getsize(os.path.join(directory, name)),
                }
                for name in sorted(layout)
            ]
            for name, url in extra.items():
                assets.append({"name": name, "browser_download_url": url, "size": 4096})
            sink = io.StringIO()
            module.release_assets = lambda tag, repo, gh: assets
            try:
                with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                    got = main(["vSELFTEST"])
            finally:
                module.release_assets = listing
        if got != want:
            failures.append(f"release mode, {label}: main returned {got}, expected {want}")
        return sink.getvalue()

    release = {**correct_release(), DARLING_ZIP: darling_zip()}
    by_tag("a correct release", release, 0)
    by_tag("an unsigned Setup.exe", {**release, setup: False}, 1)
    by_tag("an unsigned script in the Darling zip", {**release, DARLING_ZIP: darling_zip(upgrade=False)}, 1)
    by_tag("an unsigned standalone script", {**release, "install-darling.ps1": False}, 1)
    by_tag("a Darling zip missing one script", {**release, DARLING_ZIP: missing_one}, 2)
    by_tag("a Darling zip with its scripts only under a subfolder", {**release, DARLING_ZIP: viewer_only}, 2)
    by_tag("a release with no Darling zip", correct_release(), 2)
    by_tag(
        "a release whose Darling zip was renamed",
        {**correct_release(), "PerformanceMonitorService-3.10.0.zip": darling_zip()},
        2,
    )
    by_tag(
        "a Darling zip with a member in an unsupported compression method",
        {**release, DARLING_ZIP: darling_zip(upgrade="deflate64")},
        2,
    )
    out = by_tag(
        "a Darling zip with a corrupt deflate stream",
        {**release, DARLING_ZIP: darling_zip(upgrade="corrupt")},
        2,
    )
    # The unreadable member is the error, not the whole archive: the members beside it are still
    # checked and listed.
    expect(
        "release mode dropped the readable members of a zip with one corrupt member",
        f"{DARLING_ZIP} :: install-darling.ps1" in out and f"{DARLING_ZIP} :: upgrade-darling.ps1" in out,
    )
    by_tag("a zip64 Darling zip", {**release, DARLING_ZIP: {**darling_zip(), ZIP64: True}}, 2)
    # The last entry is an unsigned exe: read, it fails the release (1); left out of a walk that
    # ended early, the release would pass. A walk that ends early is a read error instead (#5796).
    by_tag("a Darling zip whose last member is unsigned", {**release, DARLING_ZIP: late_unsigned}, 1)
    by_tag(
        "a Darling zip whose central directory is short",
        {**release, DARLING_ZIP: {**late_unsigned, CD_SHORT: True}},
        2,
    )
    by_tag(
        "a Darling zip whose last central-directory entry has a bad signature",
        {**release, DARLING_ZIP: {**late_unsigned, CD_BAD_SIG: True}},
        2,
    )
    by_tag(
        "an asset whose ranged read fails, beside a correct release",
        release,
        2,
        **{"Extra-Portable.zip": pathlib.Path(tempfile.gettempdir(), "no-such-5796.zip").as_uri()},
    )
    for kind in FORGED_SCRIPTS:
        by_tag(f"a Darling zip whose script is {kind}", {**release, DARLING_ZIP: darling_zip(install=kind)}, 1)

    for f in failures:
        print(f"SELF-TEST FAIL: {f}", file=sys.stderr)
    if failures:
        return 1
    print(f"self-test: {checks} assertions passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tag", nargs="?", help="release tag, e.g. v3.7.0")
    parser.add_argument("--self-test", action="store_true", dest="self_test")
    parser.add_argument(
        "--verify-dir",
        metavar="DIR",
        help=(
            "release guard: every .exe and .ps1 under DIR, including inside zips/nupkgs, must be "
            "signed, and the Darling zip must hold its three scripts"
        ),
    )
    parser.add_argument("--repo", default="erikdarlingdata/PerformanceMonitor")
    parser.add_argument("--gh", default="gh", help="gh executable (use the identity wrapper)")
    parser.add_argument(
        "--include-dlls",
        action="store_true",
        help="also check .dll members inside archives (slower; the shipped defect was .exe only)",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()
    if args.verify_dir:
        return verify_local_dir(args.verify_dir)
    if not args.tag:
        parser.error("a release tag is required unless --self-test or --verify-dir is given")

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
                        {
                            "asset": f.asset,
                            "path": f.path,
                            "size": f.size,
                            "signed": f.signed,
                            "allowed_unsigned": not f.signed
                            and allowance_for(f.asset, f.path) is not None,
                        }
                        for f in report.findings
                    ],
                    "unsigned_count": len(report.unsigned),
                    "allowed_unsigned_count": len(report.allowed),
                    "errors": report.errors,
                    "census_failures": report.census,
                },
                indent=2,
            )
        )
    else:
        scripts = sum(1 for f in report.findings if is_script(f.path or f.asset))
        print(
            f"{report.tag}: {len(report.findings) - scripts} executable(s) and "
            f"{scripts} script(s) checked\n"
        )
        width = max((len(f.label) for f in report.findings), default=0)
        allowed_labels = {f.label for f, _ in report.allowed}
        for f in sorted(report.findings, key=lambda x: (x.signed, x.label)):
            if f.signed:
                mark = "signed  "
            elif f.label in allowed_labels:
                mark = "allowed "
            else:
                mark = "UNSIGNED"
            print(f"  {mark}  {f.label:<{width}}  {f.size:>10,} bytes")
        for f, allowance in report.allowed:
            print(f"\nallowed unsigned: {f.label}\n  {allowance.reason}")
        for e in report.errors:
            print(f"\n  ERROR  {e}", file=sys.stderr)

    if report.errors and not report.findings:
        return 2
    if not report.findings:
        print("\nFATAL: no executables or scripts found — a release with nothing to check is a broken run.",
              file=sys.stderr)
        return 2
    if report.unsigned:
        print(
            f"\n{len(report.unsigned)} unsigned executable(s) or script(s). See #3288 and #5796.",
            file=sys.stderr,
        )
    for failure in report.census:
        print(f"\nCENSUS FAIL: {failure}", file=sys.stderr)
    # A file that could not be read was not checked, so an error fails the run whatever else was
    # found: three readable signed scripts and one unreadable one is not a clean release (#5796).
    if report.errors:
        print(f"\n{len(report.errors)} asset(s) or member(s) could not be read.", file=sys.stderr)
    if report.census or report.errors:
        return 2
    return 1 if report.unsigned else 0


if __name__ == "__main__":
    sys.exit(main())
