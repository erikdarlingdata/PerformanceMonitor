#!/usr/bin/env python3
"""Stage and write back the executables `vpk pack` generates, signed after packing (#3288).

WHY THIS RUNS AFTER PACKING RATHER THAN INSIDE IT

  Velopack generates three executables during `vpk pack` -- the portable launcher stub,
  `Update.exe` (`Squirrel.exe` under its internal name) and `Setup.exe`. They do not exist when
  the pre-pack signing rounds run, so those rounds cannot reach them.

  Signing them from inside packing is not available either. SignPath's open-source signing
  policy requires origin verification through a trusted build system: a request is accepted only
  through `signpath/github-action-submit-signing-request`, which attests repository, branch,
  commit and build-job URL. That is a workflow step, and a workflow step cannot be invoked from
  inside a running `vpk pack`. A raw API-token submission carries no attestation and the policy
  answers 403.

  So the two standalone assets are signed by an Action step after packing. `Setup.exe` and
  `Portable.zip` carry no recorded hash: `releases.<channel>.json` records a SHA256 and a Size
  for the two `.nupkg` files and nothing else, so rewriting the standalone assets invalidates
  nothing. The copies of the stub and `Squirrel.exe` inside the `.nupkg` stay unsigned, are
  allowlisted by name AND container in verify_release_signatures.py, and are the only two
  executables in a release that are.

WHAT IT SIGNS, AND HOW IT FINDS THEM

  Per product directory: the single `*-Setup.exe`, and every `.exe` at the ROOT of the single
  `*Portable.zip`. The root entries are the deployed launcher and `Update.exe`; the signed app
  payload sits one level down in `current/` and is not resubmitted.

  The root rule is positional, not by name, because the launcher is named after the Velopack
  pack id rather than the application's own executable: the Darling Viewer portable zip's root
  entry is `PerformanceMonitorDarlingViewer.exe` while the application is
  `PerformanceMonitor.Darling.Viewer.exe` and lives at `current/`.

THE PORTABLE ZIP IS A SHIPPED ARTIFACT

  So `apply` rebuilds it member by member rather than re-creating it. Every member that is not
  replaced keeps its local header, name, extra field and raw compressed bytes verbatim; the
  central directory keeps every field except the local-header offsets, which shift. The two
  replaced members are re-deflated with the original compression method. `apply` then reopens
  the result and asserts that property before the rebuilt zip replaces the original: identical
  entry names in identical order, identical per-entry metadata, byte-identical raw members
  except the replaced ones, and identical decompressed content except the replaced ones.

USAGE

  postpack_signing.py collect --stage DIR --manifest FILE \
      --product lite=releases/velopack-lite \
      --product darlingviewer=releases/velopack-darlingviewer

  postpack_signing.py apply --signed DIR --manifest FILE

  postpack_signing.py --self-test

  Exit status is 0 on success and 1 on any failure. There is no partial success: `apply` writes
  a rebuilt zip over the original only after the rebuild has been verified.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import shutil
import struct
import sys
import zipfile
import zlib
from dataclasses import dataclass, asdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import verify_release_signatures as vrs  # noqa: E402  the one PE parse in the repo

MANIFEST_VERSION = 1

_LFH = b"PK\x03\x04"
_CDH = b"PK\x01\x02"
_EOCD = b"PK\x05\x06"
_LFH_LEN = 30
# Bit 3 puts the sizes in a trailing data descriptor instead of the local header, which makes a
# member's raw extent unknowable from the central directory alone.
_FLAG_DATA_DESCRIPTOR = 0x0008
_U32_MAX = 0xFFFFFFFF
_U16_MAX = 0xFFFF

# The fields a rebuilt zip has to reproduce exactly. `compress_size`, `CRC` and `file_size` are
# compared separately because the replaced members legitimately change all three.
_PRESERVED_FIELDS = (
    "compress_type",
    "date_time",
    "external_attr",
    "internal_attr",
    "create_system",
    "create_version",
    "extract_version",
    "flag_bits",
    "comment",
    "extra",
)


class PostPackError(RuntimeError):
    """A precondition, a postcondition or an input shape is wrong."""


@dataclass
class Member:
    """One zip member as the central directory describes it, plus two digests.

    `content_sha` covers the DECOMPRESSED bytes; `raw_sha` covers the local header, the name,
    the extra field and the raw compressed bytes. The first says the content survived, the
    second says the bytes did.
    """

    name: str
    compress_type: int
    date_time: tuple
    external_attr: int
    internal_attr: int
    create_system: int
    create_version: int
    extract_version: int
    flag_bits: int
    comment: bytes
    extra: bytes
    crc: int
    file_size: int
    compress_size: int
    content_sha: str
    raw_sha: str


def _dos_datetime(date_time: tuple) -> bytes:
    year, month, day, hour, minute, second = date_time
    dosdate = (year - 1980) << 9 | month << 5 | day
    dostime = hour << 11 | minute << 5 | (second // 2)
    return struct.pack("<HH", dostime, dosdate)


def _read_eocd(handle, size: int) -> tuple[int, int, int, bytes]:
    """(total_entries, cd_size, cd_offset, archive_comment)."""
    window = min(66 * 1024, size)
    handle.seek(size - window)
    tail = handle.read(window)
    i = tail.rfind(_EOCD)
    if i < 0:
        raise PostPackError("no end-of-central-directory record")
    disk, cd_disk, here, total, cd_size, cd_offset, clen = struct.unpack_from("<HHHHIIH", tail, i + 4)
    if disk or cd_disk:
        raise PostPackError(f"multi-disk archive (disk {disk}, cd on {cd_disk})")
    if here != total:
        raise PostPackError(f"split central directory ({here} of {total} entries on this disk)")
    if total == _U16_MAX or cd_size == _U32_MAX or cd_offset == _U32_MAX:
        raise PostPackError("zip64 archive: this rebuilder writes only the 32-bit format")
    return total, cd_size, cd_offset, tail[i + 22 : i + 22 + clen]


def snapshot(path: str) -> list[Member]:
    """Describe every member of `path`, in central-directory order."""
    out: list[Member] = []
    with open(path, "rb") as raw, zipfile.ZipFile(path) as zf:
        size = os.path.getsize(path)
        total, _, _, _ = _read_eocd(raw, size)
        infos = zf.infolist()
        if len(infos) != total:
            raise PostPackError(f"{path}: eocd says {total} entries, central directory has {len(infos)}")
        for info in infos:
            if info.flag_bits & _FLAG_DATA_DESCRIPTOR:
                raise PostPackError(f"{path} :: {info.filename}: data descriptor, raw extent unknown")
            raw.seek(info.header_offset)
            head = raw.read(_LFH_LEN)
            if head[:4] != _LFH:
                raise PostPackError(f"{path} :: {info.filename}: no local file header at {info.header_offset}")
            nlen, elen = struct.unpack_from("<HH", head, 26)
            tail = raw.read(nlen + elen)
            body = raw.read(info.compress_size)
            if len(body) != info.compress_size:
                raise PostPackError(f"{path} :: {info.filename}: short raw read")
            out.append(
                Member(
                    name=info.filename,
                    compress_type=info.compress_type,
                    date_time=tuple(info.date_time),
                    external_attr=info.external_attr,
                    internal_attr=info.internal_attr,
                    create_system=info.create_system,
                    create_version=info.create_version,
                    extract_version=info.extract_version,
                    flag_bits=info.flag_bits,
                    comment=info.comment,
                    extra=info.extra,
                    crc=info.CRC,
                    file_size=info.file_size,
                    compress_size=info.compress_size,
                    content_sha=hashlib.sha256(zf.read(info)).hexdigest(),
                    raw_sha=hashlib.sha256(head + tail + body).hexdigest(),
                )
            )
    return out


def rebuild(source: str, replacements: dict[str, bytes], destination: str) -> None:
    """Write `source` to `destination` with `replacements` substituted by member name."""
    with open(source, "rb") as raw, zipfile.ZipFile(source) as zf:
        infos = zf.infolist()
    size = os.path.getsize(source)
    with open(source, "rb") as raw:
        total, _, _, archive_comment = _read_eocd(raw, size)
        if len(infos) != total:
            raise PostPackError(f"{source}: eocd says {total} entries, central directory has {len(infos)}")
        unknown = sorted(set(replacements) - {i.filename for i in infos})
        if unknown:
            raise PostPackError(f"{source}: no such member(s) to replace: {unknown}")

        central = bytearray()
        with open(destination, "wb") as out:
            for info in infos:
                if info.flag_bits & _FLAG_DATA_DESCRIPTOR:
                    raise PostPackError(f"{source} :: {info.filename}: data descriptor, raw extent unknown")
                raw.seek(info.header_offset)
                head = raw.read(_LFH_LEN)
                if head[:4] != _LFH:
                    raise PostPackError(f"{source} :: {info.filename}: no local file header")
                nlen, elen = struct.unpack_from("<HH", head, 26)
                name_and_extra = raw.read(nlen + elen)
                if len(name_and_extra) != nlen + elen:
                    raise PostPackError(f"{source} :: {info.filename}: short header read")
                if head[10:14] != _dos_datetime(info.date_time):
                    raise PostPackError(
                        f"{source} :: {info.filename}: local and central timestamps disagree"
                    )
                offset = out.tell()

                if info.filename in replacements:
                    blob = replacements[info.filename]
                    if info.compress_type == zipfile.ZIP_STORED:
                        body = blob
                    elif info.compress_type == zipfile.ZIP_DEFLATED:
                        engine = zlib.compressobj(9, zlib.DEFLATED, -15)
                        body = engine.compress(blob) + engine.flush()
                    else:
                        raise PostPackError(
                            f"{source} :: {info.filename}: compression method "
                            f"{info.compress_type} cannot be rewritten"
                        )
                    crc = zlib.crc32(blob) & _U32_MAX
                    csize, usize = len(body), len(blob)
                    head = bytearray(head)
                    struct.pack_into("<III", head, 14, crc, csize, usize)
                    head = bytes(head)
                else:
                    body = raw.read(info.compress_size)
                    if len(body) != info.compress_size:
                        raise PostPackError(f"{source} :: {info.filename}: short raw read")
                    crc, csize, usize = info.CRC, info.compress_size, info.file_size

                if csize > _U32_MAX or usize > _U32_MAX:
                    raise PostPackError(f"{source} :: {info.filename}: member needs zip64")
                out.write(head)
                out.write(name_and_extra)
                out.write(body)

                central += _CDH
                central += struct.pack(
                    "<HHHH",
                    info.create_version | (info.create_system << 8),
                    info.extract_version,
                    info.flag_bits,
                    info.compress_type,
                )
                central += _dos_datetime(info.date_time)
                central += struct.pack("<III", crc, csize, usize)
                central += struct.pack(
                    "<HHHHHII",
                    nlen,
                    len(info.extra),
                    len(info.comment),
                    0,
                    info.internal_attr,
                    info.external_attr,
                    offset,
                )
                central += name_and_extra[:nlen]
                central += info.extra
                central += info.comment

            cd_offset = out.tell()
            if cd_offset > _U32_MAX or len(central) > _U32_MAX or len(infos) > _U16_MAX:
                raise PostPackError(f"{source}: rebuilt archive needs zip64")
            out.write(central)
            out.write(
                _EOCD
                + struct.pack(
                    "<HHHHIIH",
                    0,
                    0,
                    len(infos),
                    len(infos),
                    len(central),
                    cd_offset,
                    len(archive_comment),
                )
                + archive_comment
            )


def assert_faithful(before: list[Member], rebuilt_path: str, replacements: dict[str, bytes]) -> None:
    """Fail unless `rebuilt_path` differs from `before` in exactly the replaced members."""
    with zipfile.ZipFile(rebuilt_path) as zf:
        bad = zf.testzip()
    if bad is not None:
        raise PostPackError(f"{rebuilt_path}: member '{bad}' fails its CRC")

    after = snapshot(rebuilt_path)
    if [m.name for m in after] != [m.name for m in before]:
        b, a = [m.name for m in before], [m.name for m in after]
        raise PostPackError(
            f"{rebuilt_path}: entry list changed "
            f"({len(b)} -> {len(a)} entries; added {sorted(set(a) - set(b))[:5]}, "
            f"removed {sorted(set(b) - set(a))[:5]}, order differs: {b != a})"
        )

    for old, new in zip(before, after):
        for field in _PRESERVED_FIELDS:
            if getattr(new, field) != getattr(old, field):
                raise PostPackError(
                    f"{rebuilt_path} :: {old.name}: {field} changed "
                    f"{getattr(old, field)!r} -> {getattr(new, field)!r}"
                )
        if old.name in replacements:
            want = hashlib.sha256(replacements[old.name]).hexdigest()
            if new.content_sha != want:
                raise PostPackError(f"{rebuilt_path} :: {old.name}: content is not the signed file")
            if new.raw_sha == old.raw_sha:
                raise PostPackError(f"{rebuilt_path} :: {old.name}: unchanged, so it was not replaced")
        else:
            if new.raw_sha != old.raw_sha:
                raise PostPackError(f"{rebuilt_path} :: {old.name}: raw bytes changed")
            if (new.crc, new.file_size, new.compress_size, new.content_sha) != (
                old.crc,
                old.file_size,
                old.compress_size,
                old.content_sha,
            ):
                raise PostPackError(f"{rebuilt_path} :: {old.name}: content changed")


def is_signed(path: str) -> bool:
    """Whether `path` carries a certificate table. A file that is not a PE image is a refusal.

    Reading a non-PE as "unsigned" would submit it for signing and then report it as signed or
    not on the strength of bytes that are not a PE header at all.
    """
    try:
        with open(path, "rb") as handle:
            offset, length = vrs.certificate_table(handle.read(vrs.HEADER_BYTES))
    except (OSError, vrs.ReadError, struct.error) as exc:
        raise PostPackError(f"{path}: not a PE image ({exc})") from exc
    return offset != 0 and length != 0


def exactly_one(directory: str, pattern: str, what: str) -> str:
    hits = sorted(glob.glob(os.path.join(directory, pattern)))
    if len(hits) != 1:
        raise PostPackError(
            f"{directory}: expected exactly one {what} matching '{pattern}', found "
            f"{len(hits)}: {[os.path.basename(h) for h in hits]}"
        )
    return hits[0]


def portable_root_executables(archive: str) -> list[str]:
    """The `.exe` members at the archive root: the deployed launcher and `Update.exe`."""
    with zipfile.ZipFile(archive) as zf:
        names = [info.filename for info in zf.infolist()]
    roots = [
        name
        for name in names
        if name.lower().endswith(".exe") and "/" not in name and "\\" not in name
    ]
    if not roots:
        raise PostPackError(
            f"{archive}: no .exe at the archive root. The deployed launcher and Update.exe "
            f"live there; {len(names)} entries were read."
        )
    return roots


def collect(stage: str, manifest_path: str, products: list[tuple[str, str]]) -> int:
    if os.path.exists(stage):
        shutil.rmtree(stage)
    os.makedirs(stage, exist_ok=True)

    manifest: dict = {"version": MANIFEST_VERSION, "products": []}
    staged: dict[str, str] = {}
    already: list[str] = []

    for name, directory in products:
        if not os.path.isdir(directory):
            raise PostPackError(f"{directory}: not a directory")
        setup = exactly_one(directory, "*-Setup.exe", "installer")
        portable = exactly_one(directory, "*Portable.zip", "portable archive")

        entry: dict = {
            "name": name,
            "setup": {"path": setup, "stage": f"{name}-setup/{os.path.basename(setup)}"},
            "portable": {"path": portable, "members": []},
        }

        def stage_file(rel: str, writer) -> None:
            if rel in staged:
                raise PostPackError(f"two files stage to the same path '{rel}'")
            target = os.path.join(stage, rel)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            writer(target)
            if is_signed(target):
                already.append(rel)
            staged[rel] = target

        stage_file(entry["setup"]["stage"], lambda dst: shutil.copyfile(setup, dst))

        def write_blob(destination: str, blob: bytes) -> None:
            with open(destination, "wb") as handle:
                handle.write(blob)

        with zipfile.ZipFile(portable) as zf:
            for member in portable_root_executables(portable):
                rel = f"{name}-portable/{member}"
                blob = zf.read(member)
                stage_file(rel, lambda dst, blob=blob: write_blob(dst, blob))
                entry["portable"]["members"].append({"member": member, "stage": rel})

        manifest["products"].append(entry)
        print(f"  {name}: {os.path.basename(setup)}")
        for member in entry["portable"]["members"]:
            print(f"  {name}: {os.path.basename(portable)} :: {member['member']}")

    if not staged:
        raise PostPackError("nothing was staged. An empty signing request signs nothing.")

    os.makedirs(os.path.dirname(os.path.abspath(manifest_path)), exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")

    print(f"\ncollect: {len(staged)} file(s) staged under {stage}")
    if already:
        print(f"collect: {len(already)} already carry a signature: {sorted(already)}")
    return 0


def apply(signed_dir: str, manifest_path: str) -> int:
    with open(manifest_path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    if manifest.get("version") != MANIFEST_VERSION:
        raise PostPackError(f"{manifest_path}: manifest version {manifest.get('version')!r} is not {MANIFEST_VERSION}")

    def signed_blob(rel: str) -> bytes:
        path = os.path.join(signed_dir, rel)
        if not os.path.isfile(path):
            raise PostPackError(f"the signing response has no '{rel}' under {signed_dir}")
        if not is_signed(path):
            raise PostPackError(f"{rel} came back from signing without a certificate table")
        with open(path, "rb") as handle:
            return handle.read()

    # Every file in the response is read and checked before any artifact is written, so a bad
    # response leaves the packed output exactly as `vpk pack` left it. Writing first and failing
    # afterwards would still fail the release, but it would leave an unsigned installer on disk
    # under a name the next step would otherwise have uploaded.
    blobs: dict[str, bytes] = {}
    for product in manifest["products"]:
        blobs[product["setup"]["stage"]] = signed_blob(product["setup"]["stage"])
        for member in product["portable"]["members"]:
            blobs[member["stage"]] = signed_blob(member["stage"])

    written = 0
    for product in manifest["products"]:
        setup = product["setup"]
        blob = blobs[setup["stage"]]
        with open(setup["path"], "wb") as handle:
            handle.write(blob)
        if not is_signed(setup["path"]):
            raise PostPackError(f"{setup['path']}: unsigned after the write-back")
        written += 1
        print(f"  {product['name']}: {os.path.basename(setup['path'])} signed")

        archive = product["portable"]["path"]
        replacements = {m["member"]: blobs[m["stage"]] for m in product["portable"]["members"]}
        if not replacements:
            raise PostPackError(f"{archive}: no members to replace")
        before = snapshot(archive)
        rebuilt = archive + ".rebuilt"
        try:
            rebuild(archive, replacements, rebuilt)
            assert_faithful(before, rebuilt, replacements)
            os.replace(rebuilt, archive)
        finally:
            if os.path.exists(rebuilt):
                os.remove(rebuilt)
        with zipfile.ZipFile(archive) as zf:
            for member in replacements:
                head = zf.read(member)[: vrs.HEADER_BYTES]
                offset, length = vrs.certificate_table(head)
                if offset == 0 or length == 0:
                    raise PostPackError(f"{archive} :: {member}: unsigned after the rebuild")
        written += len(replacements)
        print(
            f"  {product['name']}: {os.path.basename(archive)} rebuilt with "
            f"{len(replacements)} signed member(s), {len(before)} entries preserved"
        )

    if not written:
        raise PostPackError("the manifest named no files. An empty write-back signs nothing.")
    print(f"\napply: {written} executable(s) written back and verified")
    return 0


# ---------------------------------------------------------------------------------------------
# Self-test. It builds portable archives and installers whose shape is the measured shape of the
# real v3.7.0 assets: no directory entries, no extra fields, no comments, UTF-8 name flag on
# every entry, one stored `.portable` marker, and the launcher at the root under the pack id
# while the application payload sits at `current/`.
# ---------------------------------------------------------------------------------------------


def _portable_zip(path: str, root_exe: str, app_exe: str, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        marker = zipfile.ZipInfo(".portable", date_time=(2026, 9, 12, 10, 0, 0))
        marker.compress_type = zipfile.ZIP_STORED
        marker.flag_bits |= 0x800
        zf.writestr(marker, b"")
        ordered = [(root_exe, vrs._synth_pe(signed=False)), ("Update.exe", vrs._synth_pe(signed=False))]
        ordered += [(f"current/{app_exe}", vrs._synth_pe(signed=True))]
        ordered += sorted(members.items())
        for name, blob in ordered:
            info = zipfile.ZipInfo(name, date_time=(2026, 9, 12, 10, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.flag_bits |= 0x800
            zf.writestr(info, blob)


def _product_dir(root: str, name: str, pack_id: str, app_exe: str) -> str:
    directory = os.path.join(root, f"velopack-{name}")
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, f"{pack_id}-{name}-Setup.exe"), "wb") as handle:
        handle.write(vrs._synth_pe(signed=False))
    _portable_zip(
        os.path.join(directory, f"{pack_id}-{name}-Portable.zip"),
        f"{pack_id}.exe",
        app_exe,
        {"current/Some.Library.dll": b"x" * 300, "current/sq.version": b"3.7.2"},
    )
    return directory


def _divergent_local_header(path: str) -> None:
    """Set the UTF-8 name flag in one member's LOCAL header only, leaving the central one clear.

    Two bytes, no size or offset change. It gives a zip whose per-member metadata, CRC, sizes and
    content are all readable from the central directory and all unchanged, while its raw bytes
    differ from anything a rewriter that reconstructs local headers from the central directory
    would produce. That is the only thing the raw-bytes comparison decides on its own.
    """
    with zipfile.ZipFile(path) as zf:
        target = zf.infolist()[1]
    with open(path, "r+b") as handle:
        handle.seek(target.header_offset + 6)
        flags = struct.unpack("<H", handle.read(2))[0]
        handle.seek(target.header_offset + 6)
        handle.write(struct.pack("<H", flags | 0x800))


def _sign_stage(stage: str, suffix: str = "-signed", only: str = "") -> str:
    """Return a directory that mirrors `stage` with a certificate table appended to each file.

    A SignPath response is the same container with the same entry paths, signed. `--verify-dir`
    and this script both read "is the certificate table non-empty", so appending a table is the
    right stand-in: it is exactly the state the guard distinguishes.

    `only` restricts the appending to staged paths containing that substring, which produces the
    response shape that matters most -- some files signed, some returned as submitted.
    """
    signed = stage + suffix
    if os.path.exists(signed):
        shutil.rmtree(signed)
    for base, _, files in os.walk(stage):
        for name in files:
            source = os.path.join(base, name)
            rel = os.path.relpath(source, stage)
            target = os.path.join(signed, rel)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(source, "rb") as handle:
                blob = bytearray(handle.read())
            if only and only not in rel.replace(os.sep, "/"):
                with open(target, "wb") as handle:
                    handle.write(bytes(blob))
                continue
            table = b"\x00" * 0x1C0
            offset = len(blob)
            pe_off = struct.unpack_from("<I", blob, 0x3C)[0]
            magic = struct.unpack_from("<H", blob, pe_off + 24)[0]
            directories = pe_off + 24 + (112 if magic == 0x20B else 96)
            struct.pack_into("<II", blob, directories + 4 * 8, offset, len(table))
            with open(target, "wb") as handle:
                handle.write(bytes(blob) + table)
    return signed


def _rebuilt_from_central(source: str, root: str) -> str:
    """Rewrite `source` with local headers reconstructed from the central directory.

    What a `zipfile`-based round trip produces: every field the central directory carries is
    preserved, and any local-header field that disagrees with it is silently normalised.
    """
    destination = os.path.join(root, "from-central.zip")
    with zipfile.ZipFile(source) as src, zipfile.ZipFile(destination, "w") as dst:
        for info in src.infolist():
            fresh = zipfile.ZipInfo(info.filename, date_time=info.date_time)
            fresh.compress_type = info.compress_type
            fresh.flag_bits = info.flag_bits
            fresh.external_attr = info.external_attr
            fresh.internal_attr = info.internal_attr
            fresh.create_system = info.create_system
            fresh.create_version = info.create_version
            fresh.extract_version = info.extract_version
            fresh.comment = info.comment
            fresh.extra = info.extra
            dst.writestr(fresh, src.read(info))
    return destination


def self_test() -> int:
    import tempfile

    failures: list[str] = []
    checks = 0

    def expect_raises(label: str, fn) -> None:
        nonlocal checks
        checks += 1
        try:
            fn()
        except PostPackError:
            return
        except Exception as exc:  # noqa: BLE001  any other exception is still a wrong failure
            failures.append(f"{label}: raised {type(exc).__name__} instead of PostPackError: {exc}")
            return
        failures.append(f"{label}: succeeded instead of refusing")

    def expect_ok(label: str, fn):
        nonlocal checks
        checks += 1
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{label}: refused a correct input ({type(exc).__name__}: {exc})")
            return None

    def expect(label: str, condition: bool) -> None:
        nonlocal checks
        checks += 1
        if not condition:
            failures.append(label)

    with tempfile.TemporaryDirectory() as root:
        lite = _product_dir(root, "lite", "PerformanceMonitorLite", "PerformanceMonitorLite.exe")
        # The Darling Viewer case: the root launcher carries the PACK ID, and the application's
        # own executable name appears only under current/. A name-based rule misses the launcher
        # and submits the already-signed payload instead.
        viewer = _product_dir(
            root,
            "darlingviewer",
            "PerformanceMonitorDarlingViewer",
            "PerformanceMonitor.Darling.Viewer.exe",
        )

        stage = os.path.join(root, "stage")
        manifest = os.path.join(root, "manifest.json")
        expect_ok(
            "collect refused a correct pair of product directories",
            lambda: collect(stage, manifest, [("lite", lite), ("darlingviewer", viewer)]),
        )
        with open(manifest, encoding="utf-8") as handle:
            recorded = json.load(handle)
        rels = sorted(
            [p["setup"]["stage"] for p in recorded["products"]]
            + [m["stage"] for p in recorded["products"] for m in p["portable"]["members"]]
        )
        expect(
            f"collect staged {rels} rather than the six expected files",
            rels
            == [
                "darlingviewer-portable/PerformanceMonitorDarlingViewer.exe",
                "darlingviewer-portable/Update.exe",
                "darlingviewer-setup/PerformanceMonitorDarlingViewer-darlingviewer-Setup.exe",
                "lite-portable/PerformanceMonitorLite.exe",
                "lite-portable/Update.exe",
                "lite-setup/PerformanceMonitorLite-lite-Setup.exe",
            ],
        )
        expect(
            "collect staged a member from current/, which is signed pre-pack",
            not any("current" in rel for rel in rels),
        )

        # Two installers, or none, means the glob is matching something other than what packing
        # produced -- silently signing the wrong file, or the wrong number of them.
        two = os.path.join(root, "two-setups")
        os.makedirs(two, exist_ok=True)
        shutil.copytree(lite, two, dirs_exist_ok=True)
        shutil.copyfile(
            os.path.join(lite, "PerformanceMonitorLite-lite-Setup.exe"),
            os.path.join(two, "PerformanceMonitorLite-previous-Setup.exe"),
        )
        expect_raises(
            "collect accepted two *-Setup.exe in one product directory",
            lambda: collect(os.path.join(root, "s2"), os.path.join(root, "m2.json"), [("lite", two)]),
        )

        none = os.path.join(root, "no-portable")
        os.makedirs(none, exist_ok=True)
        shutil.copyfile(
            os.path.join(lite, "PerformanceMonitorLite-lite-Setup.exe"),
            os.path.join(none, "PerformanceMonitorLite-lite-Setup.exe"),
        )
        expect_raises(
            "collect accepted a product directory with no portable archive",
            lambda: collect(os.path.join(root, "s3"), os.path.join(root, "m3.json"), [("lite", none)]),
        )

        notpe = os.path.join(root, "not-pe")
        os.makedirs(notpe, exist_ok=True)
        with open(os.path.join(notpe, "Thing-lite-Setup.exe"), "wb") as handle:
            handle.write(b"this is not a PE image")
        _portable_zip(
            os.path.join(notpe, "Thing-lite-Portable.zip"), "Thing.exe", "Thing.exe", {}
        )
        expect_raises(
            "collect accepted an installer that is not a PE image",
            lambda: collect(os.path.join(root, "s4"), os.path.join(root, "m4.json"), [("lite", notpe)]),
        )

        # apply refuses a response that is missing a file, and one that comes back unsigned.
        expect_raises(
            "apply accepted a signing response with nothing in it",
            lambda: apply(os.path.join(root, "empty-response"), manifest),
        )
        def artifact_digests() -> dict[str, str]:
            out = {}
            for directory in (lite, viewer):
                for name in sorted(os.listdir(directory)):
                    with open(os.path.join(directory, name), "rb") as handle:
                        out[f"{directory}/{name}"] = hashlib.sha256(handle.read()).hexdigest()
            return out

        untouched = artifact_digests()
        expect_raises(
            "apply accepted files that came back without a certificate table",
            lambda: apply(stage, manifest),
        )
        expect(
            "apply modified a packed artifact before refusing the signing response",
            artifact_digests() == untouched,
        )

        # The response shape that separates "refuses" from "refuses without writing": the
        # installers come back signed and the portable members come back as submitted. A run that
        # checks each file as it writes it leaves a signed installer beside an unbuilt zip.
        partial = _sign_stage(stage, suffix="-partial", only="-setup/")
        untouched = artifact_digests()
        expect_raises(
            "apply accepted a response in which only some files came back signed",
            lambda: apply(partial, manifest),
        )
        expect(
            "apply wrote the signed installers before refusing a partially signed response",
            artifact_digests() == untouched,
        )

        signed = _sign_stage(stage)
        before_lite = snapshot(os.path.join(lite, "PerformanceMonitorLite-lite-Portable.zip"))
        expect_ok("apply refused a correct signing response", lambda: apply(signed, manifest))

        after_lite = snapshot(os.path.join(lite, "PerformanceMonitorLite-lite-Portable.zip"))
        expect(
            f"the rebuilt portable zip has {len(after_lite)} entries, not {len(before_lite)}",
            len(after_lite) == len(before_lite),
        )
        expect(
            "the rebuilt portable zip reordered or renamed its entries",
            [m.name for m in after_lite] == [m.name for m in before_lite],
        )
        replaced = {"PerformanceMonitorLite.exe", "Update.exe"}
        unchanged_raw_held = all(
            new.raw_sha == old.raw_sha
            for old, new in zip(before_lite, after_lite)
            if old.name not in replaced
        )
        expect("a member that was not replaced changed its raw bytes", unchanged_raw_held)
        changed = {
            new.name for old, new in zip(before_lite, after_lite) if new.raw_sha != old.raw_sha
        }
        expect(f"the rebuild changed {sorted(changed)} rather than the two root executables", changed == replaced)
        with zipfile.ZipFile(os.path.join(lite, "PerformanceMonitorLite-lite-Portable.zip")) as zf:
            signed_now = {
                name: vrs.certificate_table(zf.read(name)[: vrs.HEADER_BYTES]) != (0, 0)
                for name in replaced
            }
        expect(f"the rebuilt root executables are not signed: {signed_now}", all(signed_now.values()))
        expect(
            "the installer is unsigned after apply",
            is_signed(os.path.join(lite, "PerformanceMonitorLite-lite-Setup.exe")),
        )

        # The faithfulness check has to catch a rebuild that loses structure rather than content.
        # Compress-Archive and a plain zipfile round-trip both drop the stored `.portable` marker's
        # method and reorder entries, which is the failure this guards.
        source = os.path.join(viewer, "PerformanceMonitorDarlingViewer-darlingviewer-Portable.zip")
        before_viewer = snapshot(source)
        lossy = os.path.join(root, "lossy.zip")
        with zipfile.ZipFile(source) as src, zipfile.ZipFile(lossy, "w", zipfile.ZIP_DEFLATED) as dst:
            for name in reversed(src.namelist()):
                dst.writestr(name, src.read(name))
        expect_raises(
            "the faithfulness check accepted a reordered, re-created archive",
            lambda: assert_faithful(before_viewer, lossy, {}),
        )

        # And a rebuild that preserves structure but does not actually replace anything.
        noop = os.path.join(root, "noop.zip")
        rebuild(source, {}, noop)
        expect_ok("the faithfulness check rejected a byte-faithful rebuild", lambda: assert_faithful(before_viewer, noop, {}))
        expect_raises(
            "the faithfulness check accepted a rebuild that replaced nothing it was told to",
            lambda: assert_faithful(
                before_viewer, noop, {"PerformanceMonitorDarlingViewer.exe": vrs._synth_pe(signed=True)}
            ),
        )
        expect_raises(
            "rebuild accepted a member name that is not in the archive",
            lambda: rebuild(source, {"NoSuchFile.exe": b"x"}, os.path.join(root, "bad.zip")),
        )

        # The case that isolates the raw-bytes comparison from every other assertion: same
        # entries, same order, same metadata, same content, re-deflated at a different level.
        # Only a check on the compressed bytes can tell this apart from a faithful rebuild, and
        # a rebuild that recompresses is the one that would silently rewrite a shipped artifact.
        recompressed = os.path.join(root, "recompressed.zip")
        with zipfile.ZipFile(source) as src, zipfile.ZipFile(recompressed, "w") as dst:
            for info in src.infolist():
                fresh = zipfile.ZipInfo(info.filename, date_time=info.date_time)
                fresh.compress_type = info.compress_type
                fresh.flag_bits = info.flag_bits
                fresh.external_attr = info.external_attr
                fresh.internal_attr = info.internal_attr
                fresh.create_system = info.create_system
                fresh.create_version = info.create_version
                fresh.extract_version = info.extract_version
                fresh.comment = info.comment
                fresh.extra = info.extra
                fresh._compresslevel = 1
                dst.writestr(fresh, src.read(info))
        expect_raises(
            "the faithfulness check accepted a recompressed archive as byte-faithful",
            lambda: assert_faithful(before_viewer, recompressed, {}),
        )

        # The raw-bytes comparison on its own. This pair is identical in every field the central
        # directory carries -- names, order, method, timestamps, attributes, CRC, both sizes and
        # content -- and differs only in two bytes of one LOCAL header, which is what a rewriter
        # that reconstructs local headers rather than copying them changes.
        divergent = os.path.join(root, "divergent.zip")
        with zipfile.ZipFile(divergent, "w") as zf:
            for name, blob in (("a.txt", b"a" * 400), ("b.txt", b"b" * 400), ("c.txt", b"c" * 400)):
                info = zipfile.ZipInfo(name, date_time=(2026, 9, 12, 10, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                zf.writestr(info, blob)
        reconstructed = snapshot(divergent)
        _divergent_local_header(divergent)
        divergent_snapshot = snapshot(divergent)
        expect(
            "patching a local header changed a field the central directory reports",
            [
                (m.name, m.compress_type, m.crc, m.file_size, m.compress_size, m.content_sha)
                for m in divergent_snapshot
            ]
            == [
                (m.name, m.compress_type, m.crc, m.file_size, m.compress_size, m.content_sha)
                for m in reconstructed
            ],
        )
        faithful_copy = os.path.join(root, "divergent-copy.zip")
        rebuild(divergent, {}, faithful_copy)
        expect_ok(
            "rebuild did not copy a local header that disagrees with the central directory",
            lambda: assert_faithful(divergent_snapshot, faithful_copy, {}),
        )
        expect_raises(
            "the faithfulness check accepted an archive whose local headers were reconstructed",
            lambda: assert_faithful(divergent_snapshot, _rebuilt_from_central(divergent, root), {}),
        )

        # Order, isolated from content: two members with identical content and metadata under
        # different names, swapped. Only a check on the entry ORDER, or one on raw bytes that
        # includes the member name, tells the two archives apart.
        twins = os.path.join(root, "twins.zip")
        swapped = os.path.join(root, "twins-swapped.zip")
        for target, names in ((twins, ("one.bin", "two.bin")), (swapped, ("two.bin", "one.bin"))):
            with zipfile.ZipFile(target, "w") as zf:
                for name in names:
                    info = zipfile.ZipInfo(name, date_time=(2026, 9, 12, 10, 0, 0))
                    info.compress_type = zipfile.ZIP_DEFLATED
                    zf.writestr(info, b"identical" * 40)
        expect_raises(
            "the faithfulness check accepted two same-content members in swapped order",
            lambda: assert_faithful(snapshot(twins), swapped, {}),
        )

    for failure in failures:
        print(f"SELF-TEST FAIL: {failure}", file=sys.stderr)
    if failures:
        return 1
    print(f"self-test: {checks} assertions passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", nargs="?", choices=("collect", "apply"))
    parser.add_argument("--self-test", action="store_true", dest="run_self_test")
    parser.add_argument("--stage", help="collect: directory to stage the unsigned files into")
    parser.add_argument("--signed", help="apply: directory holding the signing response")
    parser.add_argument("--manifest", help="the file collect writes and apply reads")
    parser.add_argument(
        "--product",
        action="append",
        default=[],
        metavar="NAME=DIR",
        help="collect: a product name and its `vpk pack` output directory",
    )
    args = parser.parse_args()

    if args.run_self_test:
        return self_test()
    if not args.mode:
        parser.error("a mode is required unless --self-test is given")
    if not args.manifest:
        parser.error("--manifest is required")

    try:
        if args.mode == "collect":
            if not args.stage:
                parser.error("--stage is required for collect")
            if not args.product:
                parser.error("at least one --product NAME=DIR is required for collect")
            products = []
            for spec in args.product:
                name, _, directory = spec.partition("=")
                if not name or not directory:
                    parser.error(f"--product expects NAME=DIR, got {spec!r}")
                products.append((name, directory))
            return collect(args.stage, args.manifest, products)
        if not args.signed:
            parser.error("--signed is required for apply")
        return apply(args.signed, args.manifest)
    except PostPackError as exc:
        print(f"postpack: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
