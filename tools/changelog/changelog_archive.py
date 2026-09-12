#!/usr/bin/env python3
"""Split CHANGELOG.md into a compacted index plus one prose archive per minor version.

GitHub refuses to render a blob past a few hundred kilobytes, and the single-file history had
grown to 2,160,009 bytes, so the file the project keeps its record in could not be read in the web UI at
all (#3387). The record itself is worth keeping at that length: each entry explains a mechanism
and carries its measurements. So the prose moves rather than shrinks.

    CHANGELOG.md            every version, every entry reduced to its bold title and the issue
                            references the entry carries. [Unreleased] and everything before
                            3.0.0 keep their full text: [Unreleased] is where entries are
                            authored, and pre-3.0 entries carry no prose to move.
    docs/changelog/<M.m>.md the full prose for one minor version, verbatim.

THE TRANSFORM IS LOSSLESS AND THAT IS CHECKABLE. `roundtrip` substitutes each archive's prose
back into the pre-split CHANGELOG.md and asserts the result is byte-identical to it. A size
reduction and a spot check are not proof: a parser that silently passes an entry through, or
silently drops one, produces both.

Sub-commands
    split      write the index and the archives from the current CHANGELOG.md
    census     write the per-archive entry counts and prose hashes the C# pin reads
    verify     structural checks that need no history: entry counts, every [#N] resolving in the
               file that uses it, per-file size ceilings
    roundtrip  reconstruct a git revision's CHANGELOG.md from the working tree's index plus
               archives and compare byte for byte
    --self-test
               run `roundtrip` against deliberately corrupted archives and assert it REPORTS
               each corruption. A reconstruction check that cannot see a dropped entry is not a
               check, so the instrument is exercised rather than trusted.

Line endings: .gitattributes is `* text=auto eol=crlf`, so the working tree is CRLF and the blob
is LF. Everything here reads and writes bytes and is explicit about which form it holds.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
import tempfile
from pathlib import Path

# The commit whose CHANGELOG.md this layout was derived from. `roundtrip` defaults to it: the
# claim being checked is that the archives hold what was taken out of THAT file, and the commit
# is immutable, so the check does not decay. Later edits to [Unreleased] make the comparison
# report drift in that one section, which is the honest answer rather than a stale pass.
PRE_SPLIT_REV = "3653e8d63a086965c9b75c8e484a14f1d01ac79a"

ARCHIVE_DIR = "docs/changelog"

# The census the C# pin reads, so the two tools share one set of numbers instead of mirroring
# them by hand - a hand-mirrored literal goes stale and then reports its own staleness as a
# defect in whatever it was checking.
CENSUS = "tools/changelog/archive-census.txt"

CENSUS_HEADER = [
    "# One row per archived minor-version file: version, the number of top-level entries in it,",
    "# and the sha256 of its version heading plus its prose with LF endings - the archive header",
    "# above the heading and the link-definition block below it are NOT covered, because neither",
    "# is content that was taken out of CHANGELOG.md.",
    "#",
    "# Regenerate at a release cut, after `split`:",
    "#     python3 tools/changelog/changelog_archive.py census",
    "#",
    "# ChangelogIndexAndArchiveTests reads this file, so a row that stops matching its archive",
    "# fails CI. It is the durable half of the proof; `roundtrip` is the whole-document half and",
    "# needs the pre-split revision to compare against.",
]

# Only a released 3.x version is archived. [Unreleased] is excluded because it is the authoring
# surface; pre-3.0 is excluded because its entries are already terse - mean 157 bytes against
# 1,916 in 3.7.0 - so compacting them would move nothing and delete the record's oldest half.
ARCHIVED = re.compile(r"^## \[(3\.\d+\.\d+)\](?: |$)")

VERSION_HEADING = re.compile(r"^## \[(\d+\.\d+\.\d+)\](?: |$)")
ANY_HEADING = re.compile(r"^## \[")
# A link definition, and nothing else in this file has this shape: every one of the 1,289 in the
# pre-split file matches, and no line of prose starts with '[' at all. That is what makes
# "strip the trailing run of definitions" an exact operation rather than a guess.
LINK_DEF = re.compile(r"^\[[^\]]+\]: \S+$")
ISSUE_REF = re.compile(r"\[#(\d+)\]")
# Non-greedy, so the title is the FIRST bold span. All 1,329 bold entries match it; the naive
# form that also demands a following "([#N])" matches only 853, because 210 entries close the
# bold span AFTER the references and 96 carry none.
BOLD_TITLE = re.compile(r"^- \*\*(.+?)\*\*")

LAYOUT_NOTE_BEGIN = "<!-- changelog-layout:begin -->"
LAYOUT_NOTE_END = "<!-- changelog-layout:end -->"

INDEX_CEILING = 750 * 1024
ARCHIVE_CEILING = 1024 * 1024


def read_lines(data: bytes) -> list[str]:
    """Lines without their terminator, from either ending form.

    Deliberately agnostic: `git show` hands out the LF blob while the working tree is CRLF, and
    the round trip has to compare one against the other. Whether the files on disk actually carry
    CRLF is a separate question, asserted by `verify` rather than inferred here.
    """
    lines = data.decode("utf-8").split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return [l[:-1] if l.endswith("\r") else l for l in lines]


def join_crlf(lines: list[str]) -> bytes:
    return "".join(line + "\r\n" for line in lines).encode("utf-8")


def join_lf(lines: list[str]) -> bytes:
    return "".join(line + "\n" for line in lines).encode("utf-8")


class Section:
    """One `## [version]` block: its heading, its prose, and its trailing definitions.

    `trailer` is the maximal trailing run of blank and link-definition lines. The pre-split file
    parks definitions in sixteen such runs at section ends rather than one block at the bottom,
    and every definition line in it falls inside one - asserted, not assumed. Splitting there
    means the index keeps every definition where it already sits, so reconstruction needs no
    record of where any of them came from.
    """

    def __init__(self, heading: str, prose: list[str], trailer: list[str]) -> None:
        self.heading = heading
        self.prose = prose
        self.trailer = trailer

    @property
    def version(self) -> str | None:
        m = VERSION_HEADING.match(self.heading)
        return m.group(1) if m else None

    @property
    def archived(self) -> bool:
        return ARCHIVED.match(self.heading) is not None

    @property
    def lines(self) -> list[str]:
        return [self.heading] + self.prose + self.trailer


def parse(lines: list[str]) -> tuple[list[str], list[Section]]:
    starts = [i for i, l in enumerate(lines) if ANY_HEADING.match(l)]
    if not starts:
        raise SystemExit("no '## [' version headings found")
    preamble = lines[: starts[0]]

    sections = []
    for k, start in enumerate(starts):
        end = starts[k + 1] if k + 1 < len(starts) else len(lines)
        cut = end
        while cut - 1 > start and (lines[cut - 1] == "" or LINK_DEF.match(lines[cut - 1])):
            cut -= 1
        prose = lines[start + 1 : cut]
        stray = [i + 1 for i in range(start + 1, cut) if LINK_DEF.match(lines[i])]
        if stray:
            raise SystemExit(
                f"{lines[start]}: link definitions at lines {stray} sit inside the prose rather "
                "than the section's trailing block, so the split point is not unambiguous"
            )
        sections.append(Section(lines[start], prose, lines[cut:end]))
    return preamble, sections


def is_crlf(data: bytes) -> bool:
    """Every line ends CRLF and no stray CR survives - the checkout form `eol=crlf` produces."""
    return data.count(b"\r") == data.count(b"\r\n") == data.count(b"\n") > 0


def definitions(lines: list[str]) -> dict[str, str]:
    out = {}
    for line in lines:
        if LINK_DEF.match(line):
            out[line[1 : line.index("]")]] = line
    return out


def entries(prose: list[str]) -> list[list[str]]:
    """Top-level list items, each with its continuation and nested lines.

    One walker, expressed over group_prose: two near-identical loops over the same shapes are how a
    count and a rewrite stop agreeing about what an entry is.
    """
    return [payload for kind, payload in group_prose(prose) if kind == "entry"]


def compact(entry: list[str]) -> str:
    """One index line for one entry.

    A bold-titled entry becomes its title span plus every [#N] the entry carries that the title
    does not already contain, so no reference is dropped and none is duplicated. An entry with no
    bold title has no title to compact to, so its first line is carried whole - three of them in
    the archived range, all in 3.2.0 and 3.3.0.
    """
    first = entry[0]
    refs: list[str] = []
    for line in entry:
        for ref in ISSUE_REF.findall(line):
            if ref not in refs:
                refs.append(ref)

    m = BOLD_TITLE.match(first)
    if m is None:
        return first

    span = first[: m.end()]
    in_title = set(ISSUE_REF.findall(span))
    extra = [r for r in refs if r not in in_title]
    if not extra:
        return span
    return span + " (" + ", ".join(f"[#{r}]" for r in extra) + ")"


def archive_path(version: str) -> str:
    major, minor, _ = version.split(".")
    return f"{ARCHIVE_DIR}/{major}.{minor}.md"


def archive_body(lines: list[str]) -> list[str]:
    """An archive's `heading + prose`, recovered from the archive file.

    Symmetric with the index rule: take the one version heading to the end of the file, then drop
    the trailing run of blank and definition lines. Nothing about the header above the heading
    matters, so it can be rewritten without touching what the round trip reads.
    """
    heads = [i for i, l in enumerate(lines) if VERSION_HEADING.match(l)]
    if len(heads) != 1:
        raise SystemExit(f"an archive must carry exactly one version heading, found {len(heads)}")
    cut = len(lines)
    while cut - 1 > heads[0] and (lines[cut - 1] == "" or LINK_DEF.match(lines[cut - 1])):
        cut -= 1
    return lines[heads[0] : cut]


def strip_layout_note(preamble: list[str]) -> list[str]:
    """The pre-split preamble: drop the marked layout note, then trailing blanks back to one."""
    out, skipping = [], False
    for line in preamble:
        if line.strip() == LAYOUT_NOTE_BEGIN:
            skipping = True
            continue
        if line.strip() == LAYOUT_NOTE_END:
            skipping = False
            continue
        if not skipping:
            out.append(line)
    while len(out) > 1 and out[-1] == "" and out[-2] == "":
        out.pop()
    return out


LAYOUT_NOTE = [
    LAYOUT_NOTE_BEGIN,
    "This file is an **index**. From 3.0.0 on, each released entry is compacted to its title and",
    "the issues it references, and the full prose lives in one file per minor version under",
    f"[`{ARCHIVE_DIR}/`]({ARCHIVE_DIR}/). GitHub will not render a blob of the size the whole",
    "history in one file had reached, so the prose moved rather than got shorter.",
    "",
    "`[Unreleased]` keeps its full prose here, because this is where it is written. At the release",
    "cut it is archived and compacted like every other version:",
    "",
    "    python3 tools/changelog/changelog_archive.py split",
    "",
    "Releases before 3.0.0 are not archived: those entries carry no prose to move.",
    LAYOUT_NOTE_END,
]


def index_link(path: str) -> str:
    return f"Full entries: [{path}]({path})"


def build(repo: Path, preamble: list[str], sections: list[Section]) -> tuple[list[str], dict[str, list[str]]]:
    """Return the index's lines and each archive's lines.

    Idempotent, which matters because a release cut runs this on an already-split tree and only
    the newly-released version has prose in the index. A section whose archive already exists
    takes its prose FROM that archive: reading the already-compacted index lines instead would
    rewrite the archive as its own index and destroy the prose in one pass.
    """
    all_defs = definitions([l for s in sections for l in s.trailer] + preamble)

    index = strip_layout_note(preamble) + LAYOUT_NOTE + [""]
    archives: dict[str, list[str]] = {}

    for section in sections:
        if not section.archived:
            index.extend(section.lines)
            continue

        version = section.version
        assert version is not None
        path = archive_path(version)

        prose = section.prose
        existing = repo / path
        compacted = index_link(path) in prose
        if existing.exists():
            if not compacted:
                raise SystemExit(
                    f"{version}: {path} exists but CHANGELOG.md still carries prose for it rather "
                    f"than a '{index_link(path)}' line. Two copies of the prose disagree and this "
                    "cannot tell which is current - reconcile them by hand."
                )
            prose = archive_body(read_lines(existing.read_bytes()))[1:]
        elif compacted:
            raise SystemExit(
                f"{version}: CHANGELOG.md points at {path} but that file is missing - the prose is "
                "gone rather than compacted, and rewriting from here would make the loss permanent."
            )
        index.extend([section.heading, "", index_link(path)])
        for kind, payload in group_prose(prose):
            if kind == "entry":
                index.append(compact(payload))
            else:
                index.extend(["", payload, ""])
        index.extend(section.trailer)

        heading_and_prose = [section.heading] + prose
        used = ordered_refs(heading_and_prose)
        missing = [r for r in used if f"#{r}" not in all_defs]
        if missing:
            raise SystemExit(f"{version}: no link definition for {missing}")
        archives[path] = (
            archive_header(version)
            + heading_and_prose
            + [""]
            + [all_defs[f"#{r}"] for r in sorted(used, key=int)]
        )

    return index, archives


def group_prose(prose: list[str]):
    """Walk a section's prose as an ordered stream of headings and entries."""
    i = 0
    while i < len(prose):
        line = prose[i]
        if line.startswith("### "):
            yield ("heading", line)
            i += 1
        elif line.startswith("- "):
            j = i + 1
            while j < len(prose) and not prose[j].startswith("- ") and not prose[j].startswith("#"):
                j += 1
            while j > i + 1 and prose[j - 1] == "":
                j -= 1
            yield ("entry", prose[i:j])
            i = j
        elif line == "":
            i += 1
        else:
            yield ("other", line)
            i += 1


def ordered_refs(lines: list[str]) -> list[str]:
    out: list[str] = []
    for line in lines:
        for ref in ISSUE_REF.findall(line):
            if ref not in out:
                out.append(ref)
    return out


def archive_header(version: str) -> list[str]:
    return [
        f"# PerformanceMonitor {version}",
        "",
        f"The full changelog entries for {version}. [CHANGELOG.md](../../CHANGELOG.md) carries the",
        "compacted index for every version, this one included, and is where `[Unreleased]` is written.",
        "",
    ]


def cmd_split(repo: Path, write: bool) -> int:
    original = (repo / "CHANGELOG.md").read_bytes()
    preamble, sections = parse(read_lines(original))
    index, archives = build(repo, preamble, sections)

    outputs = {"CHANGELOG.md": join_crlf(index)}
    for path, lines in archives.items():
        outputs[path] = join_crlf(lines)

    for path, data in sorted(outputs.items()):
        ceiling = INDEX_CEILING if path == "CHANGELOG.md" else ARCHIVE_CEILING
        flag = "OK " if len(data) <= ceiling else "OVER"
        print(f"{flag} {path:28} {len(data):>9,} bytes (ceiling {ceiling:,})")
        if write:
            target = repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
    print(f"{'wrote' if write else 'would write'} {len(outputs)} files")
    return 0


def census_rows(repo: Path) -> list[tuple[str, int, str]]:
    rows = []
    _, sections = parse(read_lines((repo / "CHANGELOG.md").read_bytes()))
    for section in sections:
        if not section.archived:
            continue
        body = archive_body(read_lines((repo / archive_path(section.version)).read_bytes()))
        digest = hashlib.sha256(join_lf(body)).hexdigest()
        rows.append((section.version, len(entries(body[1:])), digest))
    return rows


def read_census(repo: Path) -> dict[str, tuple[int, str]]:
    out = {}
    for line in read_lines((repo / CENSUS).read_bytes()):
        if not line.strip() or line.startswith("#"):
            continue
        version, count, digest = line.split()
        out[version] = (int(count), digest)
    return out


def cmd_census(repo: Path, write: bool) -> int:
    rows = census_rows(repo)
    lines = CENSUS_HEADER + [""] + [f"{v}  {n:>4}  {d}" for v, n, d in rows]
    for line in lines[len(CENSUS_HEADER) + 1 :]:
        print(line)
    if write:
        (repo / CENSUS).write_bytes(join_crlf(lines))
        print(f"wrote {CENSUS}")
    return 0


def cmd_verify(repo: Path, quiet: bool = False) -> int:
    failures: list[str] = []

    def check(ok: bool, message: str) -> None:
        if not quiet:
            print(("PASS " if ok else "FAIL ") + message)
        if not ok:
            failures.append(message)

    index_bytes = (repo / "CHANGELOG.md").read_bytes()
    index_lines = read_lines(index_bytes)
    _, sections = parse(index_lines)
    index_text = "\n".join(index_lines)

    check(len(index_bytes) <= INDEX_CEILING,
          f"CHANGELOG.md is {len(index_bytes):,} bytes, ceiling {INDEX_CEILING:,}")
    check(is_crlf(index_bytes), "CHANGELOG.md on disk is CRLF throughout")

    index_defs = definitions(index_lines)
    index_used = set(ordered_refs([l for l in index_lines if not LINK_DEF.match(l)]))
    undefined = sorted(r for r in index_used if f"#{r}" not in index_defs)
    check(not undefined, f"every [#N] in CHANGELOG.md resolves there ({len(index_used)} distinct)")
    if undefined and not quiet:
        print("      undefined:", undefined[:20])

    total_entries = 0
    for section in sections:
        prose = section.prose
        if section.archived:
            path = archive_path(section.version)
            data = (repo / path).read_bytes()
            lines = read_lines(data)
            check(len(data) <= ARCHIVE_CEILING,
                  f"{path} is {len(data):,} bytes, ceiling {ARCHIVE_CEILING:,}")
            check(is_crlf(data), f"{path} on disk is CRLF throughout")
            body = archive_body(lines)
            check(body[0] == section.heading,
                  f"{path} carries the index's own heading for {section.version}")
            check(f"]({path})" in index_text, f"CHANGELOG.md links {path}")
            defs = definitions(lines)
            used = set(ordered_refs([l for l in lines if not LINK_DEF.match(l)]))
            missing = sorted(r for r in used if f"#{r}" not in defs)
            check(not missing, f"every [#N] in {path} resolves there ({len(used)} distinct)")
            if missing and not quiet:
                print("      undefined:", missing[:20])
            extra = sorted(k for k in defs if k.startswith("#") and k[1:] not in used)
            check(not extra, f"{path} defines nothing it does not reference")
            if extra and not quiet:
                print("      unreferenced:", extra[:20])
            prose = body[1:]

        section_entries = entries(prose)
        bold = [e for e in section_entries if e[0].startswith("- **")]
        total_entries += len(bold)
        if section.archived:
            index_entries = entries(section.prose)
            check(len(index_entries) == len(section_entries),
                  f"{section.version}: {len(index_entries)} index lines for "
                  f"{len(section_entries)} archived entries")

    # A FLOOR, not an equality: [Unreleased] grows with every lane, so an exact total would report
    # its own staleness on the next ordinary changelog edit. 1,329 is the pre-split census, and
    # entries only ever move between sections, so the total can never legitimately drop below it.
    check(total_entries >= 1329,
          f"{total_entries} bold-titled entries across every version, floor 1,329 (pre-split census)")

    # What makes the split reversible in one commit: CHANGELOG.md keeps the whole pre-split
    # definition set, in the sixteen positions it already occupied, so no definition had to be
    # relocated and `roundtrip` needs no record of where any of them came from. An archive's own
    # block is an addition, not a move. The two definitions CHANGELOG.md carries without
    # referencing (#85, #86) were unreferenced before the split as well.
    archive_refs = set()
    for section in sections:
        if section.archived:
            lines = read_lines((repo / archive_path(section.version)).read_bytes())
            archive_refs |= set(ordered_refs([l for l in lines if not LINK_DEF.match(l)]))
    orphaned = sorted(r for r in archive_refs if f"#{r}" not in index_defs)
    check(not orphaned,
          f"CHANGELOG.md still defines every ref the archives use ({len(archive_refs)} distinct)")
    if orphaned and not quiet:
        print("      missing from the index:", orphaned[:20])
    unreferenced = sorted(k for k in index_defs if k.startswith("#") and k[1:] not in index_used)
    if not quiet:
        print(f"INFO CHANGELOG.md holds {len(index_defs)} definitions, {len(unreferenced)} of them "
              f"unreferenced there: {unreferenced}")

    census = read_census(repo)
    observed = {v: (n, d) for v, n, d in census_rows(repo)}
    check(set(census) == set(observed),
          f"{CENSUS} has a row for every archive and no others ({len(observed)} archives)")
    for version in sorted(observed):
        if version not in census:
            continue
        check(census[version] == observed[version],
              f"{version}: {observed[version][0]} entries, prose sha256 "
              f"{observed[version][1][:16]} matches {CENSUS}")

    return 1 if failures else 0


def git_show(repo: Path, rev: str, path: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(repo), "show", f"{rev}:{path}"],
        check=True, stdout=subprocess.PIPE).stdout


def reconstruct(repo: Path, archive_override: dict[str, bytes] | None = None) -> list[str]:
    """The pre-split document, rebuilt from the index plus the archives and nothing else."""
    index_lines = read_lines((repo / "CHANGELOG.md").read_bytes())
    preamble, sections = parse(index_lines)

    out = strip_layout_note(preamble)
    for section in sections:
        if not section.archived:
            out.extend(section.lines)
            continue
        path = archive_path(section.version)
        data = (archive_override or {}).get(path) or (repo / path).read_bytes()
        out.extend(archive_body(read_lines(data)))
        out.extend(section.trailer)
    return out


def prose_only(lines: list[str]) -> list[str]:
    return [l for l in lines if not LINK_DEF.match(l)]


def cmd_roundtrip(repo: Path, rev: str, archive_override=None, quiet=False) -> int:
    """Substitute every archive's prose back into a revision's CHANGELOG.md and compare.

    Two verdicts, and the difference between them is the point. IDENTICAL is the whole document,
    byte for byte, which is what this returned at the split. Once the index moves on - a new entry
    in [Unreleased], a new release archived, a new link definition appended - the bytes cannot
    match any more, so the fallback compares the thing that must never change: the PROSE of every
    version the revision already had. It reports which is which rather than quietly grading itself
    on the easier claim.
    """
    want = git_show(repo, rev, "CHANGELOG.md")
    got = join_lf(reconstruct(repo, archive_override))

    if got == want:
        if not quiet:
            print(f"IDENTICAL  {len(want):,} bytes, sha256 {hashlib.sha256(want).hexdigest()}")
            print(f"           the whole of {rev[:12]}:CHANGELOG.md, rebuilt from CHANGELOG.md "
                  f"+ {ARCHIVE_DIR}/")
        return 0

    want_pre, want_sections = parse(read_lines(want))
    got_pre, got_sections = parse(read_lines(got))
    got_by_heading = {s.heading: s for s in got_sections}

    failures: list[str] = []
    notes: list[str] = []

    if prose_only(want_pre) != prose_only(got_pre):
        failures.append("the preamble's prose differs")

    for section in want_sections:
        if section.version is None:
            notes.append(f"{section.heading} is the authoring surface, so its drift is expected")
            continue
        rebuilt = got_by_heading.get(section.heading)
        if rebuilt is None:
            failures.append(f"{section.heading} is gone from the rebuilt document")
            continue
        want_prose = prose_only(section.prose)
        got_prose = prose_only(rebuilt.prose)
        if want_prose == got_prose:
            continue
        where = next((i for i in range(max(len(want_prose), len(got_prose)))
                      if want_prose[i:i + 1] != got_prose[i:i + 1]), 0)
        failures.append(
            f"{section.heading}: prose differs at line {where + 1} of the section\n"
            f"             at {rev[:12]}: {(want_prose[where:where + 1] or ['<missing>'])[0][:150]}\n"
            f"             rebuilt     : {(got_prose[where:where + 1] or ['<missing>'])[0][:150]}")

    extra = [s.heading for s in got_sections if s.heading not in {w.heading for w in want_sections}]
    if extra:
        notes.append(f"released since {rev[:12]}: {', '.join(extra)}")

    # Every definition the revision carried must still resolve somewhere in the set, or an issue
    # link that used to work now renders as literal text.
    want_defs = definitions(read_lines(want))
    got_defs = definitions(read_lines(got))
    lost = sorted(k for k, v in want_defs.items() if got_defs.get(k) != v)
    if lost:
        failures.append(f"{len(lost)} link definitions lost or changed: {lost[:10]}")

    if not quiet:
        for note in notes:
            print(f"NOTE       {note}")
        for failure in failures:
            print(f"FAIL       {failure}")
        if failures:
            print(f"DIFFERENT  rebuilt {len(got):,} bytes against {len(want):,} at {rev[:12]}")
        else:
            print(f"PROSE INTACT  every version {rev[:12]} held is byte-identical in prose, and "
                  "every link definition still resolves")
            print(f"              NOT the whole document: the index has moved on since then, so "
                  "this is the durable half of the claim rather than the byte comparison")
    return 1 if failures else 0


def cmd_self_test(repo: Path, rev: str) -> int:
    """Cripple the instrument and confirm it notices.

    A round trip that passes against a corrupted archive proves nothing about the uncorrupted one,
    so each corruption below is a defect the check MUST report, and the weakened comparisons are
    there to show what a check reduced to a size or a line count would have missed.
    """
    failures: list[str] = []

    def expect(name: str, ok: bool) -> None:
        print(("PASS " if ok else "FAIL ") + name)
        if not ok:
            failures.append(name)

    expect("the uncorrupted tree round-trips", cmd_roundtrip(repo, rev, quiet=True) == 0)

    path = archive_path("3.7.0")
    pristine = (repo / path).read_bytes()
    pristine_lines = read_lines(pristine)

    entry_lines = [i for i, l in enumerate(pristine_lines) if l.startswith("- **")]
    first, last = entry_lines[0], entry_lines[-1]

    # Flip one character rather than substitute a word: a replace() whose needle happens not to
    # occur mutates nothing, and then every assertion about it passes while asserting nothing.
    # This one cannot be a no-op, and the `deep` line is chosen past the first 1,000 bytes so the
    # weakened comparisons below are answering about a real edit.
    deep = entry_lines[len(entry_lines) // 2]
    assert sum(len(l) + 2 for l in pristine_lines[:deep]) > 1000, "the deep line is in the prefix"
    original = pristine_lines[deep]
    at = len(original) // 2
    flipped = original[:at] + ("x" if original[at] != "x" else "y") + original[at + 1 :]
    assert flipped != original and len(flipped) == len(original)

    mutations = {
        "a dropped entry is reported":
            pristine_lines[:first] + pristine_lines[first + 1 :],
        "a dropped final entry is reported":
            pristine_lines[:last] + pristine_lines[last + 1 :],
        "a one-character edit deep in the prose is reported":
            pristine_lines[:deep] + [flipped] + pristine_lines[deep + 1 :],
        "a truncated entry is reported":
            [l[: len(l) // 2] if i == deep else l for i, l in enumerate(pristine_lines)],
        "two swapped entries are reported":
            swap_first_two_entries(pristine_lines),
    }
    for name, lines in mutations.items():
        expect(name, cmd_roundtrip(repo, rev, {path: join_crlf(lines)}, quiet=True) == 1)

    # The SAME real edit, against comparisons reduced to a prefix or a line count. Both MUST come
    # back clean: that is what makes the byte comparison load-bearing rather than decorative, and
    # it is why "the file got smaller" and "the line counts agree" are not acceptable substitutes.
    want = git_show(repo, rev, "CHANGELOG.md")
    edited = join_crlf(pristine_lines[:deep] + [flipped] + pristine_lines[deep + 1 :])
    rebuilt = join_lf(reconstruct(repo, {path: edited}))
    expect("a 1,000-byte-prefix comparison misses that edit, so it is not the check",
           rebuilt[:1000] == want[:1000] and rebuilt != want)
    expect("a line-count comparison misses that edit, so it is not the check",
           len(read_lines(rebuilt)) == len(read_lines(want)) and rebuilt != want)
    expect("a byte-length comparison misses that edit, so it is not the check",
           len(rebuilt) == len(want) and rebuilt != want)

    # Where roundtrip deliberately does not look, and which check covers it instead. CHANGELOG.md
    # holds the canonical definition set in its original positions, so roundtrip never reads an
    # archive's own block and cannot see one go missing - it would rebuild the pre-split file
    # perfectly while shipping an archive whose every issue link renders as literal text. That is
    # `verify`'s to catch, and the pair below is what stops either check being assumed to cover
    # the other's half.
    stripped = join_crlf([l for l in pristine_lines if not LINK_DEF.match(l)])
    expect("roundtrip still passes with an archive's definitions dropped (it reads prose only)",
           cmd_roundtrip(repo, rev, {path: stripped}, quiet=True) == 0)
    with tempfile.TemporaryDirectory() as tmp:
        expect("verify reports an archive's dropped definitions",
               cmd_verify(materialize(repo, Path(tmp) / "corrupt", {path: stripped}), quiet=True) == 1)
        expect("verify passes on the same tree uncorrupted",
               cmd_verify(materialize(repo, Path(tmp) / "clean", {}), quiet=True) == 0)

    assert (repo / path).read_bytes() == pristine, "the self-test altered a file on disk"
    print(f"{path} unchanged on disk, sha256 {hashlib.sha256(pristine).hexdigest()[:16]}")
    return 1 if failures else 0


def materialize(repo: Path, target: Path, overrides: dict[str, bytes]) -> Path:
    """A throwaway copy of just the changelog set, so a corruption never reaches the real tree."""
    wanted = ["CHANGELOG.md", CENSUS] + [
        f"{ARCHIVE_DIR}/{p.name}" for p in sorted((repo / ARCHIVE_DIR).glob("*.md"))]
    for rel in wanted:
        destination = target / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(overrides.get(rel) or (repo / rel).read_bytes())
    return target


def swap_first_two_entries(lines: list[str]) -> list[str]:
    idx = [i for i, l in enumerate(lines) if l.startswith("- **")][:2]
    out = list(lines)
    out[idx[0]], out[idx[1]] = out[idx[1]], out[idx[0]]
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["split", "census", "verify", "roundtrip"], nargs="?",
                        default="verify")
    parser.add_argument("--repo", default=None, help="repository root (default: derived from this file)")
    parser.add_argument("--rev", default=PRE_SPLIT_REV, help="revision to round-trip against")
    parser.add_argument("--dry-run", action="store_true",
                        help="split / census: report, write nothing")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    repo = Path(args.repo) if args.repo else Path(__file__).resolve().parents[2]

    if args.self_test:
        return cmd_self_test(repo, args.rev)
    if args.command == "split":
        return cmd_split(repo, not args.dry_run)
    if args.command == "census":
        return cmd_census(repo, not args.dry_run)
    if args.command == "roundtrip":
        return cmd_roundtrip(repo, args.rev)
    return cmd_verify(repo)


if __name__ == "__main__":
    sys.exit(main())
