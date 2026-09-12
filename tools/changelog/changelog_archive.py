#!/usr/bin/env python3
"""Split CHANGELOG.md into a compacted index plus one prose archive per minor version.

GitHub refuses to render a blob past a few hundred kilobytes, and the single-file history had
grown to 2.06 MB, so the file the project keeps its record in could not be read in the web UI at
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
from pathlib import Path

# The commit whose CHANGELOG.md this layout was derived from. `roundtrip` defaults to it: the
# claim being checked is that the archives hold what was taken out of THAT file, and the commit
# is immutable, so the check does not decay. Later edits to [Unreleased] make the comparison
# report drift in that one section, which is the honest answer rather than a stale pass.
PRE_SPLIT_REV = "3653e8d63a086965c9b75c8e484a14f1d01ac79a"

ARCHIVE_DIR = "docs/changelog"

# Only a released 3.x version is archived. [Unreleased] is excluded because it is the authoring
# surface; pre-3.0 is excluded because its entries are already terse - mean 213 bytes against
# 1,915 in 3.7.0 - so compacting them would move nothing and delete the record's oldest half.
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
    """Top-level list items, each with its continuation and nested lines."""
    out = []
    i = 0
    while i < len(prose):
        if prose[i].startswith("- "):
            j = i + 1
            while j < len(prose) and not prose[j].startswith("- ") and not prose[j].startswith("#"):
                j += 1
            # A loose list puts a blank line between items; it belongs to neither.
            while j > i + 1 and prose[j - 1] == "":
                j -= 1
            out.append(prose[i:j])
            i = j
        else:
            i += 1
    return out


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


def build(preamble: list[str], sections: list[Section]) -> tuple[list[str], dict[str, list[str]]]:
    """Return the index's lines and each archive's lines."""
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

        index.extend([section.heading, "", f"Full entries: [{path}]({path})"])
        for kind, payload in group_prose(section.prose):
            if kind == "entry":
                index.append(compact(payload))
            else:
                index.extend(["", payload, ""])
        index.extend(section.trailer)

        heading_and_prose = [section.heading] + section.prose
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
    index, archives = build(preamble, sections)

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


def cmd_verify(repo: Path) -> int:
    failures: list[str] = []

    def check(ok: bool, message: str) -> None:
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
    if undefined:
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
            if missing:
                print("      undefined:", missing[:20])
            extra = sorted(k for k in defs if k.startswith("#") and k[1:] not in used)
            check(not extra, f"{path} defines nothing it does not reference")
            if extra:
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

    check(total_entries == 1329,
          f"{total_entries} bold-titled entries across every version (1,329 pre-split)")

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
    if orphaned:
        print("      missing from the index:", orphaned[:20])
    unreferenced = sorted(k for k in index_defs if k.startswith("#") and k[1:] not in index_used)
    print(f"INFO CHANGELOG.md holds {len(index_defs)} definitions, {len(unreferenced)} of them "
          f"unreferenced there: {unreferenced}")

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


def cmd_roundtrip(repo: Path, rev: str, archive_override=None, quiet=False) -> int:
    want = git_show(repo, rev, "CHANGELOG.md")
    got = join_lf(reconstruct(repo, archive_override))

    if got == want:
        digest = hashlib.sha256(want).hexdigest()
        if not quiet:
            print(f"IDENTICAL  {len(want):,} bytes, sha256 {digest}")
            print(f"           reconstructed from CHANGELOG.md + {ARCHIVE_DIR}/ against {rev[:12]}")
        return 0

    want_lines, got_lines = read_lines(want), read_lines(got)
    if not quiet:
        print(f"DIFFERENT  reconstructed {len(got):,} bytes against {len(want):,} at {rev[:12]}")
        print(f"           {len(got_lines):,} lines against {len(want_lines):,}")
        for i in range(max(len(want_lines), len(got_lines))):
            a = want_lines[i] if i < len(want_lines) else "<missing>"
            b = got_lines[i] if i < len(got_lines) else "<missing>"
            if a != b:
                print(f"           first difference at line {i + 1}")
                print(f"             pre-split: {a[:160]}")
                print(f"             rebuilt  : {b[:160]}")
                break
    return 1


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

    entry_at = next(i for i, l in enumerate(pristine_lines) if l.startswith("- **"))

    mutations = {
        "a dropped entry is reported":
            pristine_lines[:entry_at] + pristine_lines[entry_at + 1 :],
        "a dropped final entry is reported":
            [l for i, l in enumerate(pristine_lines)
             if i != max(i for i, x in enumerate(pristine_lines) if x.startswith("- **"))],
        "a one-character edit deep in the prose is reported":
            [l.replace("collector", "collecter", 1) if i == entry_at else l
             for i, l in enumerate(pristine_lines)],
        "a truncated entry is reported":
            [l[: len(l) // 2] if i == entry_at else l for i, l in enumerate(pristine_lines)],
        "two swapped entries are reported":
            swap_first_two_entries(pristine_lines),
    }
    for name, lines in mutations.items():
        corrupt = join_crlf(lines)
        expect(name, cmd_roundtrip(repo, rev, {path: corrupt}, quiet=True) == 1)

    # The same corruptions against comparisons that only look at a prefix or a line count. These
    # MUST come back clean: that is what makes the byte comparison above load-bearing rather than
    # decorative, and it is the reason a "first 1,000 bytes" or "same number of lines" check is
    # not an acceptable substitute.
    want = git_show(repo, rev, "CHANGELOG.md")
    edited = join_crlf(mutations["a one-character edit deep in the prose is reported"])
    truncated_prefix_agrees = (
        join_lf(reconstruct(repo, {path: edited}))[:1000] == want[:1000])
    expect("a 1,000-byte-prefix comparison misses the edit (so it is not the check)",
           truncated_prefix_agrees)
    line_count_agrees = (
        len(read_lines(join_lf(reconstruct(repo, {path: edited})))) == len(read_lines(want)))
    expect("a line-count comparison misses the edit (so it is not the check)", line_count_agrees)

    assert (repo / path).read_bytes() == pristine, "the self-test altered a file on disk"
    print(f"{path} unchanged on disk, sha256 {hashlib.sha256(pristine).hexdigest()[:16]}")
    return 1 if failures else 0


def swap_first_two_entries(lines: list[str]) -> list[str]:
    idx = [i for i, l in enumerate(lines) if l.startswith("- **")][:2]
    out = list(lines)
    out[idx[0]], out[idx[1]] = out[idx[1]], out[idx[0]]
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["split", "verify", "roundtrip"], nargs="?",
                        default="verify")
    parser.add_argument("--repo", default=None, help="repository root (default: derived from this file)")
    parser.add_argument("--rev", default=PRE_SPLIT_REV, help="revision to round-trip against")
    parser.add_argument("--dry-run", action="store_true", help="split: report sizes, write nothing")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    repo = Path(args.repo) if args.repo else Path(__file__).resolve().parents[2]

    if args.self_test:
        return cmd_self_test(repo, args.rev)
    if args.command == "split":
        return cmd_split(repo, not args.dry_run)
    if args.command == "roundtrip":
        return cmd_roundtrip(repo, args.rev)
    return cmd_verify(repo)


if __name__ == "__main__":
    sys.exit(main())
