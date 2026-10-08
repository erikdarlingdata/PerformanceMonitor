#!/usr/bin/env python3
"""The test-class map producer (#5459 change 4, slice 3).  Standard library only.

nightly.yml's `test-map-shard` jobs run the whole Darling.Tests and Lite.Tests suites under AltCover, a few
shards each, and its `test-map` job joins the shards into `test-map.json.gz`, the file ci-select.py's
`map_select()` reads (schema 1, documented there).  Three subcommands:

  cut           Cut a runner class listing (`-list classes/json`) into shards by class-name hash, exactly as
                build.yml's Darling PostgreSQL shards do (SHA-256 of the UTF-8 name, first four bytes as a
                little-endian uint32, modulo the shard count), then into command-line-sized chunks.  Writes
                `listed.txt` (this shard's classes) and `chunk-<n>.txt` (one class per line).
  shard-report  Turn one shard's AltCover OpenCover reports (one per chunk) and xunit reports into
                `shard.json`: per class the repo files its tests visited, its test seconds, its test and skip
                counts.  Reports are unioned, so a retry or a second chunk adds, never replaces.
  build         Join every shard's `shard.json` with what the test sources say and write the map:
                  classes:{suite:{Class:{own, files, seconds}}}   suite `darling` or `lite`, simple class name
                  text_patterns:{Class:[globs] or "tree"}
                `files` holds what coverage saw plus the type-name rule (a type named in the test source maps
                to the file that declares it, unless more than --max-ident-files files declare that name).
                `text_patterns` holds the path and tool-name literals of the test source, and "tree" for a class
                that walks the whole checkout as text.  Coverage cannot see a class that only reads text, a
                const, or a reflected description; these rules are what keeps such a class from being a hole.

Nothing here talks to a server, and nothing here fails a build: the workflow treats every error as "no map".
"""

from __future__ import annotations

import argparse
import fnmatch
import glob
import gzip
import hashlib
import importlib.util
import json
import os
import re
import struct
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

SCHEMA = 1
HERE = os.path.dirname(os.path.abspath(__file__))
SUITE_DIRS = {"darling": "Darling/Darling.Tests", "lite": "Lite.Tests"}
SUITE_BY_NAME = {"Darling.Tests": "darling", "Lite.Tests": "lite", "darling": "darling", "lite": "lite"}
CLASS_DECL = re.compile(r"^[ \t]*(?:(?:public|internal|private|protected|sealed|static|abstract|partial|unsafe)\s+)*class\s+(\w+)",
                        re.M)
TYPE_DECL = re.compile(r"\b(?:class|struct|enum|record|interface)\s+([A-Z]\w*)")
GUARD_MARKER = re.compile(r"Trait\(\s*\"Stage\"\s*,\s*\"Guard\"\s*\)")
TEXT_READ = re.compile(r"File\.ReadAll|RepoRoot|ReadAllText|ReadLines\(|ReadRepoFile|RepoFile")
STRING = r'"(?:[^"\\\n]|\\.)*"'
LITERAL = re.compile(STRING)
RUN = re.compile(STRING + r"(?:\s*,\s*" + STRING + r")+")
PATHLIKE = re.compile(r"^[\w.\-*]+(?:/[\w.\-*]+)*/?$")
TOOL_NAME = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$")
IDENT = re.compile(r"\b[A-Z][A-Za-z0-9_]{2,}\b")
FILE_EXT = {"cs", "js", "css", "xaml", "sql", "ps1", "yml", "yaml", "json", "html", "csproj", "props", "targets",
            "md", "xml", "sh", "py", "psm1", "psd1", "txt", "config", "svg", "resx", "mjs", "conf", "ini", "cmd", "bat"}
MAX_BASENAME_MATCHES = 25
MAX_TOOL_FILES = 6
MEMBER_REF = re.compile(r"\b([A-Z][A-Za-z0-9_]{2,})\.([A-Z][A-Za-z0-9_]{2,})\b")
MAX_MEMBER_FILES = 5  # `Type.Member` of a partial type: the files that declare the member, when there are this few
MAX_NAMED_PARTIALS = 12
MAX_OVERFLOW_FILES = 60  # a class that would be a hole may take the files of a type declared in more files than the cap
# A repo path written in a comment or doc cref (`Lite/Analysis/AnomalyDetector.cs:120`): the author names the file the
# class pins, so a change to it selects the class (#5459 slice 4, the Darling/Lite twin pairs).
COMMENT_PATH = re.compile(r"(?<![\w./\-])((?:[\w.\-]+/)+[\w\-]+\.[A-Za-z]{1,8})\b")
TEST_DIR = re.compile(r"(^|/)[\w.]*Tests?/")
SKIP_DIRS = ("bin/", "obj/", "node_modules/")


def norm(path: str) -> str:
    return path.replace("\\", "/")


def _probe():
    sys.dont_write_bytecode = True  # the checkout is not a place for a __pycache__
    spec = importlib.util.spec_from_file_location("altcover_probe", os.path.join(HERE, "altcover-probe.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("altcover_probe", mod)
    spec.loader.exec_module(mod)
    return mod


def simple_name(full: str) -> str:
    return re.split(r"[.+/]", full)[-1]


def read_listing(path: str) -> list[str]:
    with open(path, encoding="utf-8-sig") as handle:
        text = handle.read()
    return json.loads(text[text.index("["):text.rindex("]") + 1])


# ---------------------------------------------------------------------------------------------------------
# cut
# ---------------------------------------------------------------------------------------------------------

def shard_of(name: str, total: int) -> int:
    """build.yml's cut: BitConverter.ToUInt32 of the first four SHA-256 bytes is little-endian."""
    return struct.unpack("<I", hashlib.sha256(name.encode("utf-8")).digest()[:4])[0] % total


def chunk_names(names: list[str], budget: int) -> list[list[str]]:
    chunks: list[list[str]] = []
    current: list[str] = []
    length = 0
    for name in names:
        cost = len("-class") + 1 + len(name) + 1
        if current and length + cost > budget:
            chunks.append(current)
            current, length = [], 0
        current.append(name)
        length += cost
    if current:
        chunks.append(current)
    return chunks


def cmd_cut(args) -> int:
    classes = read_listing(args.list)
    if not classes:
        print("the class listing returned no classes", file=sys.stderr)
        return 1
    mine = [c for c in classes if shard_of(c, args.shards) == args.shard]
    if not mine:
        print("shard %d selected zero of %d classes" % (args.shard, len(classes)), file=sys.stderr)
        return 1
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "listed.txt"), "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(mine) + "\n")
    chunks = chunk_names(mine, args.budget)
    for number, chunk in enumerate(chunks, 1):
        with open(os.path.join(args.out, "chunk-%d.txt" % number), "w", encoding="utf-8", newline="\n") as handle:
            handle.write("\n".join(chunk) + "\n")
    print("shard %d of %d: %d of %d classes in %d chunk(s)" % (args.shard, args.shards, len(mine), len(classes), len(chunks)))
    return 0


# ---------------------------------------------------------------------------------------------------------
# shard-report
# ---------------------------------------------------------------------------------------------------------

def xunit_stats(paths: list[str]) -> dict[str, list[float]]:
    """{class: [seconds, tests, skipped, failed]} summed over the reports."""
    out: dict[str, list[float]] = {}
    for path in paths:
        if not os.path.exists(path):
            continue
        for _, el in ET.iterparse(path, events=("end",)):
            if el.tag != "test":
                continue
            row = out.setdefault((el.get("type") or "").replace("/", "+"), [0.0, 0, 0, 0])
            try:
                row[0] += float(el.get("time") or 0)
            except ValueError:
                pass
            row[1] += 1
            result = (el.get("result") or "").lower()
            row[2] += result == "skip"
            row[3] += result == "fail"
            el.clear()
    return out


def cmd_shard_report(args) -> int:
    probe = _probe()
    listed = [ln.strip() for ln in open(args.listed, encoding="utf-8") if ln.strip()]
    wanted = {c.replace("/", "+") for c in listed}
    ctx: dict[str, set[str]] = {}
    for report in args.report:
        if not os.path.exists(report):
            print("missing report " + report, file=sys.stderr)
            continue
        files, _stats, _totals, _prefix = probe.parse_report(report, args.root)
        for cls, found in files.items():
            ctx.setdefault(cls, set()).update(found)
    stats = xunit_stats(args.xml)
    classes = {}
    for cls in sorted(wanted):
        row = stats.get(cls, [0.0, 0, 0, 0])
        classes[cls] = {"files": sorted(ctx.get(cls, ())), "seconds": round(row[0], 3), "tests": int(row[1]),
                        "skipped": int(row[2]), "failed": int(row[3]), "seen": cls in ctx}
    out = {"suite": SUITE_BY_NAME.get(args.suite, args.suite), "shard": args.shard, "tests_exit": args.tests_exit,
           "listed": sorted(wanted), "classes": classes}
    with open(args.out, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(out, handle, separators=(",", ":"))
    seen = sum(1 for c in classes.values() if c["seen"])
    print("%s shard %s: %d classes listed, %d with a test context, %d with files" % (
        out["suite"], args.shard, len(classes), seen, sum(1 for c in classes.values() if c["files"])))
    return 0


# ---------------------------------------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------------------------------------

def tracked_files(root: str, listing: str | None) -> list[str]:
    if listing:
        with open(listing, encoding="utf-8") as handle:
            return sorted({norm(ln.strip()) for ln in handle if ln.strip()})
    out = subprocess.run(["git", "-C", root, "ls-files", "-z"], capture_output=True, check=True).stdout.decode("utf-8")
    return sorted({norm(p) for p in out.split("\0") if p})


def read_text(root: str, rel: str) -> str:
    try:
        with open(os.path.join(root, rel), encoding="utf-8-sig", errors="replace") as handle:
            return handle.read()
    except OSError:
        return ""


def unescape(literal: str) -> str:
    return literal[1:-1].replace("\\\\", "/").replace('\\"', '"')


def _glob_hits(pattern: str, tracked: list[str]) -> list[str]:
    return [f for f in tracked if fnmatch.fnmatchcase(f, pattern) or fnmatch.fnmatchcase(f, "*/" + pattern)]


class Repo:
    """What the checkout holds: tracked paths, type declarations and tool-name literals."""

    def __init__(self, root: str, tracked: list[str]):
        self.root = root
        self.tracked = [f for f in tracked if not f.startswith(SKIP_DIRS) and "/obj/" not in f and "/bin/" not in f]
        self.tracked_set = set(self.tracked)
        self.top_dirs = {f.split("/", 1)[0] for f in self.tracked if "/" in f}
        self.dirs: set[str] = set()  # every directory that holds a tracked file, as a path from the root
        for f in self.tracked:
            parts = f.split("/")[:-1]
            for i in range(1, len(parts) + 1):
                self.dirs.add("/".join(parts[:i]))
        self.memo: dict[tuple[str, bool], list[str]] = {}
        self.by_basename: dict[str, list[str]] = {}
        for f in self.tracked:
            self.by_basename.setdefault(f.rsplit("/", 1)[-1], []).append(f)
        self.types: dict[str, set[str]] = {}
        self.tools: dict[str, set[str]] = {}
        self.cs_words: dict[str, frozenset[str]] = {}  # the identifiers of each .cs file, for the `Type.Member` rule
        for f in self.tracked:
            if not f.endswith(".cs"):
                continue
            text = read_text(root, f)
            self.cs_words[f] = frozenset(re.findall(r"\w+", text))
            for name in TYPE_DECL.findall(text):
                self.types.setdefault(name, set()).add(f)
            if not TEST_DIR.search(f):
                for lit in LITERAL.findall(text):
                    inner = lit[1:-1]
                    if len(inner) >= 6 and TOOL_NAME.match(inner):
                        self.tools.setdefault(inner, set()).add(f)

    def resolve(self, candidate: str, reads_text: bool) -> list[str]:
        """Patterns for a path-like literal of a test source; empty when it names nothing in the checkout."""
        key = (candidate, reads_text)
        if key not in self.memo:
            self.memo[key] = self._resolve(candidate, reads_text)
        return self.memo[key]

    def _resolve(self, candidate: str, reads_text: bool) -> list[str]:
        c = candidate.strip("/")
        if not c or c in (".", "..") or not PATHLIKE.match(candidate):
            return []
        if "*" in c:
            return [c] if _glob_hits(c, self.tracked) else []
        if c in self.tracked_set:
            return [c]
        suffix = [f for f in self.by_basename.get(c.rsplit("/", 1)[-1], []) if f.endswith("/" + c)]
        if suffix:
            return suffix[:MAX_BASENAME_MATCHES] if len(suffix) <= MAX_BASENAME_MATCHES else []
        if "/" not in c:
            if c.rsplit(".", 1)[-1] in FILE_EXT and "." in c:
                hits = self.by_basename.get(c, [])
                return hits if 0 < len(hits) <= MAX_BASENAME_MATCHES else []
            return [c + "/**"] if reads_text and c in self.top_dirs else []
        return [c + "/**"] if c in self.dirs else []


def analyze_source(text: str, repo: Repo, own_file: str, own_types: set[str]) -> dict:
    """What one test source file says about the files it depends on."""
    reads = bool(TEXT_READ.search(text))
    literals = [unescape(m) for m in LITERAL.findall(text)]
    candidates = list(literals)
    candidates += [m for m in COMMENT_PATH.findall(text) if m.rsplit(".", 1)[-1] in FILE_EXT]
    for run in RUN.findall(text):
        parts = [unescape(x) for x in LITERAL.findall(run)]
        if all(PATHLIKE.match(p) for p in parts):
            candidates.append("/".join(p.strip("/") for p in parts))
    patterns: dict[str, None] = {}
    bare_glob = False
    for cand in dict.fromkeys(candidates):
        cand = cand.replace("\\", "/")
        if "*" in cand and "/" not in cand.strip("/") and PATHLIKE.match(cand):
            bare_glob = True  # `*.cs`, `*`: a search pattern that names no place, so it cannot narrow a tree walk
            continue
        for pat in repo.resolve(cand, reads):
            if pat != own_file:
                patterns[pat] = None
    tools = 0
    for lit in dict.fromkeys(literals):
        if len(lit) >= 6 and TOOL_NAME.match(lit):
            hits = repo.tools.get(lit, set())
            if 0 < len(hits) <= MAX_TOOL_FILES:
                for f in sorted(hits):
                    patterns[f] = None
                    tools += 1
    walks = reads and "AllDirectories" in text
    return {"patterns": list(patterns), "reads_text": reads, "walks_tree": walks, "tool_hits": tools,
            # A class that walks directories as text and names no place to walk, or only a bare wildcard, reads the tree.
            "tree": walks and (bare_glob or not patterns)}


def type_files(text: str, repo: Repo, own_file: str, own_types: set[str], max_files: int) -> set[str]:
    """The type-name rule: a type named in the test source maps to the file(s) declaring it."""
    found: set[str] = set()
    for name in set(IDENT.findall(text)):
        if name in own_types:
            continue
        files = repo.types.get(name)
        if files and len(files) <= max_files:
            found |= files
        elif files:
            # A partial type declared in many files: the files named after it (`Foo.cs`, `Foo.Plans.cs`) are the ones
            # a test that names the type most plausibly pins; the rest come only through coverage or `Type.Member`.
            named = {f for f in files if f.rsplit("/", 1)[-1].startswith(name + ".") and f != own_file}
            found |= named if len(named) <= MAX_NAMED_PARTIALS else {f for f in named if f.endswith("/" + name + ".cs")}
    found.discard(own_file)
    return found


def member_files(text: str, repo: Repo, own_file: str, max_files: int) -> set[str]:
    """`Type.Member` where Type is a partial type declared in more than `max_files` files: the files of Type that
    mention Member, when that is at most MAX_MEMBER_FILES. A test that pins a const SQL string of a partial class
    (`ViewerDataService.CollectionHealthSql`) runs no code in it, so coverage never sees the file."""
    found: set[str] = set()
    for typ, member in set(MEMBER_REF.findall(text)):
        files = repo.types.get(typ)
        if not files or len(files) <= max_files:
            continue
        hits = {f for f in files if f != own_file and member in repo.cs_words.get(f, ())}
        if 0 < len(hits) <= MAX_MEMBER_FILES:
            found |= hits
    return found


def overflow_type_files(text: str, repo: Repo, own_file: str, own_types: set[str], max_files: int) -> set[str]:
    """Files of the types the type-name rule dropped for being declared in more than `max_files` files (partial
    classes). Only a class that would otherwise be a hole takes them: more files, never fewer."""
    found: set[str] = set()
    for name in set(IDENT.findall(text)):
        if name in own_types:
            continue
        files = repo.types.get(name)
        if files and len(files) > max_files:
            found |= files
    found.discard(own_file)
    return found


def test_references(root: str, repo: Repo) -> dict[str, set[str]]:
    """Test class name -> the test source files that name it (another file than its own). A class named in a test
    file is pinned by it, so a change to that file selects the class: the Darling and Lite twin tests name each other
    in crefs and comments, and a sibling class in the same live store is named by the class that extends it."""
    decl_files: dict[str, set[str]] = {}
    texts: dict[str, str] = {}
    for rel in repo.tracked:
        if not rel.endswith(".cs") or not any(rel.startswith(d + "/") for d in SUITE_DIRS.values()):
            continue
        text = read_text(root, rel)
        texts[rel] = text
        for cls in CLASS_DECL.findall(text):
            decl_files.setdefault(cls, set()).add(rel)
    refs: dict[str, set[str]] = {}
    for rel, text in texts.items():
        for name in set(IDENT.findall(text)):
            own = decl_files.get(name)
            if own and rel not in own:
                refs.setdefault(name, set()).add(rel)
    return refs


def xaml_siblings(path: str, repo: Repo) -> set[str]:
    """`Foo.xaml.cs` and generated `Foo.g.cs` stand for `Foo.xaml`: a change to the markup changes what they run."""
    out: set[str] = set()
    if path.endswith(".xaml.cs"):
        if path[:-3] in repo.tracked_set:
            out.add(path[:-3])
    elif path.endswith((".g.cs", ".g.i.cs")):
        stem = path.rsplit("/", 1)[-1].split(".g", 1)[0]
        out.update(repo.by_basename.get(stem + ".xaml", []))
    return out


def load_shards(directory: str) -> dict[str, list[dict]]:
    shards: dict[str, list[dict]] = {}
    for path in sorted(glob.glob(os.path.join(directory, "**", "shard*.json"), recursive=True)):
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        shards.setdefault(data["suite"], []).append(data)
    return shards


def build_map(root: str, tracked: list[str], shards: dict[str, list[dict]], sha: str, built_at: str, tool: str,
              max_ident_files: int) -> tuple[dict, dict]:
    """Returns (the map, the facts the summary prints)."""
    repo = Repo(root, tracked)
    refs = test_references(root, repo)
    suites = {}
    patterns_out: dict[str, "list[str] | str"] = {}
    used: set[str] = set()
    facts = {"suites": {}, "holes": [], "skipped_all": [], "unlisted_contexts": 0}
    for suite, test_dir in SUITE_DIRS.items():
        parts = shards.get(suite, [])
        if not parts:
            raise SystemExit("no shard reports for suite " + suite)
        # simple name -> merged coverage over every shard / retry that reported it
        merged: dict[str, dict] = {}
        for part in parts:
            for full, row in part["classes"].items():
                m = merged.setdefault(simple_name(full), {"files": set(), "seconds": 0.0, "tests": 0, "skipped": 0,
                                                          "seen": False})
                m["files"].update(row["files"])
                m["seconds"] = max(m["seconds"], row["seconds"])  # a retry re-runs the class: keep the longer, not the sum
                m["tests"] = max(m["tests"], row["tests"])
                m["skipped"] = max(m["skipped"], row["skipped"])
                m["seen"] = m["seen"] or row["seen"]
        sources: dict[str, dict] = {}  # simple class name -> {own files, ident files, patterns...}
        for rel in repo.tracked:
            if not rel.startswith(test_dir + "/") or not rel.endswith(".cs"):
                continue
            text = read_text(root, rel)
            decls = CLASS_DECL.findall(text)
            if not decls:
                continue
            own_types = set(TYPE_DECL.findall(text))
            guard = bool(GUARD_MARKER.search(text))
            info = analyze_source(text, repo, rel, own_types)
            idents = type_files(text, repo, rel, own_types, max_ident_files) | member_files(text, repo, rel, max_ident_files)
            over = overflow_type_files(text, repo, rel, own_types, max_ident_files)
            for cls in decls:
                s = sources.setdefault(cls, {"own": set(), "ident": set(), "patterns": {}, "tree": False, "guard": False,
                                             "overflow": set()})
                s["own"].add(rel)
                s["ident"] |= idents
                s["overflow"] |= over
                s["guard"] = s["guard"] or guard
                s["tree"] = s["tree"] or info["tree"]
                for pat in info["patterns"]:
                    s["patterns"][pat] = None
        entries: dict[str, dict] = {}
        covered = holes = guards = 0
        for cls, m in sorted(merged.items()):
            src = sources.get(cls)
            files: set[str] = set()
            for f in m["files"]:
                f = norm(f)
                if f in repo.tracked_set:
                    files.add(f)
                    files |= xaml_siblings(f, repo)
                else:
                    files |= xaml_siblings(f, repo)
            own = sorted(src["own"]) if src else []
            ident = set(src["ident"]) if src else set()
            files |= ident
            files -= set(own)
            pats: "list[str] | str" = []
            if src and not src["guard"]:
                named_by = sorted(refs.get(cls, set()) - set(own))
                pats = "tree" if src["tree"] else list(dict.fromkeys(list(src["patterns"]) + named_by))
                # A class that would be a hole (no product file, no pattern) takes the files of the partial types it
                # names, however many there are; past the cap it reads the tree. More files, never fewer.
                if not pats and not any(not TEST_DIR.search(f) for f in files) and src["overflow"]:
                    if len(src["overflow"]) <= MAX_OVERFLOW_FILES:
                        files |= src["overflow"] - set(own)
                    else:
                        pats = "tree"
            used.update(files)
            used.update(own)
            entry = {"own": own, "files": sorted(files), "seconds": round(m["seconds"], 3)}
            entries[cls] = entry
            if pats:
                key = patterns_out.get(cls)
                if pats == "tree" or key == "tree":
                    patterns_out[cls] = "tree"
                else:
                    patterns_out[cls] = sorted(set(pats) | set(key or []))
            # A test method's own body and the shared test helpers are always visited; only a product file counts.
            has_cov = any(not TEST_DIR.search(f) for f in files)
            if src and src["guard"]:
                guards += 1
            elif has_cov or pats:
                covered += 1
            else:
                holes += 1
                facts["holes"].append((suite, cls, "every test skipped" if m["tests"] and m["skipped"] == m["tests"]
                                       else "no test context" if not m["seen"] else "no product file"))
            if m["tests"] and m["skipped"] == m["tests"]:
                facts["skipped_all"].append((suite, cls))
        suites[suite] = entries
        facts["suites"][suite] = {"classes": len(entries), "covered": covered, "guard": guards, "holes": holes,
                                  "shards": len(parts), "tests_exit": [p.get("tests_exit") for p in parts],
                                  "listed": sum(len(p["listed"]) for p in parts)}
    universe = sorted(used)
    index = {f: i for i, f in enumerate(universe)}
    for entries in suites.values():
        for e in entries.values():
            own = [index[f] for f in e["own"]]
            e["own"] = own[0] if len(own) == 1 else own
            e["files"] = [index[f] for f in e["files"]]
    result = {"schema": SCHEMA, "sha": sha, "built_at": built_at, "tool": tool, "files": universe, "classes": suites,
              "text_patterns": dict(sorted(patterns_out.items()))}
    # Product .cs files no class reaches: a change to one makes the PR gate run everything (FULL), which is the safe answer.
    known = set(universe)
    unreached = [f for f in repo.tracked if f.endswith(".cs") and f not in known and not TEST_DIR.search(f)]
    facts["unreached_product_files"] = len(unreached)
    facts["tracked_cs"] = sum(1 for f in repo.tracked if f.endswith(".cs"))
    return result, facts


def summary_text(test_map: dict, facts: dict, gz_bytes: int, raw_bytes: int) -> str:
    lines = ["### Test map", "",
             "sha `%s`, built %s, tool `%s`." % (test_map["sha"][:10], test_map["built_at"], test_map["tool"]), "",
             "| suite | shards | classes listed | in the map | covered | Guard | holes |", "|---|---|---|---|---|---|---|"]
    for suite, f in facts["suites"].items():
        lines.append("| %s | %d | %d | %d | %d | %d | %d |" % (suite, f["shards"], f["listed"], f["classes"], f["covered"],
                                                               f["guard"], f["holes"]))
    patterns = test_map["text_patterns"]
    lines += ["", "- files in the universe: %d (of %d tracked .cs files; %d product .cs files no class reaches, so a change to "
              "one is a FULL run)" % (len(test_map["files"]), facts["tracked_cs"], facts["unreached_product_files"]),
              "- text patterns: %d classes (%d read the whole tree)" % (len(patterns), sum(1 for v in patterns.values() if v == "tree")),
              "- size: %d bytes gzip, %d bytes raw" % (gz_bytes, raw_bytes),
              "- classes whose every test skipped: %d" % len(facts["skipped_all"]),
              "- holes (no files and no pattern): %d" % len(facts["holes"])]
    if facts["holes"]:
        lines += ["", "<details><summary>holes</summary>", ""]
        lines += ["- %s %s (%s)" % hole for hole in facts["holes"][:200]]
        lines += ["", "</details>"]
    return "\n".join(lines) + "\n"


def cmd_build(args) -> int:
    root = os.path.abspath(args.root)
    tracked = tracked_files(root, args.tracked)
    shards = load_shards(args.shards)
    for item in args.expect:
        suite, _, count = item.partition("=")
        have = sorted({p["shard"] for p in shards.get(suite, [])})
        if have != list(range(int(count))):
            print("suite %s: expected shards 0..%d, found %s - no map written" % (suite, int(count) - 1, have),
                  file=sys.stderr)
            return 1
    built_at = args.built_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    test_map, facts = build_map(root, tracked, shards, args.sha, built_at, args.tool, args.max_ident_files)
    raw = json.dumps(test_map, separators=(",", ":")).encode("utf-8")
    with open(args.out, "wb") as handle:
        handle.write(gzip.compress(raw, 9, mtime=0))
    size = os.path.getsize(args.out)
    text = summary_text(test_map, facts, size, len(raw))
    if args.summary:
        with open(args.summary, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
    print(text)
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = ap.add_subparsers(dest="command", required=True)
    cut = sub.add_parser("cut")
    cut.add_argument("--list", required=True)
    cut.add_argument("--shards", type=int, required=True)
    cut.add_argument("--shard", type=int, required=True)
    cut.add_argument("--budget", type=int, default=30000)
    cut.add_argument("--out", required=True)
    cut.set_defaults(func=cmd_cut)
    rep = sub.add_parser("shard-report")
    rep.add_argument("--suite", required=True)
    rep.add_argument("--shard", type=int, required=True)
    rep.add_argument("--root", required=True)
    rep.add_argument("--listed", required=True)
    rep.add_argument("--report", action="append", default=[])
    rep.add_argument("--xml", action="append", default=[])
    rep.add_argument("--tests-exit", type=int, default=0)
    rep.add_argument("--out", required=True)
    rep.set_defaults(func=cmd_shard_report)
    bld = sub.add_parser("build")
    bld.add_argument("--root", required=True)
    bld.add_argument("--shards", required=True, help="a folder holding every shard.json (searched recursively)")
    bld.add_argument("--sha", required=True)
    bld.add_argument("--built-at")
    bld.add_argument("--tool", default="altcover")
    bld.add_argument("--tracked", help="a file listing the tracked paths (default: git ls-files)")
    bld.add_argument("--expect", action="append", default=[], help="suite=count: refuse to build unless shards 0..count-1 all reported")
    bld.add_argument("--max-ident-files", type=int, default=5)
    bld.add_argument("--out", required=True)
    bld.add_argument("--summary")
    bld.set_defaults(func=cmd_build)
    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
