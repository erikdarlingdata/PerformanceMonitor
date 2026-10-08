#!/usr/bin/env python3
"""AltCover per-test coverage probe helper (#5459 change 4, slice 2).  Standard library only.

The nightly workflow's dispatch-only `altcover-probe` job uses two subcommands:

  select     Pick about 100 test classes from a runner class listing (`-list classes/json`), spread over
             three kinds: plain unit classes, TestHost / web classes, and live PostgreSQL classes.  The kind
             comes from the class's source text (a TestServer / WebApplicationFactory mention is web, a
             DARLING_TEST_PG mention is live; Stage=Guard classes are skipped because they read source text
             and never run product code).  Deterministic: the sorted candidates are sampled at even steps.

  summarize  Turn an AltCover OpenCover report (`--callContext=[Fact] --callContext=[Theory]`) into
             (a) a summary of the probe: overhead, share of visits that carry a test context, holes, and
             (b) the schema-1 `classes` section of the test map the plan describes:
                 {schema, tool, files:[universe], classes:{suite:{Class:{own, files:[ids], seconds}}}}
             `own` is the repo-relative file that declares the class; `files` are indexes into the universe
             of product source files the class's tests visited; `seconds` is the class's summed test time.

Both subcommands only read files; nothing here talks to a server.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
import xml.etree.ElementTree as ET

SCHEMA = 1
CLASS_DECL = re.compile(r"\bclass\s+([A-Za-z_][A-Za-z0-9_]*)")
WEB_MARKERS = ("WebApplicationFactory", "UseTestServer", "TestServer", "Microsoft.AspNetCore.TestHost")
LIVE_MARKER = "DARLING_TEST_PG"
RUNTIME_MARKER = "DARLING_TEST_PGRUNTIME"
GUARD_MARKER = re.compile(r"Trait\(\s*\"Stage\"\s*,\s*\"Guard\"\s*\)")


def norm(path: str) -> str:
    return path.replace("\\", "/")


def simple_name(full: str) -> str:
    return re.split(r"[.+/]", full)[-1]


def scan_sources(dirs, root):
    """Map simple class name -> {file, kind} for every test source file under `dirs`."""
    found = {}
    for base in dirs:
        for folder, subdirs, names in os.walk(base):
            subdirs[:] = [d for d in subdirs if d not in ("bin", "obj", ".git", "node_modules")]
            for name in names:
                if not name.endswith(".cs"):
                    continue
                path = os.path.join(folder, name)
                try:
                    with open(path, encoding="utf-8-sig", errors="replace") as handle:
                        text = handle.read()
                except OSError:
                    continue
                if GUARD_MARKER.search(text):
                    kind = "guard"
                elif any(marker in text for marker in WEB_MARKERS):
                    kind = "web"
                elif LIVE_MARKER in text and RUNTIME_MARKER not in text:
                    kind = "live"
                else:
                    kind = "plain"
                rel = norm(os.path.relpath(path, root))
                for cls in CLASS_DECL.findall(text):
                    found.setdefault(cls, {"file": rel, "kind": kind})
    return found


def spread(names, count):
    names = sorted(names)
    if count <= 0 or not names:
        return []
    if count >= len(names):
        return names
    step = len(names) / count
    return [names[int(i * step)] for i in range(count)]


def cmd_select(args):
    with open(args.list, encoding="utf-8-sig") as handle:
        text = handle.read()
    start = text.index("[")
    listed = json.loads(text[start:text.rindex("]") + 1])
    sources = scan_sources(args.tests_dir, args.root)
    buckets = {"plain": [], "web": [], "live": []}
    for full in listed:
        info = sources.get(simple_name(full))
        if info and info["kind"] in buckets:
            buckets[info["kind"]].append(full)
    want_web = min(args.web, len(buckets["web"]))
    want_live = min(args.live, len(buckets["live"]))
    want_plain = max(args.total - want_web - want_live, 0)
    chosen = {
        "plain": spread(buckets["plain"], want_plain),
        "web": spread(buckets["web"], want_web),
        "live": spread(buckets["live"], want_live),
    }
    manifest = {}
    lines = []
    for kind, names in chosen.items():
        for full in names:
            manifest[full] = {"kind": kind, "file": sources[simple_name(full)]["file"]}
            lines.append(full)
    with open(args.out, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")
    with open(args.manifest, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, indent=1, sort_keys=True)
    print("listed %d classes; candidates plain=%d web=%d live=%d; chose plain=%d web=%d live=%d (%d total)" % (
        len(listed), len(buckets["plain"]), len(buckets["web"]), len(buckets["live"]),
        len(chosen["plain"]), len(chosen["web"]), len(chosen["live"]), len(lines)))
    return 0 if lines else 1


def class_of_tracked(name: str) -> str:
    """`System.Void Ns.Outer/Inner::Method(System.String)` -> `Ns.Outer+Inner` (the declaring type)."""
    left = name.split("::", 1)[0]
    return left.split(" ")[-1].replace("/", "+")


def parse_report(path, root):
    """Stream an OpenCover report.  Returns (context -> set(files), file stats, totals)."""
    root_norm = norm(root).rstrip("/").lower() + "/"
    ctx_files: dict[str, set] = {}
    file_stats: dict[str, list] = {}  # file -> [visits, visits with a context]
    totals = {"modules": 0, "methods": 0, "points": 0, "points_visited": 0, "points_visited_with_context": 0,
              "visits": 0, "visits_with_context": 0, "tracked_methods": 0, "tracked_refs": 0,
              "ref_owner": {}}
    stack = []
    files = {}
    pairs = set()
    method_file = None
    point = None  # [file, vc, ref_vc, ref_count]

    def finish_point():
        nonlocal point
        if point is None:
            return
        file, vc, ref_vc, ref_count = point
        totals["points"] += 1
        if vc > 0:
            totals["points_visited"] += 1
            totals["visits"] += vc
            totals["visits_with_context"] += min(ref_vc, vc) if ref_count else 0
            if ref_count:
                totals["points_visited_with_context"] += 1
            stat = file_stats.setdefault(file, [0, 0])
            stat[0] += vc
            stat[1] += min(ref_vc, vc) if ref_count else 0
        point = None

    for event, el in ET.iterparse(path, events=("start", "end")):
        tag = el.tag
        if event == "start":
            stack.append(tag)
            if tag == "Module":
                totals["modules"] += 1
                files = {}
                pairs = set()
            elif tag == "File":
                files[el.get("uid")] = norm(el.get("fullPath") or "")
            elif tag == "Method":
                totals["methods"] += 1
                method_file = None
            elif tag == "FileRef" and "Method" in stack:
                method_file = el.get("uid")
            elif tag in ("SequencePoint", "MethodPoint"):
                finish_point()
                point = [el.get("fileid") or method_file, int(el.get("vc") or 0), 0, 0]
            elif tag == "TrackedMethodRef":
                uid = el.get("uid")
                vc = int(el.get("vc") or 0)
                owner = stack[-3] if len(stack) >= 3 else "?"
                totals["tracked_refs"] += 1
                totals["ref_owner"][owner] = totals["ref_owner"].get(owner, 0) + 1
                file = (point[0] if point is not None else None) or method_file
                if point is not None:
                    point[2] += vc
                    point[3] += 1
                pairs.add((uid, file))
        else:
            if tag in ("SequencePoint", "MethodPoint"):
                finish_point()
            elif tag == "Method":
                finish_point()
                el.clear()
            elif tag == "TrackedMethod":
                totals["tracked_methods"] += 1
            elif tag == "TrackedMethods":
                pass
            elif tag == "Module":
                names = {}
                for tm in el.iter("TrackedMethod"):
                    names[tm.get("uid")] = tm.get("name") or ""
                for uid, fuid in pairs:
                    name = names.get(uid)
                    if not name or fuid not in files:
                        continue
                    full = files[fuid]
                    low = full.lower()
                    if not low.startswith(root_norm):
                        continue
                    ctx_files.setdefault(class_of_tracked(name), set()).add(full[len(root_norm):])
                el.clear()
            stack.pop()
    return ctx_files, file_stats, totals, root_norm


def xunit_times(path):
    """Per-class seconds and outcome counts from an xunit `-xml` report (v2 and v3 shapes)."""
    seconds: dict[str, float] = {}
    counts = {"pass": 0, "fail": 0, "skip": 0}
    if not path or not os.path.exists(path):
        return seconds, counts
    for _, el in ET.iterparse(path, events=("end",)):
        if el.tag == "test":
            cls = (el.get("type") or "").replace("/", "+")
            try:
                seconds[cls] = seconds.get(cls, 0.0) + float(el.get("time") or 0)
            except ValueError:
                pass
            result = (el.get("result") or "").lower()
            counts["pass" if result == "pass" else "fail" if result == "fail" else "skip"] += 1
            el.clear()
    return seconds, counts


def cmd_summarize(args):
    ctx_files, file_stats, totals, root_prefix = parse_report(args.report, args.root)
    with open(args.manifest, encoding="utf-8") as handle:
        manifest = json.load(handle)
    plain_seconds, plain_counts = xunit_times(args.plain_xml)
    instr_seconds, instr_counts = xunit_times(args.instr_xml)
    timing = {}
    if args.timing and os.path.exists(args.timing):
        with open(args.timing, encoding="utf-8-sig") as handle:
            timing = json.load(handle)

    test_prefixes = tuple(p.strip("/") + "/" for p in args.test_dir_prefix)

    def is_test_file(rel):
        return rel.startswith(test_prefixes)

    # Product-file universe and per-class rows.
    universe = sorted({f for fs in ctx_files.values() for f in fs if not is_test_file(f)})
    index = {f: i for i, f in enumerate(universe)}
    selected = {cls.replace("/", "+"): info for cls, info in manifest.items()}
    classes = {}
    holes = {"no_context": [], "no_product_files": []}
    per_kind = {}
    for cls, info in sorted(selected.items()):
        got = ctx_files.get(cls)
        if got is None:
            holes["no_context"].append(cls)
            continue
        product = sorted(index[f] for f in got if f in index)
        if not product:
            holes["no_product_files"].append(cls)
        classes[cls] = {"own": info.get("file"), "files": product,
                        "seconds": round(instr_seconds.get(cls, plain_seconds.get(cls, 0.0)), 3)}
        per_kind.setdefault(info["kind"], []).append(len(product))
    unselected = sorted(c for c in ctx_files if c not in selected)

    def stat(values):
        if not values:
            return None
        return {"n": len(values), "min": min(values), "median": statistics.median(values), "max": max(values)}

    visits = totals["visits"]
    with_ctx = totals["visits_with_context"]
    weakest = sorted(
        ((f, v, c) for f, (v, c) in file_stats.items()
         if f.lower().startswith(root_prefix) and v >= 5 and not is_test_file(f[len(root_prefix):])),
        key=lambda row: (row[2] / row[1], -row[1]))[:25]
    plain_total = timing.get("plain_seconds")
    instr_total = timing.get("instrumented_seconds")
    summary = {
        "schema": SCHEMA,
        "tool": args.tool,
        "classes_selected": len(selected),
        "classes_with_files": len(classes) - len(holes["no_product_files"]),
        "holes": holes,
        "contexts_for_unselected_classes": unselected[:50],
        "files_in_universe": len(universe),
        "files_per_class": {kind: stat(vals) for kind, vals in sorted(per_kind.items())},
        "visits": {
            "points_in_report": totals["points"], "points_visited": totals["points_visited"],
            "points_visited_with_context": totals["points_visited_with_context"],
            "visits": visits, "visits_with_context": with_ctx,
            "share_with_context": round(with_ctx / visits, 4) if visits else None,
            "tracked_methods": totals["tracked_methods"], "tracked_refs": totals["tracked_refs"],
            "tracked_ref_owner_tags": totals["ref_owner"],
        },
        "weakest_context_files": [{"file": f[len(root_prefix):], "visits": v, "with_context": c}
                                  for f, v, c in weakest],
        "timing_seconds": {"plain": plain_total, "instrumented": instr_total,
                           "ratio": round(instr_total / plain_total, 2) if plain_total and instr_total else None,
                           "instrument_step": timing.get("instrument_seconds"),
                           "xunit_plain_sum": round(sum(plain_seconds.values()), 1),
                           "xunit_instrumented_sum": round(sum(instr_seconds.values()), 1)},
        "outcomes": {"plain": plain_counts, "instrumented": instr_counts},
    }
    with open(args.summary_out, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(summary, handle, indent=1)
    suite = args.suite
    with open(args.map_out, "w", encoding="utf-8", newline="\n") as handle:
        json.dump({"schema": SCHEMA, "tool": args.tool, "files": universe, "classes": {suite: classes}},
                  handle, separators=(",", ":"))
    print(json.dumps(summary, indent=1)[:6000])
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sel = sub.add_parser("select")
    sel.add_argument("--list", required=True)
    sel.add_argument("--tests-dir", action="append", required=True)
    sel.add_argument("--root", required=True)
    sel.add_argument("--total", type=int, default=100)
    sel.add_argument("--web", type=int, default=25)
    sel.add_argument("--live", type=int, default=12)
    sel.add_argument("--out", required=True)
    sel.add_argument("--manifest", required=True)
    sel.set_defaults(func=cmd_select)
    summ = sub.add_parser("summarize")
    summ.add_argument("--report", required=True)
    summ.add_argument("--root", required=True)
    summ.add_argument("--manifest", required=True)
    summ.add_argument("--plain-xml")
    summ.add_argument("--instr-xml")
    summ.add_argument("--timing")
    summ.add_argument("--suite", default="Darling.Tests")
    summ.add_argument("--tool", default="altcover")
    summ.add_argument("--test-dir-prefix", action="append", default=["Darling/Darling.Tests", "Lite.Tests"])
    summ.add_argument("--summary-out", required=True)
    summ.add_argument("--map-out", required=True)
    summ.set_defaults(func=cmd_summarize)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
