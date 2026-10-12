#!/usr/bin/env python3
"""AltCover per-test coverage probe helper (#5459 change 4, slice 2).  Standard library only.

The nightly workflow's dispatch-only `altcover-probe` job uses three subcommands:

  select     Pick test classes from a runner class listing (`-list classes/json`), spread over kinds that
             come from the class's source text.  Profile `darling`: plain unit classes, TestHost / web classes
             (a TestServer / WebApplicationFactory mention) and live PostgreSQL classes (a DARLING_TEST_PG
             mention).  Profile `lite`: plain classes, DuckDB-backed classes and classes that start collectors
             (a RemoteCollectorService mention).  Stage=Guard classes are skipped because they read source text
             and never run product code.  Deterministic: the sorted candidates are sampled at even steps.

  unwrap-skips  An instrumented run reports a dynamic skip as an AggregateException failure (see the function).
             This rewrites the instrumented xunit report so those tests are skips again, but only for tests
             the plain run also skipped.

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
LITE_COLLECTOR_MARKERS = ("RemoteCollectorService",)
LITE_DUCKDB_MARKERS = ("DuckDB", "DuckDb", "LocalDataService")
# Default per-kind sample counts (the rest of --total is plain).
PROFILE_WANTS = {"darling": {"web": 25, "live": 12}, "lite": {"collector": 8, "duckdb": 20}}
SKIP_TOKEN = "$XunitDynamicSkip$"
TEXT_READ = re.compile(r"File\.ReadAll|RepoRoot|ReadAllText|ReadLines\(")
REFLECTION = re.compile(r"GetCustomAttribute|GetMethod\(|GetTypes\(|GetProperties\(|GetFields\(|typeof\(")
GUARD_MARKER = re.compile(r"Trait\(\s*\"Stage\"\s*,\s*\"Guard\"\s*\)")


def norm(path: str) -> str:
    return path.replace("\\", "/")


def simple_name(full: str) -> str:
    return re.split(r"[.+/]", full)[-1]


def classify(text, profile):
    if GUARD_MARKER.search(text):
        return "guard"
    if profile == "lite":
        if any(marker in text for marker in LITE_COLLECTOR_MARKERS):
            return "collector"
        if any(marker in text for marker in LITE_DUCKDB_MARKERS):
            return "duckdb"
        return "plain"
    if any(marker in text for marker in WEB_MARKERS):
        return "web"
    if LIVE_MARKER in text and RUNTIME_MARKER not in text:
        return "live"
    return "plain"


def scan_sources(dirs, root, profile="darling"):
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
                kind = classify(text, profile)
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
    sources = scan_sources(args.tests_dir, args.root, args.profile)
    wants = dict(PROFILE_WANTS[args.profile])
    for item in args.want:
        name, _, number = item.partition("=")
        wants[name] = int(number)
    buckets = {"plain": []}
    buckets.update({kind: [] for kind in wants})
    for full in listed:
        info = sources.get(simple_name(full))
        if info and info["kind"] in buckets:
            buckets[info["kind"]].append(full)
    counts = {kind: min(number, len(buckets[kind])) for kind, number in wants.items()}
    counts["plain"] = max(args.total - sum(counts.values()), 0)
    chosen = {kind: spread(buckets[kind], counts[kind]) for kind in buckets}
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
    print("%s: listed %d classes; candidates %s; chose %s (%d total)" % (
        args.profile, len(listed), " ".join("%s=%d" % (k, len(v)) for k, v in sorted(buckets.items())),
        " ".join("%s=%d" % (k, len(v)) for k, v in sorted(chosen.items())), len(lines)))
    return 0 if lines else 1


def failure_message(test):
    failure = test.find("failure")
    if failure is None:
        return ""
    message = failure.find("message")
    return (message.text or "") if message is not None else ""


def cmd_unwrap_skips(args):
    """Turn an instrumented run's wrapped dynamic skips back into skips.

    AltCover rewrites an instrumented async [Fact] so that the method waits on its own task (the failure's stack
    is `Task.Wait` called from the test method itself).  A dynamic skip (`Assert.Skip*`) thrown inside the async
    body therefore surfaces as an AggregateException whose message does not start with the skip token, and
    xunit v3 reports a failure instead of a skip.  The wrap is in the rewritten test method, not in product or test
    source, and 760 Darling test files use dynamic skips, so the report is repaired here instead.

    Guards: a test is converted only when its failure text carries the skip token AND the plain run of the same
    test was a skip.  A test the plain run did not skip, or any other failure, stays a failure.  Nothing is
    ever converted to a pass.  The original report is kept next to the result as `<name>.raw.xml`.

    Without `--plain` (the map producer runs every class once, instrumented, and has no plain run to compare with)
    a failure that carries the skip token is converted on the token alone.  Only `Assert.Skip*` writes that token,
    and the producer reads coverage from the run, not the pass/fail counts, so this cannot hide a real failure."""
    plain_skips = None  # None: no plain run to compare with (the map producer), so the skip token alone decides
    if args.plain:
        plain_skips = set()
        for _, el in ET.iterparse(args.plain, events=("end",)):
            if el.tag == "test":
                if (el.get("result") or "").lower() == "skip":
                    plain_skips.add(el.get("name"))
                el.clear()
    tree = ET.parse(args.instr)
    converted = []
    kept = []
    for test in tree.getroot().iter("test"):
        if (test.get("result") or "").lower() != "fail":
            continue
        message = failure_message(test)
        if SKIP_TOKEN not in message:
            continue
        if plain_skips is not None and test.get("name") not in plain_skips:
            kept.append(test.get("name"))
            continue
        reason = message[message.index(SKIP_TOKEN) + len(SKIP_TOKEN):].split("\n", 1)[0].strip().rstrip(")")
        failure = test.find("failure")
        if failure is not None:
            test.remove(failure)
        test.set("result", "Skip")
        ET.SubElement(test, "reason").text = reason
        converted.append(test.get("name"))
    if converted:
        # Counts live on the collection and assembly elements; move each converted test from failed to skipped.
        moved_names = set(converted)
        for parent in tree.getroot().iter():
            if parent.tag not in ("collection", "assembly"):
                continue
            moved = sum(1 for test in parent.iter("test") if test.get("name") in moved_names)
            if moved and parent.get("failed") is not None and parent.get("skipped") is not None:
                parent.set("failed", str(max(int(parent.get("failed")) - moved, 0)))
                parent.set("skipped", str(int(parent.get("skipped")) + moved))
        raw = os.path.splitext(args.instr)[0] + ".raw.xml"
        if not os.path.exists(raw):
            os.replace(args.instr, raw)
        tree.write(args.instr, encoding="utf-8", xml_declaration=True)
    print("unwrap-skips: %d converted back to skips, %d left failing (skip token, but the plain run did not skip)" % (
        len(converted), len(kept)))
    for name in converted[:12]:
        print("  skip restored: " + name)
    for name in kept[:12]:
        print("  still failing: " + name)
    return 0


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
              "visits": 0, "visits_with_context": 0, "tracked_methods": 0, "tracked_refs": 0, "tracked_uid_collisions": 0, "unresolved_refs": 0, "points_partly_outside_context": 0, "points_fully_in_context": 0, "context_refs_on_visited_points": 0,
              "ref_owner": {}}
    stack = []
    names: dict[str, str] = {}
    resolved: set = set()
    files = {}
    pairs = set()
    mod_stats: dict = {}  # file uid -> [visits, visits with a context], per module (uids repeat across modules)
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
                # A point whose per-context visit counts add up to less than its total was also reached
                # outside every context (a constructor, a fixture, a pool thread started by the test).
                totals["points_partly_outside_context" if ref_vc < vc else "points_fully_in_context"] += 1
                totals["context_refs_on_visited_points"] += ref_count
            stat = mod_stats.setdefault(file, [0, 0])
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
                mod_stats = {}
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
                for fuid, (visit_count, ctx_count) in mod_stats.items():
                    if fuid in files:
                        stat = file_stats.setdefault(files[fuid], [0, 0])
                        stat[0] += visit_count
                        stat[1] += ctx_count
                # TrackedMethod uids are numbered across the whole report, and a product module's points
                # refer to test methods listed in the TEST module, so names are resolved after the last
                # module rather than inside the module that holds the reference.
                for tm in el.iter("TrackedMethod"):
                    uid, name = tm.get("uid"), tm.get("name") or ""
                    if uid in names and names[uid] != name:
                        totals["tracked_uid_collisions"] += 1
                    names[uid] = name
                for uid, fuid in pairs:
                    if fuid in files:
                        resolved.add((uid, files[fuid]))
                el.clear()
            stack.pop()
    for uid, full in resolved:
        name = names.get(uid)
        if not name:
            totals["unresolved_refs"] += 1
            continue
        if full.lower().startswith(root_norm):
            ctx_files.setdefault(class_of_tracked(name), set()).add(full[len(root_norm):])
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


def hole_detail(info, visited, root):
    """Why a class has no product file: what it did visit, and what its own source text suggests."""
    detail = {"kind": info.get("kind"), "test_files_visited": sorted(visited)[:6]}
    path = os.path.join(root, info.get("file") or "")
    try:
        with open(path, encoding="utf-8-sig", errors="replace") as handle:
            text = handle.read()
    except OSError:
        return detail
    detail["reads_source_text"] = bool(TEXT_READ.search(text))
    detail["uses_reflection_or_typeof"] = bool(REFLECTION.search(text))
    detail["needs_pg_runtime"] = RUNTIME_MARKER in text
    return detail


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
    holes = {"no_context": [], "no_product_files": [], "detail": {}}
    per_kind = {}
    for cls, info in sorted(selected.items()):
        got = ctx_files.get(cls)
        if got is None:
            holes["no_context"].append(cls)
            continue
        product = sorted(index[f] for f in got if f in index)
        if not product:
            holes["no_product_files"].append(cls)
            holes["detail"][cls] = hole_detail(info, got, args.root)
        classes[cls] = {"own": info.get("file"), "files": product,
                        "seconds": round(instr_seconds.get(cls, plain_seconds.get(cls, 0.0)), 3)}
        per_kind.setdefault(info["kind"], []).append(len(product))
    unselected = sorted(c for c in ctx_files if c not in selected)

    def stat(values):
        if not values:
            return None
        return {"n": len(values), "min": min(values), "median": statistics.median(values), "max": max(values)}

    # Visits to product files only: test-method bodies are always inside a context by construction, so the
    # overall share flatters the tool.  `outside_root` is source the checkout does not hold (generated code).
    visits = with_ctx = test_visits = outside_visits = 0
    for path, (visit_count, ctx_count) in file_stats.items():
        if not path.lower().startswith(root_prefix):
            outside_visits += visit_count
        elif is_test_file(path[len(root_prefix):]):
            test_visits += visit_count
        else:
            visits += visit_count
            with_ctx += ctx_count
    weakest = sorted(
        ((f, v, c) for f, (v, c) in file_stats.items()
         if f.lower().startswith(root_prefix) and v >= 5 and not is_test_file(f[len(root_prefix):])),
        key=lambda row: (row[2] / row[1], -row[1]))[:25]
    # File-level view, the one the map cares about: a product file the run visited but never attributed
    # to any test class cannot select a single class.
    product_files = {f[len(root_prefix):]: v for f, (v, c) in file_stats.items()
                     if f.lower().startswith(root_prefix) and not is_test_file(f[len(root_prefix):]) and v > 0}
    attributed = set(universe)
    unattributed = sorted(((v, f) for f, v in product_files.items() if f not in attributed), reverse=True)
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
            "product_visits": visits, "product_visits_with_context": with_ctx,
            "share_with_context": round(with_ctx / visits, 4) if visits else None,
            "test_file_visits": test_visits, "outside_root_visits": outside_visits,
            "points_partly_outside_context": totals["points_partly_outside_context"],
            "points_fully_in_context": totals["points_fully_in_context"],
            "contexts_per_context_bearing_point": round(
                totals["context_refs_on_visited_points"] / totals["points_visited_with_context"], 2)
            if totals["points_visited_with_context"] else None,
            "unresolved_refs": totals["unresolved_refs"], "tracked_uid_collisions": totals["tracked_uid_collisions"],
            "tracked_methods": totals["tracked_methods"], "tracked_refs": totals["tracked_refs"],
            "tracked_ref_owner_tags": totals["ref_owner"],
        },
        "files_visited_by_any_code": len(product_files),
        "files_attributed_to_a_class": len(attributed & set(product_files)),
        "files_visited_never_attributed": len(unattributed),
        "never_attributed_top": [{"file": f, "visits": v} for v, f in unattributed[:40]],
        "weakest_context_files": [{"file": f[len(root_prefix):], "visits": v, "with_context": c}
                                  for f, v, c in weakest],
        "timing_seconds": {"plain": plain_total, "instrumented": instr_total,
                           "ratio": round(instr_total / plain_total, 2) if plain_total and instr_total else None,
                           "instrument_step": timing.get("instrument_seconds"),
                           "line_and_branch_mode": {k: v for k, v in timing.items() if k.startswith("line_")},
                           "method_mode_report_bytes": os.path.getsize(args.report),
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
    sel.add_argument("--profile", choices=sorted(PROFILE_WANTS), default="darling")
    sel.add_argument("--want", action="append", default=[], help="kind=count, overrides the profile default")
    sel.add_argument("--list", required=True)
    sel.add_argument("--tests-dir", action="append", required=True)
    sel.add_argument("--root", required=True)
    sel.add_argument("--total", type=int, default=100)
    sel.add_argument("--out", required=True)
    sel.add_argument("--manifest", required=True)
    sel.set_defaults(func=cmd_select)
    unwrap = sub.add_parser("unwrap-skips")
    unwrap.add_argument("--plain", help="the plain run's xunit report; without it the skip token alone converts")
    unwrap.add_argument("--instr", required=True)
    unwrap.set_defaults(func=cmd_unwrap_skips)
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
