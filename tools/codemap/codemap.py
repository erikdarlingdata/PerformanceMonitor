#!/usr/bin/env python3
"""The code map generator (#9323). Standard library only.

Reads this repository from its root (the file list comes from `git ls-files`) and writes three files:

  docs/codemap.json   The facts as data, for tools and AI coding agents.
  docs/CODEMAP.md     The same content as Markdown. GitHub shows this one as a page.
  docs/codemap.html   The same content as one static page (inline CSS, no scripts, no outside assets).
                      GitHub shows an .html file as source, so read it from a clone or a docs host.

Two sources feed the map:

  * The repo itself: the projects, the MCP tools, the store migrations, the test projects and the CI workflows.
  * tools/codemap/areas.json: the hand-written layer. Each area has a description and a "start here" list. The script
    fails when a listed folder or file does not exist, so a rename cannot leave a stale pointer.

The output has no timestamp, so a run with no repo change gives byte-identical files.

Usage:
  python tools/codemap/codemap.py           Write the three files.
  python tools/codemap/codemap.py --check   Regenerate in memory. Exit 1 when a file on disk differs, 0 when it matches.
                                            It ignores which commit the files name, because that is always one behind.
"""
import argparse
import html
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AREAS_FILE = ROOT / "tools" / "codemap" / "areas.json"
OUT_JSON = ROOT / "docs" / "codemap.json"
OUT_MD = ROOT / "docs" / "CODEMAP.md"
OUT_HTML = ROOT / "docs" / "codemap.html"

STORE_MIGRATIONS = "Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs"
STORE_DIR = "Darling/PerformanceMonitor.Darling.Storage"
MCP_PRODUCTS = (("Darling", "Darling/"), ("Lite", "Lite/"))
MCP_ATTRIBUTE = re.compile(r'^\s*\[McpServerTool\(\s*Name\s*=\s*"([^"]+)"')
MIGRATION_ROW = re.compile(r"^\s+new Migration\(\s*([^,]+),")
SCHEMA_VERSION = 1


class MapError(Exception):
    """A problem the person running the script can fix. Printed without a traceback."""


def git(*args):
    result = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        raise MapError("git %s failed: %s" % (" ".join(args), result.stderr.strip()))
    return result.stdout


def read_text(rel):
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def all_tracked_files():
    return sorted(f for f in git("ls-files").splitlines() if f)


def tracked_files():
    return [f for f in all_tracked_files() if not f.startswith("deprecated/")]


def project_kind(rel, name, root_element):
    sdk = root_element.get("Sdk", "")
    output = ""
    for node in root_element.iter("OutputType"):
        output = (node.text or "").strip()
    if "Tests" in name or "TestChild" in name:
        return "test"
    if rel.startswith("tools/") or "/tools/" in rel:
        return "tool"
    if output in ("Exe", "WinExe") or sdk.endswith((".Web", ".Worker")):
        return "app"
    return "library"


def find_projects(files):
    projects = []
    for rel in files:
        if not rel.endswith(".csproj"):
            continue
        try:
            root_element = ET.fromstring(read_text(rel).lstrip("﻿"))
        except ET.ParseError as error:
            raise MapError("Cannot read %s: %s" % (rel, error))
        frameworks = []
        for tag in ("TargetFramework", "TargetFrameworks"):
            for node in root_element.iter(tag):
                frameworks.extend(part.strip() for part in (node.text or "").split(";") if part.strip())
        name = rel.rsplit("/", 1)[-1][: -len(".csproj")]
        projects.append({
            "path": rel,
            "name": name,
            "folder": rel.rsplit("/", 1)[0],
            "target_frameworks": sorted(set(frameworks)),
            "kind": project_kind(rel, name, root_element),
        })
    return sorted(projects, key=lambda p: p["path"])


def find_mcp_tools(files):
    tools = []
    for rel in files:
        if not rel.endswith(".cs"):
            continue
        product = next((name for name, prefix in MCP_PRODUCTS if rel.startswith(prefix)), None)
        if product is None or any("Tests" in part for part in rel.split("/")):
            continue
        for line in read_text(rel).splitlines():
            match = MCP_ATTRIBUTE.match(line)
            if match:
                tools.append({"name": match.group(1), "product": product, "file": rel})
    return sorted(tools, key=lambda t: (t["product"], t["name"], t["file"]))


def resolve_version(expression, store_files):
    """A migration row names its version as a number or as Type.Member. Find the number."""
    expression = expression.strip()
    if expression.isdigit():
        return int(expression)
    match = re.fullmatch(r"([A-Za-z_]\w*)\.([A-Za-z_]\w*)", expression)
    if match:
        owner, member = match.groups()
        pattern = re.compile(r"\b(?:const\s+int|int)\s+%s\s*=\s*(\d+)\s*;" % re.escape(member))
        for rel in store_files:
            if rel.rsplit("/", 1)[-1] == owner + ".cs":
                found = pattern.search(read_text(rel))
                if found:
                    return int(found.group(1))
        for rel in store_files:
            text = read_text(rel)
            if re.search(r"\b(?:class|struct)\s+%s\b" % re.escape(owner), text):
                found = pattern.search(text)
                if found:
                    return int(found.group(1))
    raise MapError("Cannot resolve the migration version '%s' in %s. Teach resolve_version about it." % (expression, STORE_MIGRATIONS))


def find_migrations(files):
    if STORE_MIGRATIONS not in files:
        raise MapError("%s is not tracked. Update STORE_MIGRATIONS in codemap.py." % STORE_MIGRATIONS)
    store_files = [f for f in files if f.startswith(STORE_DIR + "/") and f.endswith(".cs")]
    versions = []
    for line in read_text(STORE_MIGRATIONS).splitlines():
        match = MIGRATION_ROW.match(line)
        if match:
            versions.append(resolve_version(match.group(1), store_files))
    if not versions:
        raise MapError("No 'new Migration(' rows found in %s." % STORE_MIGRATIONS)
    return {"file": STORE_MIGRATIONS, "count": len(versions), "newest_version": max(versions)}


def find_tests(files, projects):
    tests = []
    for project in projects:
        if project["kind"] != "test":
            continue
        prefix = project["folder"] + "/"
        count = sum(1 for f in files if f.startswith(prefix) and f.endswith("Tests.cs"))
        tests.append({"project": project["path"], "name": project["name"], "test_classes": count})
    return sorted(tests, key=lambda t: t["project"])


def parse_triggers(text):
    """The trigger names under `on:` of a workflow. No YAML library: the files use a plain layout."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        match = re.match(r"""^["']?on["']?\s*:\s*(.*)$""", line)
        if not match:
            continue
        rest = re.sub(r"\s+#.*$", "", match.group(1)).strip()
        if rest:
            return sorted(t.strip(" \"'") for t in rest.strip("[]{}").split(",") if t.strip(" \"'"))
        names = []
        for following in lines[index + 1:]:
            if following.strip() == "" or following.lstrip().startswith("#"):
                continue
            if not following.startswith(" "):
                break
            key = re.match(r"^  ([A-Za-z_]+)\s*:", following)
            if key:
                names.append(key.group(1))
        return sorted(set(names))
    return []


def find_workflows(files):
    workflows = []
    for rel in files:
        if not (rel.startswith(".github/workflows/") and rel.endswith((".yml", ".yaml"))):
            continue
        text = read_text(rel)
        name = re.search(r"^name:\s*(.+?)\s*$", text, re.MULTILINE)
        workflows.append({
            "file": rel,
            "name": name.group(1).strip("\"'") if name else rel.rsplit("/", 1)[-1],
            "triggers": parse_triggers(text),
        })
    return sorted(workflows, key=lambda w: w["file"])


def load_areas(projects):
    try:
        source = json.loads(AREAS_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise MapError("Cannot read %s: %s" % (AREAS_FILE.relative_to(ROOT), error))
    tracked = set(all_tracked_files())
    problems = []
    areas = []
    for area in source["areas"]:
        folders = area["folders"]
        for folder in folders:
            prefix = folder.rstrip("/") + "/"
            if not any(f.startswith(prefix) for f in tracked):
                problems.append("area '%s': folder '%s' holds no tracked file" % (area["id"], folder))
        for entry in area["start_here"]:
            if entry["path"] not in tracked:
                problems.append("area '%s': start-here file '%s' is not tracked" % (area["id"], entry["path"]))
        if not 3 <= len(area["start_here"]) <= 6 and area["id"] != "deprecated":
            problems.append("area '%s': start_here must list 3 to 6 files" % area["id"])
        under = [p["path"] for p in projects
                 if any(p["folder"] == f.rstrip("/") or p["folder"].startswith(f.rstrip("/") + "/") for f in folders)]
        areas.append({
            "id": area["id"],
            "name": area["name"],
            "description": area["description"],
            "folders": folders,
            "projects": sorted(under),
            "start_here": area["start_here"],
        })
    contributing = read_text("CONTRIBUTING.md")
    for command in source["build_commands"]:
        if command not in contributing:
            problems.append("build command '%s' is not in CONTRIBUTING.md" % command)
    if problems:
        raise MapError("areas.json points at things that are gone:\n  " + "\n  ".join(problems))
    return source["overview"], source["build_commands"], areas


def build_data(commit=None):
    files = tracked_files()
    projects = find_projects(files)
    overview, build_commands, areas = load_areas(projects)
    return {
        "schema": SCHEMA_VERSION,
        "generated_from": commit or git("rev-parse", "--short", "HEAD").strip(),
        "overview": overview,
        "build_commands": build_commands,
        "areas": areas,
        "projects": projects,
        "mcp_tools": find_mcp_tools(files),
        "migrations": find_migrations(files),
        "tests": find_tests(files, projects),
        "ci": find_workflows(files),
    }


# ---------------------------------------------------------------- rendering

KIND_TEXT = {"app": "App", "library": "Library", "test": "Test", "tool": "Tool"}


def counts(data):
    tools = data["mcp_tools"]
    kinds = {}
    for project in data["projects"]:
        kinds[project["kind"]] = kinds.get(project["kind"], 0) + 1
    return {
        "projects": len(data["projects"]),
        "kinds": kinds,
        "darling_tools": sum(1 for t in tools if t["product"] == "Darling"),
        "lite_tools": sum(1 for t in tools if t["product"] == "Lite"),
        "test_classes": sum(t["test_classes"] for t in data["tests"]),
    }


def glance(data):
    c = counts(data)
    m = data["migrations"]
    kinds = ", ".join("%d %s" % (n, k) for k, n in sorted(c["kinds"].items()))
    return [
        "%d projects (%s)." % (c["projects"], kinds),
        "%d Darling MCP tools and %d Lite MCP tools." % (c["darling_tools"], c["lite_tools"]),
        "%d store migrations. The newest is version %d." % (m["count"], m["newest_version"]),
        "%d test classes in %d test projects." % (c["test_classes"], len(data["tests"])),
        "%d CI workflows." % len(data["ci"]),
    ]


def repo_link(path):
    return "../" + path


def md_cell(text):
    return str(text).replace("|", "\\|")


def render_markdown(data):
    out = []
    add = out.append
    add("# Code map")
    add("")
    add("Audience: contributors.")
    add("")
    add("This page is generated from the repository by `tools/codemap/codemap.py`. Do not edit it by hand. "
        "The area text comes from `tools/codemap/areas.json`. The same content is in `docs/codemap.json` for tools "
        "and in `docs/codemap.html` as one static page.")
    add("")
    add("## Overview")
    add("")
    for paragraph in data["overview"]:
        add(paragraph)
        add("")
    add("In numbers:")
    add("")
    for line in glance(data):
        add("- " + line)
    add("")
    add("To build, run these from the repository root:")
    add("")
    add("```")
    for command in data["build_commands"]:
        add(command)
    add("```")
    add("")
    add("## Areas")
    add("")
    add("Each area lists the folders it covers and the files to open first.")
    add("")
    for area in data["areas"]:
        add("### " + area["name"])
        add("")
        add(area["description"])
        add("")
        add("Folders: " + ", ".join("[`%s`](%s)" % (f, repo_link(f.rstrip("/") + "/")) for f in area["folders"]))
        add("")
        add("Start here:")
        add("")
        for entry in area["start_here"]:
            add("- [`%s`](%s): %s" % (entry["path"], repo_link(entry["path"]), entry["note"]))
        add("")
    add("## Projects")
    add("")
    add("| Project | Kind | Target framework |")
    add("|---|---|---|")
    for p in data["projects"]:
        add("| [`%s`](%s) | %s | %s |" % (md_cell(p["path"]), repo_link(p["path"]), KIND_TEXT[p["kind"]],
                                          ", ".join(p["target_frameworks"]) or "none"))
    add("")
    m = data["migrations"]
    add("## Store migrations")
    add("")
    add("Darling's PostgreSQL store has %d migrations. The newest is version %d. The list is in [`%s`](%s). "
        "Version numbers are not gapless, so the count is lower than the newest version." %
        (m["count"], m["newest_version"], m["file"], repo_link(m["file"])))
    add("")
    add("## MCP tools")
    add("")
    add("A tool is a method that carries the `McpServerTool` attribute. Each row names the tool and the file that declares it.")
    add("")
    for product in ("Darling", "Lite"):
        rows = [t for t in data["mcp_tools"] if t["product"] == product]
        add("### %s MCP tools (%d)" % (product, len(rows)))
        add("")
        add("| Tool | Declared in |")
        add("|---|---|")
        for t in rows:
            add("| `%s` | [`%s`](%s) |" % (t["name"], md_cell(t["file"]), repo_link(t["file"])))
        add("")
    add("## Tests")
    add("")
    add("A test class is a file whose name ends in `Tests.cs`.")
    add("")
    add("| Test project | Test classes |")
    add("|---|---|")
    for t in data["tests"]:
        add("| [`%s`](%s) | %d |" % (md_cell(t["project"]), repo_link(t["project"]), t["test_classes"]))
    add("")
    add("## CI")
    add("")
    add("| Workflow | Name | Runs on |")
    add("|---|---|---|")
    for w in data["ci"]:
        add("| [`%s`](%s) | %s | %s |" % (md_cell(w["file"]), repo_link(w["file"]), md_cell(w["name"]),
                                           ", ".join("`%s`" % t for t in w["triggers"]) or "none"))
    add("")
    add("---")
    add("")
    add("Generated from commit `%s`. To refresh it, run `python tools/codemap/codemap.py`." % data["generated_from"])
    add("")
    return "\n".join(out)


CSS = """
:root{--bg:#fff;--fg:#1d2127;--muted:#5b6470;--line:#d9dde3;--soft:#f4f6f8;--accent:#0b5cad;--code:#eef1f4}
@media (prefers-color-scheme:dark){:root{--bg:#14171b;--fg:#e4e7eb;--muted:#9aa3ae;--line:#2c323a;--soft:#1c2025;--accent:#6fb1f2;--code:#23282e}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
main{max-width:60rem;margin:0 auto;padding:1.5rem 16px 3rem}
h1{font-size:1.9rem;margin:0 0 .25rem}
h2{font-size:1.35rem;margin:2.2rem 0 .6rem;padding-top:.6rem;border-top:1px solid var(--line)}
h3{font-size:1.05rem;margin:1.4rem 0 .4rem}
a{color:var(--accent)}
code{font:.88em ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;background:var(--code);padding:.08em .3em;border-radius:3px;overflow-wrap:anywhere}
pre{background:var(--code);padding:.7rem .9rem;border-radius:6px;overflow-x:auto}
pre code{background:none;padding:0}
p.lead,.muted{color:var(--muted)}
nav ul{display:flex;flex-wrap:wrap;gap:.4rem 1rem;list-style:none;padding:0;margin:.8rem 0}
.area{background:var(--soft);border:1px solid var(--line);border-radius:8px;padding:.2rem 1rem .8rem;margin:1rem 0}
.area ul{padding-left:1.2rem;margin:.3rem 0}
.table-wrap{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:.93rem}
th,td{text-align:left;padding:.35rem .6rem;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:600}
footer{margin-top:2.5rem;padding-top:1rem;border-top:1px solid var(--line);color:var(--muted);font-size:.9rem}
""".strip()


def e(text):
    return html.escape(str(text), quote=True)


def a(path, label=None):
    return '<a href="%s"><code>%s</code></a>' % (e(repo_link(path)), e(label or path))


def table(headers, rows):
    head = "".join("<th>%s</th>" % e(h) for h in headers)
    body = "".join("<tr>%s</tr>" % "".join("<td>%s</td>" % cell for cell in row) for row in rows)
    return '<div class="table-wrap"><table><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div>' % (head, body)


def render_html(data):
    out = []
    add = out.append
    add("<!doctype html>")
    add('<html lang="en">')
    add("<head>")
    add('<meta charset="utf-8">')
    add('<meta name="viewport" content="width=device-width, initial-scale=1">')
    add("<title>Code map</title>")
    add("<style>%s</style>" % CSS)
    add("</head>")
    add("<body>")
    add("<main>")
    add("<h1>Code map</h1>")
    add('<p class="lead">Audience: contributors. This page is generated from the repository by '
        "<code>tools/codemap/codemap.py</code>. Do not edit it by hand.</p>")
    add("<nav><ul>")
    for anchor, label in (("overview", "Overview"), ("areas", "Areas"), ("projects", "Projects"), ("migrations", "Store migrations"),
                          ("mcp", "MCP tools"), ("tests", "Tests"), ("ci", "CI")):
        add('<li><a href="#%s">%s</a></li>' % (anchor, label))
    add("</ul></nav>")
    add('<h2 id="overview">Overview</h2>')
    for paragraph in data["overview"]:
        add("<p>%s</p>" % e(paragraph))
    add("<p>In numbers:</p>")
    add("<ul>%s</ul>" % "".join("<li>%s</li>" % e(line) for line in glance(data)))
    add("<p>To build, run these from the repository root:</p>")
    add("<pre><code>%s</code></pre>" % e("\n".join(data["build_commands"])))
    add('<h2 id="areas">Areas</h2>')
    add("<p>Each area lists the folders it covers and the files to open first.</p>")
    for area in data["areas"]:
        add('<section class="area" id="area-%s">' % e(area["id"]))
        add("<h3>%s</h3>" % e(area["name"]))
        add("<p>%s</p>" % e(area["description"]))
        add("<p>Folders: %s</p>" % ", ".join(a(f.rstrip("/") + "/", f) for f in area["folders"]))
        add("<p>Start here:</p>")
        add("<ul>%s</ul>" % "".join("<li>%s: %s</li>" % (a(x["path"]), e(x["note"])) for x in area["start_here"]))
        add("</section>")
    add('<h2 id="projects">Projects</h2>')
    add(table(["Project", "Kind", "Target framework"],
              [[a(p["path"]), e(KIND_TEXT[p["kind"]]), e(", ".join(p["target_frameworks"]) or "none")] for p in data["projects"]]))
    m = data["migrations"]
    add('<h2 id="migrations">Store migrations</h2>')
    add("<p>Darling's PostgreSQL store has %d migrations. The newest is version %d. The list is in %s. "
        "Version numbers are not gapless, so the count is lower than the newest version.</p>" %
        (m["count"], m["newest_version"], a(m["file"])))
    add('<h2 id="mcp">MCP tools</h2>')
    add("<p>A tool is a method that carries the <code>McpServerTool</code> attribute. Each row names the tool and the file that declares it.</p>")
    for product in ("Darling", "Lite"):
        rows = [t for t in data["mcp_tools"] if t["product"] == product]
        add("<h3>%s MCP tools (%d)</h3>" % (e(product), len(rows)))
        add(table(["Tool", "Declared in"], [["<code>%s</code>" % e(t["name"]), a(t["file"])] for t in rows]))
    add('<h2 id="tests">Tests</h2>')
    add("<p>A test class is a file whose name ends in <code>Tests.cs</code>.</p>")
    add(table(["Test project", "Test classes"], [[a(t["project"]), e(t["test_classes"])] for t in data["tests"]]))
    add('<h2 id="ci">CI</h2>')
    add(table(["Workflow", "Name", "Runs on"],
              [[a(w["file"]), e(w["name"]), " ".join("<code>%s</code>" % e(t) for t in w["triggers"]) or "none"] for w in data["ci"]]))
    add("<footer>Generated from commit <code>%s</code>. To refresh it, run <code>python tools/codemap/codemap.py</code>.</footer>" %
        e(data["generated_from"]))
    add("</main>")
    add("</body>")
    add("</html>")
    return "\n".join(out) + "\n"


def render_all(commit=None):
    data = build_data(commit)
    return {
        OUT_JSON: json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        OUT_MD: render_markdown(data),
        OUT_HTML: render_html(data),
    }


def on_disk_commit():
    try:
        return json.loads(OUT_JSON.read_text(encoding="utf-8"))["generated_from"]
    except (OSError, ValueError, KeyError):
        return None


def normalized(raw):
    """Git can check a file out with CRLF line ends (.gitattributes). Compare with LF line ends."""
    return raw.replace(b"\r\n", b"\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate docs/codemap.json, docs/CODEMAP.md and docs/codemap.html.")
    parser.add_argument("--check", action="store_true",
                        help="Regenerate in memory. Exit 1 when a file on disk differs.")
    args = parser.parse_args(argv)
    try:
        # A commit changes HEAD, so the commit named in the files is never HEAD after the files are committed. In --check
        # mode the commit that is already on disk is kept, and only the content is compared.
        commit = on_disk_commit() if args.check else None
        outputs = render_all(commit)
    except MapError as error:
        print("codemap: %s" % error, file=sys.stderr)
        return 2
    if args.check:
        stale = []
        for path, text in outputs.items():
            wanted = text.encode("utf-8")
            if not path.exists() or normalized(path.read_bytes()) != wanted:
                stale.append(path.relative_to(ROOT).as_posix())
        if stale:
            print("The code map is out of date: %s. Run: python tools/codemap/codemap.py" % ", ".join(stale))
            return 1
        print("The code map is up to date.")
        return 0
    for path, text in outputs.items():
        path.write_bytes(text.encode("utf-8"))
        print("wrote %s" % path.relative_to(ROOT).as_posix())
    return 0


if __name__ == "__main__":
    sys.exit(main())
