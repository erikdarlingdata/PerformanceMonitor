#!/usr/bin/env python3
"""Checks for fork-ci-result.py (#8188). Run: python -I .github/scripts/tests/fork-ci-result-test.py

Every fact the decision reads (the finished run's jobs, the open pull requests at the run's head, their files, the newer runs,
the reviews and each reviewer's permission) comes through a stub fetcher keyed on the API path, so no check talks to GitHub.
The stub records every call, so a check can also say which reads a decision must not make and which parameters a read carries."""
import json
import importlib.util
import os
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
script = os.path.join(here, "..", "fork-ci-result.py")
failures = []

if not os.path.exists(script):
    failures.append("fork-ci-result.py: the script does not exist, so none of the checks below can pass")
    print("\n".join(failures))
    sys.exit(1)

spec = importlib.util.spec_from_file_location("fr", script)
fr = importlib.util.module_from_spec(spec)
sys.modules["fr"] = fr
spec.loader.exec_module(fr)


def check(name, actual, expected):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


REPO = "owner/repo"
FORK = "someone/repo"
SHA = "a" * 40
OTHER_SHA = "b" * 40
RUN_ID = 500
PR_PATH = ".github/workflows/pr.yml"


def run_object(run_event="pull_request", head_repo=FORK, head_sha=SHA, branch="patch-1", run_id=RUN_ID, conclusion="success",
               path=PR_PATH):
    """A workflow run as the event payload and the runs API describe it. head_repo None leaves the head repository out."""
    run = {"id": run_id, "event": run_event, "head_sha": head_sha, "head_branch": branch, "path": path, "conclusion": conclusion}
    if head_repo is not None:
        run["head_repository"] = {"full_name": head_repo, "owner": {"login": head_repo.split("/")[0]}}
    return run


def event(repo=REPO, **kw):
    return {"workflow_run": run_object(**kw), "repository": {"full_name": repo}}


def dispatch_event(run_id="500", repo=REPO):
    return {"inputs": {"run_id": run_id}, "repository": {"full_name": repo}}


class Stub:
    """fetch(path, params, paginate) answers from a dict of path -> JSON, records every call, and raises FetchError for a path
    it does not know (a check that reaches an unexpected read fails loudly)."""

    def __init__(self, answers):
        self.answers = answers
        self.asked = []
        self.calls = []

    def __call__(self, path, params=None, paginate=False):
        self.asked.append(path)
        self.calls.append((path, dict(params or {}), paginate))
        if path not in self.answers:
            raise fr.FetchError(f"stub has no answer for {path}")
        answer = self.answers[path]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def params_of(self, path):
        return [(p, pg) for (c, p, pg) in self.calls if c == path]


JOBS = f"repos/{REPO}/actions/runs/{RUN_ID}/jobs"
RUN = f"repos/{REPO}/actions/runs/{RUN_ID}"
PULLS = f"repos/{REPO}/pulls"
FILES = f"repos/{REPO}/pulls/7/files"
RUNS = f"repos/{REPO}/actions/workflows/pr.yml/runs"
REVIEWS = f"repos/{REPO}/pulls/7/reviews"


def PERM(login):
    return f"repos/{REPO}/collaborators/{login}/permission"


def jobs(verdict="success", present=True):
    out = [{"name": "CI plan", "conclusion": "success"}, {"name": "Build the test projects", "conclusion": "success"}]
    if present:
        out.append({"name": "CI verdict", "conclusion": verdict})
    return {"jobs": out}


def pull(number=7, sha=SHA, repo=FORK):
    return {"number": number, "head": {"sha": sha, "repo": {"full_name": repo}}}


def runs(*entries):
    return {"workflow_runs": [{"id": i, "head_repository": {"full_name": r}} for i, r in entries]}


def review(login, state="APPROVED", sha=SHA, submitted="2026-11-07T10:00:00Z"):
    return {"user": {"login": login}, "state": state, "commit_id": sha, "submitted_at": submitted}


def perm(permission="write", role_name=None):
    return {"permission": permission, "role_name": role_name or permission}


MAINTAINER = [review("maint")]
PERMS = {"maint": perm("write"), "admin": perm("admin"), "lead": perm("write", "maintain"), "reader": perm("read"),
         "triager": perm("read", "triage")}


def answers(verdict="success", pulls=None, files=(), newer=(), reviews=None, perms=PERMS):
    out = {
        JOBS: jobs(verdict),
        PULLS: [pull()] if pulls is None else pulls,
        FILES: [{"filename": f} for f in files],
        RUNS: runs(*newer),
        REVIEWS: MAINTAINER if reviews is None else reviews,
    }
    for login, p in perms.items():
        out[PERM(login)] = p
    return out


def decide(ev=None, **kw):
    stub = Stub(answers(**kw))
    return fr.decide(ev or event(), stub), stub


# --- the happy path: a green fork run, head unchanged, approved on that sha by a maintainer, writes success --------------------
d, stub = decide()
check("green: writes", d.write, True)
check("green: state", d.state, "success")
check("green: description", d.description, "CI passed")
check("green: sha", d.sha, SHA)
check("green: run id", getattr(d, "run_id", None), str(RUN_ID))
check("green: reads the jobs, the pulls, the files, the runs, the reviews and the reviewer's permission",
      sorted(stub.asked), sorted([JOBS, PULLS, FILES, RUNS, REVIEWS, PERM("maint")]))

# --- a red verdict writes failure, names the conclusion, and reads no reviews ---------------------------------------------------
d, stub = decide(verdict="failure")
check("red: writes", d.write, True)
check("red: state", d.state, "failure")
check("red: description names the conclusion", "failure" in d.description and "CI verdict" in d.description, True)
check("red: reads no reviews", REVIEWS in stub.asked, False)
d, _ = decide(verdict="cancelled")
check("cancelled verdict: state", (d.write, d.state), (True, "failure"))
d, _ = decide(verdict="timed_out")
check("timed out verdict: state", (d.write, d.state), (True, "failure"))
check("description fits a commit status", len(d.description) <= 140, True)

# --- the run's own conclusion must be success too: a verdict job that exited 0 on a failed run does not pass ------------------
d, _ = decide(event(conclusion="failure"))
check("run failed, verdict success: state", (d.write, d.state), (True, "failure"))
check("run failed, verdict success: description names the run", "run" in d.description and "failure" in d.description, True)
d, _ = decide(event(conclusion="cancelled"))
check("run cancelled, verdict success: state", (d.write, d.state), (True, "failure"))
d, stub = decide(event(conclusion=None))
check("run not completed: no write", d.write, False)
check("run not completed: no reads", stub.asked, [])

# --- only a pull request run from a fork is relayed; the local checks read nothing -----------------------------------------------
d, stub = decide(event(run_event="push"))
check("push run: no write", d.write, False)
check("push run: no reads", stub.asked, [])
d, stub = decide(event(run_event="workflow_dispatch"))
check("dispatch run: no write", d.write, False)
d, stub = decide(event(head_repo=REPO))
check("own repository: no write", d.write, False)
check("own repository: reason", "this repository" in d.reason, True)
check("own repository: no reads", stub.asked, [])
d, stub = decide(event(head_repo=None))
check("no head repository: no write", d.write, False)
check("no head repository: no reads", stub.asked, [])
d, stub = decide(event(head_sha="not-a-sha"))
check("head sha that is no sha: no write", d.write, False)
check("head sha that is no sha: no reads", stub.asked, [])
d, stub = decide(event(head_sha=SHA.upper()))
check("upper-case sha: accepted", d.write, True)
d, stub = decide(event(path=".github/workflows/other.yml"))
check("a run of another workflow: no write", d.write, False)
check("a run of another workflow: no reads", stub.asked, [])

# --- a run with no verdict (the plan asked for nothing) writes nothing, as the run itself would ------------------------------------
stub = Stub({JOBS: jobs(present=False)})
d = fr.decide(event(), stub)
check("no verdict job: no write", d.write, False)
check("no verdict job: reason", "CI verdict" in d.reason, True)
check("no verdict job: stops after the jobs read", stub.asked, [JOBS])
stub = Stub({JOBS: jobs(verdict="skipped")})
d = fr.decide(event(), stub)
check("skipped verdict: no write", d.write, False)
stub = Stub({JOBS: jobs(verdict=None)})
d = fr.decide(event(), stub)
check("verdict with no conclusion yet: no write", d.write, False)

# --- the head commit must still be the pull request's head ---------------------------------------------------------------------------
d, stub = decide(pulls=[pull(sha=OTHER_SHA)])
check("head moved: no write", d.write, False)
check("head moved: reason names the sha", SHA[:9] in d.reason, True)
check("head moved: stops before the files and the runs", sorted(stub.asked), sorted([JOBS, PULLS]))
d, _ = decide(pulls=[])
check("no open pull request: no write", d.write, False)
d, _ = decide(pulls=[pull(sha=SHA, repo="third/repo")])
check("same branch name from another fork: no write", d.write, False)
d, _ = decide(pulls=[pull(number=7, sha=SHA), pull(number=8, sha=OTHER_SHA)])
check("one of two pull requests at the sha: writes", d.write, True)

# --- a pull request that changes a file its run executes, or reads to decide what runs, is not vouched for -------------------------
d, stub = decide(files=["Lite/Services/X.cs", ".github/workflows/pr.yml"])
check("workflow change: no write", d.write, False)
check("workflow change: reason names the file", ".github/workflows/pr.yml" in d.reason, True)
check("workflow change: stops before the runs", RUNS in stub.asked, False)
d, _ = decide(files=[".github/workflows/new.yml"])
check("new workflow file: no write", d.write, False)
d, _ = decide(files=["Darling/X.cs", ".github/scripts/ci-plan.py"])
check("the plan script: no write", d.write, False)
check("the plan script: reason names the file", ".github/scripts/ci-plan.py" in d.reason, True)
d, _ = decide(files=[".github/scripts/run-ci-leg.ps1"])
check("the leg script: no write", d.write, False)
d, _ = decide(files=[".github/scripts/tests/ci-plan-test.py"])
check("a script test: no write", d.write, False)
d, _ = decide(files=[".github/darling-paths-filter.yml"])
check("a path filter the plan reads: no write", d.write, False)
d, _ = decide(files=[".github/ci-quarantine.txt"])
check("the quarantine list: no write", d.write, False)
d, _ = decide(files=["Darling/tools/fetch-pg-runtime.ps1"])
check("the PostgreSQL fetch script the legs run: no write", d.write, False)
d, _ = decide(files=["global.json"])
check("the SDK pin the build installs: no write", d.write, False)
d, _ = decide(files=["Directory.Packages.props"])
check("the package versions the restore reads: no write", d.write, False)
d, _ = decide(files=["Darling/Darling.Tests/Darling.Tests.csproj"])
check("a test project file: no write", d.write, False)
d, _ = decide(files=["Lite.Tests/Lite.Tests.csproj"])
check("the Lite test project file: no write", d.write, False)
# the build auto-imports MSBuild files from any folder on the way up to a project, and the restore reads every lock file, so a
# guarded name or extension is guarded anywhere in the tree, whatever its case
d, _ = decide(files=["Darling/Darling.Tests/Directory.Build.targets"])
check("a nested Directory.Build.targets: no write", d.write, False)
d, _ = decide(files=["Darling/Darling.Tests/Directory.Build.props"])
check("a nested Directory.Build.props: no write", d.write, False)
d, _ = decide(files=["tools/Directory.Packages.props"])
check("a nested Directory.Packages.props: no write", d.write, False)
d, _ = decide(files=["Darling/global.json"])
check("a nested global.json: no write", d.write, False)
d, _ = decide(files=["Lite/NuGet.Config"])
check("a nested nuget.config in another case: no write", d.write, False)
d, _ = decide(files=["PerformanceMonitor.Common/packages.lock.json"])
check("a lock file: no write", d.write, False)
d, _ = decide(files=["Lite/PerformanceMonitorLite.csproj"])
check("a product project file: no write", d.write, False)
d, _ = decide(files=["Darling/build/Versions.props"])
check("any .props file: no write", d.write, False)
d, _ = decide(files=["Darling/build/Run.Targets"])
check("any .targets file, whatever its case: no write", d.write, False)
d, _ = decide(files=["Lite.Tests/ci.runsettings"])
check("a .runsettings file: no write", d.write, False)
d, _ = decide(files=["Darling/Darling.Tests/Properties/launch.rsp"])
check("an .rsp file: no write", d.write, False)
d, _ = decide(files=["Darling/PerformanceMonitor.Darling.Service/X.cs", "Lite/Services/Y.cs", "Darling/Darling.Tests/XTests.cs",
                     "docs/README.md"])
check("product, test and documentation files only: writes", d.write, True)
d, _ = decide(files=["docs/.github/workflows/not-a-workflow.yml", "Darling/props/notes.md", "docs/global.json.md", "Lite/targets.cs"])
check("paths that only contain a guarded name: writes", d.write, True)
names, exts = getattr(fr, "NOT_RELAYED_NAMES", None), getattr(fr, "NOT_RELAYED_EXTENSIONS", None)
check("the guarded names and extensions are listed once",
      (isinstance(names, tuple), isinstance(exts, tuple), ".csproj" in (exts or ()), "global.json" in (names or ())), (True, True, True, True))

# --- a newer PR CI run on the same pull request writes CI result instead ------------------------------------------------------------
d, stub = decide(newer=[(RUN_ID + 1, FORK)])
check("newer run: no write", d.write, False)
check("newer run: reason names the run", str(RUN_ID + 1) in d.reason, True)
d, _ = decide(newer=[(RUN_ID - 1, FORK), (RUN_ID, FORK)])
check("older and same run: writes", d.write, True)
d, _ = decide(newer=[(RUN_ID + 1, "third/repo")])
check("newer run of another fork with the same branch name: writes", d.write, True)
d, _ = decide(newer=[(str(RUN_ID + 1), FORK)])
check("run ids as strings compare as numbers", d.write, False)
d, stub = decide()
runs_params = stub.params_of(RUNS)
check("newer runs: one read, of this head sha, paginated",
      (len(runs_params), runs_params[0][0].get("head_sha") if runs_params else None, runs_params[0][1] if runs_params else None),
      (1, SHA, True))

# --- success needs a maintainer's approval of this exact commit; a green run without one is pending, not success ----------------------
d, stub = decide(reviews=[])
check("no review: writes", d.write, True)
check("no review: state", d.state, "pending")
check("no review: description says approval is needed", "approv" in d.description.lower(), True)
check("no review: description fits a commit status", len(d.description) <= 140, True)
check("no review: no permission read", any(p.startswith(f"repos/{REPO}/collaborators/") for p in stub.asked), False)
d, _ = decide(reviews=[review("maint", sha=OTHER_SHA)])
check("approval of an earlier head: pending", d.state, "pending")
check("approval of an earlier head: reason", "approv" in d.reason.lower(), True)
d, stub = decide(reviews=[review("reader")])
check("approval by a reader: pending", d.state, "pending")
check("approval by a reader: the permission was read", PERM("reader") in stub.asked, True)
d, _ = decide(reviews=[review("triager")])
check("approval by a triager: pending", d.state, "pending")
d, _ = decide(reviews=[review("admin")])
check("approval by an admin: success", d.state, "success")
d, _ = decide(reviews=[review("lead")])
check("approval by a maintain role: success", d.state, "success")
d, _ = decide(reviews=[review("reader"), review("maint")])
check("a reader and a maintainer: success", d.state, "success")
d, _ = decide(reviews=[review("maint", "COMMENTED")])
check("a comment is no approval: pending", d.state, "pending")
d, _ = decide(reviews=[review("maint", "CHANGES_REQUESTED")])
check("changes requested: pending", d.state, "pending")
d, _ = decide(reviews=[review("maint", "DISMISSED")])
check("a dismissed approval: pending", d.state, "pending")
d, _ = decide(reviews=[review("maint", "APPROVED", submitted="2026-11-07T10:00:00Z"),
                       review("maint", "CHANGES_REQUESTED", submitted="2026-11-07T11:00:00Z")])
check("approval then changes requested by the same maintainer: pending", d.state, "pending")
d, _ = decide(reviews=[review("maint", "CHANGES_REQUESTED", submitted="2026-11-07T10:00:00Z"),
                       review("maint", "APPROVED", submitted="2026-11-07T11:00:00Z")])
check("changes requested then approval by the same maintainer: success", d.state, "success")
d, _ = decide(reviews=[review("maint", "APPROVED", submitted="2026-11-07T10:00:00Z"),
                       review("maint", "COMMENTED", submitted="2026-11-07T11:00:00Z")])
check("approval then a comment by the same maintainer: success", d.state, "success")
d, _ = decide(reviews=[review("maint", "PENDING")])
check("a pending review is no approval: pending", d.state, "pending")
d, _ = decide(reviews=[review("maint", sha=OTHER_SHA, submitted="2026-11-07T10:00:00Z"),
                       review("maint", "CHANGES_REQUESTED", sha=OTHER_SHA, submitted="2026-11-07T09:00:00Z")])
check("reviews of other commits do not count either way", d.state, "pending")
d, _ = decide(reviews=[{"user": None, "state": "APPROVED", "commit_id": SHA}])
check("a review with no user: pending", d.state, "pending")
d, stub = decide(reviews=[review("maint", sha=SHA.upper())])
check("approval recorded with an upper-case sha: success", d.state, "success")
d, stub = decide(reviews=[review("maint")], perms={"maint": perm("none", "none")})
check("a former collaborator: pending", d.state, "pending")

# --- a read that fails writes nothing and is reported, never guessed --------------------------------------------------------------------
for path in (JOBS, PULLS, FILES, RUNS, REVIEWS, PERM("maint")):
    a = answers()
    a[path] = fr.FetchError("boom")
    try:
        fr.decide(event(), Stub(a))
        failures.append(f"fetch failure at {path}: decide returned instead of raising FetchError")
    except fr.FetchError:
        pass

# --- a manual dispatch names the run; the run's facts come from the API and the decision is the same -------------------------------
a = answers()
a[RUN] = run_object()
stub = Stub(a)
d = fr.decide(dispatch_event(), stub)
check("dispatch: writes", (d.write, d.state, d.sha, getattr(d, "run_id", None)), (True, "success", SHA, str(RUN_ID)))
check("dispatch: reads the run first", stub.asked[:1], [RUN])
a = answers()
a[RUN] = run_object(path=".github/workflows/other.yml")
d = fr.decide(dispatch_event(), Stub(a))
check("dispatch of another workflow's run: no write", d.write, False)
a = answers()
a[RUN] = run_object(head_repo=REPO)
d = fr.decide(dispatch_event(), Stub(a))
check("dispatch of this repository's run: no write", d.write, False)
a = answers()
a[RUN] = run_object(conclusion=None)
d = fr.decide(dispatch_event(), Stub(a))
check("dispatch of a run still going: no write", d.write, False)
stub = Stub(answers())
d = fr.decide(dispatch_event(run_id="12; rm -rf /"), stub)
check("dispatch with a run id that is no number: no write", d.write, False)
check("dispatch with a run id that is no number: no reads", stub.asked, [])
stub = Stub(answers())
d = fr.decide({"repository": {"full_name": REPO}}, stub)
check("an event with neither run nor input: no write", d.write, False)
check("an event with neither run nor input: no reads", stub.asked, [])

# --- the outputs the workflow's write step reads --------------------------------------------------------------------------------------
with tempfile.TemporaryDirectory() as tmp:
    out = os.path.join(tmp, "out.txt")
    try:
        fr.write_output(fr.Decision(write=True, state="success", description="CI passed", sha=SHA, run_id="500", reason="approved"), out)
    except TypeError as exc:
        failures.append(f"output: Decision takes no run_id ({exc})")
        fr.write_output(fr.Decision(write=True, state="success", description="CI passed", sha=SHA, reason="approved"), out)
    with open(out, encoding="utf-8") as fh:
        lines = dict(ln.rstrip("\n").split("=", 1) for ln in fh if "=" in ln)
    check("output: write", lines.get("write"), "true")
    check("output: state", lines.get("state"), "success")
    check("output: description", lines.get("description"), "CI passed")
    check("output: sha", lines.get("sha"), SHA)
    check("output: run id", lines.get("run_id"), "500")
    fr.write_output(fr.Decision(write=False, reason="the head moved"), out)
    with open(out, encoding="utf-8") as fh:
        lines = dict(ln.rstrip("\n").split("=", 1) for ln in fh if "=" in ln)
    check("output: skip write", lines.get("write"), "false")
    check("output: skip sha empty", lines.get("sha"), "")
    check("output: skip reason", lines.get("reason"), "the head moved")

    # main(): the event file in, the outputs out; a failed read exits 1 and writes write=false
    ev = os.path.join(tmp, "event.json")
    with open(ev, "w", encoding="utf-8") as fh:
        json.dump(event(), fh)
    rc = fr.main(["--event", ev, "--github-output", out], fetch=Stub(answers()))
    with open(out, encoding="utf-8") as fh:
        lines = dict(ln.rstrip("\n").split("=", 1) for ln in fh if "=" in ln)
    check("main: exit 0", rc, 0)
    check("main: write", (lines.get("write"), lines.get("state"), lines.get("sha"), lines.get("run_id")), ("true", "success", SHA, str(RUN_ID)))
    a = answers()
    a[RUNS] = fr.FetchError("HTTP 502")
    rc = fr.main(["--event", ev, "--github-output", out], fetch=Stub(a))
    with open(out, encoding="utf-8") as fh:
        lines = dict(ln.rstrip("\n").split("=", 1) for ln in fh if "=" in ln)
    check("main on a failed read: exit 1", rc, 1)
    check("main on a failed read: write=false", lines.get("write"), "false")
    with open(ev, "w", encoding="utf-8") as fh:
        json.dump(event(run_event="push"), fh)
    rc = fr.main(["--event", ev, "--github-output", out], fetch=Stub({}))
    check("main on a push run: exit 0, nothing read", rc, 0)

# --- the real fetcher builds a read-only gh api call and never a shell string -----------------------------------------------------------
cmd = fr.gh_api_command("repos/o/r/pulls", {"head": "someone:patch; rm -rf /", "state": "open"}, paginate=True)
check("gh command: a list, not a string", isinstance(cmd, list), True)
check("gh command: GET", "-X" in cmd and cmd[cmd.index("-X") + 1] == "GET", True)
check("gh command: paginate", "--paginate" in cmd, True)
check("gh command: the parameter is one argument, untouched", "head=someone:patch; rm -rf /" in cmd, True)
cmd = fr.gh_api_command("repos/o/r/pulls", {}, paginate=False)
check("gh command: no paginate by default", "--paginate" in cmd, False)

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("fork-ci-result-test: all checks passed")
