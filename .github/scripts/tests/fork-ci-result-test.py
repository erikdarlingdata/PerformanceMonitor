#!/usr/bin/env python3
"""Checks for fork-ci-result.py (#8188). Run: python -I .github/scripts/tests/fork-ci-result-test.py

Every fact the decision reads (the finished run's jobs, the open pull requests at the run's head, their files, the newer runs)
comes through a stub fetcher keyed on the API path, so no check talks to GitHub. The stub records every path it is asked for,
so a check can also say which reads a decision must not make."""
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


def event(run_event="pull_request", head_repo=FORK, head_sha=SHA, branch="patch-1", run_id=RUN_ID, repo=REPO):
    """A workflow_run event payload with the fields the decision reads. head_repo None leaves the head repository out."""
    run = {"id": run_id, "event": run_event, "head_sha": head_sha, "head_branch": branch, "path": ".github/workflows/pr.yml"}
    if head_repo is not None:
        run["head_repository"] = {"full_name": head_repo, "owner": {"login": head_repo.split("/")[0]}}
    return {"workflow_run": run, "repository": {"full_name": repo}}


class Stub:
    """fetch(path, params, paginate) answers from a dict of path -> JSON, records the paths asked, and raises FetchError
    for a path it does not know (a check that reaches an unexpected read fails loudly)."""

    def __init__(self, answers):
        self.answers = answers
        self.asked = []

    def __call__(self, path, params=None, paginate=False):
        self.asked.append(path)
        if path not in self.answers:
            raise fr.FetchError(f"stub has no answer for {path}")
        answer = self.answers[path]
        if isinstance(answer, Exception):
            raise answer
        return answer


JOBS = f"repos/{REPO}/actions/runs/{RUN_ID}/jobs"
PULLS = f"repos/{REPO}/pulls"
FILES = f"repos/{REPO}/pulls/7/files"
RUNS = f"repos/{REPO}/actions/workflows/pr.yml/runs"


def jobs(verdict="success", present=True):
    out = [{"name": "CI plan", "conclusion": "success"}, {"name": "Build the test projects", "conclusion": "success"}]
    if present:
        out.append({"name": "CI verdict", "conclusion": verdict})
    return {"jobs": out}


def pull(number=7, sha=SHA, repo=FORK):
    return {"number": number, "head": {"sha": sha, "repo": {"full_name": repo}}}


def runs(*entries):
    return {"workflow_runs": [{"id": i, "head_repository": {"full_name": r}} for i, r in entries]}


def answers(verdict="success", pulls=None, files=(), newer=()):
    return {
        JOBS: jobs(verdict),
        PULLS: [pull()] if pulls is None else pulls,
        FILES: [{"filename": f} for f in files],
        RUNS: runs(*newer),
    }


def decide(ev=None, **kw):
    stub = Stub(answers(**kw))
    return fr.decide(ev or event(), stub), stub


# --- the happy path: a green fork run whose head is still the pull request's head writes success ------------------------
d, stub = decide()
check("green: writes", d.write, True)
check("green: state", d.state, "success")
check("green: description", d.description, "CI passed")
check("green: sha", d.sha, SHA)
check("green: reads the jobs, the pulls, the files and the runs", sorted(stub.asked), sorted([JOBS, PULLS, FILES, RUNS]))

# --- a red verdict writes failure, and the description names the verdict job's conclusion -----------------------------------
d, _ = decide(verdict="failure")
check("red: writes", d.write, True)
check("red: state", d.state, "failure")
check("red: description names the conclusion", "failure" in d.description and "CI verdict" in d.description, True)
d, _ = decide(verdict="cancelled")
check("cancelled verdict: state", (d.write, d.state), (True, "failure"))
d, _ = decide(verdict="timed_out")
check("timed out verdict: state", (d.write, d.state), (True, "failure"))
check("description fits a commit status", len(d.description) <= 140, True)

# --- only a pull request run from a fork is relayed; the two local checks read nothing ------------------------------------
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

# --- a run with no verdict (the plan asked for nothing) writes nothing, as the run itself would ----------------------------
stub = Stub({JOBS: jobs(present=False), PULLS: [pull()], FILES: [], RUNS: runs()})
d = fr.decide(event(), stub)
check("no verdict job: no write", d.write, False)
check("no verdict job: reason", "CI verdict" in d.reason, True)
check("no verdict job: stops after the jobs read", stub.asked, [JOBS])
stub = Stub({JOBS: jobs(verdict="skipped"), PULLS: [pull()], FILES: [], RUNS: runs()})
d = fr.decide(event(), stub)
check("skipped verdict: no write", d.write, False)
stub = Stub({JOBS: jobs(verdict=None), PULLS: [pull()], FILES: [], RUNS: runs()})
d = fr.decide(event(), stub)
check("verdict with no conclusion yet: no write", d.write, False)

# --- the head commit must still be the pull request's head ---------------------------------------------------------------------
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

# --- a pull request that changes a workflow file is not vouched for -----------------------------------------------------------
d, stub = decide(files=["Lite/Services/X.cs", ".github/workflows/pr.yml"])
check("workflow change: no write", d.write, False)
check("workflow change: reason names the file", ".github/workflows/pr.yml" in d.reason, True)
check("workflow change: stops before the runs", RUNS in stub.asked, False)
d, _ = decide(files=[".github/workflows/new.yml"])
check("new workflow file: no write", d.write, False)
d, _ = decide(files=[".github/scripts/ci-plan.py", ".github/scripts/tests/ci-plan-test.py", "Darling/X.cs"])
check("scripts and product files: writes", d.write, True)
d, _ = decide(files=["docs/.github/workflows/not-a-workflow.yml"])
check("a path that only contains the folder name: writes", d.write, True)

# --- a newer PR CI run on the same pull request writes CI result instead ------------------------------------------------------
d, stub = decide(newer=[(RUN_ID + 1, FORK)])
check("newer run: no write", d.write, False)
check("newer run: reason names the run", str(RUN_ID + 1) in d.reason, True)
d, _ = decide(newer=[(RUN_ID - 1, FORK), (RUN_ID, FORK)])
check("older and same run: writes", d.write, True)
d, _ = decide(newer=[(RUN_ID + 1, "third/repo")])
check("newer run of another fork with the same branch name: writes", d.write, True)
d, _ = decide(newer=[(str(RUN_ID + 1), FORK)])
check("run ids as strings compare as numbers", d.write, False)

# --- a read that fails writes nothing and is reported, never guessed -----------------------------------------------------------------
for path in (JOBS, PULLS, FILES, RUNS):
    a = answers()
    a[path] = fr.FetchError("boom")
    try:
        fr.decide(event(), Stub(a))
        failures.append(f"fetch failure at {path}: decide returned instead of raising FetchError")
    except fr.FetchError:
        pass

# --- the outputs the workflow's write step reads ------------------------------------------------------------------------------------
with tempfile.TemporaryDirectory() as tmp:
    out = os.path.join(tmp, "out.txt")
    fr.write_output(fr.Decision(write=True, state="success", description="CI passed", sha=SHA, reason="all checks passed"), out)
    with open(out, encoding="utf-8") as fh:
        lines = dict(ln.rstrip("\n").split("=", 1) for ln in fh if "=" in ln)
    check("output: write", lines.get("write"), "true")
    check("output: state", lines.get("state"), "success")
    check("output: description", lines.get("description"), "CI passed")
    check("output: sha", lines.get("sha"), SHA)
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
    check("main: write", (lines.get("write"), lines.get("state"), lines.get("sha")), ("true", "success", SHA))
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

# --- the real fetcher builds a read-only gh api call and never a shell string --------------------------------------------------------
calls = []
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
print(f"fork-ci-result-test: all checks passed")
