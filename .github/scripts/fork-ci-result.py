#!/usr/bin/env python3
"""fork-ci-result.py: decides whether the `CI result` commit status is written for a finished PR CI run of a fork pull request,
and with what state (#8188). Run by fork-ci-result.yml, in this repository's context, when a PR CI run completes.

A pull request from a fork runs PR CI with a read-only token, so its CI verdict job cannot write `CI result`, the status the dev
branch rule requires, and the pull request stays blocked however green its checks are. This script reads the facts the verdict
needs from the GitHub API, never from anything the run produced: the run's `CI verdict` job conclusion, the open pull requests
whose head is the run's head commit, the files those pull requests change, and whether a newer PR CI run exists for the same
head repository and branch. It writes its decision to $GITHUB_OUTPUT; the workflow's one write step reads it. The script itself
never writes to GitHub.

The decision, in order; the first rule that answers wins:
  1. the run is not a pull_request run, or its head repository is this repository: nothing to do, the run writes its own status;
  2. the run's head commit is not a commit sha, or the run names no head repository: nothing to do;
  3. the run has no `CI verdict` job, or that job was skipped or has no conclusion: nothing to do, the run itself would have written
     no status either (the plan asked for no work);
  4. no open pull request from that head repository and branch has the run's head commit as its head: the head moved, and the run
     for the new head writes the status;
  5. such a pull request changes a file its run executes, or reads to decide what runs (NOT_RELAYED_PREFIXES and
     NOT_RELAYED_FILES below): the run executed the pull request's own copy of PR CI and of everything PR CI calls, so its
     `CI verdict` conclusion says only what those copies chose to say, and this script does not vouch for it (merged by hand);
  6. a newer PR CI pull_request run exists for the same head repository and branch: it writes the status (the same rule the
     verdict job applies before it posts a failure);
  7. otherwise: success when the verdict job's conclusion is success, failure for any other conclusion.

A read that fails raises FetchError: main() reports it and exits 1 with write=false, so nothing is written on a guess.

Usage: python -I .github/scripts/fork-ci-result.py --event "$GITHUB_EVENT_PATH" --github-output "$GITHUB_OUTPUT"
Checks: python -I .github/scripts/tests/fork-ci-result-test.py
"""
import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass

VERDICT_JOB = "CI verdict"
PR_WORKFLOW = "pr.yml"
SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
# The files a PR CI run executes, or reads to decide what runs. A pull_request run takes the pull request's own copies, so a fork
# that changes one of these controls what its `CI verdict` job concludes, and its run is not relayed. One list, read by
# _not_relayed_file; a prefix guards a tree, a file guards itself.
NOT_RELAYED_PREFIXES = (
    ".github/",       # the workflows, the CI scripts and their tests, the path filters, the quarantine list and the CI history
    "Darling/tools/",  # fetch-pg-runtime.ps1 and fetch-pg-target.ps1, which the build and the test legs run
)
NOT_RELAYED_FILES = (
    "global.json",               # the SDK version the build installs
    "nuget.config",              # the feeds the restore reads
    "Directory.Build.props",     # read by the plan and by every build
    "Directory.Packages.props",  # the package versions the locked restore pins
    "Darling/Darling.Tests/Darling.Tests.csproj",    # the test projects: a project file decides which tests exist
    "Lite.Tests/Lite.Tests.csproj",
    "Exporter/PerformanceMonitor.Exporter.Tests/PerformanceMonitor.Exporter.Tests.csproj",
)
DESCRIPTION_MAX = 140  # a commit status description holds 140 characters


class FetchError(Exception):
    """A GitHub API read that did not answer."""


@dataclass(frozen=True)
class Decision:
    write: bool
    state: str = ""  # success | failure
    description: str = ""
    sha: str = ""
    reason: str = ""


def skip(reason: str) -> Decision:
    return Decision(write=False, reason=reason)


def gh_api_command(path: str, params: "dict[str, str] | None", paginate: bool) -> "list[str]":
    """The argument list of a read-only `gh api` call: GET, every parameter as its own `-f key=value` argument (gh URL-encodes
    it), --paginate when asked. A list, so no value is ever read by a shell."""
    cmd = ["gh", "api", "-X", "GET", path]
    if paginate:
        cmd += ["--paginate", "--slurp"]
    for key, value in (params or {}).items():
        cmd += ["-f", f"{key}={value}"]
    return cmd


def gh_fetch(path: str, params: "dict[str, str] | None" = None, paginate: bool = False):
    """GET through gh api and return the parsed JSON. With paginate, the pages are one flat list."""
    try:
        done = subprocess.run(gh_api_command(path, params, paginate), capture_output=True, text=True, check=False)
    except OSError as exc:
        raise FetchError(f"{path}: gh could not be started ({exc})") from exc
    if done.returncode != 0:
        raise FetchError(f"{path}: gh api exited {done.returncode}: {done.stderr.strip()[:300]}")
    try:
        data = json.loads(done.stdout)
    except ValueError as exc:
        raise FetchError(f"{path}: the answer is not JSON ({exc})") from exc
    if paginate:
        flat = []
        for page in data if isinstance(data, list) else [data]:
            flat.extend(page if isinstance(page, list) else [page])
        return flat
    return data


def _verdict_conclusion(repo: str, run_id, fetch) -> "str | None":
    jobs = (fetch(f"repos/{repo}/actions/runs/{run_id}/jobs", {"per_page": "100"}) or {}).get("jobs") or []
    for job in jobs:
        if job.get("name") == VERDICT_JOB:
            return job.get("conclusion")
    return None


def _pulls_at_head(repo: str, owner: str, branch: str, head_repo: str, sha: str, fetch) -> "list[int]":
    pulls = fetch(f"repos/{repo}/pulls", {"state": "open", "head": f"{owner}:{branch}", "per_page": "100"}, paginate=True) or []
    out = []
    for pr in pulls:
        head = pr.get("head") or {}
        if (head.get("sha") or "").lower() == sha.lower() and (head.get("repo") or {}).get("full_name") == head_repo:
            out.append(pr.get("number"))
    return out


def _not_relayed_file(repo: str, number, fetch) -> "str | None":
    """The first changed file of the pull request that the run executes or reads to decide what runs, or None."""
    files = fetch(f"repos/{repo}/pulls/{number}/files", {"per_page": "100"}, paginate=True) or []
    for entry in files:
        name = entry.get("filename") or ""
        if name.startswith(NOT_RELAYED_PREFIXES) or name in NOT_RELAYED_FILES:
            return name
    return None


def _newer_run(repo: str, branch: str, head_repo: str, run_id, fetch) -> "int | None":
    runs = (fetch(f"repos/{repo}/actions/workflows/{PR_WORKFLOW}/runs",
                  {"event": "pull_request", "branch": branch, "per_page": "20"}) or {}).get("workflow_runs") or []
    newer = [int(r["id"]) for r in runs
             if int(r.get("id") or 0) > int(run_id) and (r.get("head_repository") or {}).get("full_name") == head_repo]
    return max(newer) if newer else None


def decide(event: dict, fetch) -> Decision:
    """The decision for one workflow_run event payload. fetch(path, params=None, paginate=False) answers a GitHub API GET with
    parsed JSON or raises FetchError; the rules that need no fact of GitHub's run first and read nothing."""
    run = event.get("workflow_run") or {}
    repo = (event.get("repository") or {}).get("full_name") or ""
    run_event = run.get("event")
    if run_event != "pull_request":
        return skip(f"the run is a {run_event} run, not a pull request run")
    head_repository = run.get("head_repository") or {}
    head_repo = head_repository.get("full_name")
    if not head_repo:
        return skip("the run names no head repository")
    if head_repo == repo:
        return skip("the pull request is from this repository, so its own run writes CI result")
    sha = run.get("head_sha") or ""
    if not SHA_RE.match(sha):
        return skip("the run's head commit is not a commit sha")
    owner = (head_repository.get("owner") or {}).get("login") or head_repo.split("/")[0]
    branch = run.get("head_branch") or ""
    run_id = run.get("id")

    conclusion = _verdict_conclusion(repo, run_id, fetch)
    if conclusion in (None, "skipped"):
        return skip(f"the run has no {VERDICT_JOB} conclusion (the job is missing, skipped or unfinished), so it writes no status")

    numbers = _pulls_at_head(repo, owner, branch, head_repo, sha, fetch)
    if not numbers:
        return skip(f"no open pull request from {head_repo}:{branch} has {sha[:9]} as its head, so the head moved or the pull request closed")

    for number in numbers:
        changed = _not_relayed_file(repo, number, fetch)
        if changed:
            return skip(f"pull request #{number} changes {changed}, a file its run executes or reads to decide what runs, so the "
                        "run's verdict is the pull request's own and is not relayed; such a pull request is merged by hand")

    newer = _newer_run(repo, branch, head_repo, run_id, fetch)
    if newer is not None:
        return skip(f"a newer PR CI run ({newer}) exists for {head_repo}:{branch}, and it writes CI result")

    if conclusion == "success":
        return Decision(write=True, state="success", description="CI passed", sha=sha, reason="CI verdict succeeded")
    return Decision(write=True, state="failure", description=f"CI failed: {VERDICT_JOB} {conclusion}"[:DESCRIPTION_MAX], sha=sha,
                    reason=f"{VERDICT_JOB} concluded {conclusion}")


def write_output(decision: Decision, path: str) -> None:
    """The `key=value` lines the workflow's write step reads: write, state, description, sha, reason. The description and
    the reason are this script's own text and hold no line breaks."""
    lines = {
        "write": "true" if decision.write else "false",
        "state": decision.state,
        "description": decision.description.replace("\n", " ")[:DESCRIPTION_MAX],
        "sha": decision.sha,
        "reason": decision.reason.replace("\n", " "),
    }
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        for key, value in lines.items():
            fh.write(f"{key}={value}\n")


def main(argv: "list[str] | None" = None, fetch=gh_fetch) -> int:
    parser = argparse.ArgumentParser(description="Decide the CI result status of a fork pull request's finished PR CI run.")
    parser.add_argument("--event", required=True, help="the workflow_run event payload ($GITHUB_EVENT_PATH)")
    parser.add_argument("--github-output", required=True, help="where the decision's key=value lines go ($GITHUB_OUTPUT)")
    args = parser.parse_args(argv)
    with open(args.event, encoding="utf-8") as fh:
        event = json.load(fh)
    try:
        decision = decide(event, fetch)
    except FetchError as exc:
        write_output(skip(f"a read failed: {exc}"), args.github_output)
        print(f"::error title=CI result not relayed::A GitHub API read failed, so nothing is written: {exc}")
        return 1
    write_output(decision, args.github_output)
    if decision.write:
        print(f"CI result will be set to {decision.state} on {decision.sha}: {decision.reason}")
    else:
        print(f"::notice title=CI result not relayed::{decision.reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
