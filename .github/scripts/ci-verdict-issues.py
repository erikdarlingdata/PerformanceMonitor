#!/usr/bin/env python3
"""The issues PR CI's verdict job files (#8202). Two commands, both called from pr.yml's verdict job with github.token:

  rerun-passes   A pull request run whose verdict passed on run_attempt 2 or later. Every earlier attempt's failed test leg
                 is read for its [FAIL] lines, and one issue per failing test names the pull request, the head sha, the
                 leg, and the run, attempt and job URLs. An open issue with the same title gets a comment instead of a
                 second issue. It reports only: every failure of its own is a warning and exit 0, so it never fails the
                 pull request.

  dev-red-body   The body of the "dev is red" issue for a red dev push: the sha, the run, the failed jobs, and every dev
                 commit from the red sha back to (not including) the last dev commit whose `CI result` status is success,
                 one line each with the short sha, the pull request number from the squash title, and the title. The list
                 is paged, so a sweep longer than one page is listed whole.

Every GitHub read goes through one fetcher (gh api, GET only) and every write through one writer (gh issue ...). Both
are parameters of run(), so the tests in tests/ci-verdict-issues-test.py replace them with stubs.
"""
import argparse
import re
import subprocess
import sys
from dataclasses import dataclass

PAGE = 100
# The newest dev commits looked at for the swept list. A sweep is 1 to about 40 commits; the cap bounds the status reads.
MAX_COMMITS = 300
MAX_ISSUE_PAGES = 10
TITLE_PREFIX = "Test passed only on a rerun: "
TITLE_LIMIT = 256
# xunit's console line for a failed test: optional timestamp, the test's display name, " [FAIL]".
FAIL_LINE = re.compile(r"^(?:\S+Z\s+)?\s*(?P<test>\S.*?)\s+\[FAIL\]\s*$")
# The squash title's trailing "(#N)" is the pull request.
PR_SUFFIX = re.compile(r"\s*\(#(?P<number>\d+)\)\s*$")


class FetchError(Exception):
    """A GitHub read that failed; the caller decides what the absence means."""


@dataclass(frozen=True)
class Occurrence:
    attempt: int
    leg: str
    job_url: str


# --- GitHub access -----------------------------------------------------------------------------------------------------


def gh_api_args(path):
    return ["gh", "api", "-X", "GET", path]


class GhFetcher:
    """Reads through `gh api`. json() parses the response; text() returns it as is (a job log)."""

    def _get(self, path):
        proc = subprocess.run(gh_api_args(path), capture_output=True, text=True)
        if proc.returncode != 0:
            raise FetchError(f"gh api {path} failed ({proc.returncode}): {proc.stderr.strip()[:300]}")
        return proc.stdout

    def json(self, path):
        import json
        try:
            return json.loads(self._get(path))
        except ValueError as e:
            raise FetchError(f"gh api {path} returned no JSON: {e}") from e

    def text(self, path):
        return self._get(path)


def gh_write(args, body):
    """gh issue <args> --body-file <file>: the body goes through a file, never through an argument."""
    import tempfile
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=False) as fh:
        fh.write(body)
        path = fh.name
    subprocess.run(["gh", *args, "--body-file", path], check=True)


def paged(fetcher, path, key=None, max_pages=None):
    """Every item of a list endpoint, page by page, until a page is short or max_pages is hit."""
    items = []
    sep = "&" if "?" in path else "?"
    page = 1
    while max_pages is None or page <= max_pages:
        got = fetcher.json(f"{path}{sep}per_page={PAGE}&page={page}")
        if key is not None:
            got = got.get(key, [])
        items.extend(got)
        if len(got) < PAGE:
            break
        page += 1
    return items


# --- rerun-passes: the decisions ---------------------------------------------------------------------------------------


def should_report(event_name, state, run_attempt):
    """Only a pull request run whose verdict is success on a rerun. A push, a failure, a verdict left to a newer run
    (empty state) and the first attempt have nothing to report."""
    if event_name != "pull_request" or state != "success":
        return False
    try:
        return int(run_attempt) > 1
    except (TypeError, ValueError):
        return False


def failed_test_legs(jobs):
    """The test legs that failed. A build, exporter, plan or verdict failure names no test."""
    return [j for j in jobs if str(j.get("name", "")).startswith("Test leg") and j.get("conclusion") == "failure"]


def failing_tests(log_text):
    """The test names on the [FAIL] lines, in log order, once each."""
    seen = []
    for line in log_text.splitlines():
        m = FAIL_LINE.match(line)
        if m:
            name = m.group("test")
            if name not in seen:
                seen.append(name)
    return seen


def rerun_title(test):
    title = TITLE_PREFIX + test
    if len(title) > TITLE_LIMIT:
        title = TITLE_PREFIX + "..." + test[-(TITLE_LIMIT - len(TITLE_PREFIX) - 3):]
    return title


def attempt_url(run_url, attempt):
    return f"{run_url}/attempts/{attempt}"


def rerun_body(test, pr_number, head_sha, run_url, run_attempt, occurrences):
    lines = [
        f"`{test}` failed on an earlier attempt of a run that passed on a rerun.",
        "",
        f"Pull request #{pr_number} at {head_sha} got its green `CI result` from attempt {run_attempt} of {run_url}.",
        "",
        "The earlier attempts that failed this test:",
    ]
    for o in occurrences:
        lines.append(f"- attempt {o.attempt}, {o.leg}: {attempt_url(run_url, o.attempt)} (job {o.job_url})")
    lines.append("")
    lines.append("A test that passes on a rerun passes by luck. Make it deterministic, or move it to a collection that serializes what it shares.")
    return "\n".join(lines) + "\n"


def find_open_issue(issues, title):
    for issue in issues:
        if "pull_request" in issue:
            continue
        if issue.get("title") == title:
            return issue.get("number")
    return None


def rerun_passes(args, fetcher, write):
    if not should_report(args.event, args.state, args.run_attempt):
        print(f"Nothing to report: event={args.event} state={args.state!r} run_attempt={args.run_attempt!r}.")
        return
    run_url = f"{args.server_url}/{args.repo}/actions/runs/{args.run_id}"
    by_test = {}
    for attempt in range(1, int(args.run_attempt)):
        jobs = paged(fetcher, f"repos/{args.repo}/actions/runs/{args.run_id}/attempts/{attempt}/jobs", key="jobs")
        for leg in failed_test_legs(jobs):
            tests = failing_tests(fetcher.text(f"repos/{args.repo}/actions/jobs/{leg['id']}/logs"))
            if not tests:
                print(f"::notice title=A failed leg named no test::{leg['name']} of attempt {attempt} failed with no [FAIL] line "
                      f"(a crash or a timeout), so no issue is filed for it: {leg.get('html_url', '')}")
                continue
            for test in tests:
                by_test.setdefault(test, []).append(Occurrence(attempt=attempt, leg=leg["name"], job_url=leg.get("html_url", "")))
    if not by_test:
        print("No earlier attempt failed a test leg on a named test.")
        return
    open_issues = paged(fetcher, f"repos/{args.repo}/issues?state=open", max_pages=MAX_ISSUE_PAGES)
    for test, occurrences in by_test.items():
        title = rerun_title(test)
        body = rerun_body(test, args.pr_number, args.head_sha, run_url, int(args.run_attempt), occurrences)
        existing = find_open_issue(open_issues, title)
        if existing is not None:
            write(["issue", "comment", str(existing), "--repo", args.repo], body)
            print(f"Commented on #{existing} for {test}.")
        else:
            write(["issue", "create", "--repo", args.repo, "--title", title], body)
            print(f"Filed an issue for {test}.")


# --- dev-red-body: the swept commits -----------------------------------------------------------------------------------


def pr_number_from_subject(subject):
    m = PR_SUFFIX.search(subject)
    return f"#{m.group('number')}" if m else None


def swept_commits(commits, is_green):
    """The commits from the red one (first) back to the first whose CI result is green, which is not included.
    Returns (swept, stop_sha); stop_sha is None when no green commit was found."""
    swept = []
    for i, c in enumerate(commits):
        sha = c["sha"]
        if i > 0 and is_green(sha):
            return swept, sha
        swept.append(c)
    return swept, None


def commit_line(c):
    subject = c.get("commit", {}).get("message", "").split("\n")[0]
    pr = pr_number_from_subject(subject)
    title = PR_SUFFIX.sub("", subject) if pr else subject
    return f"- {c['sha'][:9]} {pr or '(no PR)'} {title.replace('@', '(at)')}"


def dev_red_body(repo, red_sha, run_url, failed, fetcher):
    head = [f"The dev push CI failed at {red_sha}.", "", f"Run: {run_url}", f"Failed:{failed or ' see the run'}", ""]
    try:
        commits = paged(fetcher, f"repos/{repo}/commits?sha={red_sha}", max_pages=(MAX_COMMITS + PAGE - 1) // PAGE)
    except FetchError as e:
        return "\n".join(head + [f"Commits since the last green dev run: the list could not be read ({e})."]) + "\n"
    commits = commits[:MAX_COMMITS]

    def is_green(sha):
        try:
            statuses = paged(fetcher, f"repos/{repo}/commits/{sha}/statuses", max_pages=1)
        except FetchError:
            return False
        return any(s.get("context") == "CI result" and s.get("state") == "success" for s in statuses)

    swept, stop = swept_commits(commits, is_green)
    lines = head + [f"Commits since the last green dev run (newest first), {len(swept)} swept:"]
    lines += [commit_line(c) for c in swept]
    if stop:
        lines.append(f"(stopped at {stop[:9]}, the last dev commit with a green CI result)")
    else:
        lines.append(f"(no dev commit with a green CI result among the {len(commits)} looked at)")
    return "\n".join(lines) + "\n"


# --- entry -------------------------------------------------------------------------------------------------------------


def parse(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("rerun-passes")
    r.add_argument("--repo", required=True)
    r.add_argument("--server-url", required=True)
    r.add_argument("--event", required=True)
    r.add_argument("--state", default="")
    r.add_argument("--run-id", required=True)
    r.add_argument("--run-attempt", default="")
    r.add_argument("--pr-number", default="")
    r.add_argument("--head-sha", default="")
    d = sub.add_parser("dev-red-body")
    d.add_argument("--repo", required=True)
    d.add_argument("--red-sha", required=True)
    d.add_argument("--run-url", required=True)
    d.add_argument("--failed", default="")
    d.add_argument("--out", required=True)
    return p.parse_args(argv)


def run(argv, fetcher=None, write=gh_write):
    args = parse(argv)
    fetcher = fetcher if fetcher is not None else GhFetcher()
    if args.command == "rerun-passes":
        try:
            rerun_passes(args, fetcher, write)
        except Exception as e:  # noqa: BLE001 - this step reports and never fails the pull request
            print(f"::warning title=Rerun report not written::{type(e).__name__}: {e}")
        return 0
    body = dev_red_body(args.repo, args.red_sha, args.run_url, args.failed, fetcher)
    with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(body)
    return 0


if __name__ == "__main__":
    sys.exit(run(sys.argv[1:]))
