#!/usr/bin/env python3
"""Checks for ci-verdict-issues.py (#8202). Run: python -I .github/scripts/tests/ci-verdict-issues-test.py

Every GitHub read goes through a stub fetcher keyed on the API path, and every write through a recorder, so no token,
network or run is needed. The checks pin the decisions (when a rerun is reported, which legs and tests count, which open
issue is updated) and the text of the issues the verdict job files."""
import contextlib
import importlib.util
import io
import os
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
script = os.path.join(here, "..", "ci-verdict-issues.py")
if not os.path.exists(script):
    print("ci-verdict-issues.py: the script does not exist, so none of the checks below can pass")
    sys.exit(1)
spec = importlib.util.spec_from_file_location("vi", script)
vi = importlib.util.module_from_spec(spec)
sys.modules["vi"] = vi
spec.loader.exec_module(vi)

failures = []


def check(name, actual, expected):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


REPO = "owner/repo"
SERVER = "https://github.example"
RUN_ID = "42372420112"
HEAD = "01a9525ffe3f0f2bf5979eaf7ca9e60b550a8d70"
TEST_A = "PerformanceMonitorLite.Tests.ArchiveSpanReadTests.ASlowReadsLogLine_SeparatesTheLockWaitFromTheQueryTime"
TEST_B = "PerformanceMonitorLite.Tests.ArchiveSpanReadTests.AnArchiveMoveDuringTheLockWait_DoesNotLeaveAFalseDataStart"
TS = "2026-11-07T12:41:53.3163230Z"

LEG_LOG = f"""2026-11-07T12:40:24.3167241Z    Darling.Tests  Total: 2425, Errors: 0, Failed: 0, Skipped: 3, Not Run: 0, Time: 425.721s
2026-11-07T12:41:53.3163230Z     {TEST_A} [FAIL]
2026-11-07T12:41:53.3163231Z       Assert.Equal() Failure
2026-11-07T12:41:53.3163230Z     {TEST_A} [FAIL]
2026-11-07T12:42:43.1725205Z    Lite.Tests  Total: 776, Errors: 0, Failed: 1, Skipped: 0, Not Run: 0, Time: 136.824s
2026-11-07T12:42:43.2284187Z leg 6 of 16 FAILED: Lite.Tests slice 6 of 16 (exit 1)
2026-11-07T12:42:43.2524526Z ##[error]Process completed with exit code 1.
"""


def job(job_id, name, conclusion, attempt):
    return {"id": job_id, "name": name, "conclusion": conclusion, "run_attempt": attempt,
            "html_url": f"{SERVER}/{REPO}/actions/runs/{RUN_ID}/job/{job_id}"}


class Stub:
    """A fetcher keyed on the API path (query string included). Records every path read; raises on one it does not know."""

    def __init__(self, pages=None, texts=None):
        self.pages = dict(pages or {})
        self.texts = dict(texts or {})
        self.reads = []

    def json(self, path):
        self.reads.append(path)
        if path not in self.pages:
            raise vi.FetchError(f"no stub for {path}")
        return self.pages[path]

    def text(self, path):
        self.reads.append(path)
        if path not in self.texts:
            raise vi.FetchError(f"no stub for {path}")
        return self.texts[path]


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, args, body):
        self.calls.append((list(args), body))


def page(path, items, key=None):
    """One stub page: the first page of `path` holds every item (fewer than a page, so the walk stops)."""
    sep = "&" if "?" in path else "?"
    full = f"{path}{sep}per_page=100&page=1"
    return {full: ({key: items} if key else items)}


def attempt_jobs(attempt, jobs):
    return page(f"repos/{REPO}/actions/runs/{RUN_ID}/attempts/{attempt}/jobs", jobs, key="jobs")


NO_ISSUES = page(f"repos/{REPO}/issues?state=open", [])

# --- should_report ---------------------------------------------------------------------------------------------------

check("report: a pull request that passed on attempt 2", vi.should_report("pull_request", "success", "2"), True)
check("report: attempt 3", vi.should_report("pull_request", "success", "3"), True)
check("report: not on the first attempt", vi.should_report("pull_request", "success", "1"), False)
check("report: not a push", vi.should_report("push", "success", "2"), False)
check("report: not a failed verdict", vi.should_report("pull_request", "failure", "2"), False)
check("report: not a verdict left to a newer run", vi.should_report("pull_request", "", "2"), False)
check("report: not an attempt that is no number", vi.should_report("pull_request", "success", ""), False)

# --- the log parse ---------------------------------------------------------------------------------------------------

check("log: the [FAIL] lines name the tests, once each, without the timestamp", vi.failing_tests(LEG_LOG), [TEST_A])
check("log: the leg's own FAILED line is not a test", vi.failing_tests("2026-01-01T00:00:00.0Z leg 6 of 16 FAILED: Lite.Tests slice 6 of 16 (exit 1)\n"), [])
check("log: an empty log names nothing", vi.failing_tests(""), [])
check("log: a line with no timestamp still parses", vi.failing_tests(f"    {TEST_B} [FAIL]\n"), [TEST_B])
check("log: order is the log's", vi.failing_tests(f"{TS} {TEST_B} [FAIL]\n{TS} {TEST_A} [FAIL]\n"), [TEST_B, TEST_A])
check("log: a theory's display name keeps its arguments", vi.failing_tests(f'{TS}     Ns.Class.Method(value: "a b", n: 2) [FAIL]\n'),
      ['Ns.Class.Method(value: "a b", n: 2)'])

# --- which jobs count ------------------------------------------------------------------------------------------------

jobs = [job(1, "CI plan", "success", 1), job(2, "Build the test projects", "failure", 1), job(3, "Test leg 6/16", "failure", 1),
        job(4, "Test leg 7/16", "success", 1), job(5, "Test leg 8/16", "cancelled", 1), job(6, "CI verdict", "failure", 1),
        job(7, "Exporter build and test", "failure", 1)]
check("legs: only a test leg that failed", [j["id"] for j in vi.failed_test_legs(jobs)], [3])

# --- titles and bodies -----------------------------------------------------------------------------------------------

check("title: names the test", vi.rerun_title(TEST_A), f"Test passed only on a rerun: {TEST_A}")
long_name = "A." * 200 + "Method"
title = vi.rerun_title(long_name)
check("title: a long name keeps its tail and fits a GitHub title", (len(title) <= 256, title.endswith("Method")), (True, True))
check("title: the same test gives the same title", vi.rerun_title(long_name), title)

occurrences = [vi.Occurrence(attempt=1, leg="Test leg 6/16", job_url=f"{SERVER}/{REPO}/actions/runs/{RUN_ID}/job/3"),
               vi.Occurrence(attempt=2, leg="Test leg 6/16", job_url=f"{SERVER}/{REPO}/actions/runs/{RUN_ID}/job/9")]
body = vi.rerun_body(TEST_A, pr_number="8145", head_sha=HEAD, run_url=f"{SERVER}/{REPO}/actions/runs/{RUN_ID}", run_attempt=3,
                     occurrences=occurrences)
for needle in (f"`{TEST_A}`", "#8145", HEAD, "attempt 3", f"{SERVER}/{REPO}/actions/runs/{RUN_ID}",
               f"{SERVER}/{REPO}/actions/runs/{RUN_ID}/attempts/1", f"{SERVER}/{REPO}/actions/runs/{RUN_ID}/attempts/2",
               "Test leg 6/16", f"{SERVER}/{REPO}/actions/runs/{RUN_ID}/job/3", f"{SERVER}/{REPO}/actions/runs/{RUN_ID}/job/9"):
    check(f"body: carries {needle[:40]}", needle in body, True)
check("body: one line per earlier attempt", body.count("- attempt "), 2)

issues = [{"number": 10, "title": "dev is red"}, {"number": 11, "title": vi.rerun_title(TEST_A)},
          {"number": 12, "title": vi.rerun_title(TEST_B), "pull_request": {"url": "x"}}]
check("open issue: found by its exact title", vi.find_open_issue(issues, vi.rerun_title(TEST_A)), 11)
check("open issue: a pull request with that title is not an issue", vi.find_open_issue(issues, vi.rerun_title(TEST_B)), None)
check("open issue: none", vi.find_open_issue(issues, "Test passed only on a rerun: Other"), None)

# --- rerun-passes end to end -----------------------------------------------------------------------------------------


def rerun(attempt, pages, texts, state="success", event="pull_request"):
    stub = Stub(pages, texts)
    rec = Recorder()
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = vi.run(["rerun-passes", "--repo", REPO, "--server-url", SERVER, "--event", event, "--state", state,
                       "--run-id", RUN_ID, "--run-attempt", str(attempt), "--pr-number", "8145", "--head-sha", HEAD],
                      fetcher=stub, write=rec)
    return code, stub, rec, out.getvalue()


log_a = {f"repos/{REPO}/actions/jobs/3/logs": LEG_LOG}
log_b = {f"repos/{REPO}/actions/jobs/9/logs": f"{TS} {TEST_B} [FAIL]\n"}

# The shape of #8145: attempt 1 failed leg 6 on one test, attempt 2 failed leg 6 on another, attempt 3 passed.
pages = {**attempt_jobs(1, [job(1, "CI plan", "success", 1), job(3, "Test leg 6/16", "failure", 1), job(4, "Test leg 7/16", "success", 1)]),
         **attempt_jobs(2, [job(9, "Test leg 6/16", "failure", 2)]), **NO_ISSUES}
code, stub, rec, out = rerun(3, pages, {**log_a, **log_b})
check("rerun: exits 0", code, 0)
check("rerun: one issue per failing test", [c[0][:2] for c in rec.calls], [["issue", "create"], ["issue", "create"]])
check("rerun: the titles", sorted(c[0][c[0].index("--title") + 1] for c in rec.calls), sorted([vi.rerun_title(TEST_A), vi.rerun_title(TEST_B)]))
check("rerun: the body names the attempt and the leg", all("Test leg 6/16" in c[1] and "attempt" in c[1] for c in rec.calls), True)
check("rerun: the attempts read are the earlier ones only",
      sorted(p for p in stub.reads if "/attempts/" in p),
      sorted([f"repos/{REPO}/actions/runs/{RUN_ID}/attempts/1/jobs?per_page=100&page=1", f"repos/{REPO}/actions/runs/{RUN_ID}/attempts/2/jobs?per_page=100&page=1"]))
check("rerun: only the failed legs' logs are read", sorted(p for p in stub.reads if p.endswith("/logs")), sorted(log_a) + sorted(log_b))

# The same test in both attempts: one issue, two occurrences.
pages_same = {**attempt_jobs(1, [job(3, "Test leg 6/16", "failure", 1)]), **attempt_jobs(2, [job(9, "Test leg 6/16", "failure", 2)]),
              **NO_ISSUES}
code, stub, rec, out = rerun(3, pages_same, {**log_a, f"repos/{REPO}/actions/jobs/9/logs": LEG_LOG})
check("rerun: the same test twice is one issue", len(rec.calls), 1)
check("rerun: with both attempts in it", rec.calls[0][1].count("- attempt ") if rec.calls else 0, 2)

# An open issue for the test: a comment on it, not a second issue.
open_issue = page(f"repos/{REPO}/issues?state=open", [{"number": 77, "title": vi.rerun_title(TEST_A)}])
pages_open = {**attempt_jobs(1, [job(3, "Test leg 6/16", "failure", 1)]), **open_issue}
code, stub, rec, out = rerun(2, pages_open, log_a)
check("rerun: an open issue for the test gets a comment", [c[0][:3] for c in rec.calls], [["issue", "comment", "77"]])
check("rerun: the comment names the pull request and the head", "#8145" in rec.calls[0][1] and HEAD in rec.calls[0][1], True)

# A failed leg whose log names no test (a crash, a timeout): nothing is filed, the leg is named in a notice.
pages_crash = {**attempt_jobs(1, [job(3, "Test leg 6/16", "failure", 1)]), **NO_ISSUES}
code, stub, rec, out = rerun(2, pages_crash, {f"repos/{REPO}/actions/jobs/3/logs": "Fatal error. exit -1073741819\n"})
check("rerun: a leg with no named test files nothing", rec.calls, [])
check("rerun: and says so", "Test leg 6/16" in out and "::notice" in out, True)

# A failed earlier attempt with no failed test leg (a build failure): nothing to report.
pages_build = {**attempt_jobs(1, [job(2, "Build the test projects", "failure", 1)]), **NO_ISSUES}
code, stub, rec, out = rerun(2, pages_build, {})
check("rerun: a build failure files nothing", (code, rec.calls), (0, []))

# The first attempt, a push, or a failed verdict: no reads and no writes.
for attempt, state, event, name in ((1, "success", "pull_request", "attempt 1"), (2, "failure", "pull_request", "a failure"),
                                    (2, "success", "push", "a push"), (2, "", "pull_request", "an empty state")):
    code, stub, rec, out = rerun(attempt, {}, {}, state=state, event=event)
    check(f"rerun: {name} reads nothing", (code, stub.reads, rec.calls), (0, [], []))

# A read that fails: a warning, exit 0, nothing written. The pull request is never failed by this step.
code, stub, rec, out = rerun(2, NO_ISSUES, {})
check("rerun: a failed read writes nothing and exits 0", (code, rec.calls), (0, []))
check("rerun: and warns", "::warning" in out, True)

# --- the dev is red body ---------------------------------------------------------------------------------------------

check("subject: the last (#N) is the pull request", vi.pr_number_from_subject("Legends keep each name distinct (#8186) (#8194)"), "#8194")
check("subject: one reference", vi.pr_number_from_subject("Fix the thing (#12)"), "#12")
check("subject: none", vi.pr_number_from_subject("Fix the thing"), None)
check("subject: a reference in the middle is not the pull request", vi.pr_number_from_subject("Fix #12 and more"), None)


def commits(*pairs):
    return [{"sha": sha, "commit": {"message": subject + "\n\nbody"}} for sha, subject in pairs]


RED = "e4eea0a08dca7870a477dbbf9429a371270de0ce"
history = commits((RED, "FinOps columns size to their values (#8113) (#8130)"),
                  ("1111111111111111111111111111111111111111", "Second swept merge (#8120)"),
                  ("2222222222222222222222222222222222222222", "Third, with an @mention (#8119)"),
                  ("3333333333333333333333333333333333333333", "The last green one (#8100)"),
                  ("4444444444444444444444444444444444444444", "Older (#8099)"))
green = {"3333333333333333333333333333333333333333"}

swept, stop = vi.swept_commits(history, lambda sha: sha in green)
check("swept: every commit after the last green one, the red one first", [c["sha"][:1] for c in swept], ["e", "1", "2"])
check("swept: the stop is the last green sha", stop, "3333333333333333333333333333333333333333")
swept, stop = vi.swept_commits(history, lambda sha: False)
check("swept: no green within the history lists it all", (len(swept), stop), (5, None))
swept, stop = vi.swept_commits(history[:1], lambda sha: False)
check("swept: the red commit alone", ([c["sha"] for c in swept], stop), ([RED], None))


def statuses(sha, green_):
    return {f"repos/{REPO}/commits/{sha}/statuses?per_page=100&page=1":
            [{"context": "CI result", "state": "success" if green_ else "failure"}, {"context": "other", "state": "success"}]}


def red_body(hist, greens, pages_extra=None):
    pages = page(f"repos/{REPO}/commits?sha={RED}", hist)
    for c in hist:
        pages.update(statuses(c["sha"], c["sha"] in greens))
    pages.update(pages_extra or {})
    stub = Stub(pages)
    with tempfile.TemporaryDirectory() as d:
        out_file = os.path.join(d, "body.txt")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = vi.run(["dev-red-body", "--repo", REPO, "--red-sha", RED, "--run-url", f"{SERVER}/{REPO}/actions/runs/1",
                           "--failed", " test (failure)", "--out", out_file], fetcher=stub, write=Recorder())
        with open(out_file, encoding="utf-8") as fh:
            text = fh.read()
    return code, stub, text


code, stub, text = red_body(history, green)
lines = text.splitlines()
check("red body: exits 0", code, 0)
check("red body: the first line names the red sha", lines[0], f"The dev push CI failed at {RED}.")
check("red body: the run and the failed jobs", (f"Run: {SERVER}/{REPO}/actions/runs/1" in lines, "Failed: test (failure)" in lines), (True, True))
check("red body: the heading counts the swept commits", any(l.startswith("Commits since the last green dev run") and "3 " in l for l in lines), True)
check("red body: one line per swept commit, short sha, pull request, title",
      [l for l in lines if l.startswith("- ")],
      ["- e4eea0a08 #8130 FinOps columns size to their values (#8113)",
       "- 111111111 #8120 Second swept merge",
       "- 222222222 #8119 Third, with an (at)mention"])
check("red body: the stop line names the last green sha", any(l == "(stopped at 333333333, the last dev commit with a green CI result)" for l in lines), True)
check("red body: the green commit is not in the list", "333333333" in "\n".join(l for l in lines if l.startswith("- ")), False)
check("red body: the statuses of the commits after the green one are not read", any("/commits/4444444444444444444444444444444444444444/" in p for p in stub.reads), False)
check("red body: the red commit's own status is not read", any(f"/commits/{RED}/" in p for p in stub.reads), False)

code, stub, text = red_body(history, set())
check("red body: no green found says so", "(no dev commit with a green CI result among the 5 looked at)" in text, True)
check("red body: and lists them all", text.count("\n- "), 5)

# #7819: a red full run sweeps back to the last green `CI full suite`, and a green `CI result` above it does not stop the sweep.
full_pages = page(f"repos/{REPO}/commits?sha={RED}", history)
for sha, ctx in [("1111111111111111111111111111111111111111", "CI result"), ("3333333333333333333333333333333333333333", "CI full suite")]:
    full_pages[f"repos/{REPO}/commits/{sha}/statuses?per_page=100&page=1"] = [{"context": ctx, "state": "success"}]
with tempfile.TemporaryDirectory() as d:
    out_file = os.path.join(d, "body.txt")
    with contextlib.redirect_stdout(io.StringIO()):
        code = vi.run(["dev-red-body", "--repo", REPO, "--red-sha", RED, "--run-url", "u", "--failed", " x",
                       "--kind", "full suite", "--context", "CI full suite", "--out", out_file], fetcher=Stub(full_pages), write=Recorder())
    with open(out_file, encoding="utf-8") as fh:
        full_text = fh.read()
full_lines = full_text.splitlines()
check("red full body: exits 0 and names the kind", (code, full_lines[0]), (0, f"The dev full suite CI failed at {RED}."))
check("red full body: sweeps past a green CI result to the green CI full suite",
      [l for l in full_lines if l.startswith("- ")],
      ["- e4eea0a08 #8130 FinOps columns size to their values (#8113)", "- 111111111 #8120 Second swept merge",
       "- 222222222 #8119 Third, with an (at)mention"])
check("red full body: the stop line names CI full suite", "(stopped at 333333333, the last dev commit with a green CI full suite)" in full_lines, True)
check("red full body: the heading names CI full suite", any(l.startswith("Commits since the last dev commit with a green CI full suite") for l in full_lines), True)

code, stub, text = red_body(commits((RED, "A merge with no pull request number")), set())
check("red body: a subject without a pull request number", "- e4eea0a08 (no PR) A merge with no pull request number" in text, True)

stub = Stub({})
with tempfile.TemporaryDirectory() as d:
    out_file = os.path.join(d, "body.txt")
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = vi.run(["dev-red-body", "--repo", REPO, "--red-sha", RED, "--run-url", f"{SERVER}/{REPO}/actions/runs/1",
                       "--failed", " test (failure)", "--out", out_file], fetcher=stub, write=Recorder())
    with open(out_file, encoding="utf-8") as fh:
        text = fh.read()
check("red body: a failed read still writes the body", (code, lines[0] in text), (0, True))
check("red body: and says the list could not be read", "could not be read" in text, True)

# A long sweep: the commit list is paged and every page is read until a short one.
long_hist = commits(*[(f"{i:040x}", f"Merge {i} (#{9000 + i})") for i in range(150)])
long_hist[0]["sha"] = RED
pages = {f"repos/{REPO}/commits?sha={RED}&per_page=100&page=1": long_hist[:100],
         f"repos/{REPO}/commits?sha={RED}&per_page=100&page=2": long_hist[100:]}
for c in long_hist:
    pages.update(statuses(c["sha"], False))
pages.update(statuses(long_hist[120]["sha"], True))
stub = Stub(pages)
with tempfile.TemporaryDirectory() as d:
    out_file = os.path.join(d, "body.txt")
    with contextlib.redirect_stdout(io.StringIO()):
        code = vi.run(["dev-red-body", "--repo", REPO, "--red-sha", RED, "--run-url", "u", "--failed", " x", "--out", out_file],
                      fetcher=stub, write=Recorder())
    with open(out_file, encoding="utf-8") as fh:
        text = fh.read()
check("red body: a sweep past one page is listed whole", text.count("\n- "), 120)
check("red body: the second page was read", f"repos/{REPO}/commits?sha={RED}&per_page=100&page=2" in stub.reads, True)

# --- the gh command shapes -------------------------------------------------------------------------------------------

check("gh: a json read is a GET with one path argument",
      vi.gh_api_args("repos/o/r/actions/runs/1/attempts/1/jobs?per_page=100&page=1"),
      ["gh", "api", "-X", "GET", "repos/o/r/actions/runs/1/attempts/1/jobs?per_page=100&page=1"])

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-verdict-issues-test: all checks passed")
