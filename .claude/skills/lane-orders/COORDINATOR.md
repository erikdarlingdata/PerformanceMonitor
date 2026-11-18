# Coordinator checklist for lane agents

The coordinator reads this file before dispatching lanes and before merging their PRs. A lane agent does not need it: `SKILL.md` in this folder holds the lane's orders.

The coordinator fills these per wave, in every brief:
- `<LANE>`, `<ISSUES>`, `<DEADLINE>`;
- `<RIG_PORT>`, `<RIG_DIR>`, `<PG_RUNTIME_ZIP>`.

## Before dispatch

- Rank the queue and get the OK the guardrails require.
- Post every ruling on its issue and put it in the first prompt.
- Pre-approve any store migration by name. Every other lane stops and reports instead.
- Fast-forward the main checkout's local `dev`. A `lane` agent loads the skill from that checkout, and a stale checkout can lack it.

## At dispatch

- Dispatch each lane as a `lane` agent (Claude Code: `subagent_type: "lane"`) where the harness defines that type. It runs on the default tier (Sonnet) in its own worktree, with the skill loaded and a context watchdog attached. A worker agent may be used instead when its first step is creating its own worktree under `.claude/worktrees/`. Where neither type exists, use a general agent on the default tier with worktree isolation (Claude Code: `isolation: "worktree"`). Put the orders in its brief.
- A lane that fixes review findings also runs on the default tier. The top tier reviews, and the default tier fixes what the review found.
- Give each lane its own rig port.
- Isolated worktrees live under `.claude/worktrees/` inside the repo, so a repo-scanning test must not exclude that path.
- Set a wall-clock check at each lane's deadline. Do not wait on notifications.
- Measure a field number before a plan depends on it. A design built on a derived figure (a lag, a row count, a timing) gets that figure measured first, read-only, by whoever has access to the field.
- Stack or serialize PRs that touch the same files. Each parallel PR on the same files needs a conflict fix and another CI run.
- Give browser and click-through checks their own small lane, apart from the code change. Screenshots and page reads fill a lane's context fast.
- Size each lane to finish under 200k. A lane's context grows about 1.9k tokens per call from a 24-40k base, so 200k is about 80-90 calls. A lane that converts tools gets one twin pair (two tools on both products), or two tools on one product.
- Check that the effort level took effect: grep a lane transcript for `"effort":`. An `effort:` set in the agent file does not always apply.
- Every head clause about a status keeps its condition's scope: "none collected in the window" is not "nothing collected yet". Leave no state out.
- Paste into the brief the few lines a lane needs (an example, a ruling) instead of pointing it at a PR or file. A pointer costs every lane a read of 8-10k characters.
- Label each issue the lane takes `in-progress` (`gh issue edit <n> --add-label in-progress`). Remove the label when the issue closes or the work stops.
- Before dispatching several lanes for one issue, create its issue branch (`git -C <repo> push origin origin/dev:refs/heads/fix/<issue>-<slug>`) and name it in every brief, with the lane that writes the PR summary. Open a separate PR only for a part that must ship before the rest. An issue that is really several issues is split into issues first. Reviews of an issue PR built in waves: one round per wave on that wave's commits only, plus one final round on the whole PR.

## At each report

1. Stop the agent.
2. Check that the shared checkout is clean.
3. Read the PR's core product diff against the checklist below. Lane PRs arrive as drafts, which can't be merged from the UI, but a lane can open one ready instead, so read the diff before anything can merge. Check every sentence of the PR body against the diff, and that it has one "Release note:" line.
4. A security PR, or an irreversible data rewrite, gets a review round before it leaves draft.
5. Mark it ready (`gh pr ready <n>`). Wait for the run after "ready" to go green. Then merge by hand: `gh pr merge <n> --merge --admin --match-head-commit <full sha>`. Never use `--auto`.
6. The release builds its changelog entries from `git log --merges`. Nobody splices `CHANGELOG.md` per PR.

## Verification checklist

The invariants agents most often miss:
- **Storage precision:** PostgreSQL and DuckDB keep microseconds. A .NET tick is not stored.
- **NULL and tie ordering:** a descending sort puts NULLs first in PostgreSQL. A SQL predicate on a nullable column is NULL-safe.
- **Plans that need an index some stores won't have yet:** fall back to the old shape when the index is missing.
- **Translated or locale-specific text:** `lc_messages` isn't always English.
- **Per-row versus per-snapshot semantics:** "newest row per key" is not "the newest snapshot".
- **Lite parity**, claimed as checked. Verify the claim, not just its presence.
- **Tests that pin spelling:** no assert on how SQL or source text is spelled.
- **Public text:** no attribution lines, no words the PR body must avoid (see "Finish" in `SKILL.md`).

## At each wave boundary

Backtrack every issue number the wave filed or touched:
- close what is done;
- give every open issue a disposition comment and a work-order rank;
- fix the `in-progress` labels;
- run `pwsh -NoProfile -File .claude/skills/lane-orders/rig-sweep.ps1` and act on its summary. It stops the rigs whose lane is gone, and lists unregistered `postgres.exe` postmasters for you to check by PID (#5787).

A wave that files follow-ups faster than it closes them hides its own backlog.

After each wave, measure it with `lane-audit.py`, as the guardrails' "Measure every wave" section says. Record the WAVE line, and the defects you fixed in each PR, in the handoff.

## Census of failures

When a PR's required check fails on a test it doesn't touch, don't just re-run it. Take a census of the last two days' failed first attempts: `gh run list --workflow Build`, each run's attempts through the REST `actions/runs/<id>/attempts/<n>/jobs` endpoint, and a `[FAIL]` grep of each failed job's log. Fix every flake it finds, in one PR. A flake that stalls every lane outranks the rest of the queue.
