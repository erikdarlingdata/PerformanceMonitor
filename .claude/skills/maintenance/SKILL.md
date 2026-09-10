---
name: maintenance
description: Quarterly maintenance pass (every 1-3 months) — dependency/security audit, build health, and repo hygiene for PerformanceMonitor and PerformanceStudio
argument-hint: [optional: deps | build | all]
disable-model-invocation: false
---

# Routine Maintenance Pass

A repeatable every-1-3-months health check for the .NET desktop + SQL-monitoring repos
(PerformanceMonitor = WPF, PerformanceStudio = Avalonia; same dependency/build/release shape).
`$ARGUMENTS` optionally scopes it (`deps`, `build`, or `all` — default `all`).

Work top to bottom. Do read-only scans first and report findings; make fixes on a feature
branch/worktree (branch protection: never commit to `dev`/`main`), build + test, then PR to `dev`.
Low-risk patch/minor bumps can go in one PR; majors and anything release-critical get their own.

## A. Dependencies & Security

> **Scope — scan every project, not just the solution.** A repo's `.sln` may not list every project. PerformanceStudio's `PlanViewer.sln` omits `server/PlanShare.csproj` and the SSMS VSIX (`PlanViewer.Ssms`, `PlanViewer.Ssms.Installer`), so the `dotnet list <Solution>.sln …` scans and the solution build all silently skip them. Run the checks against those projects too. The net472 VSIX is old-style, so `dotnet list` is unreliable on it — read its `<PackageReference>`s by hand (its `Microsoft.VSSDK.BuildTools` is intentionally held on the 17.x line; 18.x is un-restorable from nuget.org and targets VS 18, not VS 2022). Where the `.sln` covers all projects, the solution is enough.

1. **Outdated packages.** `dotnet list <Solution>.sln package --outdated`
   - Take low-risk **patch/minor** bumps (Microsoft.Extensions.*, Test.Sdk, etc.).
   - **Majors get their own effort** — especially the update framework (Velopack: bump the library AND the `vpk` CLI pin in the release workflow — `.github/workflows/release.yml` for PerformanceStudio — together; validate the in-app + Setup.exe update path) and anything release-critical.
   - **Engine-wrapped bindings** (e.g. DuckDB.NET) trail the native engine — bump when the binding catches up, and validate behavior (for DuckDB run `tools/CompactionRepro` re: the parquet-COPY memory-limit floor before changing the version).
   - After version edits, **if the repo uses lock files** (`packages.lock.json` — PerformanceMonitor does; PerformanceStudio does not), regenerate them: `dotnet restore <Solution>.sln --force-evaluate` (CI restores `--locked-mode`).

2. **Vulnerable packages (security).** `dotnet list <Solution>.sln package --vulnerable --include-transitive`
   - Must be **zero**. Any hit (incl. transitive) is urgent — bump or pin to a fixed version. This catches CVEs the `--outdated` check does not.
   - **Code-level pass, not just packages:** run the `security-review` skill/agent on the diff since the last maintenance pass. These apps have real attack surface beyond their dependencies — PerformanceStudio's MCP server opens a local network listener, both store DB credentials (Windows Credential Manager), and both parse untrusted input (e.g. execution-plan XML). Triage anything it flags.
     - **Calibrate severity to the deployment.** PerformanceStudio runs on a single-user personal laptop, and its MCP tools are strictly read-only (no arbitrary SQL, no writes/config changes). So loopback-bound / opt-in / local-IPC findings — the MCP listener, the named-pipe single-instance server — are **Low/informational here, not High**: there's no other local user or attacker to exploit them, and read-only tools can at most leak data they already return. Reserve High for *remotely reachable* vectors (e.g. a missing `Host`/`Origin` check that allows DNS rebinding) or credential disclosure. This calibration would change only if Studio shipped the MCP server enabled-by-default or ran on a shared/multi-user host. Don't re-raise the same local-IPC findings at High each pass.

3. **Deprecated packages.** `dotnet list <Solution>.sln package --deprecated` — replace anything abandoned.

4. **Framework / runtime currency.** Confirm the TFM is on a **supported, released** .NET (do NOT move to a preview). Take the latest servicing patch of the current major. WPF/Avalonia track the runtime/their own NuGet — check both.

5. **CI tool & action pins.** In `.github/workflows/*.yml`: confirm `uses:` actions are on current majors, and that any `dotnet tool install` is **version-pinned to match its library** (e.g. `vpk --version` must equal the Velopack PackageReference). Bump GitHub Actions that are behind / deprecated.

6. **Dependabot (optional — not a finding).** Two separate features: *security alerts* (passive CVE notifications that close the gap between manual `--vulnerable` passes — mild value) and *version-update PRs* (automated bump PRs — redundant and noisy once you do periodic manual sweeps). For PerformanceStudio, Erik relies on the manual passes; treat Dependabot as **optional and do not report it as a finding**. If alerts are ever wanted they're a one-toggle enable (repo Settings → Code security, no config file); the version-update PRs aren't wanted.

## B. Code & Build Health

7. **Zero-warning build.** Build the whole solution and capture warnings — the standard is **0**:
   ```
   dotnet build <Solution>.sln -c Debug --nologo 2>&1 | Select-String ": warning "
   ```
   - Kill the running app(s) first OR build in a worktree — a running Dashboard/Lite/Studio locks its `bin` DLLs (MSB3021). Note: an incremental no-op build emits no warnings; force a clean compile of any project you're checking.
   - Fix each warning honestly (don't add to `NoWarn` to silence). Remove dead code (e.g. an unused test seam → CS0649).

8. **NoWarn review.** Scan each csproj `<NoWarn>`: every suppressed rule should have a reason (these repos keep inline comments). Remove suppressions that no longer fire; don't let the list grow silently. Most existing CA suppressions are intentional high-count ones — leave those.

9. **Stale markers.** `grep -rE "\b(TODO|FIXME|HACK|XXX)\b" --include=*.cs` — resolve or file an issue; a `// TODO: restore to X` next to a non-X value may mean the comment is the leftover (ask before flipping).

10. **Git repo hygiene.**
    - **Line endings / `.gitattributes`.** If the repo has no `.gitattributes`, line endings drift (CRLF/LF mixed, `core.autocrlf=false`) and bulk edits balloon diffs. Add one and run a **dedicated** `git add --renormalize .` commit (its own PR, when no other work is in flight — it touches nearly every file).
    - **Stale branches & worktrees.** `git worktree list` — remove leftover worktrees (anything under `.claude/worktrees/` or other agent/isolation worktrees) with `git worktree remove`. Then `git fetch --prune` to drop stale remote-tracking refs, and audit: `git branch --merged origin/dev` (local branches already in dev — safe to delete) and `git branch -r` (remote branches from closed/merged PRs). Delete merged/dead branches; **keep intentionally-parked ones** (note which and why — e.g. a blocked-upgrade branch like `upgrade/avalonia-12`). For branches *you didn't create*, surface and confirm before deleting rather than assuming abandoned. Confirm open PRs are still wanted.

## C. App data & runtime hygiene (lighter)

11. **Retention / archive end-to-end.** Confirm the app's data retention/purge and (Lite) parquet archiving actually prune old data, and logs rotate. A new time-series table must be registered for retention/archive or it grows forever.

12. **Perf regression spot-check.** Re-run the UI-latency-under-load harness if available; watch known hot spots (e.g. the Lite Blocking tab render hitch). Quick collector-health pass.

## D. Release & platform currency (lighter)

13. **Release infra freshness.** Test servers online/patched; signing cert (SignPath) not near expiry; cloud creds (`az`/`aws`) valid; the `release-checklist` skill still accurate.
    - **Cross-platform publish smoke.** `dotnet publish` the desktop app for the non-Windows runtimes it ships (PerformanceStudio: `linux-x64`, `osx-arm64`/`osx-x64`) and confirm each still produces a runnable app. The Avalonia/SkiaSharp native pins are fragile — the Linux `SkiaSharp.NativeAssets.Linux` pin exists to guard GitHub issue #139 — and a Windows-only build won't catch a broken Linux/macOS runtime.

14. **SQL Server / cloud drift + bundled tools.** New SQL Server CU/version, new DMVs/columns, Azure SQL DB / RDS changes (cloud collector paths have a bug history); refresh bundled community procs (sp_WhoIsActive, sp_BlitzLock, sp_HealthParser, sp_HumanEventsBlockViewer).

## Output

Report per section: ✅ clean / ⚠️ findings (with the fix made or recommended). Group merged PRs and
"parked" items (e.g. a major bump deferred) so the next pass knows where things stand.
