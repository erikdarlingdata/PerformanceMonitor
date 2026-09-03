---
name: release-checklist
description: Pre-release checklist for PerformanceMonitor — version bumps, changelog, cloud testing, and validation (Lite + Darling; Full Dashboard and CLI Installer are deprecated and out of this process)
argument-hint: [version]
disable-model-invocation: false
---

# Pre-Release Checklist

Run through the full release prep checklist for PerformanceMonitor. `$ARGUMENTS` is the target version (e.g., `3.3.0`).

> **Scope (since v3.3.0):** the Full Dashboard and the CLI Installer are DEPRECATED and OUT of this process — they ship no release artifacts and get no live install/upgrade testing here. They still build and their test suites still run in CI on the release event, and their csprojs still get version bumps (below). The release artifacts are: Lite (zip + Velopack Setup.exe), the Darling service zip, and the Darling Viewer Setup.exe.

## Checklist

Work through each step in order. Report status as you go. If a step fails, stop and report.

### 1. Version Bumps

Bump `<Version>`, `<AssemblyVersion>`, `<FileVersion>`, and `<InformationalVersion>` in all 6 csproj files:
- `Lite/PerformanceMonitorLite.csproj`
- `Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj` (carries the Version/AssemblyVersion/FileVersion triplet, no InformationalVersion)
- `Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj` (same triplet)
- `deprecated/Dashboard/Dashboard.csproj` — still bumped even though deprecated: the `check-version-bump` workflow reads THIS file
- `deprecated/Installer/PerformanceMonitorInstaller.csproj` — still bumped for consistency (CI builds it on release events)
- `deprecated/Installer.Core/Installer.Core.csproj` — same

The `check-version-bump` workflow only reads `Dashboard.csproj`, so the other five are on THIS checklist to catch — nothing in CI enforces them.

Verify no other files contain hardcoded version strings that need updating.

### 2. CHANGELOG

Ensure `CHANGELOG.md` has an entry for the new version with:
- **Important** section if there are store/schema changes or breaking changes (Darling store migrations, PG runtime upgrades with offline windows, operator actions required post-upgrade)
- **Added** section for new features
- **Changed** section for behavior changes
- **Fixed** section for bug fixes
- Reference links at the bottom for all issue numbers

If a week of parallel PRs left duplicate section headers in `[Unreleased]` (two `### Fixed` blocks from keep-both merges), consolidate them at cut time with a content-preserving script that VERIFIES the entry-line multiset is unchanged — never re-sort by hand (a 242-line silent reorder shipped that way once).

If the previous version's changelog entry is missing, add that too.

### 2b. README Sync

Cross-reference `README.md` against the changelog and ensure all user-facing changes are reflected:
- **Lite collector table**: New collectors added, count updated
- **Lite/Darling tab lists**: New tabs listed
- **Edition comparison table**: Collector counts, feature rows updated
- **Managed platform support table**: New platform behaviors documented
- **Permissions section**: Updated if new grants are needed
- **Deprecation callout**: still accurate about what ships and what doesn't

Do NOT add internal/implementation changes — only user-facing features and behavior.

### 3. Upgrade Scripts — Schema Change Audit (conditional)

Applies ONLY when `install/` or `upgrades/` changed since the last release (the deprecated Dashboard edition remains on bug-fix support, so its schema discipline still holds when it IS touched):

1. All schema changes MUST go through the `upgrades/{from}-to-{to}/` folder — never ad hoc, never baked into base install scripts. View-only changes flow through `CREATE OR ALTER` re-runs and need no upgrade script.
2. **List all .sql files in the upgrade folder and compare against `upgrade.txt`** — every script must be listed or it won't run (PR #608 shipped a script not listed in `upgrade.txt`).
3. Verify each script uses `IF NOT EXISTS` checks for idempotency, and that any column additions in `01_install_database.sql` CREATE TABLE blocks have corresponding ALTER TABLE scripts in the upgrade folder.

### 4. Build Verification

Run a FULL rebuild and check the warnings line, not just errors:
```
dotnet build PerformanceMonitor.sln -c Debug -t:Rebuild
```
All projects must succeed with **0 Warning(s) / 0 Error(s)** — the repo's bar is zero-warning, test projects included (they sit outside the WarningsAsErrors gate, so a warm incremental build can hide a warning a Rebuild surfaces).

(The deprecated Installer/Dashboard test suites run in CI on the release event with the CI's own filter; they are no longer run from this checklist.)

### 5. Cloud Platform Testing (shared collector layer — test via DARLING)

**This step is never skipped for a release** (defer within the release window is OK; skip is not). Lite and Darling share the collector layer (`PerformanceMonitor.Collectors`), so the cloud-specific collector paths — Azure SQL DB edition detection, the master/database-level fallbacks, RDS capability skips — are exercised once, **through Darling**, which is headless and fully scriptable (no GUI clicking). Lite is covered by the shared code plus its unit suites; run a Lite-side pass too only when a LITE-specific cloud behavior shipped this release (connection-alert flows, webhook delivery, UI surfacing of cloud rows — their authoritative validation is `Lite.Tests/AzureMasterFallbackTests` + `ConnectionEdgeDetectorTests` and the alert-policy suites).

Spin up temporary instances with the Azure CLI (`az`) and AWS CLI (`aws`):

**Azure SQL DB:** (export `SQL_TEST_PASSWORD` with a freshly generated throwaway password before running these; never hardcode one)
1. Create a resource group, logical server, and 2+ databases. **Use a region geographically close to the machine running the collector** -- cross-country latency causes collector timeouts. From US East that is eastus or eastus2:
   ```
   az group create --name rg-release-test --location eastus2
   az sql server create --name pm-release-test --resource-group rg-release-test --location eastus2 --admin-user sqladmin --admin-password "$SQL_TEST_PASSWORD"
   az sql server firewall-rule create --resource-group rg-release-test --server pm-release-test --name AllowAll --start-ip-address 0.0.0.0 --end-ip-address 255.255.255.255
   az sql db create --resource-group rg-release-test --server pm-release-test --name testdb1 --edition GeneralPurpose --compute-model Serverless --family Gen5 --capacity 1 --auto-pause-delay 60 --min-capacity 0.5 --max-size 1GB
   ```
2. Add the Azure logical server to a Darling instance (a local scratch service or an existing dogfooding instance) and let it collect 2+ cycles.
3. Verify against the store (psql or the web dashboard):
   - `database_size_stats` / size collectors cover ALL databases, not just master (the #1631 fallback class)
   - server row shows engine edition 5 and collectors inapplicable to Azure SQL DB are SKIPPED as not-applicable, not erroring
   - `collection_log` clean for the cloud server across the cycles
   - the serverless DB's 60s auto-pause + resume (error 40613 transients) does not wedge collection — rows resume after the pause without a service restart
4. Firewall churn: DELETE the allow rule, wait 3–5 minutes, re-add it. Collection must resume on its own without restarting the service. (CAVEAT, verified v3.2.0: a bare rule-deletion often does NOT sever an actively-collecting client — connection pooling keeps the open connection alive and Azure gates only NEW connections — so treat a no-outage result as inconclusive rather than a pass; the recovery logic's authoritative validation is the unit suites.)
5. Clean up: `az group delete --name rg-release-test --yes --no-wait`

**AWS RDS:** (same `SQL_TEST_PASSWORD` throwaway as above)
1. Create an RDS SQL Server instance. **Use a region geographically close to the machine running the collector** -- cross-country latency causes collector timeouts. From US East that is us-east-1:
   ```
   aws rds create-db-instance --db-instance-identifier pm-release-test --db-instance-class db.t3.xlarge --engine sqlserver-ee --master-username admin --master-user-password "$SQL_TEST_PASSWORD" --allocated-storage 20 --region us-east-1
   ```
2. **Network reachability is NOT automatic** (bit on v3.3.0: the fresh instance answered "server was not found" until fixed). Verify `PubliclyAccessible` is true, and open 1433 on the instance's security group — the account-default SG has no public inbound:
   ```
   aws rds describe-db-instances --db-instance-identifier pm-release-test --region us-east-1 --query 'DBInstances[0].[PubliclyAccessible,VpcSecurityGroups[0].VpcSecurityGroupId]' --output text
   aws ec2 authorize-security-group-ingress --group-id <sg-id> --protocol tcp --port 1433 --cidr 0.0.0.0/0 --region us-east-1
   ```
3. Add the RDS endpoint to the same Darling instance (`encrypt_mode` Mandatory + `trust_server_certificate` true — RDS serves its own CA), let it collect 2+ cycles.
4. Verify: the Agent-surface collectors (`running_jobs`, `job_history`, `agent_status`) short-circuit as capability skips (0 rows at ~0ms, no errors); everything else lands rows; `collection_log` clean.
5. Clean up — the instance AND the SG hole (it is usually the account-DEFAULT security group; leaving 1433/0.0.0.0/0 on it is a standing exposure for anything else in that VPC):
   ```
   aws rds delete-db-instance --db-instance-identifier pm-release-test --skip-final-snapshot --region us-east-1
   aws ec2 revoke-security-group-ingress --group-id <sg-id> --protocol tcp --port 1433 --cidr 0.0.0.0/0 --region us-east-1
   ```

Success criteria: no collector errors in the store's `collection_log` for either cloud server, all applicable collectors landing rows, capability skips recorded as skips rather than failures.

### 6. Lite Collector Validation

Check Lite's latest dev build thoroughly:
- Review the Lite error log at `%LOCALAPPDATA%\PerformanceMonitorLite\logs\` for any errors
- Launch Lite, connect to at least one server, and monitor collector health
- Check for collector failures in the Collection Health tab
- Verify data is populating correctly in all tabs

### 7. Smoke Test New Features

Quick click-through of any new features or significant changes listed in the changelog — Lite, the Darling web dashboard, and the Darling Viewer.

### 8. Desktop App Upgrade — Velopack Setup.exe + single-instance handoff

Covers the **desktop apps' own** upgrade via Velopack (`*-Setup.exe`) for **Lite** and the **Darling Viewer**. Test the upgrade over a **running** prior version:

1. Install the prior release's Setup.exe, launch the app, and **minimize it to the tray** (leave it running).
2. Run the new release's Setup.exe (and separately test Help → About → download → restart). Confirm the **new** version actually runs afterward — not the stale in-memory one.
3. **Single-instance upgrade handoff (#1148):** with the old version running in the tray, launch the new build → expect the *"A previous version is still running — close it and continue?"* prompt → old closes → new takes over. A same-version relaunch should just surface the running window (no prompt). An older build launched over a newer one should surface the newer (no eviction).
4. **Elevated case:** run the old version **as administrator**, launch the new one non-elevated → expect the *"Restart as administrator"* prompt; elevating completes the takeover. A *same-version* elevated instance should just surface (no UAC).
5. Confirm Lite never closes the Darling Viewer and vice-versa (scoped by exe name).

Local proxy without a release: bump `<Version>`, rebuild, run over the old build → expect the close-and-takeover prompt. The decision logic is unit-tested (`Lite.Tests/SingleInstanceDecisionTests`); this step validates the live Win32/Velopack seam (`SingleInstanceCoordinator` / `ProcessInspector` in `PerformanceMonitor.Ui`). When the seam saw no changes since the prior release, a quick post-publish regression pass is acceptable instead of a pre-cut gate. Design: `plans/single-instance-upgrade-handoff.md`.

### 9. Nightly Build Verification

Before cutting the release, verify the nightly build has been clean:
- Check that the most recent nightly build completed successfully (GitHub Actions) — verify the gated legs BY NAME (`build`, `Darling PostgreSQL tests`); a green wrapper exit code is not evidence, and the nightly publishes assets even when the test leg fails
- Confirm no community bug reports against the nightly in the last 24-48 hours
- If any issues were reported and fixed, wait for a new clean nightly before proceeding

### 10. Commit and PR

- Do the version-bump + changelog commits on a branch off `dev` (e.g. `release/v{version}`) — NEVER commit directly to `dev` or `main` (branch protection)
- Push the branch and merge it into `dev` first (open that PR with base `dev`)
- Then open the release PR **from `dev` to `main`** — the head MUST be `dev`. `check-pr-branch.yml` rejects any PR to `main` whose head is not `dev`, so a `release/*` → `main` PR fails CI (the "all jobs failed" email)
- At release-PR merge time, dev must equal the sha the field validation ran against (or the delta must be explicitly accepted)

### 11. Tag and Release

After PR is merged to main:
```
git checkout main && git pull
git tag v{version}
git push origin v{version}
```

Then create the GitHub Release from the tag — `build.yml` triggers on `release: [published]` only (NOT `created`, and NOT on tag push alone). Publish a real release; do **not** use `--draft` (a draft fires only `created`, so the build/sign workflow won't run):
```
gh release create v{version} --title "v{version}" --notes "See CHANGELOG.md for details."
```

The build workflow will then run, compile all artifacts, sign them (if SignPath is configured), and attach them to the release. Monitor at https://github.com/erikdarlingdata/PerformanceMonitor/actions

SignPath blocks on **four** approval requests as of v3.3.0: **Lite** (zip), **Darling** (service + viewer apphost exes), **Lite (Velopack)**, and **Darling Viewer (Velopack)** — the Dashboard/Installer signing steps left with their artifacts. The `Darling` artifact-configuration slug on signpath.io signs the two apphost exes (`PerformanceMonitor.Darling.Service.exe` at the root and `viewer/PerformanceMonitor.Darling.Viewer.exe`) and leaves the `PerformanceMonitor.*` / third-party dlls and `pg-runtime.zip` alone; if it ever goes missing the single `Sign Darling` step fails the release. The Darling zip (`PerformanceMonitorDarling-{version}.zip` — service at root with `pg-runtime.zip` beside the exe, viewer in `viewer\`) joins the release assets; the pg-runtime build downloads ~340MB on a cold cache (cached across re-releases on the fetch script's content hash). Verify the Darling zip is attached and listed in `SHA256SUMS.txt`, alongside the Lite zip/Setup.exe and the Viewer Setup.exe.

## Notes

- Always test BEFORE merging PRs
- Test across the supported SQL Server version range (2016 through 2025). A machine with local instances for this lists them in the repo local CLAUDE.md, along with any that hold real data and must not be written to. Connect via pre-configured sqlcmd contexts so no credentials appear in this file or in any command it runs.
- **NEVER use Express edition** for cloud test instances — Express does not support SQL Agent, and several collectors need Agent surface area
- Azure CLI (`az`) and AWS CLI (`aws`) are both available for creating/destroying test instances
- Always clean up cloud test resources after testing to avoid charges
