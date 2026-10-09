---
name: release-checklist
description: Pre-release checklist for PerformanceMonitor — version bumps, changelog, cloud testing, and validation (Lite + Darling; Full Dashboard and CLI Installer are deprecated and out of this process)
argument-hint: [version]
disable-model-invocation: false
---

# Pre-Release Checklist

Run through the full release prep checklist for PerformanceMonitor. `$ARGUMENTS` is the target version (e.g., `3.3.0`).

> **Scope (since v3.3.0):** the Full Dashboard and the CLI Installer are DEPRECATED and OUT of this process — they ship no release artifacts and get no live install/upgrade testing here. The Darling service zip and the Linux tarball DO get live install and upgrade testing, in section 8a. They still build and their test suites still run in CI on the release event, and they inherit the single `<Version>` with everything else in the tree (below). The release artifacts are: Lite (zip + Velopack Setup.exe), the Darling service zip, and the Darling Viewer Setup.exe.

## Checklist

Work through each step in order. Report status as you go. If a step fails, stop and report.

### 1. Version Bumps

Bump `<Version>` in **`Directory.Build.props`**. That is the whole step — one line, one file.

Every project in the tree inherits it, and `AssemblyVersion` / `FileVersion` / `InformationalVersion` derive
from it at build time. Do not add any of those four elements to a csproj: `ProductVersionDeclarationTests`
fails the build on a second `<Version>` property anywhere in the tree, on a re-added derived property, and
on a workflow or script that reads a version from any path but `Directory.Build.props`.

This replaced six independently hand-bumped csproj files (#3222). Two of them had been sitting three
releases behind, `deprecated/Dashboard/Dashboard.csproj` was declaring 3.6.0 while its binary stamped
3.3.0.0, and the release gate took its judgement from that file — so nothing in CI compared the version it
gated on against the version the artifacts were named with. Both now read the file above.

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

### 2a. Archive and compact the cut version

`CHANGELOG.md` is an **index**: from 3.0.0 on, each released entry is reduced to its bold title and the issues it references, and the prose lives in `docs/changelog/<major>.<minor>.md`. The single file had reached 2,160,009 bytes, which GitHub refuses to render. `[Unreleased]` keeps its full prose because that is where entries are written, so once the heading is renamed to the new version its prose has to move:

```
python3 tools/changelog/changelog_archive.py split     # rewrites the index and the archives
python3 tools/changelog/changelog_archive.py census    # the counts and hashes the CI pin reads
python3 tools/changelog/changelog_archive.py verify    # counts, reference resolution, size ceilings
python3 tools/changelog/changelog_archive.py roundtrip # byte identity against the pre-split revision
```

`split` is idempotent — run it on an already-split tree and only the newly-released version moves. **Regenerating the census is the step to remember**: `ChangelogIndexAndArchiveTests` reads `tools/changelog/archive-census.txt`, so a new archive with no row there fails CI, and so does a row that no longer matches its archive.

Pre-3.0 versions are deliberately **not** archived: those entries carry no prose to move, and an empty file beside them would be one more thing to maintain that says nothing.

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

The statement-column census test fails while any entry or watched writer is pending, in every Darling test run, with or without the variable. Run `DARLING_RELEASE_CUT=1 Darling/Darling.Tests/bin/Debug/net10.0-windows/Darling.Tests.exe -method "*StatementCensus_PendingEntries_BlockARelease*"` to see the list early; the cut proceeds only when it passes. The release workflow (build.yml, the "Statement filter release gate" step) runs the same test with the variable set before it publishes or packages anything, so a release cannot ship while an entry is pending even if this step is skipped; run it here first to see the list early.

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
   - server row shows engine edition 5, and no collector that does not apply to Azure SQL DB errors. The SQL Agent collectors (`running_jobs`, `job_history`, `agent_status`) are marked not applicable when the server is added (`add_servers` answers "Agent surface: not applicable") and never run, so they leave no `collection_log` row and no health entry. That is the expected result, not a gap (verified v3.10.0)
   - `collection_log` clean for the cloud server across the cycles
   - the serverless DB's 60s auto-pause + resume (error 40613 transients) does not wedge collection — rows resume after the pause without a service restart
4. Firewall churn: DELETE the allow rule, wait 3–5 minutes, re-add it. Collection must resume on its own without restarting the service. (CAVEAT, verified v3.2.0: a bare rule-deletion often does NOT sever an actively-collecting client — connection pooling keeps the open connection alive and Azure gates only NEW connections — so treat a no-outage result as inconclusive rather than a pass; the recovery logic's authoritative validation is the unit suites.) To force a real outage (verified v3.10.0): open admin sessions in each user database BEFORE deleting the rule, in case the machine you test from shares the Darling host's egress IP. Delete the rule, wait 6 minutes for it to take effect, then KILL Darling's sessions (`program_name` = `PerformanceMonitorDarling`) from those sessions. KILL is denied in master even for the server admin, so collectors pooled on master keep running. Collectors that open new connections then hit 40615: the `collection_log` message must name the firewall, and those collectors must store rows again after the rule is re-added, with the service's `started_at` unchanged.
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

Success criteria: no collector errors in the store's `collection_log` for either cloud server, all applicable collectors landing rows, no inapplicable collector recorded as a failure (RDS records them as capability skips; Azure SQL DB never runs them).

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

### 8a. Darling service install and upgrade (every release)

Section 8 covers the desktop apps. This section covers the Darling service zip and the Linux tarball. They carry their own install and upgrade scripts, which a Velopack run never touches. A service install that works only on a clean machine is the shape that shipped broken in #5627. So none of this is optional, and the unit suites cannot stand in for it.

Run the Windows items on a disposable machine or VM, and point them at a throwaway test instance. Keep the machine-neutral rule: no local instance names, hosts or credentials in anything you record. A cloud test instance gets its throwaway `SQL_TEST_PASSWORD`, as in section 5.

#### (a) The Windows upgrade-shape CI job

The workflow is `.github/workflows/darling-upgrade-shapes.yml`. It runs 14 legs: seven on `windows-2022` and seven on `windows-2025`. Each leg installs a real released build with that build's own scripts, then runs this commit's install or upgrade script over it. The legs cover four shapes:
- U1: an install on the default virtual account.
- U2: an install whose service logon was changed to a non-default local account.
- U3: an install whose service was deleted.
- U4: a pre-3.9 folder directly under `C:\` that must still refuse and print the move steps.

The workflow runs on pull requests that touch `Darling/tools/*.ps1`, as the `Darling upgrade shapes` job in the nightly, and by manual dispatch. The release commit often carries no such check, so look for the run at its sha. In the release commit's checks or the nightly, confirm all 14 legs ran at that sha and are green, by name. A skipped leg is not a pass. If there is no run at that sha, start one with `gh workflow run darling-upgrade-shapes.yml --ref <release branch>` and wait for it.

Also read the two pins at the top of the workflow, `DARLING_CURRENT_RELEASE` and `DARLING_OLD_RELEASE`. The first must name the last shipped release, and the second a release from before 3.9 that is still downloadable. Record the run URL. If any leg is red or skipped, stop. A red leg blocks the release, and re-running it until it passes is not a fix.

#### (b) `upgrade-darling.ps1` over a real existing install

Do the whole procedure twice: once from the last release, and once from the oldest release still supported.

- **Which release is the oldest supported:** the oldest `Darling/Darling.Tests/Fixtures/migration-ladder-v<version>.sql` in the tree. `MigrationUpgradeLadderLiveTests` climbs the current ladder over every fixture in that folder. Its `TheMostRecentRelease_HasALadderFixture` check keeps the newest shipped release in the set. So the oldest fixture is the oldest store the tests vouch for. Read the folder at release time, and do not copy a version number into this file. Deleting a fixture drops that release from support, so make it a deliberate decision and not a tidy-up.
- **Set up the old install:** download that release's Darling zip and extract it to `C:\Program Files\PerformanceMonitorDarling`. Write a `darling.json` that points at one throwaway test instance. Install it the way that release documented: its `install-darling.ps1`, or the manual steps in the README's "Upgrading from a build that predates the script" section when that release has no script. Let it collect for at least two cycles. Before upgrading, record `MAX(version)` from `darling_schema_version` and the account the service runs as. Also record the row counts of two or three `collect.*` tables.
- **Upgrade:** extract the new release zip to a staging folder under `C:\Program Files\` that only administrators can write to. From an elevated PowerShell, run that copy's `upgrade-darling.ps1 -Source <staging folder>`. On one of the two runs, point `-Source` at the downloaded zip with `-Sha256` from the release page, so the hash path runs too. When the old release predates the script, the new zip's script still runs over it, because it finds the install through the registered service. If the README still tells those users to take the manual procedure, walk that once as well. Record which path worked.
- **Pass, at the script:** it exits 0 without a refusal. It prints that the SHA256 was verified (zip run), that `darling.json` is byte-identical, that the install manifest was written, and "Service is Running".
- **Pass, 10 to 15 minutes later:** work the five checks the script prints against the store.
  1. `MAX(version)` equals the new build's top rung.
  2. `collect.collection_log` has rows for every configured server in the last 15 minutes.
  3. The collector count is in the mid-30s, not single digits.
  4. Every non-SUCCESS row since the restart gets read. YIELDED is the lock-timeout guard working. Anything else is a finding.
  5. One table the release added or changed shows new rows.

  The row counts you recorded before the upgrade must still be there, and the service account must be unchanged. A service that is Running but not collecting is a fail.
- **What the tests already cover:** the `*RungTests` classes in `Darling/Darling.Tests` pin each migration rung's SQL and probes on every build, with no database. `MigrationUpgradeLadderLiveTests` replays the current ladder over each released store on a scratch PostgreSQL, in the `Darling PostgreSQL tests` job. `DarlingStoreUpgradeTests` and `DarlingStoreUpgradeRevertTests` cover the bundled runtime swap and the in-place PostgreSQL upgrade. They use the old and new runtimes that `Darling/tools/new-upgraded-store-fixture.ps1` builds, and the nightly sets that up.
- **What the live run adds:** none of those tests runs the upgrade script or the Windows service manager. None of them sees the file ownership and permissions of an install an older build made, or a store holding real rows.
- **Record:** the from and to versions, the install path, and the schema version before and after. Add the five check results and the tail of the script's output.

#### (c) A fresh `install-darling.ps1` run

Use a machine with no prior install and no `C:\ProgramData\PerformanceMonitorDarling` folder. Extract the release zip to `C:\Program Files\PerformanceMonitorDarling` and copy `darling.sample.json` to `darling.json`. Point it at the test instance, and run `install-darling.ps1` from an elevated PowerShell.

Pass means three things. The pre-flight `--test-connection` shows PASS for each server. The service is created on its virtual account and reaches Running. The same five store checks hold after 10 to 15 minutes.

Then run `uninstall-darling.ps1`, with `-PurgeData` on a machine you will reuse, and confirm the service is gone. Record the pre-flight lines and the five check results.

#### (d) The Linux tarball and the container image

The release also attaches `PerformanceMonitorDarling-linux-x64-<version>.tar.gz` and `SHA256SUMS-linux.txt`. The `darling-linux` job in `build.yml` builds them: a framework-dependent publish of the service, tarred with no wrapper folder. The same job pushes `ghcr.io/erikdarlingdata/performancemonitor-darling`, tagged with the version and `latest`.

Linux has no install script, no upgrade script and no systemd unit file in the tarball. The supported shapes are in the README's "Run on Linux" section. One is the compose stack in `Darling/compose/`. The other is the tarball extracted to `/opt/darling` under a unit written from the README's example, with your own PostgreSQL. 

There is no scripted Linux upgrade path. An upgrade is the same manual steps as a fresh install, over an existing store. So the release must check this by hand:

- **Artifacts:** the tarball and `SHA256SUMS-linux.txt` are on the release, the checksum matches, and the versioned and `latest` image tags exist.
- **Fresh install, compose:** bring the stack up from this release's `Darling/compose/` files on a fresh volume. Pass: `docker compose ps` shows the darling container healthy (not `unhealthy`). The log shows the config load and `Postgres store ready`. `collection_log` fills, as in the Windows checks.
- **Fresh install, systemd:** extract the tarball to `/opt/darling` and install `libgssapi-krb5-2`. Write the unit from the README. Point `DARLING_CONFIG` at a config for a throwaway PostgreSQL 15+ with TimescaleDB, and start it. Pass: the unit is active and the same store checks hold.
- **Upgrade:** repeat each shape over a store the previous release created. For compose, keep the volume and recreate the container from this release's compose file and image. For systemd, stop the unit, move `/opt/darling` aside, extract the new tarball, keep the config, and start the unit. Pass: the service starts, the store migrates to the new top rung, and collection continues with the earlier rows still present.
- **Oldest supported:** apply the same fixture rule as (b). If that release did not ship a Linux tarball, say so in the record and use the oldest release that did.
- **Record:** the same fields as (b), plus the image digest or tarball checksum you tested.

Put the outcome of (a) to (d) in the release's test record. An unchecked box blocks the tag.

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

SignPath blocks on **five** approval requests: **Lite** (zip), **Darling** (service + viewer apphost exes), **Lite (Velopack)**, **Darling Viewer (Velopack)**, and **the executables Velopack generates** — the Dashboard/Installer signing steps left with their artifacts. The fifth (#3288) comes after `vpk pack` and carries both products in one request: each `Setup.exe`, and the root launcher and `Update.exe` inside each `Portable.zip`. It uses the `SignTemplate` artifact configuration, whose `**/*.exe` and `**/*.dll` includes both need `min-matches="0"` because the batch contains no dll. Two executables per product stay unsigned by design and only inside the `.nupkg` — `lib/app/<App>_ExecutionStub.exe` and `lib/app/Squirrel.exe` — because `releases.<channel>.json` records that package's SHA256 and the delta patches against those bytes; the release guard allowlists exactly those, keyed on the container as well as the path. The `Darling` artifact-configuration slug on signpath.io signs the two apphost exes (`PerformanceMonitor.Darling.Service.exe` at the root and `viewer/PerformanceMonitor.Darling.Viewer.exe`) and leaves the `PerformanceMonitor.*` / third-party dlls and `pg-runtime.zip` alone; if it ever goes missing the single `Sign Darling` step fails the release. The Darling zip (`PerformanceMonitorDarling-{version}.zip` — service at root with `pg-runtime.zip` beside the exe, viewer in `viewer\`) joins the release assets; the pg-runtime build downloads ~340MB on a cold cache (cached across re-releases on the fetch script's content hash). Verify the Darling zip is attached and listed in `SHA256SUMS.txt`, alongside the Lite zip/Setup.exe and the Viewer Setup.exe.

## Notes

- Always test BEFORE merging PRs
- Test across the supported SQL Server version range (2016 SP2 through 2025; 2017 needs CU3 or later). A machine with local instances for this lists them in the repo local CLAUDE.md, along with any that hold real data and must not be written to. Connect via pre-configured sqlcmd contexts so no credentials appear in this file or in any command it runs.
- **NEVER use Express edition** for cloud test instances — Express does not support SQL Agent, and several collectors need Agent surface area
- Azure CLI (`az`) and AWS CLI (`aws`) are both available for creating/destroying test instances
- Always clean up cloud test resources after testing to avoid charges
