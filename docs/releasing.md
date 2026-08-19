# Release upgrade validation

Seeded after #2119, where 3.4.0 shipped a migration ladder no store from a released 3.3.0 could
climb — every pre-release check had run either a **fresh** store (full generator, no ladder) or the
dogfood box (which walks each rung in the era it ships). Nobody pointed new binaries at a store a
released build had made, which is the only path that **replays** old rungs with current-code
generator output — and the only path users take.

Two gates, one automated and one at cut time:

## 1. CI: the previous-release ladder fixture (automated, every PR)

`Darling.Tests/Fixtures/migration-ladder-v<prev>.sql` is the previous release's migration ladder
**exactly as that release resolved it** — every rung's SQL frozen from the release tag's own code,
generators included, plus the version-table DDL and stamps its migrator writes.
`MigrationUpgradeLadderLiveTests` builds that store on scratch Postgres and runs the current ladder
over it. It goes red the day a generator learns a column an old rung will replay.

**At each release cut, regenerate the fixture from the new tag** so the gate always guards the
"from the release users are actually on" path:

```
git worktree add /tmp/pm-vX.Y.Z vX.Y.Z
cd Darling/tools/generate-ladder-fixture
dotnet run -p:StorageProject=/tmp/pm-vX.Y.Z/Darling/PerformanceMonitor.Darling.Storage/PerformanceMonitor.Darling.Storage.csproj \
  -- ../../Darling.Tests/Fixtures/migration-ladder-vX.Y.Z.sql
```

then point `MigrationUpgradeLadderLiveTests.FixtureRelativePath` at the new file and retire the old
one (keeping more than one previous release is fine too — each is one more `[Fact]`).

## 2. Cut time: previous release image, same volume (the literal operator path)

Before publishing, run the PREVIOUS release's container against a fresh volume, let it initialize
and collect for a few minutes, stop it, and start the RELEASE-CANDIDATE image **on the same
volume**:

```
docker volume create pm-upgrade-check
docker run -d --name pm-old -v pm-upgrade-check:/var/lib/darling ghcr.io/erikdarlingdata/performancemonitor-darling:<prev>
# wait for "store ready", stop:
docker rm -f pm-old
docker run -d --name pm-new -v pm-upgrade-check:/var/lib/darling ghcr.io/erikdarlingdata/performancemonitor-darling:<rc>
docker logs -f pm-new   # must reach "store ready (schema vNN, M migration(s) applied)" with M > 0
```

`M > 0` is the point: zero applied means the ladder never ran and the check validated nothing.
Windows/Velopack releases ride the same store code, so the container check covers the ladder for
both — the Windows-specific surfaces (installer, service, DPAPI) have their own checks.
