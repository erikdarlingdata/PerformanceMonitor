/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2119: the upgrade-path gate the 3.4.0 release lacked. Every pre-release validation ran either a
/// FRESH store (the full generator, no ladder) or the dogfood box (which walks each rung in the era
/// it ships) — nobody pointed new binaries at a store a RELEASED build had made, which is the only
/// path that REPLAYS old rungs with current-code generator output, and the only path users take.
/// The field failure: rung 51 replayed on a 3.3.0 store referenced V54's <c>query_plan_gz</c> three
/// rungs early and every 3.3.0→3.4.0 upgrade died with 42703 at service start.
///
/// <para>The fixture is the previous release's ladder EXACTLY as that release resolved it — every
/// rung's SQL frozen from the v3.3.0 tag's own code (its generators included: its V38 dim genuinely
/// lacks the gz column), plus the same version-table DDL and stamps its <c>MigrateLockedAsync</c>
/// writes. This test builds that store on scratch Postgres and runs the CURRENT ladder over it —
/// red on the day a generator learns a column an old rung will replay, green when the rung
/// pre-adds it. Regenerated at each release cut (<c>Darling/tools/generate-ladder-fixture</c>).
/// Verified two-sided at birth: against the pre-#2120 build it fails with the exact field 42703;
/// against the fixed build it climbs V39→V54 clean, including from a mid-failure retry.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class MigrationUpgradeLadderLiveTests : IClassFixture<MigrationUpgradeLadderLiveTests.LadderDatabaseGuard>
{
    private const string SkipReason =
        "DARLING_TEST_PG not set — this test needs a scratch PostgreSQL (with the timescaledb " +
        "extension available) it may create roles and databases on.";

    private const string FixtureDirectory = "Darling/Darling.Tests/Fixtures";

    /// <summary>
    /// The scratch database is named PER FIXTURE, and that is load-bearing rather than tidy.
    ///
    /// <para>The drop below is <c>WITH (FORCE)</c>, which terminates every backend on that database. With one
    /// shared name the second fixture's drop force-killed the connection the FIRST fixture had returned to
    /// Npgsql's pool — physically open, and as far as the pool knew, reusable. Npgsql then handed that dead
    /// connection back on the next open of the same connection string, and the first write failed with
    /// "Exception while writing to stream". Which is exactly what CI reported the moment this became a
    /// Theory: v3.3.0 green, v3.5.0 red, and nothing wrong with either ladder.</para>
    /// </summary>
    internal static string ScratchDatabaseFor(string fixtureFileName)
    {
        var version = Regex.Match(fixtureFileName, @"v(\d+)\.(\d+)\.(\d+)");
        return version.Success
            ? $"darling_upgrade_ladder_test_{version.Groups[1].Value}_{version.Groups[2].Value}_{version.Groups[3].Value}"
            : "darling_upgrade_ladder_test";
    }

    /// <summary>
    /// Every released-ladder fixture in the tree, so adding one at a release cut needs no edit here.
    ///
    /// <para>Deliberately a Theory over ALL of them rather than a Fact on the newest. The defect class is a
    /// current-code generator emitting a column that an OLD rung replays, and the older the fixture the more
    /// rungs get replayed to find it — v3.3.0 climbs 47 rungs to today's top where v3.5.0 climbs 7. Retiring
    /// the old fixture at each cut, which is what this tool's own instructions say to do, would shrink that
    /// surface every release until it caught nothing but the current cycle's own rungs.</para>
    /// </summary>
    public static TheoryData<string> LadderFixtures()
    {
        /* THROW rather than return empty, and the irony is the point: a [Theory] with zero data rows reports
           zero test cases and stays GREEN. Returning an empty set here on a failed root walk-up would make
           the entire upgrade-ladder climb vanish from a CI run with nothing red to notice — which is the same
           silent-decay failure this whole change exists to fix, one layer down and in the fix itself. This
           path cannot call Assert (it runs during data generation, not inside a test), so an exception is the
           loud option available. */
        var root = FindRepoRoot()
            ?? throw new InvalidOperationException(
                "Could not locate the repository root (walked up from the test binary looking for "
                + "PerformanceMonitor.sln). The upgrade-ladder fixtures live in the source tree, and without "
                + "them this Theory would silently contribute no test cases at all.");

        var dir = Path.Combine(root, FixtureDirectory.Replace('/', Path.DirectorySeparatorChar));
        var data = new TheoryData<string>();
        foreach (var path in Directory.EnumerateFiles(dir, "migration-ladder-v*.sql").OrderBy(p => p, StringComparer.Ordinal))
        {
            data.Add(Path.GetFileName(path));
        }

        if (data.Count == 0)
        {
            /* Same reasoning: no fixtures is not "nothing to test", it is the upgrade gate missing. */
            throw new InvalidOperationException(
                $"No migration-ladder fixtures found in {dir}. The upgrade-path gate cannot run, and an empty "
                + "Theory would report success by reporting nothing.");
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(LadderFixtures))]
    public async Task PreviousReleaseStore_ClimbsTheCurrentLadder_ToTheTop(string fixtureFileName)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);

        var root = FindRepoRoot();
        Assert.True(root is not null,
            "Could not locate the repository root (walked up from the test binary looking for " +
            "PerformanceMonitor.sln) — the fixture lives in the source tree.");
        var fixturePath = Path.Combine(
            root!, FixtureDirectory.Replace('/', Path.DirectorySeparatorChar), fixtureFileName);
        Assert.True(File.Exists(fixturePath), $"Previous-release ladder fixture missing: {fixturePath}");

        await ClimbAsync(baseConnectionString!, fixturePath, ScratchDatabaseFor(fixtureFileName));
    }

    /// <summary>
    /// A climb that throws (a rung that fails, a fixture batch that does not apply) still drops its database (#9929).
    /// A database that carries the TimescaleDB extension holds a background-worker slot for its scheduler for as long as
    /// it exists, so one left behind by a failed case costs a later class a worker. The failing path is the one a finally
    /// block is easiest to get wrong, so it is pinned on its own.
    /// </summary>
    [Fact]
    public async Task AClimbThatFails_StillDropsItsScratchDatabase()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);

        const string scratchDatabase = "darling_upgrade_ladder_test_cleanup_failing";
        var fixturePath = Path.Combine(Path.GetTempPath(), $"migration-ladder-cleanup-{Guid.NewGuid():N}.sql");
        await File.WriteAllTextAsync(
            fixturePath,
            "-- header\n-- ===BATCH=== bootstrap\nSELECT 1;\n-- ===BATCH=== fails\nSELECT * FROM no_such_table_9929;\n");
        try
        {
            var failure = await Assert.ThrowsAsync<PostgresException>(
                () => ClimbAsync(baseConnectionString!, fixturePath, scratchDatabase));
            Assert.Equal("42P01", failure.SqlState);
            Assert.Empty(await LadderDatabaseGuard.ExistingAsync(baseConnectionString!, new[] { scratchDatabase }));
        }
        finally
        {
            File.Delete(fixturePath);
        }
    }

    /// <summary>
    /// A climb that finishes drops its database, and the guard that reports leftovers lists a database that is there and
    /// none that is not. The class fixture asks the same question once every case of the class has run, so a case that
    /// leaves its database behind fails this class, not a later class that needed the worker slot.
    /// </summary>
    [Fact]
    public async Task AClimbThatPasses_DropsItsScratchDatabase_AndTheGuardListsOneThatIsLeftBehind()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);

        var root = FindRepoRoot();
        Assert.True(root is not null, "Could not locate the repository root.");
        /* The newest release by version, not by file name: "v3.9.0" sorts after "v3.14.0" as text, and the newest
           fixture is the shortest climb. */
        var newest = Directory
            .EnumerateFiles(
                Path.Combine(root!, FixtureDirectory.Replace('/', Path.DirectorySeparatorChar)),
                "migration-ladder-v*.sql")
            .Select(path => (FilePath: path, Version: Regex.Match(Path.GetFileName(path), @"v(\d+\.\d+\.\d+)")))
            .Where(f => f.Version.Success)
            .OrderBy(f => System.Version.Parse(f.Version.Groups[1].Value))
            .Last()
            .FilePath;
        const string scratchDatabase = "darling_upgrade_ladder_test_cleanup_real";

        await ClimbAsync(baseConnectionString!, newest, scratchDatabase);

        Assert.Empty(await LadderDatabaseGuard.ExistingAsync(baseConnectionString!, new[] { scratchDatabase }));

        /* The guard reports a database that is there: made by hand, seen, then removed. */
        try
        {
            await using (var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString!)))
            {
                await admin.OpenAsync();
                await using var create = new NpgsqlCommand($"CREATE DATABASE {scratchDatabase}", admin);
                await create.ExecuteNonQueryAsync();
            }

            Assert.Equal(
                new[] { scratchDatabase },
                await LadderDatabaseGuard.ExistingAsync(baseConnectionString!, new[] { scratchDatabase, "darling_upgrade_ladder_test_cleanup_absent" }));
        }
        finally
        {
            await DropScratchDatabaseAsync(baseConnectionString!, scratchDatabase);
        }

        Assert.Empty(await LadderDatabaseGuard.ExistingAsync(baseConnectionString!, new[] { scratchDatabase }));
    }

    /// <summary>
    /// Builds <paramref name="scratchDatabase"/> from the fixture, climbs the current ladder over it, and drops it again
    /// whether the climb passed or threw.
    ///
    /// <para>The database is gone when the case ends, not at the start of the next run (#9929). Every database that
    /// carries the TimescaleDB extension holds one background-worker slot for its scheduler for as long as it exists,
    /// and the stock <c>timescale/timescaledb</c> image starts with 16: a store that keeps one database per ladder fixture
    /// leaves a later class with no worker to run a job on. The drop at the start stays, so a run that was killed part
    /// way through still starts clean. A failure of the drop at the end never replaces the failure of the climb it follows.</para>
    /// </summary>
    internal static async Task ClimbAsync(string baseConnectionString, string fixturePath, string scratchDatabase)
    {
        /* Scratch database, dropped and recreated per run — the fixture creates schemas, hypertables,
           roles-adjacent grants, and the version table, none of which may leak between runs. The
           darling role is cluster-level and idempotently ensured (rung SQL grants to it). */
        /* #5549: unpooled, because this connection runs the FORCE drop. A pooled one would hand the backend that ran it
           to the next test, and a TimescaleDB job run on that backend crashed it with 0xC0000005. */
        await using (var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString)))
        {
            await admin.OpenAsync();
            await using (var role = new NpgsqlCommand(
                "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling') THEN CREATE ROLE darling LOGIN; END IF; END $$",
                admin))
            {
                await role.ExecuteNonQueryAsync();
            }
        }

        await DropScratchDatabaseAsync(baseConnectionString, scratchDatabase);

        await using (var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString)))
        {
            await admin.OpenAsync();
            await using var create = new NpgsqlCommand($"CREATE DATABASE {scratchDatabase}", admin);
            await create.ExecuteNonQueryAsync();
        }

        Exception? failure = null;
        try
        {
            await ApplyAndClimbAsync(baseConnectionString, fixturePath, scratchDatabase);
        }
        catch (Exception ex)
        {
            failure = ex;
            throw;
        }
        finally
        {
            try
            {
                await DropScratchDatabaseAsync(baseConnectionString, scratchDatabase);
            }
            catch (Exception ex) when (failure is not null)
            {
                Console.Error.WriteLine($"Ladder scratch database {scratchDatabase} was not dropped after a failed climb ({ex.GetType().Name}: {ex.Message}).");
            }
        }
    }

    private static async Task ApplyAndClimbAsync(string baseConnectionString, string fixturePath, string scratchDatabase)
    {
        var scratch = new NpgsqlConnectionStringBuilder(baseConnectionString) { Database = scratchDatabase };

        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync();

        /* Batch-execute the fixture the way psql -f does: autocommit per batch, one session (the
           leading SET search_path must hold for every later batch). NOT wrapped in transactions —
           TimescaleDB continuous aggregates refuse to be created inside one, and per-batch
           atomicity is meaningless for a fixture that either loads fully or fails the test. */
        var batches = File.ReadAllText(fixturePath)
            .Split("-- ===BATCH===", StringSplitOptions.RemoveEmptyEntries)
            /* Each split element leads with the marker's own label text (" bootstrap", " V40 …") up
               to its newline — drop that line; what follows is the batch's SQL. Element 0 is the
               file header comment, which has no newline-led SQL and trims to comment-only. */
            .Select(b => b.IndexOf('\n', StringComparison.Ordinal) is var nl && nl >= 0 ? b[(nl + 1)..].Trim() : "")
            /* Keep any batch with at least one non-comment line — a rung's SQL may legitimately
               OPEN with a comment, so a StartsWith filter would silently drop a whole rung. Only
               the file header (all-comment) and empty tails fall out. */
            .Where(b => b.Split('\n').Any(line => line.Trim().Length > 0 && !line.TrimStart().StartsWith("--", StringComparison.Ordinal)));

        foreach (var batch in batches)
        {
            await using var apply = new NpgsqlCommand(batch, connection) { CommandTimeout = 300 };
            await apply.ExecuteNonQueryAsync();
        }

        /* The previous release's store now exists exactly as its own code built it. Run the CURRENT
           ladder over it — the operator's upgrade, the path #2119 broke. */
        var applied = await PgMigrations.MigrateAsync(connection, null);
        Assert.True(applied > 0,
            "The current ladder applied nothing over the previous-release fixture — either the fixture " +
            "is stale (regenerate it from the release tag) or the ladder top never moved this cycle.");

        await using (var top = new NpgsqlCommand(
            "SELECT MAX(version) FROM collect.darling_schema_version", connection))
        {
            Assert.Equal(PgMigrations.Scripts.Max(m => m.Version), Convert.ToInt32(await top.ExecuteScalarAsync()));
        }

        /* The #2119 column specifically. On the v3.3.0 fixture the V38-era dim genuinely lacks it, so its
           presence proves the replayed rungs both passed and did their work; on a fixture cut after that
           rung it is already there and this is a cheap sanity check rather than the proof. Asserted for
           every fixture either way — the column must exist at the top of the ladder, whichever floor the
           climb started from. */
        await using (var gz = new NpgsqlCommand(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'query_plan_dim' AND column_name = '"
            + PayloadDimensions.CompressedContentColumn + "'", connection))
        {
            Assert.Equal(1L, await gz.ExecuteScalarAsync());
        }
    }

    /// <summary>
    /// Drops <paramref name="scratchDatabase"/> if it exists: this process's pooled connections to it are cleared first,
    /// then its TimescaleDB jobs are quiesced, then <c>DROP DATABASE ... WITH (FORCE)</c> runs on an unpooled admin
    /// connection. The same drop serves the start of a case and its end.
    /// </summary>
    private static async Task DropScratchDatabaseAsync(string baseConnectionString, string scratchDatabase)
    {
        /* Clear this process's own pooled connections to the scratch database BEFORE forcing the drop.
           Per-fixture naming already stops one case killing another's pooled connection; this covers the
           same hazard within a single case, where the pool key would be identical. Cheap, and the
           alternative failure is a socket error thrown a long way from its cause. */
        var scratchForPool = new NpgsqlConnectionStringBuilder(baseConnectionString) { Database = scratchDatabase };
        NpgsqlConnection.ClearPool(new NpgsqlConnection(scratchForPool.ConnectionString));

        /* No TimescaleDB job worker is left in the database the FORCE drop below kills (#5480). */
        await ScratchPostgres.QuiesceTimescaleJobsAsync(baseConnectionString, scratchDatabase);

        await using var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString));
        await admin.OpenAsync();
        await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS {scratchDatabase} WITH (FORCE)", admin);
        await drop.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Runs once the class's last case has finished and fails the class when a database a ladder case builds is still on
    /// the store (#9929). The names are the ones the Theory's fixtures map to, so a database some other run left under a
    /// name no current fixture produces is not this class's to judge. No store configured, nothing to check.
    /// </summary>
    public sealed class LadderDatabaseGuard : IAsyncLifetime
    {
        public ValueTask InitializeAsync() => default;

        public async ValueTask DisposeAsync()
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            if (string.IsNullOrEmpty(baseConnectionString))
            {
                return;
            }

            var names = LadderFixtures().Select(row => ScratchDatabaseFor(row.Data)).Distinct(StringComparer.Ordinal).ToList();
            var remaining = await ExistingAsync(baseConnectionString, names);
            Assert.True(
                remaining.Count == 0,
                "Ladder scratch databases outlived the class: " + string.Join(", ", remaining)
                + ". Each holds a TimescaleDB background-worker slot for its scheduler, and a later class on a small store runs out of workers (#9929).");
        }

        /// <summary>The members of <paramref name="names"/> that exist as databases on the store.</summary>
        internal static async Task<IReadOnlyList<string>> ExistingAsync(string baseConnectionString, IReadOnlyCollection<string> names)
        {
            await using var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString));
            await admin.OpenAsync();
            await using var query = new NpgsqlCommand("SELECT datname FROM pg_database WHERE datname = ANY($1) ORDER BY datname", admin);
            query.Parameters.AddWithValue(names.ToArray());
            var found = new List<string>();
            await using var reader = await query.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                found.Add(reader.GetString(0));
            }

            return found;
        }
    }

    /// <summary>
    /// The fixture set must contain one for the MOST RECENT RELEASE — the guard that stops this guard from
    /// decaying, which is not hypothetical: the fixture generator's own instructions say "regenerate at each
    /// release cut", and both the 3.4.0 and 3.5.0 cuts shipped without it, leaving the only tested upgrade
    /// population a 3.3.0 store while every real user was upgrading from 3.5.0.
    ///
    /// <para>Derived from CHANGELOG.md's newest SHIPPED release — the newest release heading that is NOT the
    /// in-development version in Directory.Build.props — rather than from a pinned constant that would decay
    /// the same way. It is deliberately the release users upgrade FROM, not the one being cut: a fixture of
    /// the version being cut is already at the top of the ladder and would climb nothing, failing the
    /// applied>0 assertion above. The release cut adds the new heading, and this fails until the previous
    /// release's fixture exists beside it. No live PostgreSQL needed, so it runs on every build rather than
    /// only where DARLING_TEST_PG is set — the climb tests are gated, and a gate is a poor place for the
    /// check that says the gated thing is testing the right store.</para>
    /// </summary>
    [Fact]
    public void TheMostRecentRelease_HasALadderFixture()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, "Could not locate the repository root.");

        var changelog = File.ReadAllText(Path.Combine(root!, "CHANGELOG.md"));

        /* The fixture we need is the most recent SHIPPED release — the store users upgrade FROM — not the
           version currently being cut, whose changelog heading now sits at the top. A fixture of the version
           being cut is already at the top of the ladder, so the climb theory above would apply nothing and
           fail its applied>0 assertion. So take the newest changelog release that is NOT the in-development
           version carried in Directory.Build.props. */
        var props = File.ReadAllText(Path.Combine(root!, "Directory.Build.props"));
        var current = Regex.Match(props, @"<Version>(\d+\.\d+\.\d+)</Version>");
        Assert.True(current.Success, "Directory.Build.props has no <Version> to identify the in-development release.");
        var inDevelopment = current.Groups[1].Value;

        var version = Regex.Matches(changelog, @"^## \[(\d+\.\d+\.\d+)\]", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .FirstOrDefault(v => v != inDevelopment);
        Assert.True(version is not null,
            "CHANGELOG.md has no shipped release heading (one that is not the in-development version) to derive the expected fixture from.");
        var expected = $"migration-ladder-v{version}.sql";
        var path = Path.Combine(root!, FixtureDirectory.Replace('/', Path.DirectorySeparatorChar), expected);

        Assert.True(
            File.Exists(path),
            $"No migration-ladder fixture for the most recent release ({version}). The upgrade-path test is "
            + $"therefore climbing from some OLDER store than the one users are actually upgrading from. "
            + $"Regenerate it at the release cut:\n"
            + $"  git worktree add /tmp/pm-v{version} v{version}\n"
            + $"  cd Darling/tools/generate-ladder-fixture && dotnet run "
            + $"-p:StorageProject=/tmp/pm-v{version}/Darling/PerformanceMonitor.Darling.Storage/PerformanceMonitor.Darling.Storage.csproj "
            + $"-- ../../Darling.Tests/Fixtures/{expected}");
    }

    /// <summary>Walks up from the test binary to the repository root.</summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
