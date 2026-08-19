/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
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
public sealed class MigrationUpgradeLadderLiveTests
{
    private const string SkipReason =
        "DARLING_TEST_PG not set — this test needs a scratch PostgreSQL (with the timescaledb " +
        "extension available) it may create roles and databases on.";

    private const string FixtureRelativePath = "Darling/Darling.Tests/Fixtures/migration-ladder-v3.3.0.sql";

    private const string ScratchDatabase = "darling_upgrade_ladder_test";

    [Fact]
    public async Task PreviousReleaseStore_ClimbsTheCurrentLadder_ToTheTop()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);

        var root = FindRepoRoot();
        Assert.True(root is not null,
            "Could not locate the repository root (walked up from the test binary looking for " +
            "PerformanceMonitor.sln) — the fixture lives in the source tree.");
        var fixturePath = Path.Combine(root!, FixtureRelativePath.Replace('/', Path.DirectorySeparatorChar));
        Assert.True(File.Exists(fixturePath), $"Previous-release ladder fixture missing: {fixturePath}");

        /* Scratch database, dropped and recreated per run — the fixture creates schemas, hypertables,
           roles-adjacent grants, and the version table, none of which may leak between runs. The
           darling role is cluster-level and idempotently ensured (rung SQL grants to it). */
        await using (var admin = new NpgsqlConnection(baseConnectionString))
        {
            await admin.OpenAsync();
            await using (var role = new NpgsqlCommand(
                "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling') THEN CREATE ROLE darling LOGIN; END IF; END $$",
                admin))
            {
                await role.ExecuteNonQueryAsync();
            }

            await using (var drop = new NpgsqlCommand(
                $"DROP DATABASE IF EXISTS {ScratchDatabase} WITH (FORCE)", admin))
            {
                await drop.ExecuteNonQueryAsync();
            }

            await using (var create = new NpgsqlCommand($"CREATE DATABASE {ScratchDatabase}", admin))
            {
                await create.ExecuteNonQueryAsync();
            }
        }

        var scratch = new NpgsqlConnectionStringBuilder(baseConnectionString) { Database = ScratchDatabase };

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

        /* The #2119 column specifically: the fixture's V38-era dim genuinely lacks it, so its
           presence proves the replayed rungs both passed and did their work. */
        await using (var gz = new NpgsqlCommand(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'query_plan_dim' AND column_name = '"
            + PayloadDimensions.CompressedContentColumn + "'", connection))
        {
            Assert.Equal(1L, await gz.ExecuteScalarAsync());
        }
    }

    /// <summary>Same walk-up idiom as <c>DocCommentHygieneTests.FindRepoRoot</c>.</summary>
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
