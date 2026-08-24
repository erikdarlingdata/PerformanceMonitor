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
public sealed class MigrationUpgradeLadderLiveTests
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
    private static string ScratchDatabaseFor(string fixtureFileName)
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
        var data = new TheoryData<string>();
        var root = FindRepoRoot();
        if (root is null)
        {
            return data;
        }

        var dir = Path.Combine(root, FixtureDirectory.Replace('/', Path.DirectorySeparatorChar));
        foreach (var path in Directory.EnumerateFiles(dir, "migration-ladder-v*.sql").OrderBy(p => p, StringComparer.Ordinal))
        {
            data.Add(Path.GetFileName(path));
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
        var scratchDatabase = ScratchDatabaseFor(fixtureFileName);
        var fixturePath = Path.Combine(
            root!, FixtureDirectory.Replace('/', Path.DirectorySeparatorChar), fixtureFileName);
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

            /* Clear this process's own pooled connections to the scratch database BEFORE forcing the drop.
               Per-fixture naming already stops one case killing another's pooled connection; this covers the
               same hazard within a single case, where the pool key would be identical. Cheap, and the
               alternative failure is a socket error thrown a long way from its cause. */
            var scratchForPool = new NpgsqlConnectionStringBuilder(baseConnectionString) { Database = scratchDatabase };
            NpgsqlConnection.ClearPool(new NpgsqlConnection(scratchForPool.ConnectionString));

            await using (var drop = new NpgsqlCommand(
                $"DROP DATABASE IF EXISTS {scratchDatabase} WITH (FORCE)", admin))
            {
                await drop.ExecuteNonQueryAsync();
            }

            await using (var create = new NpgsqlCommand($"CREATE DATABASE {scratchDatabase}", admin))
            {
                await create.ExecuteNonQueryAsync();
            }
        }

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
    /// The fixture set must contain one for the MOST RECENT RELEASE — the guard that stops this guard from
    /// decaying, which is not hypothetical: the fixture generator's own instructions say "regenerate at each
    /// release cut", and both the 3.4.0 and 3.5.0 cuts shipped without it, leaving the only tested upgrade
    /// population a 3.3.0 store while every real user was upgrading from 3.5.0.
    ///
    /// <para>Derived from CHANGELOG.md's newest released heading rather than from a pinned version constant,
    /// because a constant is the same kind of thing that decayed: it would need remembering at exactly the
    /// moment the fixture needed remembering. The release cut moves the heading, and this fails until the
    /// fixture beside it exists. No live PostgreSQL needed, so it runs on every build rather than only where
    /// DARLING_TEST_PG is set — the climb tests are gated, and a gate is a poor place for the check that says
    /// the gated thing is testing the right store.</para>
    /// </summary>
    [Fact]
    public void TheMostRecentRelease_HasALadderFixture()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, "Could not locate the repository root.");

        var changelog = File.ReadAllText(Path.Combine(root!, "CHANGELOG.md"));
        var released = Regex.Match(changelog, @"^## \[(\d+\.\d+\.\d+)\]", RegexOptions.Multiline);
        Assert.True(released.Success, "CHANGELOG.md has no released version heading to derive the expected fixture from.");

        var version = released.Groups[1].Value;
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
