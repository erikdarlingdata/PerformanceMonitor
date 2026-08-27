/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V81 rung (#2515) — tempdb's growth CEILING on <c>tempdb_stats</c>. It was the top of the ladder when
/// it landed; V82 (#2530) has since been appended, and the assertions that belonged to being TOP moved with
/// it rather than being re-pinned here.
///
/// <para><b>What it fixes.</b> <c>TempDbSpaceInfo.UsedPercent</c> divided by
/// <c>total_reserved + unallocated</c>, and both halves come from <c>dm_db_file_space_usage</c>, which
/// reports the data files AS CURRENTLY ALLOCATED. That is distance to the next AUTOGROW. It reads as real
/// headroom on a pre-sized on-prem box only because such a tempdb has already grown to its cap.</para>
///
/// <para><b>The measurement that settled it.</b> On <c>GP_S_Gen5_2</c> tempdb is four data files of 16 MB
/// with <c>max_size</c> 2,097,152 pages each — a 65,536 MB ceiling against 62.44 MB allocated. One ~57 MB
/// <c>#temp</c> table reads 95.7% full against the allocation and 0.09% against the cap, so shipping the
/// Azure collection gate removal (#2512 / #2516) on the old denominator would have armed an alert that fires
/// on the first busy minute of every Azure target at the shipped 80% default.</para>
///
/// <para><b>Not a size floor</b>, which was the other candidate: a floor suppresses the alert on a genuinely
/// full LARGE tempdb at the moment it starts growing, still fires at the autogrow boundary once cleared, and
/// picks a value that silently redefines "small" for every on-prem and RDS target already relying on today's
/// behaviour.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TempDbCeilingStoreTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -815151;
    private static readonly string TestServerKey = TestServerId.ToString(CultureInfo.InvariantCulture);
    private const string TestServerName = "tempdb-ceiling-e2e";

    /* ---------------- the rung ---------------- */

    [Fact]
    public void TheRungIsRegisteredAndIsTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("tempdb-max-size", PgMigrations.Scripts.Single(s => s.Version == 81).Name);

        /* Demoted at V82 (#2530). This used to pin 81 as the ladder TOP; that claim belongs to whichever
           rung is newest, and leaving it here would have made every future rung edit this file. What
           stays is the claim this rung actually owns — it is registered, at its own number, and the
           ladder it sits in is still strictly ordered and dense above the sanctioned V45 hole. */
        Assert.True(versions.Max() >= 81);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// One nullable column and a view refresh. Nullable with no DEFAULT keeps this a catalog-only change in
    /// PostgreSQL, which stays instant on a compressed hypertable. No backfill, and none is possible: a row
    /// collected before this rung genuinely does not know the ceiling, and NULL says so where any number
    /// would claim a measurement nobody took.
    /// </summary>
    [Fact]
    public void TheRungAddsTheColumn_Idempotently_AndWithoutADefault()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 81).Sql;

        Assert.Contains("ALTER TABLE collect.tempdb_stats", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS max_size_mb numeric(18,2)", sql, StringComparison.Ordinal);

        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The view refresh, which is the half of this rung that is easy to forget and impossible to notice.
    /// Postgres FREEZES a view's <c>SELECT *</c> column list at CREATE, and the analysis fact collector reads
    /// tempdb through that V4 passthrough — so without this, an UPGRADED store would keep serving the old
    /// column list and <c>analyze_server</c> would go on scoring against the allocation while the alert had
    /// already moved to the ceiling. Two surfaces, one server, two different answers.
    /// </summary>
    [Fact]
    public void TheRungRefreshesThePassthroughView_WhichTheAnalysisReadGoesThrough()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 81).Sql;

        Assert.Contains(
            "CREATE OR REPLACE VIEW collect.v_tempdb_stats AS SELECT * FROM collect.tempdb_stats;",
            sql, StringComparison.Ordinal);

        Assert.Contains("FROM v_tempdb_stats", PgFactCollector.TempDbSql, StringComparison.Ordinal);
        Assert.Contains("MIN(max_size_mb)", PgFactCollector.TempDbSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The collector column has to exist for the rung to be adding the same thing a FRESH store generates.
    /// Both stores build their DDL from <c>PayloadColumns</c>, so a rung whose type disagreed with the
    /// generator would split the two populations permanently and invisibly.
    /// </summary>
    [Fact]
    public void TheGeneratedSchemaAndTheRungAgreeOnTheColumn()
    {
        var generated = PgSchemaGenerator.CreateTable(PerformanceMonitor.Collectors.TempDbStatsCollector.Instance);

        Assert.Contains("max_size_mb numeric(18,2)", generated, StringComparison.Ordinal);
        Assert.Contains("max_size_mb numeric(18,2)", PgMigrations.Scripts.Single(s => s.Version == 81).Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeAsksForTheColumn_AndTheThreePlacesAgree()
    {
        Assert.Contains(
            "table_name = 'tempdb_stats' AND column_name = 'max_size_mb'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var mapParameters = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .GetParameters().Length;

        var viewerSource = ReadViewerSource();

        /* Demoted at V82 (#2530): this read `mapParameters - 1`, which is the NEWEST sentinel's ordinal, so
           the moment a rung was appended it silently started testing that rung's wiring instead of this
           one's. Pinned at 56 — this rung's own ordinal — which cannot slide. The "and no more than that"
           half is the top rung's to assert, and it moved to MonitoredEngineKindStoreTests with it. */
        Assert.True(mapParameters > 56);
        Assert.Contains("reader.GetBoolean(56)", viewerSource, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeMapsAStoreAtExactly81To81()
    {
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* 56 positional sentinels, then this rung's own, then FALSE for anything a later rung appends. The
           leading count is FIXED at this rung's ordinal deliberately: deriving it from arity reads
           identically while this is the top rung, then slides one place right per new rung — the assertion
           keeps passing while quietly testing a newer arm. */
        var all = Enumerable.Repeat(true, 56).Cast<object>().ToArray();
        object[] Args(bool ownFlag) => all
            .Concat(new object[] { ownFlag })
            .Concat(Enumerable.Repeat((object)false, arity - 57))
            .ToArray();

        Assert.Equal(81, (int)method.Invoke(null, Args(true))!);
        Assert.Equal(80, (int)method.Invoke(null, Args(false))!);
    }

    /* ---------------- the arithmetic ---------------- */

    /// <summary>
    /// THE MEASUREMENT, as arithmetic. The Azure shape is the one from the issue verbatim — 59.75 MB reserved
    /// and 2.69 MB unallocated inside four 16 MB files whose <c>max_size</c> sums to 65,536 MB — and the whole
    /// point of the fix is that this must NOT clear the shipped 80% default.
    /// </summary>
    [Fact]
    public void TheAzureShape_ReadsAsEmptyAgainstItsCeiling_AndDoesNotClearTheDefaultThreshold()
    {
        var azure = new TempDbSpaceInfo { TotalReservedMb = 59.75, UnallocatedMb = 2.69, MaxSizeMb = 65_536 };

        /* 62.44 MB allocated — the number the old denominator used, and it is a thousandth of the ceiling. */
        Assert.Equal(62.44, azure.AllocatedMb, precision: 2);
        Assert.Equal(65_536d, azure.CapacityMb, precision: 2);

        Assert.Equal(0.0912, azure.UsedPercent, precision: 4);
        Assert.True(azure.UsedPercent < DefaultTempDbThresholdPercent);

        /* And the number the old denominator produced, kept here so the size of the correction is on the
           record rather than being re-derived by hand: the same snapshot used to read 95.7% full. */
        var withoutTheCeiling = new TempDbSpaceInfo { TotalReservedMb = 59.75, UnallocatedMb = 2.69 };
        Assert.Equal(95.69, withoutTheCeiling.UsedPercent, precision: 2);
        Assert.True(withoutTheCeiling.UsedPercent >= DefaultTempDbThresholdPercent);
    }

    /// <summary>
    /// The on-prem shape that must NOT move: an unlimited data file has no ceiling to measure against, so the
    /// current allocation stays the denominator and the percentage is exactly what it was before this rung.
    /// That is what keeps every unlimited-growth on-prem and RDS target reporting the number it reports today.
    /// </summary>
    [Fact]
    public void TheUnlimitedOnPremShape_StillDividesByTheAllocation()
    {
        var unlimited = new TempDbSpaceInfo { TotalReservedMb = 800, UnallocatedMb = 200, MaxSizeMb = -1 };

        Assert.Equal(1000d, unlimited.CapacityMb, precision: 2);
        Assert.Equal(80d, unlimited.UsedPercent, precision: 3);
        Assert.True(unlimited.UsedPercent >= DefaultTempDbThresholdPercent);

        /* A snapshot from before the column existed reports 0, a different fact with the same answer: nobody
           measured a ceiling, so the allocation is the only honest denominator. */
        var notMeasured = new TempDbSpaceInfo { TotalReservedMb = 800, UnallocatedMb = 200 };
        Assert.Equal(unlimited.UsedPercent, notMeasured.UsedPercent, precision: 9);
    }

    /// <summary>
    /// The on-prem shape that DOES move, stated plainly because it is the one thing this rung changes about
    /// numbers an operator is already looking at: a tempdb with a fixed <c>max_size</c> it has not grown into
    /// reports a LOWER percentage than before. That is the correction — 800 MB inside a 1,000 MB allocation
    /// that may grow to 4,000 MB is 20% of the way to the wall, not 80%.
    /// </summary>
    [Fact]
    public void TheCappedOnPremShape_ReportsLowerThanItUsedTo()
    {
        var capped = new TempDbSpaceInfo { TotalReservedMb = 800, UnallocatedMb = 200, MaxSizeMb = 4_000 };

        Assert.Equal(20d, capped.UsedPercent, precision: 3);
        Assert.True(capped.UsedPercent < DefaultTempDbThresholdPercent);

        /* And it climbs back to the alert as the files actually approach the cap, which is the behaviour the
           old denominator could never produce: 3,400 MB reserved inside a 4,000 MB ceiling is 85%. */
        var nearlyFull = new TempDbSpaceInfo { TotalReservedMb = 3_400, UnallocatedMb = 100, MaxSizeMb = 4_000 };
        Assert.Equal(85d, nearlyFull.UsedPercent, precision: 3);
        Assert.True(nearlyFull.UsedPercent >= DefaultTempDbThresholdPercent);
    }

    /// <summary>
    /// A ceiling below what is already allocated cannot make the percentage exceed 100. The larger of the two
    /// wins, because a file that has grown past a later-lowered cap is at its wall rather than beyond it.
    /// </summary>
    [Fact]
    public void ACeilingBelowTheAllocation_DoesNotProduceAPercentageAbove100()
    {
        var shrunkCap = new TempDbSpaceInfo { TotalReservedMb = 950, UnallocatedMb = 50, MaxSizeMb = 400 };

        Assert.Equal(1000d, shrunkCap.CapacityMb, precision: 2);
        Assert.Equal(95d, shrunkCap.UsedPercent, precision: 3);
    }

    /// <summary>
    /// The shipped default, read off the config rather than typed here — the threshold the assertions above
    /// compare against has to be the one the product actually ships or they prove nothing about it.
    /// </summary>
    private static int DefaultTempDbThresholdPercent => new DarlingConfig().Alerts.TempDbSpaceThresholdPercent;

    [Fact]
    public void TheDefaultThresholdIsStill80()
    {
        Assert.Equal(80, DefaultTempDbThresholdPercent);
    }

    /* ---------------- gated live E2E ---------------- */

    /// <summary>
    /// All three states of the ceiling through the REAL read against live Postgres. The pure arithmetic
    /// above cannot see a column that is selected but not mapped, an ordinal off by one, or a NULL that
    /// arrives as something other than zero — and every one of those would put the alert back on the
    /// allocation silently.
    /// </summary>
    [Fact]
    public async Task EndToEnd_TheCeilingSurvivesTheStoreRoundTrip_ForAllThreeStates()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb-ceiling test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(postgres);

        var bodySucceeded = false;
        try
        {
            var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified).AddMinutes(-1);

            /* The Azure shape, verbatim from the issue's measurement. */
            await InsertAsync(connection, collectionTime, 5.44m, 1.81m, 0m, 59.75m, 2.69m, 65_536m, ct);

            var azure = await adapter.GetTempDbSpaceAsync(TestServerKey, ct);
            Assert.NotNull(azure);
            Assert.Equal(65_536d, azure!.MaxSizeMb, precision: 2);
            Assert.Equal(0.0912, azure.UsedPercent, precision: 4);
            Assert.True(azure.UsedPercent < DefaultTempDbThresholdPercent,
                "62 MB allocated against a 65,536 MB cap must not clear the 80% default.");

            /* The unlimited on-prem shape, newer so it wins the ORDER BY. -1 must survive the round trip AS
               -1: the percentage would come out at 80 either way here, but the SIGN is the only thing that
               separates "this tempdb has no ceiling" from "nobody measured one", and it is what the alert
               detail renders as Unlimited rather than Unknown. */
            await InsertAsync(connection, collectionTime.AddSeconds(30), 500m, 250m, 50m, 800m, 200m, -1m, ct);

            var unlimited = await adapter.GetTempDbSpaceAsync(TestServerKey, ct);
            Assert.Equal(-1d, unlimited!.MaxSizeMb, precision: 2);
            Assert.Equal(80d, unlimited.UsedPercent, precision: 3);

            /* And a pre-rung row, whose ceiling is genuinely NULL. It must arrive as 0 rather than throwing
               or reading as a zero-megabyte cap — this is what every historical row in a real store looks
               like the moment the migration lands. */
            await InsertAsync(connection, collectionTime.AddSeconds(60), 500m, 250m, 50m, 800m, 200m, null, ct);

            var history = await adapter.GetTempDbSpaceAsync(TestServerKey, ct);
            Assert.Equal(0d, history!.MaxSizeMb, precision: 2);
            Assert.Equal(80d, history.UsedPercent, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, DateTime collectionTime,
        decimal userMb, decimal internalMb, decimal versionStoreMb,
        decimal totalReservedMb, decimal unallocatedMb, decimal? maxSizeMb,
        System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb, total_reserved_mb, unallocated_mb, top_session_id, top_session_tempdb_mb, max_size_mb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            connection);

        command.Parameters.AddWithValue(collectionTime.Ticks);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(userMb);
        command.Parameters.AddWithValue(internalMb);
        command.Parameters.AddWithValue(versionStoreMb);
        command.Parameters.AddWithValue(totalReservedMb);
        command.Parameters.AddWithValue(unallocatedMb);
        command.Parameters.AddWithValue(55);
        command.Parameters.AddWithValue(12.5m);
        command.Parameters.AddWithValue(maxSizeMb.HasValue ? maxSizeMb.Value : (object)DBNull.Value);

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM tempdb_stats WHERE server_id = {TestServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static string ReadViewerSource() =>
        System.IO.File.ReadAllText(System.IO.Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs"));

    private static string RepoRoot([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        for (var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (System.IO.Directory.Exists(System.IO.Path.Combine(dir.FullName, "PerformanceMonitor.Common")))
            {
                return dir.FullName;
            }
        }

        throw new System.IO.DirectoryNotFoundException($"Could not locate the repo root walking up from {thisFile}");
    }
}
