/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2515, Lite's half: the tempdb growth ceiling through the REAL DuckDB store and the REAL reads.
///
/// <para>Darling's <c>TempDbCeilingStoreTests</c> pins the same behaviour against live Postgres. Both are
/// needed and neither substitutes for the other: the two SKUs write their own SQL against their own engine,
/// share the <c>TempDbSpaceInfo</c> that does the arithmetic, and a column selected in one adapter but not
/// the other puts exactly one product silently back on the old denominator.</para>
///
/// <para>These go through a real database rather than a source pin because the defect this guards against —
/// an ordinal off by one, a NULL arriving as something other than zero, a column added to the schema but not
/// to the read — is invisible to any assertion over query TEXT.</para>
/// </summary>
public sealed class TempDbCeilingStoreTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public TempDbCeilingStoreTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    /// <summary>
    /// The migration is the Lite twin of Darling's V81 rung, and the version has to move with it or an
    /// existing Lite database never gets the column and every read of it fails.
    /// </summary>
    [Fact]
    public void TheSchemaVersionMovedWithTheColumn()
    {
        Assert.Equal(56, DuckDbInitializer.CurrentSchemaVersion);

        var ddl = DuckDbSchemaGenerator.CreateTable(PerformanceMonitor.Collectors.TempDbStatsCollector.Instance);
        Assert.Contains("max_size_mb DECIMAL(18,2)", ddl, System.StringComparison.Ordinal);
    }

    /// <summary>
    /// The Azure shape from the issue's measurement, round-tripped: 59.75 MB reserved and 2.69 MB unallocated
    /// inside four 16 MB files whose <c>max_size</c> sums to 65,536 MB. It must read as 0.09% full rather than
    /// the 95.7% the allocation produces, which is the whole difference between silence and a page.
    /// </summary>
    [Fact]
    public async Task TheAzureShape_RoundTripsItsCeiling_AndReadsAsEmpty()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedTempDbAsync(
            reservedMb: 59.75, unallocatedMb: 2.69,
            userObjectMb: 5.44, internalObjectMb: 1.81, versionStoreMb: 0.01,
            maxSizeMb: 65_536);

        var info = await new LocalDataService(_duckDb).GetLatestTempDbSpaceAsync(TestDataSeeder.TestServerId);

        Assert.NotNull(info);
        Assert.Equal(65_536d, info!.MaxSizeMb, precision: 2);
        Assert.Equal(0.0912, info.UsedPercent, precision: 4);
        Assert.True(info.UsedPercent < 80, "62 MB allocated against a 65,536 MB cap must not clear the 80% default.");
    }

    /// <summary>
    /// Unlimited survives as -1 rather than being flattened, and takes the allocation as its denominator —
    /// so every unlimited-growth on-prem and RDS target reports exactly the number it reports today.
    /// </summary>
    [Fact]
    public async Task AnUnlimitedCeiling_SurvivesAsMinusOne_AndKeepsTheAllocationDenominator()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedTempDbAsync(reservedMb: 800, unallocatedMb: 200, maxSizeMb: -1);

        var info = await new LocalDataService(_duckDb).GetLatestTempDbSpaceAsync(TestDataSeeder.TestServerId);

        Assert.Equal(-1d, info!.MaxSizeMb, precision: 2);
        Assert.Equal(80d, info.UsedPercent, precision: 3);
    }

    /// <summary>
    /// And a row from before the migration, whose ceiling is genuinely NULL. It has to arrive as 0 — the
    /// "not measured" state — rather than as a zero-megabyte cap, which would divide by nothing. This is what
    /// every historical row in a real Lite database looks like the moment the upgrade lands.
    /// </summary>
    [Fact]
    public async Task ANullCeiling_ReadsAsNotMeasured_AndTheNumberDoesNotMove()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedTempDbAsync(reservedMb: 800, unallocatedMb: 200);

        var info = await new LocalDataService(_duckDb).GetLatestTempDbSpaceAsync(TestDataSeeder.TestServerId);

        Assert.Equal(0d, info!.MaxSizeMb, precision: 2);
        Assert.Equal(80d, info.UsedPercent, precision: 3);
    }

    /// <summary>
    /// The ANALYSIS surface has to agree with the alert, or <c>analyze_server</c> scores the same Azure target
    /// at 96% full while the pager stays quiet — two answers about one server, which is the shape of defect
    /// this whole change exists to remove. The fact carries its own denominator, so it needed the same fix.
    /// </summary>
    [Fact]
    public async Task TheAnalysisFact_ScoresAgainstTheCeilingToo()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedTempDbAsync(
            reservedMb: 59.75, unallocatedMb: 2.69,
            userObjectMb: 5.44, internalObjectMb: 1.81, versionStoreMb: 0.01,
            maxSizeMb: 65_536);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(TestDataSeeder.CreateTestContext());
        var tempdb = facts.First(f => f.Key == "TEMPDB_USAGE");

        Assert.Equal(0.000912, tempdb.Value, precision: 6);
        Assert.Equal(65_536d, tempdb.Metadata["max_size_mb"], precision: 2);

        /* The severity arm concerns at 0.75 and criticals at 0.90, so the corrected fraction scores nothing —
           where the allocation fraction (0.957) would have pinned it at the top of the scale. */
        Assert.True(tempdb.Value < 0.75);
    }
}
