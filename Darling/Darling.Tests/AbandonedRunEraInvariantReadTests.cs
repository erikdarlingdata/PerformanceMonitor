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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2926: the Collection-Health abandonment count must be right across BOTH eras of a retention window.
///
/// <para><b>The shape.</b> #2803 gave a wall-clock-budget-abandoned cycle its own <c>ABANDONED</c> status.
/// <c>collection_log</c> is an append-only hypertable, so the cycles abandoned BEFORE that cannot be
/// rewritten: they sit in the same window carrying <c>status = 'SUCCESS'</c> beside <c>rows_collected = 0</c>
/// and the abandonment's own message. A count keyed on the status alone reads them as zero abandonments and
/// as successes, so a window straddling the boundary under-reports - and it under-reports in the direction
/// that looks healthy, which is the only direction nobody checks.</para>
///
/// <para><b>Why a live store rather than a source pin.</b> The pins in
/// <c>CollectorHealthClassifierTests</c> assert the predicate is present in all four reads; they cannot
/// assert it MATCHES, because a <c>LIKE</c> pattern that matches nothing is valid SQL returning a valid
/// zero. This class seeds one row of each era and asks a real Postgres.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class AbandonedRunEraInvariantReadTests
{
    private const int ServerId = -292601;
    private const string ServerName = "abandon-era-invariant";
    private const string Collector = "procedure_stats";

    /// <summary>
    /// 200 plain successes + 2 empty-but-fine successes + 3 abandonments = 205 runs. Positioned so the two
    /// counts land on OPPOSITE sides of the 0.5% band: three of 205 is 1.46% and bands WARNING, while the
    /// one row the status alone can see is 0.49% and reads HEALTHY.
    /// </summary>
    private const int OrdinarySuccesses = 200;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* ---------------- pure: the pattern, and why it cannot be an equality ---------------- */

    /// <summary>
    /// The reads match on a pattern, and the pattern is the note format with its budget hole widened to
    /// <c>%</c>. Asserted rather than assumed so re-wording the note cannot leave four reads matching a
    /// sentence nothing writes any more - the pattern is a literal in five <c>const</c> query strings and
    /// no compiler relates it to the format.
    /// </summary>
    [Fact]
    public void TheLikePattern_IsTheNoteFormat_WithTheBudgetHoleWidened() =>
        Assert.Equal(
            EnumeratedCollectorDriver.WholeCycleBudgetNoteFormat.Replace("{0}", "%", StringComparison.Ordinal),
            EnumeratedCollectorDriver.WholeCycleBudgetNoteSqlPattern);

    /// <summary>
    /// Why the match is a pattern at all: the budget is INTERPOLATED into the message and the shipped values
    /// are not one value. <c>procedure_stats</c>, <c>query_stats</c> and <c>plan_correction</c> carry 120 s;
    /// <c>query_store</c> carries the 600 s <c>PerDatabaseWallClockBudget</c>. An equality against either
    /// rendered sentence is a filter that silently answers for one collector and stays quiet about the rest.
    /// </summary>
    [Fact]
    public void TheShippedBudgetsDiverge_SoNoRenderedSentenceCanBeMatchedByEquality()
    {
        Assert.Equal(600, (int)QueryStoreCollector.PerDatabaseWallClockBudget.TotalSeconds);
        Assert.NotEqual(
            EnumeratedCollectorDriver.WholeCycleBudgetNote(120),
            EnumeratedCollectorDriver.WholeCycleBudgetNote(600));
    }

    /// <summary>Every shipped budget renders a note the single-<c>%</c> pattern matches, prefix and suffix
    /// exact and the hole exactly as wide as the rendered number.</summary>
    [Theory]
    [InlineData(120)]
    [InlineData(600)]
    public void EveryShippedBudget_RendersANoteThePatternMatches(int budgetSeconds)
    {
        var note = EnumeratedCollectorDriver.WholeCycleBudgetNote(budgetSeconds);
        var parts = EnumeratedCollectorDriver.WholeCycleBudgetNoteSqlPattern.Split('%');

        Assert.Equal(2, parts.Length);
        Assert.StartsWith(parts[0], note, StringComparison.Ordinal);
        Assert.EndsWith(parts[1], note, StringComparison.Ordinal);
        Assert.Equal(
            parts[0].Length + budgetSeconds.ToString(CultureInfo.InvariantCulture).Length + parts[1].Length,
            note.Length);
    }

    /// <summary>
    /// The undercount, as arithmetic, with no store involved: keyed on the status alone the fixture window
    /// yields ONE abandonment of 205 runs - 0.49%, under the 0.5% band, HEALTHY. The two rows the status
    /// cannot see are what carry it to 1.46% and WARNING. Same 205 runs, same collector, opposite verdict.
    /// </summary>
    [Fact]
    public void AStatusOnlyCount_BandsTheFixtureHealthy_WhichIsTheUndercount()
    {
        Assert.Equal(CollectorHealthClassifier.Healthy, CollectorHealthClassifier.Classify(
            totalRuns: 205, successCount: 204, errorCount: 0, permissionDeniedCount: 0, abandonedCount: 1,
            hoursSinceLastSuccess: 0.1, hoursSinceLastRun: 0.1, frequencyMinutes: 1, isOnLoad: false));

        Assert.Equal(CollectorHealthClassifier.Warning, CollectorHealthClassifier.Classify(
            totalRuns: 205, successCount: 202, errorCount: 0, permissionDeniedCount: 0, abandonedCount: 3,
            hoursSinceLastSuccess: 0.1, hoursSinceLastRun: 0.1, frequencyMinutes: 1, isOnLoad: false));
    }

    /* ---------------- live: the two-era window, against a real store ---------------- */

    /// <summary>
    /// One window, both eras, through the MCP read (<c>get_collection_health</c>, and the web dashboard's
    /// grid behind it) and the fleet rollup. Every assertion here is a number a status-only count gets wrong.
    /// </summary>
    [Fact]
    public async Task BothStatusEras_AreCountedAsAbandoned_AndBandTheCollector_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live abandonment-era test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            /* Post-#2803: the status says it outright. */
            await SeedAsync(connection, ct, MinutesAgo(30), "ABANDONED", 0,
                EnumeratedCollectorDriver.WholeCycleBudgetNote(120));

            /* Pre-#2803, and the whole point: SUCCESS on disk, nothing stored, the abandonment's own note.
               The second one is a DIFFERENT budget - query_store's 600 s against the 120 s above - so a
               reader that had pinned the one rendered sentence it happened to observe would count this as
               a clean run. */
            await SeedAsync(connection, ct, MinutesAgo(29), "SUCCESS", 0,
                EnumeratedCollectorDriver.WholeCycleBudgetNote(120));
            await SeedAsync(connection, ct, MinutesAgo(28), "SUCCESS", 0,
                EnumeratedCollectorDriver.WholeCycleBudgetNote(600));

            /* The two shapes the predicate must NOT claim. Both store nothing, which is half of the
               conjunction, and both are healthy: a run whose enumeration listed nothing, and a plain
               quiet cycle with no message at all. The second is what a NULL propagating through the
               success count's NOT would have silently dropped. */
            await SeedAsync(connection, ct, MinutesAgo(27), "SUCCESS", 0,
                EnumeratedCollectorDriver.EmptyEnumerationMessage);
            await SeedAsync(connection, ct, MinutesAgo(26), "SUCCESS", 0, null);

            for (var i = 0; i < OrdinarySuccesses; i++)
            {
                await SeedAsync(connection, ct, MinutesAgo(25 - (i % 20)), "SUCCESS", 500, null);
            }

            await using var postgres = NpgsqlDataSource.Create(cs!);

            /* ── the MCP / web read ── */
            var health = await DarlingDataReader.GetCollectionHealthAsync(
                postgres, ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)), ct);
            var row = health.Single(h => h.CollectorName == Collector);

            Assert.Equal(OrdinarySuccesses + 5, row.TotalRuns);

            /* Three abandonments, one of which the status names and two of which only the row does. */
            Assert.Equal(3, row.AbandonedCount);

            /* And the same three are OUT of the success count, so the Success and Abandoned columns that
               sit side by side in both WPF grids cannot both claim the same run. The two empty-but-fine
               runs are still successes, which is the half of this that a broader predicate would break. */
            Assert.Equal(OrdinarySuccesses + 2, row.SuccessCount);
            Assert.Equal(row.TotalRuns, row.SuccessCount + row.AbandonedCount);

            /* The consequence, which is the reason the count is worth being right about. */
            Assert.True(row.AbandonRatePercent > CollectorHealthClassifier.WarningAbandonRatePercent,
                $"abandon rate was {row.AbandonRatePercent}%");
            Assert.Equal(CollectorHealthClassifier.Warning, row.HealthStatus);

            /* ── the fleet rollup, which bands through the same classifier off its own SQL ── */
            var (fleetTotal, fleetSuccess, fleetAbandoned) = await ReadFleetCountsAsync(postgres, ct);
            Assert.Equal(OrdinarySuccesses + 5, fleetTotal);
            Assert.Equal(OrdinarySuccesses + 2, fleetSuccess);
            Assert.Equal(3, fleetAbandoned);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /* ── helpers ── */

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct,
        DateTime collectionTimeUtc, string status, int rowsCollected, string? message) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, Collector,
            DarlingMcpTestData.Naive(collectionTimeUtc), 100, status, message, rowsCollected, 80, 20);

    /// <summary>
    /// The fleet read's own three counts for this sentinel server, straight off
    /// <see cref="DarlingFleetReader.FleetCollectionHealthSql"/> - the rollup composes them into private
    /// per-server tallies, and what needs asserting is that ITS SQL counts the same way.
    /// </summary>
    private static async Task<(long Total, long Success, long Abandoned)> ReadFleetCountsAsync(
        NpgsqlDataSource postgres, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(DarlingFleetReader.FleetCollectionHealthSql);
        command.CommandTimeout = 30;
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetInt32(0) != ServerId || reader.GetString(1) != Collector)
            {
                continue;
            }

            return (Convert.ToInt64(reader.GetValue(2)), Convert.ToInt64(reader.GetValue(3)),
                    Convert.ToInt64(reader.GetValue(8)));
        }

        throw new InvalidOperationException("the fleet read returned no row for the sentinel server");
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
