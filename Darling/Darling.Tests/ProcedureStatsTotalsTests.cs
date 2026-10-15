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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for the per-collection procedure totals (#5638, V176) that need no store: the rung and its text, the upsert statement, the
/// runner's call site, the retention registration, and the MCP duration trends' tier clock. The live behavior is in
/// <see cref="ProcedureStatsTotalsWriterLiveTests"/>.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ProcedureStatsTotalsTests
{
    private static string Normalize(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>Review of #5706: the retention predicate carries the floor as a parameter and never scans procedure_stats itself (#1815).</summary>
    [Fact]
    public void ThePrunePredicate_TakesTheFloorAsAParameter_AndNeverScansProcedureStats()
    {
        Assert.DoesNotContain("procedure_stats", ProcedureStatsTotals.PrunePredicate.Replace("procedure_stats_totals", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("$2", ProcedureStatsTotals.PrunePredicate, StringComparison.Ordinal);

        var delete = DarlingRetention.TimeSlicedDeleteSql(ProcedureStatsTotals.Table, "collection_time", ProcedureStatsTotals.PrunePredicate);
        Assert.DoesNotContain("FROM collect.procedure_stats)", delete, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM collect.procedure_stats ", delete, StringComparison.Ordinal);

        var retention = Normalize(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs"));
        Assert.Contains("secondParameterFactory: async (floorConnection, ct) =>", retention, StringComparison.Ordinal);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(retention, "ProcedureStatsTotals\\.MeasureRawFloorAsync\\("));

        /* Round 2 (L4): the floor is measured inside PurgeOneAsync, after its wall-budget gate and on the purge's own connection. */
        var purgeOne = retention[retention.IndexOf("private static async Task<int?> PurgeOneAsync(", StringComparison.Ordinal)..];
        var gate = purgeOne.IndexOf("SkipTableOnBudget(tableName)", StringComparison.Ordinal);
        var open = purgeOne.IndexOf("postgres.OpenConnectionAsync(", StringComparison.Ordinal);
        var factory = purgeOne.IndexOf("await secondParameterFactory(connection, cancellationToken)", StringComparison.Ordinal);
        Assert.True(gate > 0 && open > gate && factory > open, "the budget gate, then the connection, then the floor measured on it");
    }

    /// <summary>Round 2 (L3): the plain-store floor is one server-leading index probe per server with totals, never a global min().</summary>
    [Fact]
    public void ThePlainFloor_IsAnIndexProbePerServerWithTotals_NotAGlobalMin()
    {
        var sql = Normalize(ProcedureStatsTotals.PlainRawFloorSql);

        Assert.Contains("CROSS JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE p.server_id = servers.server_id\n    ORDER BY p.collection_time\n    LIMIT 1", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.procedure_stats_totals AS t", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("min(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("min(oldest.collection_time)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The read-back limit's type check (seat review of #5706). Every write goes through <c>CopyBatchOnceAsync&lt;TRow&gt;(... List&lt;TRow&gt; rows ...)</c>
    /// and the catalog's only procedure_stats definition is a <c>CollectorDefinitionBase&lt;Row&gt;</c>, so the server, Azure
    /// per-database and import paths all hand the writer a <c>List&lt;Row&gt;</c>; the helper accepts any <c>IEnumerable&lt;Row&gt;</c>.
    /// </summary>
    [Fact]
    public void TheReadBackLimit_MatchesEveryRowCollectionShape_AndEveryProcedureStatsPathCarriesRows()
    {
        var rows = new List<ProcedureStatsCollector.Row> { Named("A"), Named("B"), Named("A"), Named(null!) };
        var expected = new string?[] { "A", "B", null };

        Assert.Equal(expected, ProcedureStatsTotalsWriter.DatabaseNamesOf(rows));
        Assert.Equal(expected, ProcedureStatsTotalsWriter.DatabaseNamesOf(rows.ToArray()));
        Assert.Equal(expected, ProcedureStatsTotalsWriter.DatabaseNamesOf(rows.AsEnumerable()));
        Assert.Equal(expected, ProcedureStatsTotalsWriter.DatabaseNamesOf(new System.Collections.ObjectModel.ReadOnlyCollection<ProcedureStatsCollector.Row>(rows)));
        Assert.Empty(ProcedureStatsTotalsWriter.DatabaseNamesOf(new List<ProcedureStatsCollector.Row>())!);

        /* Rows of another type (or none) read the whole collection. */
        Assert.Null(ProcedureStatsTotalsWriter.DatabaseNamesOf(new List<string> { "A" }));
        Assert.Null(ProcedureStatsTotalsWriter.DatabaseNamesOf(null));

        /* The call-site trace: one writer call, fed by the helper, from the one generic COPY whose rows are a List<TRow>... */
        var runner = Normalize(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));
        var start = runner.IndexOf("private async Task<int> CopyBatchOnceAsync<TRow>(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var signature = runner[start..(start + 400)];
        Assert.Contains("List<TRow> rows,", signature, StringComparison.Ordinal);
        Assert.Contains("ProcedureStatsTotalsWriter.DatabaseNamesOf(rows)", runner[start..], StringComparison.Ordinal);
        Assert.DoesNotContain("procedureRows", runner[start..], StringComparison.Ordinal);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(runner, "ProcedureStatsTotalsWriter\\.UpsertBatchAsync\\("));

        /* ...and every catalog definition that writes procedure_stats carries Row (so TRow = Row on every path that totals). */
        var writers = PerformanceMonitor.Collectors.CollectorCatalog.All
            .Where(c => string.Equals(c.TargetTable, ProcedureStatsCollector.Instance.TargetTable, StringComparison.OrdinalIgnoreCase))
            .ToList();
        Assert.NotEmpty(writers);
        Assert.All(writers, c => Assert.IsAssignableFrom<PerformanceMonitor.Collectors.ICollectorDefinition<ProcedureStatsCollector.Row>>(c));
    }

    private static ProcedureStatsCollector.Row Named(string database) => new(
        database, "dbo", "p", "P ", null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null, null, null);

    /* ---- the MCP duration trends' tier clock (#5638) ---- */

    private static readonly DateTime Now = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TierClock_WithNoAsOf_IsTheWindowEndItself()
    {
        Assert.Equal(Now, DarlingMcpTrendTools.TierClock(null, Now, () => Now.AddMinutes(5)));
        Assert.Equal(Now, DarlingMcpTrendTools.TierClock("  ", Now, () => Now.AddMinutes(5)));
    }

    [Fact]
    public void TierClock_WithAnAsOf_IsOneWallClockReading_NeverTheAnchor()
    {
        var anchor = Now.AddDays(-30);
        var wall = Now.AddSeconds(3);
        var reads = 0;

        var clock = DarlingMcpTrendTools.TierClock("2026-09-12T12:00:00Z", anchor, () => { reads++; return wall; });

        Assert.Equal(wall, clock);
        Assert.NotEqual(anchor, clock);
        Assert.Equal(1, reads);
    }

    /// <summary>
    /// The regression: a 95 h window (start exactly on the raw boundary, 96 h less the 1 h margin) with no as_of routes Raw every time
    /// when the tier is decided against the instant the window was computed. Deciding it against a clock read a few milliseconds LATER
    /// (the old default) flips it to the hourly tier. Both procedure and query routes.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(250)]
    public void A95HourWindowWithNoAsOf_RoutesRawEveryTime(int millisecondsLater)
    {
        var windowEnd = Now;
        var start = windowEnd.AddHours(-95);
        var coverage = RollupCoverage.Unknown;

        Assert.Equal(TimescaleSupport.RawRetentionSpan - DarlingTrendReader.RawTierMargin, TimeSpan.FromHours(95));

        var pinned = DarlingMcpTrendTools.TierClock(null, windowEnd, () => windowEnd.AddMilliseconds(millisecondsLater));
        Assert.Equal(RetentionTier.Raw, DarlingTrendReader.ResolveProcedureDurationTrendRoute(start, RollupAvailability.All, coverage, nowUtc: pinned, windowEndUtc: windowEnd).Tier);
        Assert.Equal(RetentionTier.Raw, DarlingTrendReader.ResolveQueryDurationTrendRoute(start, RollupAvailability.All, coverage, nowUtc: pinned, windowEndUtc: windowEnd).Tier);

        /* The old behaviour, for contrast: a clock read after the window flips the same window to the hourly tier. */
        if (millisecondsLater > 0)
        {
            var late = windowEnd.AddMilliseconds(millisecondsLater);
            Assert.Equal(RetentionTier.Hourly, DarlingTrendReader.ResolveProcedureDurationTrendRoute(start, RollupAvailability.All, coverage, nowUtc: late, windowEndUtc: windowEnd).Tier);
        }
    }
}
