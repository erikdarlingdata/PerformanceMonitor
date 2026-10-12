/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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
public sealed class ProcedureStatsTotalsTests
{
    private static string Normalize(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRung_IsV176_AndEmbedsTheCreateText()
    {
        Assert.Equal(176, ProcedureStatsTotals.RungVersion);
        Assert.True(StorageVersion.SchemaVersion >= ProcedureStatsTotals.RungVersion);

        var migrations = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        Assert.Contains("new Migration(176, \"procedure-stats-totals\", V176Sql)", migrations, StringComparison.Ordinal);
        Assert.Contains("private const string V176Sql = ProcedureStatsTotals.CreateSql;", migrations, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCreateText_PinsTheShape_NoBackfill_AndTheNullsNotDistinctKey()
    {
        var sql = Normalize(ProcedureStatsTotals.CreateSql);

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.procedure_stats_totals", sql, StringComparison.Ordinal);
        Assert.Contains("server_id integer NOT NULL,", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time timestamp NOT NULL,", sql, StringComparison.Ordinal);
        Assert.Contains("database_name text,", sql, StringComparison.Ordinal);
        Assert.Contains("total_elapsed_time bigint,", sql, StringComparison.Ordinal);
        Assert.Contains("total_execution_count bigint,", sql, StringComparison.Ordinal);
        Assert.Contains("row_count bigint NOT NULL,", sql, StringComparison.Ordinal);
        Assert.Contains("max_sample_interval_seconds integer", sql, StringComparison.Ordinal);
        Assert.Contains(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_procedure_stats_totals ON collect.procedure_stats_totals (server_id, collection_time, database_name) NULLS NOT DISTINCT;",
            sql, StringComparison.Ordinal);

        /* No backfill: the rung moves no existing row. */
        Assert.DoesNotContain("INSERT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("create_hypertable", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheUpsert_ReadsBackTheStoredRows_AndRecomputesEveryMeasureOnConflict()
    {
        var sql = Normalize(ProcedureStatsTotals.UpsertSql);

        Assert.Contains("FROM collect.procedure_stats\nWHERE server_id = $1::integer\nAND   collection_time = $2::timestamp\nGROUP BY server_id, collection_time, database_name", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_elapsed_time)::bigint,", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_execution_count)::bigint,", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*),", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(sample_interval_seconds)", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, collection_time, database_name) DO UPDATE SET", sql, StringComparison.Ordinal);

        /* Recompute, never add: every measure is set from EXCLUDED, and no measure is added to its old value. */
        foreach (var measure in new[] { "total_elapsed_time", "total_execution_count", "row_count", "max_sample_interval_seconds" })
        {
            Assert.Contains($"{measure} = EXCLUDED.{measure}", sql, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("t.row_count +", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE(", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRunner_WritesTheTotalsOnTheCopysTransaction_AfterTheCopy_BeforeTheCommit()
    {
        var runner = Normalize(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));
        var start = runner.IndexOf("private async Task<int> CopyBatchOnceAsync<TRow>(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var body = runner[start..];

        var call = body.IndexOf("ProcedureStatsTotalsWriter.UpsertBatchAsync(", StringComparison.Ordinal);
        var commit = body.IndexOf("await transaction.CommitAsync(cancellationToken);", StringComparison.Ordinal);
        var ledger = body.IndexOf("QueryStatsHourLedgerWriter.AddBatchAsync(", StringComparison.Ordinal);
        Assert.True(ledger > 0 && call > ledger && commit > call, "the totals upsert sits after the COPY block and before the commit");

        /* On the COPY's transaction, for procedure_stats batches only, with no savepoint or catch around it. */
        Assert.Contains("pgConnection, transaction, server.ServerId, storedCollectionTime,\n                        rowsWritten", body, StringComparison.Ordinal);
        Assert.Contains("var totalsBatch = string.Equals(definition.TargetTable, ProcedureStatsCollector.Instance.TargetTable", body, StringComparison.Ordinal);
        Assert.Contains("|| ledgerBatch || totalsBatch", body, StringComparison.Ordinal);
        var between = body[(call - 400)..call];
        Assert.DoesNotContain("catch", between, StringComparison.Ordinal);
        Assert.DoesNotContain("SAVEPOINT", between, StringComparison.OrdinalIgnoreCase);

        /* collect.procedure_stats has exactly one writer: the runner's COPY. */
        foreach (var file in new[] { "DarlingWorker.cs", "DarlingRetention.cs" })
        {
            var text = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", file);
            Assert.DoesNotContain("INSERT INTO collect.procedure_stats ", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Retention_PurgesTheTotals_AtProcedureStatsDays_NeverBeforeTheRawRows()
    {
        var retention = Normalize(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs"));

        Assert.Contains("TimeSlicedDeleteSql(ProcedureStatsTotals.Table, \"collection_time\", ProcedureStatsTotals.PrunePredicate)", retention, StringComparison.Ordinal);
        Assert.Contains("EffectivePurgeRetentionDays(\n                \"procedure_stats\",", retention, StringComparison.Ordinal);

        var delete = DarlingRetention.TimeSlicedDeleteSql(ProcedureStatsTotals.Table, "collection_time", ProcedureStatsTotals.PrunePredicate);
        Assert.StartsWith("DELETE FROM collect.procedure_stats_totals WHERE collection_time < $1 AND collection_time < COALESCE((SELECT min(collection_time) FROM collect.procedure_stats), $1)", delete, StringComparison.Ordinal);

        /* The totals table is a plain table outside the collector catalog: no hypertable conversion, no collector schedule entry. */
        Assert.DoesNotContain(CollectorCatalogTables(), t => string.Equals(t, "procedure_stats_totals", StringComparison.Ordinal));
    }

    private static string[] CollectorCatalogTables() =>
        PerformanceMonitor.Collectors.CollectorCatalog.All.Select(c => c.TargetTable).ToArray();

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

    [Fact]
    public void TheDurationTrendTools_PassTheirOwnClock_ToBothRoutes()
    {
        var tools = Normalize(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTrendTools.cs"));

        Assert.Contains("ResolveQueryDurationTrendRoute(startUtc, rollups, coverage, nowUtc: TierClock(as_of, now), windowEndUtc: now)", tools, StringComparison.Ordinal);
        Assert.Contains("ResolveProcedureDurationTrendRoute(startUtc, rollups, coverage, nowUtc: TierClock(as_of, now), windowEndUtc: now)", tools, StringComparison.Ordinal);
        Assert.DoesNotContain("coverage, windowEndUtc: now)", tools, StringComparison.Ordinal);
    }
}
