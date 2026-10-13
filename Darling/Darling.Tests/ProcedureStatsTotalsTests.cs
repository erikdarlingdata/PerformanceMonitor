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

    /// <summary>Review of #5706: the gap fill is bounded by the server's newest earlier totals row and the batch, and recomputes like the upsert.</summary>
    [Fact]
    public void TheGapFill_TotalsTheCollectionsBetweenTheNewestEarlierTotalsAndTheBatch_AndRecomputes()
    {
        var sql = Normalize(ProcedureStatsTotals.GapFillSql);

        Assert.Contains("FROM collect.procedure_stats AS s\nWHERE s.server_id = $1::integer\nAND   s.collection_time < $2::timestamp\nAND   s.collection_time > $3::timestamp\nGROUP BY", sql, StringComparison.Ordinal);

        /* Round 2 (L2): the anchor is a bound parameter, so chunk exclusion sees both bounds; no subquery recomputes it. */
        Assert.DoesNotContain("SELECT max(", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, collection_time, database_name) DO UPDATE SET", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("t.row_count +", sql, StringComparison.Ordinal);
    }

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

        Assert.Contains("FROM collect.procedure_stats\nWHERE server_id = $1::integer\nAND   collection_time = $2::timestamp\nAND   ($3::text[] IS NULL OR database_name = ANY($3::text[]) OR ($4::boolean AND database_name IS NULL))\nGROUP BY server_id, collection_time, database_name", sql, StringComparison.Ordinal);
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

        /* collect.procedure_stats has exactly one writer, the runner's COPY (built from TargetTable, so it has no literal): no other Darling source file names an INSERT or COPY into it. */
        var scanned = 0;
        foreach (var file in Directory.EnumerateFiles(RepoFile.PathTo("Darling"), "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(RepoFile.Root, file).Replace('\\', '/');
            if (relative.Contains("/obj/", StringComparison.Ordinal) || relative.Contains("/bin/", StringComparison.Ordinal)
                || relative.StartsWith("Darling/Darling.Tests/", StringComparison.Ordinal))
            {
                continue;
            }

            scanned++;
            var text = File.ReadAllText(file);
            Assert.False(text.Contains("INSERT INTO collect.procedure_stats ", StringComparison.Ordinal), relative + " writes collect.procedure_stats");
            Assert.False(text.Contains("COPY collect.procedure_stats ", StringComparison.Ordinal), relative + " COPYs into collect.procedure_stats");
        }

        Assert.True(scanned > 300, "the scan saw " + scanned + " Darling source files");
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

    /// <summary>
    /// The gap fill runs behind a savepoint and a server-side statement_timeout, restores the timeout on success, and rolls back to the
    /// savepoint on a fault; the runner's call site stays free of any catch (the batch's own upsert is still fatal to it).
    /// </summary>
    [Fact]
    public void TheGapFill_RunsBehindASavepointAndAServerSideTimeout_AndNeverFailsTheBatch()
    {
        var writer = Normalize(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "ProcedureStatsTotalsWriter.cs"));
        var gap = writer[writer.IndexOf("private static async Task TryGapFillAsync(", StringComparison.Ordinal)..];

        var save = gap.IndexOf("transaction.SaveAsync(GapSavepointName", StringComparison.Ordinal);
        var setTimeout = gap.IndexOf("SetStatementTimeoutAsync(", StringComparison.Ordinal);
        var run = gap.IndexOf("ProcedureStatsTotals.GapFillSql", StringComparison.Ordinal);
        var restore = gap.IndexOf("SetStatementTimeoutAsync(connection, transaction, previousTimeout", StringComparison.Ordinal);
        var release = gap.IndexOf("transaction.ReleaseAsync(GapSavepointName", StringComparison.Ordinal);
        var rollback = gap.IndexOf("transaction.RollbackAsync(GapSavepointName", StringComparison.Ordinal);
        Assert.True(save > 0 && setTimeout > save && run > setTimeout && restore > run && release > restore && rollback > release, "save, bound, run, restore, release, then the rollback arm");
        Assert.Contains("catch (Exception ex) when (!cancellationToken.IsCancellationRequested)", gap, StringComparison.Ordinal);
        Assert.Contains("set_config('statement_timeout', $1, true)", writer, StringComparison.Ordinal);

        /* Round 2 (L1): the anchor and the gap test are ONE statement that runs before the savepoint; with no anchor or no gap the
           method returns before the savepoint, the timeout change and the gap statement, so the normal batch pays that one statement. */
        var probe = gap.IndexOf("ProcedureStatsTotals.GapProbeSql", StringComparison.Ordinal);
        var early = gap.IndexOf("if (reader.IsDBNull(1) || !reader.GetBoolean(2))", StringComparison.Ordinal);
        Assert.True(probe > 0 && early > probe && save > early, "probe, then the early return, then the savepoint");
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(gap, "ExecuteReaderAsync"));
        var probeSql = Normalize(ProcedureStatsTotals.GapProbeSql);
        Assert.Contains("current_setting('statement_timeout')", probeSql, StringComparison.Ordinal);
        Assert.Contains("p.collection_time <= $2::timestamp", probeSql, StringComparison.Ordinal);
        Assert.DoesNotContain("p.collection_time < $2", probeSql, StringComparison.Ordinal);
        Assert.Contains("s.collection_time > a.anchor", probeSql, StringComparison.Ordinal);
        Assert.Contains("s.collection_time < $2::timestamp", probeSql, StringComparison.Ordinal);
        Assert.Contains("EXISTS", probeSql, StringComparison.Ordinal);

        /* The gap fill is the only statement behind the savepoint: the batch's own upsert stays outside it. */
        var upsert = writer.IndexOf("ProcedureStatsTotals.UpsertSql", StringComparison.Ordinal);
        Assert.True(upsert > 0 && upsert < writer.IndexOf("private static async Task TryGapFillAsync(", StringComparison.Ordinal));
    }

    [Fact]
    public void Retention_PurgesTheTotals_AtProcedureStatsDays_NeverBeforeTheRawRows()
    {
        var retention = Normalize(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs"));

        Assert.Contains("TimeSlicedDeleteSql(ProcedureStatsTotals.Table, \"collection_time\", ProcedureStatsTotals.PrunePredicate)", retention, StringComparison.Ordinal);
        Assert.Contains("EffectivePurgeRetentionDays(\n                \"procedure_stats\",", retention, StringComparison.Ordinal);

        var delete = DarlingRetention.TimeSlicedDeleteSql(ProcedureStatsTotals.Table, "collection_time", ProcedureStatsTotals.PrunePredicate);
        Assert.StartsWith("DELETE FROM collect.procedure_stats_totals WHERE collection_time < $1 AND collection_time < COALESCE($2::timestamp, $1)", delete, StringComparison.Ordinal);

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
