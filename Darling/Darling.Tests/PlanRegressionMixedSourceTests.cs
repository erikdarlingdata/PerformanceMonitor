/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5513: PLAN_REGRESSION fell back to the raw <c>query_store_stats</c> read on 46% of a 43-server store's passes, because
/// <see cref="QueryStoreIntervalLatest.UseTable"/> is all or nothing: a coverage claim that restarted a day ago, or a table
/// whose purge had just moved its floor above the window's bound, sent the whole 15-day slice through the raw dedup, for a
/// window the table held all but a day or two of. The third answer, <see cref="QueryStoreIntervalLatest.PlanRegressionReadKind.Mixed"/>,
/// reads the table where its claim holds and raw rows for the rest, grouped together.
///
/// <para>The merge gate is the same one #3953 had: the mixed read returns the raw read's rows, column for column, at every edge
/// of the claim, including an interval that is open on one side of the edge and closed on the other, and a table whose
/// oldest intervals were purged while raw still holds them (where the table alone reads fewer intervals than raw, so the
/// comparison is not vacuous).</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionMixedSourceTests
{
    private const int ServerId = -5513001;

    /// <summary>The analysis window's start: the 14-day comparison window reaches back to <c>T0 - 4 days</c>.</summary>
    private static readonly DateTime TimeRangeStart = new(2026, 9, 11, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime T0 = TimeRangeStart.AddDays(-10);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* ---- the pure rule ----------------------------------------------------------------------------------- */

    [Fact]
    public void DecideSource_IsMixed_WhereTheOldRuleSaidRaw_AndTheTableCoversPartOfTheWindow()
    {
        var now = new DateTime(2026, 10, 7, 0, 0, 0, DateTimeKind.Unspecified);
        var rawBound = now.AddDays(-15);

        /* A timescale store: raw keeps about 4 days, the table keeps 15. A claim that restarted 2 days ago is above raw's
           floor, so the table alone is refused (the old rule: raw, 4 days of dedup). The table covers the newest 2 days and
           raw is read for the 2 before. */
        var rawFloor = now.AddDays(-4);
        var tableFloor = now.AddDays(-15).AddHours(2);
        var filledSince = now.AddDays(-2);
        Assert.False(QueryStoreIntervalLatest.UseTable(filledSince, hasPending: false, rawFloor, rawBound, tableFloor));
        var source = QueryStoreIntervalLatest.DecideSource(filledSince, hasPending: false, rawFloor, rawBound, tableFloor);
        Assert.Equal(QueryStoreIntervalLatest.PlanRegressionReadKind.Mixed, source.Kind);
        Assert.Equal(filledSince, source.CoveredFrom);

        /* The claim is older than raw's floor: nothing is uncovered, so the table alone (unchanged). */
        Assert.Equal(
            QueryStoreIntervalLatest.PlanRegressionReadKind.Table,
            QueryStoreIntervalLatest.DecideSource(now.AddDays(-6), hasPending: false, rawFloor, rawBound, tableFloor).Kind);
    }

    [Fact]
    public void DecideSource_TheTableNeverReachesBelowRawsFloor_SoTheAnswerStaysRaws()
    {
        var now = new DateTime(2026, 10, 7, 0, 0, 0, DateTimeKind.Unspecified);
        var rawBound = now.AddDays(-15);

        /* A plain store, no raw floor: the claim starts 3 days ago, so the table covers from there. */
        var plain = QueryStoreIntervalLatest.DecideSource(now.AddDays(-3), false, rawFloor: null, rawBound, now.AddDays(-15).AddHours(3));
        Assert.Equal(QueryStoreIntervalLatest.PlanRegressionReadKind.Mixed, plain.Kind);
        Assert.Equal(now.AddDays(-3), plain.CoveredFrom);

        /* A claim older than the bound, but a table whose purge has moved its floor above the bound while raw still holds
           everything: the old rule refused the table (raw holds history it dropped). The table now speaks only for
           intervals at or above its floor; raw is read for the older ones, in a span bounded above. */
        var tableFloor = now.AddDays(-15).AddHours(3);
        Assert.False(QueryStoreIntervalLatest.UseTable(now.AddDays(-30), false, rawFloor: null, rawBound, tableFloor));
        var purged = QueryStoreIntervalLatest.DecideSource(now.AddDays(-30), false, rawFloor: null, rawBound, tableFloor);
        Assert.Equal(QueryStoreIntervalLatest.PlanRegressionReadKind.Mixed, purged.Kind);
        Assert.Equal(rawBound, purged.CoveredFrom);
        Assert.Equal(tableFloor, purged.FirstExecutionFloor);
        Assert.Equal(tableFloor.AddDays(2), purged.RawUpperBound);
    }

    [Theory]
    [InlineData(null, false, true)]     // no coverage row: nothing is covered
    [InlineData(-3.0, true, true)]      // pending batches: a batch the table missed is inside the claim
    [InlineData(-3.0, false, false)]    // an empty table for the server
    public void DecideSource_StaysRaw_WhenNothingIsCovered(double? filledSinceDays, bool hasPending, bool tableHasRows)
    {
        var now = new DateTime(2026, 10, 7, 0, 0, 0, DateTimeKind.Unspecified);
        DateTime? filledSince = filledSinceDays is double f ? now.AddDays(f) : null;
        DateTime? tableFloor = tableHasRows ? now.AddDays(-14) : null;

        Assert.Equal(
            QueryStoreIntervalLatest.PlanRegressionReadKind.Raw,
            QueryStoreIntervalLatest.DecideSource(filledSince, hasPending, rawFloor: now.AddDays(-4), now.AddDays(-15), tableFloor).Kind);
    }

    /* ---- the SQL ----------------------------------------------------------------------------------------- */

    [Fact]
    public void TheMixedSql_UsesTheRawReadsOrderAndKeepsBareBoundsOnThePartitioningColumn()
    {
        var sql = PgFactCollector.PlanRegressionMixedSql;

        /* The same seven aggregates, in the same order, as the raw read's deduped CTE: the table row of an interval is its
           newest snapshot and wins the group on collection_time. */
        var orderBy = "ORDER BY collection_time DESC, execution_count DESC))[1]";
        Assert.Equal(7, CountOf(sql, orderBy));
        Assert.Equal(7, CountOf(PgFactCollector.PlanRegressionSql, orderBy));

        /* Bare parameters on collection_time (chunk exclusion), both bounds, and the two sides of the edge. */
        Assert.Contains("AND   collection_time >= $3", sql, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time < $6", sql, StringComparison.Ordinal);
        Assert.Contains("(collection_time < $4 OR first_execution_time IS NULL OR first_execution_time < $5)", sql, StringComparison.Ordinal);
        Assert.Contains("AND   collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("AND   first_execution_time >= $5", sql, StringComparison.Ordinal);

        /* The raw side keeps the raw read's own filters; the plan_agg and the suffix are the shared ones. */
        Assert.Contains("execution_type_desc = 'Regular'", sql, StringComparison.Ordinal);
        Assert.EndsWith(PgFactCollectorSuffixTail, sql, StringComparison.Ordinal);
        Assert.EndsWith(PgFactCollectorSuffixTail, PgFactCollector.PlanRegressionSql, StringComparison.Ordinal);
    }

    private const string PgFactCollectorSuffixTail = "LIMIT 20";

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /* ---- live: the mixed read is the raw read's answer ---------------------------------------------------- */

    [Fact]
    public async Task TheMixedRead_ReturnsTheRawReadsRows_AtEveryEdgeOfTheClaim_AndWhereTheTablesOldIntervalsWerePurged()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5513 parity test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await SeedAsync(new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), ct);

        var raw = await FactRowsAsync(connection, PgFactCollector.PlanRegressionSql, null, ct);
        Assert.True(raw.Count >= 2, $"the seed produced {raw.Count} PLAN_REGRESSION row(s); the comparison needs real regressions");

        var floor = (await ScalarAsync(connection, "SELECT MIN(first_execution_time) FROM collect.query_store_interval_latest WHERE server_id = " + ServerId, ct)) as DateTime?;
        Assert.NotNull(floor);
        var bound = TimeRangeStart.AddDays(-15);

        /* Below every snapshot (the table alone), one inside each interval that is open on one side and closed on the
           other (the open snapshot is 30 minutes in, the closed one 65), the middle, and past the newest snapshot (all raw). */
        var edges = new[]
        {
            T0.AddDays(-1),
            T0.AddDays(2).AddHours(12).AddMinutes(45),
            T0.AddDays(5),
            T0.AddDays(9).AddHours(12).AddMinutes(45),
            T0.AddDays(20),
        };
        foreach (var coveredFrom in edges)
        {
            var mixed = await FactRowsAsync(connection, PgFactCollector.PlanRegressionMixedSql, Mixed(coveredFrom, floor!.Value), ct);
            Assert.Equal(raw, mixed);
        }

        /* The table's purge ran: the intervals first seen before day 3 are gone from it, raw still has them. The table alone
           now reads fewer intervals than raw (the old rule's reason to refuse it); the mixed read still returns raw's. */
        await ExecAsync(connection, "DELETE FROM collect.query_store_interval_latest WHERE server_id = " + ServerId + " AND first_execution_time < @cutoff", T0.AddDays(3), ct);
        var purgedFloor = (await ScalarAsync(connection, "SELECT MIN(first_execution_time) FROM collect.query_store_interval_latest WHERE server_id = " + ServerId, ct)) as DateTime?;
        Assert.NotNull(purgedFloor);
        Assert.True(purgedFloor > floor);

        var tableAlone = await FactRowsAsync(connection, PgFactCollector.PlanRegressionTableSql, null, ct);
        Assert.NotEqual(raw, tableAlone);
        foreach (var coveredFrom in edges)
        {
            var mixed = await FactRowsAsync(connection, PgFactCollector.PlanRegressionMixedSql, Mixed(coveredFrom, purgedFloor!.Value), ct);
            Assert.Equal(raw, mixed);
        }

        /* The decision lands on these edges by itself: a claim that restarted at day 5, a plain store. */
        await ExecAsync(connection, "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @cutoff WHERE server_id = " + ServerId, T0.AddDays(5), ct);
        var source = await QueryStoreIntervalLatest.ResolveReadSourceAsync(connection, ServerId, bound, 60, null, ct);
        Assert.Equal(QueryStoreIntervalLatest.PlanRegressionReadKind.Mixed, source.Kind);
        Assert.Equal(T0.AddDays(5), source.CoveredFrom);
        Assert.Equal(raw, await FactRowsAsync(connection, PgFactCollector.PlanRegressionMixedSql, source, ct));
    }

    [Fact]
    public async Task ThePass_ReadsTheMixedSource_WhenTheClaimRestartedMidWindow_AndReportsTheRawReadsFact()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5513 pass test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var seedSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedAsync(new DarlingCollectorRunner(seedSource, new CollectorDeltaCalculator()), ct);

        await using var postgres = NpgsqlDataSource.Create(
            new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = "collect,config,public" }.ConnectionString);

        /* A coverage claim that restarted at day 5 (a gap or a purged pending batch did it): above the window's bound, so the
           table alone is refused, as the old rule did, and the pass used to read raw for all of it. */
        await ExecAsync(connection, "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @cutoff WHERE server_id = " + ServerId, T0.AddDays(5), ct);

        var mixedLog = new CapturingLogger();
        var mixedContext = NewContext();
        var mixedFact = (await new PgFactCollector(postgres, mixedLog).CollectFactsAsync(mixedContext)).Single(f => f.Key == "PLAN_REGRESSION");
        Assert.Contains(mixedLog.Lines, l => l.Contains("PLAN_REGRESSION source for server", StringComparison.Ordinal) && l.Contains("Mixed", StringComparison.Ordinal));
        Assert.False(mixedContext.PlanRegressionReadsIntervalTable, "the drill-down reads the raw SQL for the mixed source, which has the same rows");
        Assert.DoesNotContain(mixedContext.CollectionFailures, f => f.Family == "plan_regression");

        /* A pending batch forces the raw read. The two passes must report the same fact. */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at) VALUES (" + ServerId + ", @cutoff, 'qsA', @cutoff)",
            T0, ct);
        var rawLog = new CapturingLogger();
        var rawContext = NewContext();
        var rawFact = (await new PgFactCollector(postgres, rawLog).CollectFactsAsync(rawContext)).Single(f => f.Key == "PLAN_REGRESSION");
        Assert.DoesNotContain(rawLog.Lines, l => l.Contains("Mixed", StringComparison.Ordinal));
        Assert.Contains(rawLog.Lines, l => l.Contains("pending batches", StringComparison.Ordinal));

        Assert.Equal(rawFact.Value, mixedFact.Value);
        Assert.Equal(rawFact.DatabaseName, mixedFact.DatabaseName);
        Assert.Equal(rawFact.Metadata.OrderBy(kv => kv.Key).ToList(), mixedFact.Metadata.OrderBy(kv => kv.Key).ToList());
        Assert.Equal(
            rawContext.PlanRegressionOffenders?.Select(o => (o.DatabaseName, o.QueryId)).ToList(),
            mixedContext.PlanRegressionOffenders?.Select(o => (o.DatabaseName, o.QueryId)).ToList());
    }

    /* ---- helpers ----------------------------------------------------------------------------------------- */

    private static QueryStoreIntervalLatest.PlanRegressionSource Mixed(DateTime coveredFrom, DateTime firstExecutionFloor)
    {
        var spanEnd = firstExecutionFloor.AddDays(2);
        return new QueryStoreIntervalLatest.PlanRegressionSource(
            QueryStoreIntervalLatest.PlanRegressionReadKind.Mixed, coveredFrom, firstExecutionFloor, spanEnd > coveredFrom ? spanEnd : coveredFrom);
    }

    private static AnalysisContext NewContext() => new()
    {
        ServerId = ServerId,
        ServerName = "qsil-mixed-host",
        TimeRangeStart = TimeRangeStart,
        TimeRangeEnd = TimeRangeStart.AddHours(4),
        ServerUtcOffset = TimeSpan.Zero,
    };

    private static QueryStoreCollector.Row Row(
        string database, long queryId, long planId, long intervalId, DateTime first, DateTime last, long executions,
        long cpuUs, string? role = null) => new()
        {
            DatabaseName = database,
            QueryId = queryId,
            PlanId = planId,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = first,
            LastExecutionTime = last,
            QueryHash = "0xQ" + queryId.ToString(CultureInfo.InvariantCulture),
            QueryPlanHash = "0xP" + planId.ToString(CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = cpuUs,
            AvgDurationUs = cpuUs * 3,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            ReplicaRole = role,
            RuntimeStatsIntervalId = intervalId,
        };

    /// <summary>
    /// Ten daily intervals, each shipped as an open snapshot (30 minutes in) and then its closed one (65 minutes in), through
    /// the real write path so the table holds exactly what the writer keeps. Query 100 regresses on the NULL role (cheap plan
    /// days 1-3, expensive days 8-9); query 200 on a named role; query 300 has one plan.
    /// </summary>
    private static async Task SeedAsync(DarlingCollectorRunner runner, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "qsil-mixed-host",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsil-mixed", Host = "qsil-mixed-host" },
            ConnectionString = "Server=qsil-mixed-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsil-mixed-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        var interval = 1000L;
        for (var day = 0; day < 10; day++)
        {
            var start = T0.AddDays(day).AddHours(12);
            interval++;

            var rows = new List<(double Fraction, QueryStoreCollector.Row Row)>();
            void Both(string db, long query, long plan, long execs, long cpu, string? role = null)
            {
                rows.Add((0.5, Row(db, query, plan, interval, start, start.AddMinutes(25), execs / 2, cpu, role)));
                rows.Add((1.0, Row(db, query, plan, interval, start, start.AddMinutes(55), execs, cpu, role)));
            }

            if (day is >= 1 and <= 3) Both("qsA", 100, 1001, 300, 100);
            if (day is 8 or 9) Both("qsA", 100, 1002, 20000, 1000);
            if (day == 2) Both("qsB", 200, 2001, 500, 200, role: "secondary1");
            if (day == 9) Both("qsB", 200, 2002, 30000, 900, role: "secondary1");
            Both("qsB", 300, 3001, 1000, 50);

            foreach (var (fraction, batch) in new[] { 0.5, 1.0 }.Select(f => (f, rows.Where(r => r.Fraction == f).Select(r => r.Row).ToList())))
            {
                var collectionTime = start.AddMinutes(fraction == 0.5 ? 30 : 65);
                foreach (var perDatabase in batch.GroupBy(r => r.DatabaseName))
                {
                    await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, perDatabase.ToList(), server, collectionTime, context, ct);
                }
            }
        }
    }

    /// <summary>One read, bound as the collector binds it: the raw and table shapes carry $1-$3, the mixed one $1-$6.</summary>
    private static async Task<List<string>> FactRowsAsync(
        NpgsqlConnection connection, string sql, QueryStoreIntervalLatest.PlanRegressionSource? source, CancellationToken ct)
    {
        var windowStart = TimeRangeStart.AddDays(-14);
        var collectionBound = TimeRangeStart.AddDays(-15);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, windowStart);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, collectionBound);
        if (sql == PgFactCollector.PlanRegressionTableSql)
        {
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, collectionBound);
        }
        else if (sql == PgFactCollector.PlanRegressionMixedSql)
        {
            Assert.NotNull(source);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, source!.Value.CoveredFrom);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, source.Value.FirstExecutionFloor);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, source.Value.RawUpperBound);
        }

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i)
                    ? "<null>"
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";
            }

            rows.Add(string.Join("|", values));
        }

        return rows;
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, DateTime cutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("cutoff", NpgsqlDbType.Timestamp, cutoff);
        await command.ExecuteNonQueryAsync(ct);
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }
}
