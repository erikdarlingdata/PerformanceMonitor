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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #8549: the covering index on the closed day leaves of <c>collect.query_store_interval_wide</c>, on a real store. The hourly pass
/// builds it on a leaf the collector's upsert can no longer update and on no other (so the upsert stays heap-only on every leaf it
/// reaches), the Viewer's 30-day keys read and slicer then run index-only on those leaves, an INVALID leftover of an interrupted
/// build is dropped and built again, and a leaf dropped at retention takes its index along.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it cannot race live collection. */
public sealed class QueryStoreIntervalWideCoverIndexLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the #8549 live test.";

    /* The fixed clock's leaves: Now is 2026-10-08 14:55. A day leaf is closed 26 hours past its upper bound, so the 6th (upper bound
       the 7th 00:00, closed since the 8th 02:00) is closed and the 7th (closed from the 9th 02:00) is not. The expected list is written
       out, not computed from the product's rule, so a change of the margin fails here and has to be argued for. */
    private static readonly DateTime[] ClosedDays = Enumerable.Range(0, 9).Select(i => new DateTime(2026, 10, 6).AddDays(-i)).ToArray();

    private static readonly DateTime[] ReachableDays = { new(2026, 10, 7), new(2026, 10, 8), new(2026, 10, 9), new(2026, 10, 10), new(2026, 10, 11) };

    /* The plain (server_id, first_execution_time) index on the day leaves, whatever name the parent's clone got. */
    private const string PlainIndexCountSql =
        "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename LIKE 'query_store_interval_wide_p%' "
        + "AND indexdef LIKE '%USING btree (server_id, first_execution_time)'";

    private static string Leaf(DateTime day) => QueryStoreIntervalPartitions.Wide.DayPartition(day);

    /// <summary>
    /// A migrated, promoted store with a day leaf for every day from the wide table's retention floor through three days ahead of
    /// <see cref="QueryStoreIntervalRetentionTestKit.Now"/>, and every background index the service builds (the plain
    /// <c>(server_id, first_execution_time)</c> index among them). Promoted as of twelve days ago, so the legacy table ends before the
    /// first leaf and the leaves can sit on the days a real store has.
    /// </summary>
    internal static async Task<NpgsqlConnection> OpenLeafStoreAsync(ScratchPostgres scratch, ILogger logger, CancellationToken ct)
    {
        var connection = await OpenStoreAsync(scratch, ct);
        await PromoteAsync(connection, QueryStoreIntervalPartitions.Wide, Now.AddDays(-12), logger, ct);
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, QueryStoreIntervalPartitions.Wide, Now, logger, ct);
        foreach (var day in ClosedDays.Concat(ReachableDays))
        {
            Assert.True(await ExistsAsync(connection, Leaf(day), ct), $"{Leaf(day)} must exist: the test's days are the store's leaves");
        }

        return connection;
    }

    /// <summary>Runs the step until it has nothing left, as the hourly passes would; returns the passes it took.</summary>
    internal static async Task<int> BuildAllAsync(NpgsqlConnection connection, DateTime clock, ILogger logger, CancellationToken ct)
    {
        for (var pass = 1; pass <= 10; pass++)
        {
            var step = await QueryStoreIntervalWideCoverIndex.EnsureAsync(connection, QueryStoreIntervalPartitions.Wide, clock, logger, ct);
            if (step.Outcome == QueryStoreIntervalPartitions.StepOutcome.NothingToDo)
            {
                return pass - 1;
            }
        }

        throw new InvalidOperationException("the covering index step did not converge in ten passes");
    }

    /// <summary>
    /// Rows for every day of the leaves, <paramref name="rowsPerDay"/> a day, spread over <paramref name="servers"/> servers
    /// round-robin so their rows share pages, as a real store's do. <c>first_execution_time</c> is distinct per row.
    /// </summary>
    internal static Task SeedAsync(NpgsqlConnection connection, DateTime firstDay, DateTime lastDay, int rowsPerDay, int servers, CancellationToken ct) =>
        ExecAsync(connection, string.Create(CultureInfo.InvariantCulture, $@"
INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
     avg_physical_io_reads, runtime_stats_interval_id, interval_start_time_utc)
SELECT f + interval '30 minutes', (g % {servers}) + 1, 'db', (g / {servers}) % 900, (g / {servers}) % 900, 'Regular', f, f,
       repeat('x', 100), md5(((g / {servers}) % 900)::text), 1 + g % 50, 1000 + g % 500, 500, 10, 1, 1, g, date_trunc('hour', f)
FROM (SELECT d.day + g * (86400.0 / {rowsPerDay}) * interval '1 second' AS f, g
      FROM generate_series(timestamp '{firstDay:yyyy-MM-dd}', timestamp '{lastDay:yyyy-MM-dd}', interval '1 day') AS d(day)
      CROSS JOIN generate_series(0, {rowsPerDay - 1}) AS g) AS s;"), ct);

    private static async Task<bool?> CoverValidAsync(NpgsqlConnection connection, string leaf, CancellationToken ct)
    {
        var value = await new NpgsqlCommand(
            "SELECT x.indisvalid FROM pg_index AS x WHERE x.indexrelid = to_regclass($1)", connection)
        { Parameters = { new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = QueryStoreIntervalWideCoverIndex.IndexName(leaf) } } }
            .ExecuteScalarAsync(ct);
        return value is bool valid ? valid : null;
    }

    /// <summary>Every index whose definition INCLUDEs <c>avg_duration_us</c> (the covering one's mark), by name.</summary>
    private static async Task<IReadOnlyList<string>> CoveringIndexNamesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var names = new List<string>();
        await using var command = new NpgsqlCommand(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'collect' AND tablename LIKE 'query_store_interval_wide%' "
            + "AND indexdef LIKE '%INCLUDE%avg_duration_us%' ORDER BY indexname", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    [Fact]
    public async Task ThePass_BuildsTheCoveringIndexOnClosedDayLeavesOnly_NewestFirst_AndAConvergedStoreChangesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        var logger = new ListLogger();

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenLeafStoreAsync(scratch, logger, ct);
        var plainBefore = await CountAsync(connection, PlainIndexCountSql, ct);
        Assert.True(plainBefore >= 14, $"the plain (server_id, first_execution_time) index is on every leaf; found {plainBefore}");

        /* Three a pass, the newest closed leaf first (the oldest is the next one retention drops). */
        var first = await QueryStoreIntervalWideCoverIndex.EnsureAsync(connection, QueryStoreIntervalPartitions.Wide, Now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, first.Outcome);
        Assert.Equal(QueryStoreIntervalWideCoverIndex.MaxLeavesPerPass, first.Count);
        var firstThree = ClosedDays.Take(QueryStoreIntervalWideCoverIndex.MaxLeavesPerPass).ToArray();
        foreach (var day in ClosedDays)
        {
            Assert.Equal(firstThree.Contains(day) ? true : null, await CoverValidAsync(connection, Leaf(day), ct));
        }

        Assert.Equal(2, await BuildAllAsync(connection, Now, logger, ct));

        /* Closed leaves have it, valid, on the keys and INCLUDE list the reads need. */
        foreach (var day in ClosedDays)
        {
            Assert.True(await CoverValidAsync(connection, Leaf(day), ct), $"{Leaf(day)} is closed and must carry its covering index");
        }

        var definition = (string)(await new NpgsqlCommand(
            $"SELECT pg_get_indexdef('{QueryStoreIntervalWideCoverIndex.IndexName(Leaf(ClosedDays[0]))}'::regclass)", connection).ExecuteScalarAsync(ct))!;
        Assert.Contains("(server_id, first_execution_time) INCLUDE (", definition, StringComparison.Ordinal);
        Assert.Contains(QueryStoreIntervalWideCoverIndex.IncludeColumns, definition, StringComparison.Ordinal);

        /* No leaf the upsert can reach has it, nor DEFAULT, the legacy table or the parent. */
        foreach (var day in ReachableDays)
        {
            Assert.Null(await CoverValidAsync(connection, Leaf(day), ct));
        }

        Assert.Equal(
            ClosedDays.Select(day => QueryStoreIntervalWideCoverIndex.ShortIndexName(Leaf(day))).OrderBy(n => n, StringComparer.Ordinal),
            await CoveringIndexNamesAsync(connection, ct));

        /* The plain index is still there on every leaf. */
        Assert.Equal(plainBefore, await CountAsync(connection, PlainIndexCountSql, ct));

        /* A converged store pays the catalog read and nothing else. */
        var again = await QueryStoreIntervalWideCoverIndex.EnsureAsync(connection, QueryStoreIntervalPartitions.Wide, Now, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, again.Outcome);

        /* A day later the 7th has closed too. */
        Assert.Equal(1, await BuildAllAsync(connection, Now.AddDays(1), logger, ct));
        Assert.True(await CoverValidAsync(connection, Leaf(ReachableDays[0]), ct));
        Assert.Null(await CoverValidAsync(connection, Leaf(ReachableDays[1]), ct));

        Assert.DoesNotContain(logger.Lines, line => line.Level >= LogLevel.Warning);
    }

    [Fact]
    public async Task AnUpsertShapedUpdate_StaysHotOnEveryLeafTheUpsertCanReach_AndTheIndexIsWhatMakesAClosedLeafNotHot()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        var logger = new ListLogger();

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenLeafStoreAsync(scratch, logger, ct);
        await SeedAsync(connection, ClosedDays[^1], ReachableDays[1], 200, 2, ct);
        await ExecAsync(connection, "VACUUM (ANALYZE) collect.query_store_interval_wide", ct);

        /* A closed leaf before the pass: the update is HOT, so the control below shows the index is what changes it. */
        string Between(DateTime day) => $"first_execution_time >= '{day:yyyy-MM-dd}' AND first_execution_time < '{day.AddDays(1):yyyy-MM-dd}'";
        var (closedUpdated, closedHot) = await QueryStoreIntervalWideBrinIndexLiveTests.UpdateAndReadHotAsync(
            connection, QueryStoreIntervalWideBrinIndexLiveTests.UpsertShapedSet, Between(ClosedDays[0]), ct);
        Assert.Equal(200, closedUpdated);
        Assert.Equal(closedUpdated, closedHot);

        await BuildAllAsync(connection, Now, logger, ct);

        /* Every leaf the upsert can reach, today's and yesterday's with rows: all of its updates stay heap-only. */
        foreach (var day in ReachableDays.Take(2))
        {
            var (updated, hot) = await QueryStoreIntervalWideBrinIndexLiveTests.UpdateAndReadHotAsync(
                connection, QueryStoreIntervalWideBrinIndexLiveTests.UpsertShapedSet, Between(day), ct);
            Assert.Equal(200, updated);
            Assert.Equal(updated, hot);
        }

        /* The control: on a closed leaf with the index the same update is not HOT, which is why the index may sit only there. */
        var (coveredUpdated, coveredHot) = await QueryStoreIntervalWideBrinIndexLiveTests.UpdateAndReadHotAsync(
            connection, QueryStoreIntervalWideBrinIndexLiveTests.UpsertShapedSet, Between(ClosedDays[0]), ct);
        Assert.Equal(200, coveredUpdated);
        Assert.Equal(0, coveredHot);
    }

    [Fact]
    public async Task TheThirtyDayReads_RunIndexOnlyOnAClosedLeafOfAVacuumedStore_AndKeepTheirPlanOnALeafWithoutTheIndex()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        var logger = new ListLogger();

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenLeafStoreAsync(scratch, logger, ct);
        await SeedAsync(connection, ClosedDays[^1], ReachableDays[1], 24000, 8, ct);
        await ExecAsync(connection, "VACUUM (ANALYZE) collect.query_store_interval_wide", ct);

        var start = Now.AddDays(-30);
        var before = await ExplainAsync(connection, ViewerDataService.QueryStoreTopTableSql, TopParameters(start), ct);
        var slicerBefore = await ExplainAsync(connection, ViewerDataService.QueryStoreSlicerTableSql, SlicerParameters(start, Now), ct);
        Assert.DoesNotContain("_cover", before, StringComparison.Ordinal);
        Assert.DoesNotContain("_cover", slicerBefore, StringComparison.Ordinal);

        await BuildAllAsync(connection, Now, logger, ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_wide", ct);

        foreach (var (name, plan) in new[]
        {
            ("grid keys", await ExplainAsync(connection, ViewerDataService.QueryStoreTopTableSql, TopParameters(start), ct)),
            ("slicer", await ExplainAsync(connection, ViewerDataService.QueryStoreSlicerTableSql, SlicerParameters(start, Now), ct)),
        })
        {
            foreach (var day in ClosedDays)
            {
                Assert.True(
                    plan.Contains($"Index Only Scan using {QueryStoreIntervalWideCoverIndex.ShortIndexName(Leaf(day))} on query_store_interval_wide_p{day:yyyyMMdd}", StringComparison.Ordinal),
                    $"the {name} read must scan {Leaf(day)} index-only through its covering index:\n{plan}");
            }

            /* The leaves the upsert can reach have no such index, so the read there is the one it was before. */
            foreach (var day in ReachableDays.Take(2))
            {
                Assert.DoesNotContain($"p{day:yyyyMMdd}_cover", plan, StringComparison.Ordinal);
            }

            Assert.Contains("Heap Fetches: 0", plan, StringComparison.Ordinal);
        }
    }

    private static List<NpgsqlParameter> TopParameters(DateTime start) => new()
    {
        new NpgsqlParameter<int> { TypedValue = 1 },
        new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) },
        new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DBNull.Value },
        new NpgsqlParameter<int> { TypedValue = 10 },
        ViewerDataService.DatabaseFilterParameter(null),
        new NpgsqlParameter<int> { TypedValue = 250 },
    };

    private static List<NpgsqlParameter> SlicerParameters(DateTime start, DateTime end) => new()
    {
        new NpgsqlParameter<int> { TypedValue = 1 },
        new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) },
        new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) },
        ViewerDataService.DatabaseFilterParameter(null),
        new NpgsqlParameter<int> { TypedValue = 720 },
    };

    /// <summary>The plan text (EXPLAIN ANALYZE) of a statement as the product binds it, on the collect schema's search path.</summary>
    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, List<NpgsqlParameter> parameters, CancellationToken ct)
    {
        await ExecAsync(connection, "SET search_path = collect, public; SET max_parallel_workers_per_gather = 0", ct);
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + sql, connection) { CommandTimeout = 300 };
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }

        var lines = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(reader.GetString(0));
        }

        return string.Join("\n", lines);
    }

    [Fact]
    public async Task AnInvalidCoveringIndex_IsDroppedByOnePassAndBuiltAgainByALaterOne()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        var logger = new ListLogger();

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenLeafStoreAsync(scratch, logger, ct);
        await BuildAllAsync(connection, Now, logger, ct);

        /* The state a cancelled CREATE INDEX CONCURRENTLY leaves: the index is there and not valid. The rig and CI roles are superuser. */
        var leaf = Leaf(ClosedDays[2]);
        var index = QueryStoreIntervalWideCoverIndex.IndexName(leaf);
        var oldOid = await new NpgsqlCommand($"SELECT '{index}'::regclass::oid", connection).ExecuteScalarAsync(ct);
        await ExecAsync(connection, $"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{index}'::regclass", ct);
        Assert.Equal(false, await CoverValidAsync(connection, leaf, ct));

        await QueryStoreIntervalWideCoverIndex.EnsureAsync(connection, QueryStoreIntervalPartitions.Wide, Now, logger, ct);
        Assert.Null(await CoverValidAsync(connection, leaf, ct));
        Assert.Contains(logger.Lines, line => line.Level == LogLevel.Warning && line.Message.Contains("INVALID", StringComparison.Ordinal));

        await QueryStoreIntervalWideCoverIndex.EnsureAsync(connection, QueryStoreIntervalPartitions.Wide, Now, logger, ct);
        Assert.True(await CoverValidAsync(connection, leaf, ct));
        Assert.NotEqual(oldOid, await new NpgsqlCommand($"SELECT '{index}'::regclass::oid", connection).ExecuteScalarAsync(ct));
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo,
            (await QueryStoreIntervalWideCoverIndex.EnsureAsync(connection, QueryStoreIntervalPartitions.Wide, Now, logger, ct)).Outcome);
    }

    [Fact]
    public async Task TheHourlyPass_BuildsTheIndexAsADayCloses_AndRetentionDropsALeafWithItsIndex()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        var logger = new ListLogger();

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenLeafStoreAsync(scratch, logger, ct);

        /* The product's own pass: three leaves a pass, so three passes cover the nine closed leaves. */
        for (var pass = 0; pass < 3; pass++)
        {
            var result = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, Now, logger, ct);
            Assert.Equal(0, result.Failed);
        }

        foreach (var day in ClosedDays)
        {
            Assert.True(await CoverValidAsync(connection, Leaf(day), ct), $"{Leaf(day)} is closed: the hourly pass must have built its index");
        }

        foreach (var day in ReachableDays)
        {
            Assert.Null(await CoverValidAsync(connection, Leaf(day), ct));
        }

        /* Eight days on, retention drops the oldest leaves and their covering indexes go with them. */
        var oldest = ClosedDays[^1];
        Assert.True(await ExistsAsync(connection, Leaf(oldest), ct));
        var later = Now.AddDays(8);
        for (var pass = 0; pass < 3; pass++)
        {
            Assert.Equal(0, (await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, later, logger, ct)).Failed);
        }

        Assert.False(await ExistsAsync(connection, Leaf(oldest), ct));
        Assert.False(await ExistsAsync(connection, QueryStoreIntervalWideCoverIndex.IndexName(Leaf(oldest)), ct));
        Assert.True(await CoverValidAsync(connection, Leaf(new DateTime(2026, 10, 7)), ct));
    }
}
