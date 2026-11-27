/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;

using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/* #1776 own-store: each test mints a scratch database, because it materializes a continuous aggregate and drops it,
   which no other class sharing the store may see. */
/// <summary>
/// #9147, live: a raw-tier top-procedures window whose whole hours the interval-honest hourly rollup has settled ranks those
/// hours from the rollup and reads raw only for the edges. The oracle is the same reader with the rollup dropped (raw alone):
/// every row, every total and every detail field must match, for each ranking the rollup carries. The seed has rows exactly on
/// both window bounds, a first-collection row (interval 0) the raw read leaves out, a NULL schema and a CPU tie.
/// </summary>
public sealed class TopProceduresHourlyMidLiveTests
{
    private const int ServerId = -914701;
    private const string ServerName = "a9147-top-procedures-mid";
    private const int Top = 8;
    private const int Objects = 60;

    [Fact]
    public Task MidRoute_ReturnsTheRawPage_ForEveryRankingTheRollupCarries() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration, TopRanking.Executions })
            {
                await using (var source = NpgsqlDataSource.Create(scratch.ConnectionString))
                {
                    var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
                    Assert.NotNull(DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, ranking, start, end));
                }
            }

            var mid = new System.Collections.Generic.Dictionary<TopRanking, Page>();
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration, TopRanking.Executions })
            {
                mid[ranking] = await ReadAsync(scratch, ranking, start, end, ct);
                Assert.True(mid[ranking].ServedByHourlyMid, $"{ranking}: the page was not served by the hourly mid statement");
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalDailyView}");
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}");
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration, TopRanking.Executions })
            {
                var raw = await ReadAsync(scratch, ranking, start, end, ct);
                Assert.False(raw.ServedByHourlyMid);
                Assert.Equal(raw.Json, mid[ranking].Json);
                Assert.NotEqual("[]", raw.Json);
            }
        });

    [Fact]
    public Task MidRoute_CountsEveryRowOnAndAroundItsBoundsOnce() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await using (var source = NpgsqlDataSource.Create(scratch.ConnectionString))
            {
                var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
                var plan = DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, TopRanking.Cpu, start, end);
                Assert.NotNull(plan);
                Assert.Equal(BoundsMidStart(start), plan!.MidStart);
                Assert.Equal(BoundsMidEnd(end), plan.MidEnd);
            }

            var mid = new System.Collections.Generic.Dictionary<TopRanking, Page>();
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration })
            {
                mid[ranking] = await ReadAsync(scratch, ranking, start, end, ct);
                Assert.True(mid[ranking].ServedByHourlyMid, $"{ranking}: the page was not served by the hourly mid statement");
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalDailyView}");
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}");
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration })
            {
                var raw = await ReadAsync(scratch, ranking, start, end, ct);
                Assert.Equal(raw.Json, mid[ranking].Json);

                /* The oracle itself must show the layout the seed was built for, or the comparison proves nothing. */
                foreach (var kept in new[] { "BEFORE_MID", "AT_MIDSTART_HEAVY", "BEFORE_MIDEND", "AT_MIDEND", "AT_END" })
                {
                    Assert.Contains(kept, raw.Objects);
                }

                foreach (var left in new[] { "AT_START", "AFTER_END", "AT_MIDSTART_LIGHT" })
                {
                    Assert.DoesNotContain(left, raw.Objects);
                }
            }
        }, SeedShape.Bounds);

    [Fact]
    public Task MidRoute_ReadsFewerPagesThanTheRawStatement() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
            var plan = DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, TopRanking.Cpu, start, end);
            Assert.NotNull(plan);

            var rawSql = TopRankings.Apply(DarlingDataReader.TopProceduresSql, TopRanking.Cpu, hourly: false);
            var midSql = DarlingDataReader.BuildProceduresMidSql(TopRanking.Cpu, plan!);
            var rawPages = ExplainBuffers.DataPages(await ExplainAsync(connection, rawSql, start, end, null, ct));
            var midPages = ExplainBuffers.DataPages(await ExplainAsync(connection, midSql, start, end, plan, ct));
            Assert.True(midPages < rawPages, $"mid route read {midPages} pages, the raw statement {rawPages}");
        });

    [Fact]
    public Task MidRoute_ReadsRaw_WhenAWinnerHoldsTwoObjectTypes() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await AssertMidPlannedButRawPageAsync(scratch, connection, start, end, ct);
        }, SeedShape.TwoObjectTypes);

    [Fact]
    public Task MidRoute_ReadsRaw_WhenTheRollupHasAHoleInTheMid() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await AssertMidPlannedButRawPageAsync(scratch, connection, start, end, ct);
        }, SeedShape.MaterializationGap);

    /// <summary>
    /// #9280: with <see cref="ProcedureStatsNameIndex"/> the wide pass reads the uncompressed part of the window per winner, through the
    /// index, instead of every row of the server. The statement the read chooses is explained before and after the index exists: the rows
    /// its scans of <c>procedure_stats</c> return fall to a fraction (the winners' own rows plus the two raw edges), and the rows it
    /// answers are the one-statement pass's rows, with the whole window seeked and with the window split between the two arms.
    /// </summary>
    [Fact]
    public Task MidRoute_SeeksTheWinnersThroughTheNameIndex_AndReturnsTheSameRows() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var run = await RunWithAndWithoutTheIndexAsync(scratch, connection, start, end, ct);
            Assert.NotEmpty(run.Today.Rows);
            Assert.Equal(run.Today.Rows, run.Seek.Rows);
            Assert.Equal(run.Today.Rows, run.Split.Rows);
            Assert.Equal(run.Today.Rows, run.NoSeek.Rows);
            Assert.True(run.Seek.RowsReadAtLeast * 2 < run.Today.RowsReadAtMost, $"the seek read {run.Seek.RowsReadAtLeast} rows of procedure_stats at least, the one-statement pass {run.Today.RowsReadAtMost} at most");
        });

    /// <summary>
    /// #9280: a winner with a NULL database or a NULL object name has no equality to seek on, so it keeps the NULL-safe join in the seek
    /// part. Both kinds are on the page here, and the rows equal the one-statement pass's. Rows read are not compared: the NULL winners'
    /// arm reads the window, as the one-statement pass does.
    /// </summary>
    [Fact]
    public Task MidRoute_WithTheNameIndex_KeepsWinnersWithNullKeys() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var run = await RunWithAndWithoutTheIndexAsync(scratch, connection, start, end, ct);
            Assert.Contains(run.Today.Rows, r => r.StartsWith("~|", StringComparison.Ordinal));
            Assert.Contains(run.Today.Rows, r => r.Contains("|~|", StringComparison.Ordinal) && r.StartsWith("db0|dbo|~|", StringComparison.Ordinal));
            Assert.Equal(run.Today.Rows, run.Seek.Rows);
            Assert.Equal(run.Today.Rows, run.Split.Rows);
        }, SeedShape.NullKeys);

    /// <summary>
    /// #9280: the routed read answers the same page before and after the index exists, and still by the hourly mid. The seed's window is
    /// the 24 hours ending 22:20 yesterday, and the routed read binds its boundary at the clock minus 24 hours. That boundary falls inside
    /// the window on most runs, so the routed read then splits the window through the seek arms, and once the index exists it always uses
    /// the seek statement. Whichever arm the hour of the run picks, the page must not change.
    /// </summary>
    [Fact]
    public Task MidRoute_RoutedPage_IsTheSameBeforeAndAfterTheNameIndex() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            var before = await ReadAsync(scratch, TopRanking.Cpu, start, end, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, ProcedureStatsNameIndex.CreateSql);
            var after = await ReadAsync(scratch, TopRanking.Cpu, start, end, ct);
            Assert.True(before.ServedByHourlyMid);
            Assert.True(after.ServedByHourlyMid);
            Assert.NotEqual("[]", after.Json);
            Assert.Equal(before.Json, after.Json);
        });

    /// <summary>The rows (as text, every column) a mid statement answered, and the rows its scans of the table returned in an EXPLAIN ANALYZE.</summary>
    private sealed record StatementRun(System.Collections.Generic.List<string> Rows, long RowsReadAtLeast, long RowsReadAtMost);

    private sealed record IndexRuns(StatementRun Today, StatementRun NoSeek, StatementRun Seek, StatementRun Split);

    /// <summary>
    /// Runs the mid statement the read chooses on a store without <see cref="ProcedureStatsNameIndex"/> (today's statement), then builds the
    /// index and runs the chosen statement at three boundaries: before the window (everything is seeked), in the middle of it (both arms read
    /// rows) and after it (nothing is seeked). The boundary is the clock the read is handed, a day later.
    /// </summary>
    private static async Task<IndexRuns> RunWithAndWithoutTheIndexAsync(ScratchPostgres scratch, NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct)
    {
        var chunks = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        await using (var names = new NpgsqlCommand("SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_name = 'procedure_stats'", connection))
        await using (var reader = await names.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        async Task<StatementRun> RunAsync(DateTime boundary)
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
            var mid = DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, TopRanking.Cpu, start, end);
            Assert.NotNull(mid);
            var (sql, seekFrom) = await DarlingDataReader.ChooseProceduresMidSqlAsync(source, TopRanking.Cpu, mid!, boundary.AddDays(1), ct);
            var rows = new System.Collections.Generic.List<string>();
            await using (var command = MidCommand(connection, sql, start, end, mid!, seekFrom))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => reader.IsDBNull(i) ? "~" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))));
                }
            }

            var plan = await ExplainAsync(connection, sql, start, end, mid, ct, seekFrom);
            return new StatementRun(rows, ExplainBuffers.RowsReadAtLeast(plan, chunks), ExplainBuffers.RowsReadAtMost(plan, chunks));
        }

        await DarlingMcpTestData.ExecAsync(connection, ct, "ANALYZE collect.procedure_stats");
        var today = await RunAsync(start);
        await DarlingMcpTestData.ExecAsync(connection, ct, ProcedureStatsNameIndex.CreateSql);
        await DarlingMcpTestData.ExecAsync(connection, ct, "ANALYZE collect.procedure_stats");
        var seek = await RunAsync(start);
        var split = await RunAsync(start.AddHours(12));
        var noSeek = await RunAsync(end.AddHours(2));
        return new IndexRuns(today, noSeek, seek, split);
    }

    private static NpgsqlCommand MidCommand(NpgsqlConnection connection, string sql, DateTime start, DateTime end, DarlingDataReader.HourlyMid mid, DateTime? seekFrom)
    {
        var command = new NpgsqlCommand(sql, connection);
        AddMidParameters(command, start, end, mid, seekFrom);
        return command;
    }

    private static void AddMidParameters(NpgsqlCommand command, DateTime start, DateTime end, DarlingDataReader.HourlyMid? mid, DateTime? seekFrom)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Top });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        if (mid is not null)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidStart });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidEnd });
        }

        if (seekFrom is not null)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = seekFrom.Value });
        }
    }

    /// <summary>The plan takes the mid, yet the routed page equals the page with the rollup dropped (raw alone). The seed shape is one
    /// where the mid's own answer would differ from raw's, so a read that skipped its fallback fails here.</summary>
    private static async Task AssertMidPlannedButRawPageAsync(ScratchPostgres scratch, NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct)
    {
        await using (var source = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
            Assert.NotNull(DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, TopRanking.Cpu, start, end));
        }

        var routed = await ReadAsync(scratch, TopRanking.Cpu, start, end, ct);
        Assert.False(routed.ServedByHourlyMid, "the mid statement served a page the raw statement must serve");

        await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalDailyView}");
        await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}");
        var raw = await ReadAsync(scratch, TopRanking.Cpu, start, end, ct);

        Assert.Equal(raw.Json, routed.Json);
        Assert.NotEqual("[]", raw.Json);
    }

    private static async Task<object?> ExplainAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, DarlingDataReader.HourlyMid? mid, CancellationToken ct, DateTime? seekFrom = null)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        AddMidParameters(command, start, end, mid, seekFrom);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>A routed page: its rows as JSON so record fields compare whole, which statement served it, and its object names.</summary>
    private sealed record Page(string Json, bool ServedByHourlyMid, string[] Objects);

    /// <summary>The routed read through a fresh data source (coverage is cached per source).</summary>
    private static async Task<Page> ReadAsync(ScratchPostgres scratch, TopRanking ranking, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var result = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(source, ServerId, start, end, Top, databaseName: null, ranking: ranking, cancellationToken: ct);
        Assert.Equal(RetentionTier.Raw, result.Tier);
        return new Page(JsonSerializer.Serialize(result.Rows), result.ServedByHourlyMid, result.Rows.Select(r => r.ObjectName).ToArray());
    }

    /// <summary>The first whole hour after the window's start: the mid's first bucket (a bucket at the start itself would hold a row raw leaves out).</summary>
    private static DateTime BoundsMidStart(DateTime start) =>
        new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(1);

    /// <summary>The window's last whole hour: the mid ends where the end's own hour begins.</summary>
    private static DateTime BoundsMidEnd(DateTime end) =>
        new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Extra seed on top of the plain store: <see cref="TwoObjectTypes"/> gives one procedure a second object type, and
    /// <see cref="MaterializationGap"/> leaves hours of the window out of the rollup's refresh, and <see cref="Bounds"/> stamps single
    /// rows on and beside every bound of the raw edges and the mid.</summary>
    private enum SeedShape
    {
        Plain,
        TwoObjectTypes,
        MaterializationGap,
        Bounds,
        NullKeys,
    }

    /// <summary>
    /// Plants <see cref="Objects"/> procedures, one row each every five minutes from two hours before the window to one hour after it,
    /// plus the edge rows, refreshes the interval-honest hourly rollup over all of it, and runs the body. The window ends 20 minutes
    /// past an hour of the previous UTC day, so it is inside raw retention and its last part is an edge.
    /// </summary>
    private static async Task WithStoreAsync(Func<ScratchPostgres, NpgsqlConnection, DateTime, DateTime, CancellationToken, Task> body, SeedShape shape = SeedShape.Plain)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9147 top procedures mid tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #9147 top procedures mid tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* Anchored through LiveClock (#5608): a 24 hour window straddles a raw chunk boundary, and with the end placed "two hours
           ago" that boundary moves with the hour the test runs. The end sits 20 minutes past an hour of the previous UTC day, so the
           window is inside raw retention, its last part is an edge, and the layout is the same at any hour. */
        var end = LiveClock.AnchoredEndNotAfterNow().AddMinutes(20);
        var start = end.AddHours(-24);
        var hour = new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(2);

        /* Object n: CPU 1000 + n per row (P058 and P059 tie on CPU, P059 has more executions), elapsed 3000 - n,
           executions 1 + n % 7. P007 has a NULL schema. One extra row each sits exactly on the window's start (left out) and
           end (kept), and P003 gets a first-collection row (interval 0) inside the window that raw leaves out. */
        await DarlingMcpTestData.ExecAsync(connection, ct, $"""
            INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                         object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                         delta_logical_reads, delta_spills, min_worker_time, max_worker_time, sample_interval_seconds)
            SELECT 914700000000 + row_number() OVER (), t, {ServerId}, '{ServerName}', 'db' || (n % 3),
                   CASE WHEN n = 7 THEN NULL ELSE 'dbo' END, 'P' || lpad(n::text, 3, '0'), 'P', 'sh' || n, 'ph' || n,
                   1 + n % 7 + CASE WHEN n = 59 THEN 1 ELSE 0 END, CASE WHEN n = 59 THEN 1058 ELSE 1000 + n END, 3000 - n,
                   10 * n, n % 2, n, 2 * n, 300
            FROM generate_series(0, {Objects - 1}) AS n
            CROSS JOIN generate_series($1::timestamp, $2::timestamp, interval '5 minutes') AS t
            """, start.AddHours(-2).AddMinutes(1), end.AddHours(1));
        await DarlingMcpTestData.ExecAsync(connection, ct, $"""
            INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                         object_type, delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            VALUES (914709000001, $1, {ServerId}, '{ServerName}', 'db0', 'dbo', 'P000', 'P', 5, 900000, 900000, 300),
                   (914709000002, $2, {ServerId}, '{ServerName}', 'db0', 'dbo', 'P003', 'P', 5, 700000, 700000, 300),
                   (914709000003, $3, {ServerId}, '{ServerName}', 'db0', 'dbo', 'P003', 'P', 5, 990000, 990000, 0)
            """, start, end, start.AddHours(5).AddMinutes(2));

        if (shape == SeedShape.TwoObjectTypes)
        {
            /* P010 also exists as a function ('FN') with the same load, so the rollup, which has no object_type, ranks the two together
               (about 582,000 CPU, a winner) while raw ranks each type alone (about 290,000 each, below the eighth place). */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                             object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                             delta_logical_reads, delta_spills, min_worker_time, max_worker_time, sample_interval_seconds)
                SELECT 914710000000 + row_number() OVER (), t, {ServerId}, '{ServerName}', 'db' || (n % 3), 'dbo', 'P' || lpad(n::text, 3, '0'),
                       'FN', 'shf' || n, 'phf' || n, 1 + n % 7, 1000 + n, 3000 - n, 10 * n, n % 2, n, 2 * n, 300
                FROM generate_series(10, 10) AS n
                CROSS JOIN generate_series($1::timestamp, $2::timestamp, interval '5 minutes') AS t
                """, start.AddHours(-2).AddMinutes(1), end.AddHours(1));
        }

        if (shape == SeedShape.Bounds)
        {
            /* One row per object, so an object's total is that row counted once, twice (a row on a bound that both an edge and the
               mid would take) or not at all (a row on a bound neither takes). The plain objects total about 305,000 CPU, so the eighth
               place sits near that. A HEAVY row (5,000,000) ranks first when counted and drops off the page when dropped. The LIGHT row
               (200,000) is below the eighth place counted once and above it counted twice. Rows at the window start and just past its
               end are left out. The row at the end, the rows on and beside the mid's two hours are each kept once. */
            var midStart = BoundsMidStart(start);
            var midEnd = BoundsMidEnd(end);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                             object_type, delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
                VALUES (914730000001, $1, {ServerId}, '{ServerName}', 'db0', 'dbo', 'AT_START', 'P', 1, 5000000, 5000000, 300),
                       (914730000002, $2, {ServerId}, '{ServerName}', 'db0', 'dbo', 'BEFORE_MID', 'P', 1, 5000000, 5000000, 300),
                       (914730000003, $3, {ServerId}, '{ServerName}', 'db0', 'dbo', 'AT_MIDSTART_HEAVY', 'P', 1, 5000000, 5000000, 300),
                       (914730000004, $3, {ServerId}, '{ServerName}', 'db0', 'dbo', 'AT_MIDSTART_LIGHT', 'P', 1, 200000, 200000, 300),
                       (914730000005, $4, {ServerId}, '{ServerName}', 'db0', 'dbo', 'BEFORE_MIDEND', 'P', 1, 5000000, 5000000, 300),
                       (914730000006, $5, {ServerId}, '{ServerName}', 'db0', 'dbo', 'AT_MIDEND', 'P', 1, 5000000, 5000000, 300),
                       (914730000007, $6, {ServerId}, '{ServerName}', 'db0', 'dbo', 'AT_END', 'P', 1, 5000000, 5000000, 300),
                       (914730000008, $7, {ServerId}, '{ServerName}', 'db0', 'dbo', 'AFTER_END', 'P', 1, 5000000, 5000000, 300)
                """, start, midStart.AddMinutes(-1), midStart, midEnd.AddMinutes(-1), midEnd, end, end.AddSeconds(1));
        }

        if (shape == SeedShape.NullKeys)
        {
            /* Two heavy keys that rank first: PNULLDB has a NULL database, and an object with a NULL name sits in db0. Neither has an
               equality to seek on, so both go through the NULL-safe join. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                             object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                             sample_interval_seconds)
                SELECT 914740000000 + row_number() OVER (), t, {ServerId}, '{ServerName}', NULL, 'dbo', 'PNULLDB', 'P', 'shn', 'phn', 3, 2000000, 2000000, 300
                FROM generate_series($1::timestamp, $2::timestamp, interval '30 minutes') AS t
                UNION ALL
                SELECT 914741000000 + row_number() OVER (), t, {ServerId}, '{ServerName}', 'db0', 'dbo', NULL, 'P', 'sho', 'pho', 3, 1900000, 1900000, 300
                FROM generate_series($1::timestamp, $2::timestamp, interval '30 minutes') AS t
                """, start.AddHours(-2).AddMinutes(1), end.AddHours(1));
        }

        if (shape == SeedShape.MaterializationGap)
        {
            /* The load is uniform across the window, so a gap alone would not move the ranking. PGAP does all its work inside the
               skipped hours (about 3.6 million CPU, the top of the raw page), so a mid that ignored the hole would not rank it. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                             object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                             sample_interval_seconds)
                SELECT 914720000000 + row_number() OVER (), t, {ServerId}, '{ServerName}', 'db0', 'dbo', 'PGAP', 'P', 'shg', 'phg', 3, 100000, 100000, 300
                FROM generate_series($1::timestamp, $2::timestamp, interval '5 minutes') AS t
                """, start.AddHours(8).AddMinutes(1), start.AddHours(11));
        }

        /* A refresh over a sub-range leaves the rest unmaterialized. The gap case skips hours 8 to 12 of the window: raw holds
           rows there and the rollup holds no bucket, which is the hole the mid route's scan must see. */
        var refreshRanges = shape == SeedShape.MaterializationGap
            ? new[] { (start.AddHours(-3), start.AddHours(8)), (start.AddHours(12), hour) }
            : new[] { (start.AddHours(-3), hour) };
        foreach (var (lo, hi) in refreshRanges)
        {
            await using var refresh = new NpgsqlCommand(
                $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection);
            refresh.Parameters.AddWithValue(lo);
            refresh.Parameters.AddWithValue(hi);
            await refresh.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            await body(scratch, connection, start, end, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }
}
