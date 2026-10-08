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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Static pins for the hourly rollup of <c>collect.pg_io_stats</c> (#5495, V170): the read's shape, the rung, the tick and the retention
/// prune. The live facts are in <see cref="PgIoStatsHourlyLiveTests"/>. Lite has no PostgreSQL targets, so there is no twin to pin there.
/// </summary>
public sealed class PgIoStatsHourlyTests
{
    [Fact]
    public void TheStitchedRead_ReadsRawRowsOnlyAtTheTwoEdges_AndWholeHoursFromTheRollup()
    {
        var sql = PgIoStatsHourly.StitchedReadSql;

        /* Raw is bounded to the head [start, h1) and the tail [h2, end]; the middle is never scanned. */
        Assert.Contains("collection_time >= $2 AND collection_time < $5", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $6 AND collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_io_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("hour_start >= $5 AND hour_start < $6", sql, StringComparison.Ordinal);

        /* The boundary difference counts only when the previous row is inside the window: the old per-window LAG's rule. */
        Assert.Contains("prev_ct >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY backend_type, object_type, context", sql, StringComparison.Ordinal);

        /* Same output columns as the raw statement, in the same order (the reader's ordinals). */
        static List<string> Aliases(string s)
        {
            s = s.Replace("\r\n", "\n");
            var select = s[System.Text.RegularExpressions.Regex.Matches(s, @"SELECT\s+backend_type,\s+object_type").Last().Index..];
            select = select[..select.IndexOf("FROM differenced", StringComparison.Ordinal)];
            return System.Text.RegularExpressions.Regex.Matches(select, @"(?:\bAS\s+(\w+)|^\s+(backend_type|object_type|context),)", System.Text.RegularExpressions.RegexOptions.Multiline)
                .Select(m => m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value).ToList();
        }

        Assert.Equal(Aliases(DarlingPgIoReader.PgIoSql), Aliases(sql));    }

    [Fact]
    public void TheCountGuard_ComparesRawCountsWithTheRollupsRowCount_ForEveryWholeHour()
    {
        var sql = PgIoStatsHourly.GuardSql;
        Assert.Contains("count(*)", sql, StringComparison.Ordinal);
        Assert.Contains("sum(r.row_count)", sql, StringComparison.Ordinal);
        Assert.Contains("FULL JOIN", sql, StringComparison.Ordinal);
        Assert.Contains("IS DISTINCT FROM", sql, StringComparison.Ordinal);
        Assert.Contains("h1 > first_hour", sql, StringComparison.Ordinal);
        Assert.Contains("built_through", sql, StringComparison.Ordinal);
        Assert.Contains("interval '6 hours'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCountGuard_BoundsTheRawCountOnTheBoundParameters_NotOnlyOnAColumnOfACte()
    {
        /* #5495 big-store fix: the raw table is a hypertable, and chunk exclusion needs bounds it can evaluate from the
           bound parameters. The hours the rollup may serve come from the ok CTE (a column of a CTE), which cannot exclude a
           chunk, so the raw count carries $2/$3 bounds of its own next to the exact ones. */
        var sql = PgIoStatsHourly.GuardSql.Replace("\r\n", "\n");
        var raw = sql[sql.IndexOf("l AS (", StringComparison.Ordinal)..sql.IndexOf("c AS (", StringComparison.Ordinal)];
        Assert.Contains("t.collection_time >= $2::timestamp - interval '1 hour'", raw, StringComparison.Ordinal);
        Assert.Contains("t.collection_time < $3::timestamp", raw, StringComparison.Ordinal);
        Assert.Contains("t.collection_time >= ok.h1 - interval '1 hour' AND t.collection_time < ok.h2", raw, StringComparison.Ordinal);

        /* The rollup side is a plain table whose unique index leads with (server_id, hour_start): the range is already an index range. */
        Assert.Contains("r.hour_start >= ok.h1 - interval '1 hour' AND r.hour_start < ok.h2", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBuild_SeedsEachCombinationFromItsNewestEarlierRollupRowByIndex_NotBySortingEveryEarlierRow()
    {
        /* #5495: the seed was DISTINCT ON over every earlier rollup row of the server, which sorts all of them for each hour built
           (quadratic over a first fill). It now probes once per combination of the hour being built. */
        var sql = PgIoStatsHourly.BuildHourSql.Replace("\r\n", "\n");
        var seed = sql[sql.IndexOf("seed AS (", StringComparison.Ordinal)..sql.IndexOf("u AS (", StringComparison.Ordinal)];
        Assert.Contains("LATERAL", seed, StringComparison.Ordinal);
        Assert.Contains("ORDER BY p.hour_start DESC", seed, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", seed, StringComparison.Ordinal);
        Assert.DoesNotContain("DISTINCT ON", seed, StringComparison.Ordinal);

        /* The three key columns are nullable, so the probe needs a NULL-equal compare that an index can serve: array equality over the
           same expression the V170 index holds. IS NOT DISTINCT FROM is not indexable and scanned back through every earlier rollup row
           of a combination that was new or absent for a long time. */
        Assert.Contains("ARRAY[p.backend_type, p.object_type, p.context] = ARRAY[cmb.backend_type, cmb.object_type, cmb.context]", seed, StringComparison.Ordinal);
        Assert.DoesNotContain("IS NOT DISTINCT FROM", seed, StringComparison.Ordinal);
        Assert.Contains("ON collect.pg_io_stats_hourly (server_id, (ARRAY[backend_type, object_type, context]), hour_start DESC)", PgIoStatsHourly.CreateSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheGc_IsDrivenFromTheSmallStateTable_NotAScanOfTheWholeRollup()
    {
        var gc = PgIoStatsHourlyBuilder.GcSql.Replace("\r\n", "\n");
        gc = gc.TrimEnd();

        /* One statement, so one snapshot: a server disabled while it runs cannot lose its state row and keep its rollup rows. */
        Assert.Equal(gc.Length - 1, gc.IndexOf(';', StringComparison.Ordinal));
        Assert.StartsWith("WITH gone AS (", gc, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM collect.pg_io_stats_hourly_state WHERE server_id NOT IN", gc, StringComparison.Ordinal);
        Assert.Contains("RETURNING server_id", gc, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM collect.pg_io_stats_hourly WHERE server_id IN (SELECT server_id FROM gone)", gc, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_io_stats_hourly WHERE server_id NOT IN", gc, StringComparison.Ordinal);
    }

    [Fact]
    public void TheReader_TriesTheRollupInOneSnapshot_AndFallsBackToTheRawStatement()
    {
        var reader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "DarlingPgIoReader.cs");
        Assert.Contains("IsolationLevel.RepeatableRead", reader, StringComparison.Ordinal);
        Assert.Contains("PgIoStatsHourly.GuardSql", reader, StringComparison.Ordinal);
        Assert.Contains("PgIoStatsHourly.StitchedReadSql", reader, StringComparison.Ordinal);
        Assert.Contains("new NpgsqlCommand(PgIoSql, connection)", reader, StringComparison.Ordinal);
        /* The raw statement is unchanged: still the per-window LAG. */
        Assert.Contains("LAG(reads)      OVER series", DarlingPgIoReader.PgIoSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRung_IsRegistered_AndCreatesPlainEmptyTables()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == PgIoStatsHourly.RungVersion);
        Assert.Equal("pg-io-stats-hourly", rung.Name);
        Assert.Equal(170, PgIoStatsHourly.RungVersion);
        Assert.Equal(PgIoStatsHourly.CreateSql, rung.Sql);
        Assert.DoesNotContain("INSERT", PgIoStatsHourly.CreateSql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("create_hypertable", PgIoStatsHourly.CreateSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NULLS NOT DISTINCT", PgIoStatsHourly.CreateSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTick_IsAWorkerTenant_AndTheRollupIsPrunedWithTheRawRetention()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains("await BuildPgIoStatsHourlyAsync(stoppingToken);", worker, StringComparison.Ordinal);
        Assert.Contains("PgIoStatsHourlyBuilder.RunTickAsync(", worker, StringComparison.Ordinal);
        /* Its place in the tick: awaited once, outside the TimescaleDB gate, and BEFORE the module-map refresh. The plan-regression builder
           (up to a 10-minute tick) stays the last await with the refresh directly before it - the pins in DarlingModuleMapRecentRefreshLiveTests
           and PlanRegressionDailyBuilderLiveTests say so - so the rollup must not be appended after it. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(worker);
        var tickStart = code.IndexOf("private async Task RunStoreMaintenanceTickAsync(CancellationToken stoppingToken)", StringComparison.Ordinal);
        Assert.True(tickStart >= 0, "could not locate RunStoreMaintenanceTickAsync");
        var tickEnd = code.IndexOf("private async Task RefreshModuleMapRecentAsync(CancellationToken stoppingToken)", tickStart, StringComparison.Ordinal);
        Assert.True(tickEnd > tickStart, "could not bound RunStoreMaintenanceTickAsync");
        var tick = code[tickStart..tickEnd];
        const string Rollup = "await BuildPgIoStatsHourlyAsync(stoppingToken);";
        const string Refresh = "await RefreshModuleMapRecentAsync(stoppingToken);";
        Assert.Equal(1, tick.Split(Rollup).Length - 1);
        Assert.Equal(1, code.Split(Rollup).Length - 1);
        var rollupAt = tick.IndexOf(Rollup, StringComparison.Ordinal);
        var refreshAt = tick.IndexOf(Refresh, StringComparison.Ordinal);
        Assert.True(refreshAt > rollupAt, "the I/O rollup is awaited before the module-map refresh");
        Assert.True(string.IsNullOrWhiteSpace(tick[(rollupAt + Rollup.Length)..refreshAt]), "nothing sits between the I/O rollup and the module-map refresh");
        Assert.True(rollupAt > tick.IndexOf("else", StringComparison.Ordinal), "the I/O rollup sits after the gated if/else, outside the TimescaleDB gate");

        var retention = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs");
        Assert.Contains("PgIoStatsHourlyBuilder.PruneSql", retention, StringComparison.Ordinal);
        Assert.Contains("\"pg_io_stats\"", retention, StringComparison.Ordinal);

        /* The first fill is bounded by time, not by a count of builds: no new build starts after the budget, and a build still running
           at the hard cap is cancelled (TheHardCap_... below). The budget is the only start cutoff, so there is no second one to keep in step. */
        Assert.Null(typeof(PgIoStatsHourlyBuilder).GetField("StartCutoff", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static));
        Assert.Null(typeof(PgIoStatsHourlyBuilder).GetField("MaxBuildsPerTick"));
        Assert.Equal(TimeSpan.FromMinutes(2), PgIoStatsHourlyBuilder.TickBudget);
        Assert.True(PgIoStatsHourlyBuilder.TickBudget < PgIoStatsHourlyBuilder.MaxTickDuration);
        Assert.True(PgIoStatsHourlyBuilder.MaxTickDuration <= TimeSpan.FromMinutes(10));
    }
}

/// <summary>
/// Live facts for the hourly rollup of <c>collect.pg_io_stats</c> (#5495): the rollup read returns exactly what the raw statement returns
/// across a counter reset, a crash restart (counters drop, stats_reset NULL), a collector gap and a combination that appears late, for
/// windows that start and end mid-hour and a start inside the gap; the count guard refuses a window with a late row; a store below the
/// rung (no rollup tables) still reads. Each fact runs on its own scratch store when <c>DARLING_TEST_PG</c> is set.
/// </summary>
[Collection("live-postgres")]
public sealed class PgIoStatsHourlyLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the pg_io_stats hourly rollup live pins (each mints its own scratch database).";

    private static readonly DateTime Now = new(2026, 3, 20, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Start = new(2026, 3, 14, 3, 25, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Reset = new(2026, 3, 16, 9, 40, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Crash = new(2026, 3, 18, 1, 5, 0, DateTimeKind.Unspecified);
    private static readonly DateTime GapStart = new(2026, 3, 17, 5, 3, 0, DateTimeKind.Unspecified);
    private static readonly DateTime GapEnd = new(2026, 3, 17, 8, 2, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Late = new(2026, 3, 19, 6, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Ts(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string SeedSql() => $@"
INSERT INTO collect.pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
    reads, read_time_ms, writes, write_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, stats_reset,
    read_bytes, write_bytes, extend_bytes)
SELECT row_number() OVER (), t, 1, 'srv1', c.bt, 'relation', c.cx,
       a.age * c.k, (a.age * c.k)::float8, CASE WHEN c.bt = 'startup' THEN NULL ELSE a.age * 2 END, (a.age * 2)::float8,
       a.age / 3, (a.age / 3)::float8, 8192, a.age * 7, a.age / 5, a.age / 7,
       CASE WHEN t >= '{Ts(Reset)}' AND t < '{Ts(Crash)}' THEN '{Ts(Reset)}'::timestamp ELSE '{Ts(Start.AddHours(-1))}'::timestamp END,
       CASE WHEN c.bt = 'startup' THEN NULL ELSE a.age * c.k * 8192 END, a.age * 8192, a.age * 4096
FROM generate_series('{Ts(Start)}'::timestamp, '{Ts(Now.AddMinutes(-10))}'::timestamp, interval '10 minutes') AS t
CROSS JOIN (VALUES ('client backend', 'normal', 3), ('autovacuum worker', 'vacuum', 5), ('startup', 'normal', 2)) AS c(bt, cx, k)
CROSS JOIN LATERAL (SELECT (CASE WHEN t < '{Ts(Reset)}' THEN extract(epoch FROM t - '{Ts(Start)}'::timestamp)
                                  WHEN t < '{Ts(Crash)}' THEN extract(epoch FROM t - '{Ts(Reset)}'::timestamp)
                                  ELSE extract(epoch FROM t - '{Ts(Crash)}'::timestamp) END / 600)::bigint AS age) AS a
WHERE NOT (t >= '{Ts(GapStart)}' AND t < '{Ts(GapEnd)}')
AND   NOT (c.bt = 'startup' AND t < '{Ts(Late)}')";

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var p in parameters)
        {
            command.Parameters.AddWithValue(p);
        }

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => reader.IsDBNull(i) ? "~" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture))));
        }

        return rows;
    }

    private static DateTime Unspec(DateTime v) => DateTime.SpecifyKind(v, DateTimeKind.Unspecified);

    private static string Fix(string sql) => sql.Replace("FROM pg_io_stats_hourly", "FROM collect.pg_io_stats_hourly").Replace("FROM pg_io_stats\n", "FROM collect.pg_io_stats\n").Replace("FROM pg_io_stats_hourly_state", "FROM collect.pg_io_stats_hourly_state");

    private static async Task RunLiveAsync(Func<ScratchPostgres, NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body, bool build = true)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database of its own, so no other class's migrations race it.
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, "SET search_path = collect, public; INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (1, 'srv1', TRUE) ON CONFLICT (server_id) DO NOTHING", ct);
            await ExecAsync(connection, SeedSql(), ct);
            var searchPath = scratch.ConnectionString + ";Search Path=collect,public";
            await using var dataSource = NpgsqlDataSource.Create(searchPath);
            await using var query = new NpgsqlConnection(searchPath);
            await query.OpenAsync(ct);
            if (build)
            {
                var tick = await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, ct);
                Assert.True(tick.Built > 100);
                Assert.Equal(0, tick.Failed);
            }

            await body(scratch, query, dataSource, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Theory]
    [InlineData(24, 0)]
    [InlineData(72, 0)]
    [InlineData(120, 0)]
    [InlineData(120, 7)]
    [InlineData(40, 33)]
    public async Task TheStitchedRead_EqualsTheRawStatement_AcrossAResetACrashAGapAndALateCombination(int hours, int endOffsetMinutes)
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            var end = Now.AddMinutes(-endOffsetMinutes);
            var start = end.AddHours(-hours);
            var span = await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(end));
            Assert.Single(span);

            var raw = await RowsAsync(connection, DarlingPgIoReader.PgIoSql, 1, Unspec(start), Unspec(end), 1000);
            var parts = span[0].Split('|');
            var stitched = await RowsAsync(connection, Fix(PgIoStatsHourly.StitchedReadSql), 1, Unspec(start), Unspec(end), 1000,
                DateTime.Parse(parts[0], CultureInfo.InvariantCulture), DateTime.Parse(parts[1], CultureInfo.InvariantCulture));
            Assert.NotEmpty(raw);
            Assert.Equal(raw, stitched);

            /* And through the reader, which takes the rollup route on its own. */
            var viaReader = await DarlingPgIoReader.GetPgIoPageAsync(dataSource, 1, start, end, 1000, ct);
            Assert.Equal(raw.Count, viaReader.Rows.Count);
        });
    }

    [Fact]
    public async Task AWindowStartingInsideTheCollectorGap_AndAWindowStartingBeforeTheFirstBuiltHour_StillMatchRaw()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            var inGap = GapStart.AddMinutes(61);
            var end = Now.AddMinutes(-2);
            foreach (var start in new[] { inGap, inGap.AddHours(-3).AddMinutes(-20), Start })
            {
                var raw = await RowsAsync(connection, DarlingPgIoReader.PgIoSql, 1, Unspec(start), Unspec(end), 1000);
                var viaReader = await DarlingPgIoReader.GetPgIoPageAsync(dataSource, 1, start, end, 1000, ct);
                Assert.Equal(raw.Count, viaReader.Rows.Count);
                var span = await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(end));
                if (start == Start)
                {
                    Assert.Empty(span);
                }
                else
                {
                    Assert.Single(span);
                    var parts = span[0].Split('|');
                    var stitched = await RowsAsync(connection, Fix(PgIoStatsHourly.StitchedReadSql), 1, Unspec(start), Unspec(end), 1000,
                        DateTime.Parse(parts[0], CultureInfo.InvariantCulture), DateTime.Parse(parts[1], CultureInfo.InvariantCulture));
                    Assert.Equal(raw, stitched);
                }
            }
        });
    }

    [Fact]
    public async Task ALateRowInABuiltHour_FailsTheCountGuard_AndTheReaderStillAnswersFromRaw()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            var start = Now.AddHours(-48);
            Assert.Single(await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(Now)));

            /* A row dated inside a built hour that the rebuild span no longer covers. */
            await ExecAsync(connection,
                $"INSERT INTO collect.pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, reads) VALUES (999999999, '{Ts(Now.AddHours(-20).AddMinutes(1))}', 1, 'srv1', 'client backend', 'relation', 'normal', 1)", ct);
            Assert.Empty(await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(Now)));
            var viaReader = await DarlingPgIoReader.GetPgIoPageAsync(dataSource, 1, start, Now, 1000, ct);
            Assert.NotEmpty(viaReader.Rows);

            /* The next tick does not touch an hour that old, so the guard keeps refusing it until the hour ages out. */
            await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, ct);
            Assert.Empty(await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(Now)));
        });
    }

    [Fact]
    public async Task ALateRowInTheHourBeforeTheFirstRollupHour_FailsTheCountGuard_AndTheReadMatchesRaw()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            /* The window starts mid-hour (02:30), so its first rollup hour is 03:00 and the 02:00 hour is a raw edge. The 03:00
               rollup row's boundary difference was taken against the 02:55 row. */
            var start = new DateTime(2026, 3, 19, 2, 30, 0, DateTimeKind.Unspecified);
            Assert.Single(await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(Now)));

            /* A row dated 02:58 arrives long after the rebuild span has passed: a copy of the 02:55 row with the reads a little ahead. */
            await ExecAsync(connection,
                "INSERT INTO collect.pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, "
                + "reads, read_time_ms, writes, write_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, stats_reset, read_bytes, write_bytes, extend_bytes) "
                + "SELECT 999999998, collection_time + interval '3 minutes', server_id, server_name, backend_type, object_type, context, "
                + "reads + 1, read_time_ms + 1, writes, write_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, stats_reset, read_bytes, write_bytes, extend_bytes "
                + "FROM collect.pg_io_stats WHERE server_id = 1 AND backend_type = 'client backend' AND context = 'normal' AND collection_time = '2026-03-19 02:55:00'", ct);

            /* The late row is in the hour before h1, not inside [h1, h2): the guard still refuses the window. */
            Assert.Empty(await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(Now)));

            var raw = await RowsAsync(connection, DarlingPgIoReader.PgIoSql, 1, Unspec(start), Unspec(Now), 1000);
            var viaReader = await DarlingPgIoReader.GetPgIoPageAsync(dataSource, 1, start, Now, 1000, ct);
            Assert.NotEmpty(raw);
            Assert.Equal(long.Parse(raw[0].Split('|')[19], CultureInfo.InvariantCulture), viaReader.WindowTotalReads);
            Assert.Equal(raw.Count, viaReader.Rows.Count);

            /* A window that starts after that hour does not need it, and is still served from the rollup. */
            Assert.Single(await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start.AddHours(3)), Unspec(Now)));
        });
    }

    [Fact]
    public async Task ARebuildOfTheLateRowsHourAndTheNextHour_IsOneTransaction_SoAFailedNextHourLeavesNoDoubleCount()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            /* The watermark is 11:00, so a tick rebuilds 09:00 and 10:00. A row dated 09:58 lands long after the hour closed (the hour's
               last row was 09:55); 10:00's boundary difference was taken against 09:55. If 09:00 is rebuilt and committed and then
               10:00 fails, the guard sees equal counts in both hours while 10:00 still holds the stale boundary difference, and a window over
               both hours counts the 09:55 to 09:58 change twice. */
            var start = Now.AddHours(-48);
            await ExecAsync(connection,
                "INSERT INTO collect.pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, "
                + "reads, read_time_ms, writes, write_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, stats_reset, read_bytes, write_bytes, extend_bytes) "
                + "SELECT 999999997, collection_time + interval '3 minutes', server_id, server_name, backend_type, object_type, context, "
                + "reads + 1, read_time_ms + 1, writes, write_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, stats_reset, read_bytes, write_bytes, extend_bytes "
                + "FROM collect.pg_io_stats WHERE server_id = 1 AND backend_type = 'client backend' AND context = 'normal' AND collection_time = '2026-03-20 09:55:00'", ct);
            await ExecAsync(connection,
                "CREATE FUNCTION collect.fail_planted_hour() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN "
                + "IF NEW.hour_start = '2026-03-20 10:00' THEN RAISE EXCEPTION 'planted rollup build failure'; END IF; RETURN NEW; END $f$; "
                + "CREATE TRIGGER fail_planted_hour BEFORE INSERT ON collect.pg_io_stats_hourly FOR EACH ROW EXECUTE FUNCTION collect.fail_planted_hour()", ct);

            var failed = await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, ct);
            Assert.Equal(1, failed.Failed);

            var raw = await RowsAsync(connection, DarlingPgIoReader.PgIoSql, 1, Unspec(start), Unspec(Now), 1000);
            Assert.NotEmpty(raw);
            var viaReader = await DarlingPgIoReader.GetPgIoPageAsync(dataSource, 1, start, Now, 1000, ct);
            Assert.Equal(long.Parse(raw[0].Split('|')[19], CultureInfo.InvariantCulture), viaReader.WindowTotalReads);
            Assert.Equal(raw.Count, viaReader.Rows.Count);

            /* Whatever the guard says, a stitched read it allows must equal raw. */
            var span = await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(Now));
            if (span.Count == 1)
            {
                var parts = span[0].Split('|');
                var stitched = await RowsAsync(connection, Fix(PgIoStatsHourly.StitchedReadSql), 1, Unspec(start), Unspec(Now), 1000,
                    DateTime.Parse(parts[0], CultureInfo.InvariantCulture), DateTime.Parse(parts[1], CultureInfo.InvariantCulture));
                Assert.Equal(raw, stitched);
            }

            /* The next good tick rebuilds both hours together and the rollup serves the window again. */
            await ExecAsync(connection, "DROP TRIGGER fail_planted_hour ON collect.pg_io_stats_hourly", ct);
            var healed = await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, ct);
            Assert.Equal(0, healed.Failed);
            span = await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(Now));
            Assert.Single(span);
            var healedParts = span[0].Split('|');
            var healedRead = await RowsAsync(connection, Fix(PgIoStatsHourly.StitchedReadSql), 1, Unspec(start), Unspec(Now), 1000,
                DateTime.Parse(healedParts[0], CultureInfo.InvariantCulture), DateTime.Parse(healedParts[1], CultureInfo.InvariantCulture));
            Assert.Equal(raw, healedRead);
        });
    }

    [Fact]
    public async Task TheHardCap_CancelsABuildThatIsRunning_AndItsHoursStayUnbuilt_AndLaterBuildsAreDeferred()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            /* A first fill whose first build is held inside its insert: a trigger takes an advisory lock that this test owns (left alone the build ends at its 60 s statement_timeout, so the 20 s wait below fails unless the cap cancels it). The hard cap
               is a token the test fires by hand once it sees the build waiting, so no wall-clock time passes. */
            await ExecAsync(connection, "TRUNCATE collect.pg_io_stats_hourly, collect.pg_io_stats_hourly_state", ct);
            await ExecAsync(connection,
                "CREATE FUNCTION collect.hold_planted_hour() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN PERFORM pg_advisory_xact_lock(5495); RETURN NEW; END $f$; "
                + "CREATE TRIGGER hold_planted_hour BEFORE INSERT ON collect.pg_io_stats_hourly FOR EACH ROW EXECUTE FUNCTION collect.hold_planted_hour()", ct);

            /* The lock is a session lock on its own unpooled connection: closing it (the end of this test, or a failure) releases it. */
            await using var holder = new NpgsqlConnection(scratch.ConnectionString + ";Pooling=false");
            await holder.OpenAsync(ct);
            await ExecAsync(holder, "SELECT pg_advisory_lock(5495)", ct);

            using var cap = new CancellationTokenSource();
            var tick = PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, () => TimeSpan.Zero, cap.Token, ct);
            var waiting = false;
            for (var attempt = 0; attempt < 600 && !waiting; attempt++)
            {
                waiting = (await RowsAsync(connection, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND wait_event = 'advisory'"))[0] != "0";
                if (!waiting)
                {
                    await Task.Delay(50, ct);
                }
            }

            Assert.True(waiting, "the first build never reached its insert");
            cap.Cancel();
            var result = await tick.WaitAsync(TimeSpan.FromSeconds(20), ct);
            Assert.Equal(new PgIoStatsHourlyBuilder.TickResult(0, 1, 0, 0), result);

            /* The cancelled build rolled back: no rollup row, no state row, and the reader answers from raw. */
            Assert.Equal("0", (await RowsAsync(connection, "SELECT count(*) FROM collect.pg_io_stats_hourly"))[0]);
            Assert.Equal("0", (await RowsAsync(connection, "SELECT count(*) FROM collect.pg_io_stats_hourly_state"))[0]);
            Assert.NotEmpty((await DarlingPgIoReader.GetPgIoPageAsync(dataSource, 1, Now.AddHours(-48), Now, 1000, ct)).Rows);

            /* A cap that has already passed starts no build at all; the caller's own token still ends the tick. */
            await ExecAsync(connection, "DROP TRIGGER hold_planted_hour ON collect.pg_io_stats_hourly", ct);
            using var passed = new CancellationTokenSource();
            passed.Cancel();
            var late = await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, () => TimeSpan.Zero, passed.Token, ct);
            Assert.Equal(0, late.Built);
            Assert.Equal(0, late.Failed);
            Assert.True(late.Deferred > 100);
            Assert.Equal("0", (await RowsAsync(connection, "SELECT count(*) FROM collect.pg_io_stats_hourly"))[0]);
            using var stopped = new CancellationTokenSource();
            stopped.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, () => TimeSpan.Zero, CancellationToken.None, stopped.Token));

            /* The next tick (no cap fired) builds the whole fill. */
            var resumed = await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, () => TimeSpan.Zero, CancellationToken.None, ct);
            Assert.Equal(0, resumed.Failed);
            Assert.True(resumed.Built > 100);
            Assert.Single(await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(Now.AddHours(-48)), Unspec(Now)));
        }, build: false);
    }

    [Fact]
    public async Task ACombinationWithNullKeyColumns_IsSeededAcrossAGapByTheArrayProbe_AndMatchesRaw()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            /* 'bgwriter' has NULL object_type and context (the seed probe must match NULL to NULL): rows at 06:05 and 06:15, then nothing for
               two hours, then 09:05 and 09:15. The 09:00 hour is seeded from 06:00's last row across the gap. */
            await ExecAsync(connection,
                "INSERT INTO collect.pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, reads, read_time_ms) VALUES "
                + "(999999980, '2026-03-19 06:05:00', 1, 'srv1', 'bgwriter', 100, 100), (999999981, '2026-03-19 06:15:00', 1, 'srv1', 'bgwriter', 110, 110), "
                + "(999999982, '2026-03-19 09:05:00', 1, 'srv1', 'bgwriter', 140, 140), (999999983, '2026-03-19 09:15:00', 1, 'srv1', 'bgwriter', 150, 150)", ct);
            await ExecAsync(connection, "TRUNCATE collect.pg_io_stats_hourly, collect.pg_io_stats_hourly_state", ct);
            await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, ct);
            Assert.Equal("1", (await RowsAsync(connection, "SELECT count(*) FROM collect.pg_io_stats_hourly WHERE backend_type = 'bgwriter' AND hour_start = '2026-03-19 09:00' AND b_reads = 30"))[0]);

            var start = new DateTime(2026, 3, 19, 5, 30, 0, DateTimeKind.Unspecified);
            var end = Now.AddMinutes(-2);
            var raw = await RowsAsync(connection, DarlingPgIoReader.PgIoSql, 1, Unspec(start), Unspec(end), 1000);
            var span = await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(end));
            Assert.Single(span);
            var parts = span[0].Split('|');
            var stitched = await RowsAsync(connection, Fix(PgIoStatsHourly.StitchedReadSql), 1, Unspec(start), Unspec(end), 1000,
                DateTime.Parse(parts[0], CultureInfo.InvariantCulture), DateTime.Parse(parts[1], CultureInfo.InvariantCulture));
            Assert.Contains(raw, r => r.StartsWith("bgwriter|", StringComparison.Ordinal));
            Assert.Equal(raw, stitched);
        });
    }

    [Fact]
    public async Task AStoreBelowTheRung_HasNoRollupTables_AndTheReaderStillAnswersFromRaw()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            await ExecAsync(connection, "DROP TABLE collect.pg_io_stats_hourly; DROP TABLE collect.pg_io_stats_hourly_state", ct);
            var viaReader = await DarlingPgIoReader.GetPgIoPageAsync(dataSource, 1, Now.AddHours(-48), Now, 1000, ct);
            Assert.NotEmpty(viaReader.Rows);
        }, build: false);
    }

    [Fact]
    public async Task TheTick_BuildsOnlyClosedHours_IsBoundedByTheTimeBudget_AndIsIdempotent()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            Assert.Equal("True", (await RowsAsync(connection, $"SELECT built_through = '{Ts(Now.AddHours(-1))}'::timestamp FROM collect.pg_io_stats_hourly_state WHERE server_id = 1"))[0]);
            var before = (await RowsAsync(connection, "SELECT count(*), sum(row_count) FROM collect.pg_io_stats_hourly"))[0];

            var again = await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, ct);
            Assert.Equal(PgIoStatsHourlyBuilder.RebuildHours, again.Built);
            Assert.Equal(before, (await RowsAsync(connection, "SELECT count(*), sum(row_count) FROM collect.pg_io_stats_hourly"))[0]);

            /* The bound is time: a clock that reads past the budget after five builds stops the tick there, and the next tick
               (a clock that never runs out) resumes from the watermark and ends with the same rows as the unbounded fill. */
            await ExecAsync(connection, "TRUNCATE collect.pg_io_stats_hourly, collect.pg_io_stats_hourly_state", ct);
            var reads = 0;
            var capped = await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance,
                () => reads++ < 5 ? TimeSpan.Zero : PgIoStatsHourlyBuilder.TickBudget, ct);
            Assert.Equal(5, capped.Built);
            Assert.True(capped.Deferred > 100);

            var resumed = await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, () => TimeSpan.Zero, ct);
            Assert.Equal(0, resumed.Deferred);
            Assert.Equal(before, (await RowsAsync(connection, "SELECT count(*), sum(row_count) FROM collect.pg_io_stats_hourly"))[0]);
        });
    }

    [Fact]
    public async Task ADisabledServersRollupRows_AreRemovedByTheTick_ThroughTheStateTable()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            Assert.NotEqual("0", (await RowsAsync(connection, "SELECT count(*) FROM collect.pg_io_stats_hourly WHERE server_id = 1"))[0]);
            await ExecAsync(connection, "UPDATE collect.servers SET is_enabled = FALSE WHERE server_id = 1", ct);

            await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, ct);
            Assert.Equal("0", (await RowsAsync(connection, "SELECT count(*) FROM collect.pg_io_stats_hourly"))[0]);
            Assert.Equal("0", (await RowsAsync(connection, "SELECT count(*) FROM collect.pg_io_stats_hourly_state"))[0]);
        });
    }

    [Fact]
    public async Task ACombinationAbsentForSeveralHours_IsSeededFromItsLastRollupRowBeforeTheGap()
    {
        await RunLiveAsync(async (scratch, connection, dataSource, ct) =>
        {
            /* 'startup' has rows from 06:00 on the 19th only; take its 07:00-09:00 rows away so it reappears at 09:00 after a gap
               of two hours, then rebuild everything: the 09:00 hour must be seeded from 06:00's last row, not from the hour before. */
            await ExecAsync(connection, "DELETE FROM collect.pg_io_stats WHERE backend_type = 'startup' AND collection_time >= '2026-03-19 07:00' AND collection_time < '2026-03-19 09:00'", ct);
            await ExecAsync(connection, "TRUNCATE collect.pg_io_stats_hourly, collect.pg_io_stats_hourly_state", ct);
            await PgIoStatsHourlyBuilder.RunTickAsync(dataSource, Now, NullLogger.Instance, ct);

            var start = new DateTime(2026, 3, 19, 5, 30, 0, DateTimeKind.Unspecified);
            var end = Now.AddMinutes(-2);
            var raw = await RowsAsync(connection, DarlingPgIoReader.PgIoSql, 1, Unspec(start), Unspec(end), 1000);
            var span = await RowsAsync(connection, Fix(PgIoStatsHourly.GuardSql), 1, Unspec(start), Unspec(end));
            Assert.Single(span);
            var parts = span[0].Split('|');
            var stitched = await RowsAsync(connection, Fix(PgIoStatsHourly.StitchedReadSql), 1, Unspec(start), Unspec(end), 1000,
                DateTime.Parse(parts[0], CultureInfo.InvariantCulture), DateTime.Parse(parts[1], CultureInfo.InvariantCulture));
            Assert.Contains(raw, r => r.StartsWith("startup|", StringComparison.Ordinal));
            Assert.Equal(raw, stitched);
        });
    }
}
