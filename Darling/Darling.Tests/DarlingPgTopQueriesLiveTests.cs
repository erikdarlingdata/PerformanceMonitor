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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>get_pg_top_queries</c> EXECUTED against a real PostgreSQL (#2554), plus the wire shape of the id it
/// returns (#2548).
///
/// <para><b>Why this class had to exist at all.</b> Every other test on this read asserted against the SQL
/// STRING. Meanwhile the shipped query did not PARSE: #2219's <c>LEFT JOIN collect.pg_statement_text AS t</c>
/// put <c>t.queryid</c> in scope beside <c>differenced.queryid</c>, so the unqualified references became
/// ambiguous and PostgreSQL raised 42702 — a parse-time error, before a single row is examined. The read
/// therefore threw on EVERY call, on every engine including the Aurora one it exists for, from #2219 until
/// this fix, and a store with zero rows failed exactly the same way a full one did. Not one string assertion
/// could see it, and none did. **The deliverable here is the execution, not the assertions.**</para>
///
/// <para><b>The one-word fix would not have worked.</b> Qualifying only the <c>GROUP BY</c> — the obvious
/// repair, and the one the report proposed — still fails, on the select-list <c>queryid</c>, which is the
/// reference PostgreSQL actually names first. Measured before fixing, not guessed: both needed qualifying.
/// </para>
///
/// <para><b>And the capability gate on the THROW path.</b> The gate sat inside <c>if (rows.Count == 0)</c>,
/// so it could only speak when the query succeeded and returned nothing. On a target that excludes this
/// collector, a throw produced a raw SQL error where the sibling <c>get_pg_wait_stats</c> gives the honest
/// "does not run on that engine, and never will". (The excluded target used to be stock PostgreSQL; #2625
/// gave <c>pg_statement_stats</c> a vanilla <c>pg_stat_statements</c> path, so the gate is now exercised on
/// the DIALECT branch — a SQL Server target — and stock PostgreSQL joined Aurora as a capable engine whose
/// faults must still read as faults.) Guarding that is the awkward half of this class: once the ambiguity is fixed the read no
/// longer throws, so an assertion written against the ordinary empty path would pass with or WITHOUT the
/// catch-path fix and prove nothing. So the throw is INDUCED, deterministically and without touching shared
/// DDL, by seeding <c>delta_calls</c> at <c>bigint</c> extremes: <c>SUM()</c> over them widens to numeric and
/// the read's <c>CAST(... AS bigint)</c> overflows at runtime.</para>
///
/// <para><b>The discriminating half of that test is the Aurora case.</b> A "fix" that simply swallowed
/// exceptions into <c>not_collected</c> would satisfy the stock-PostgreSQL assertion perfectly. So the same
/// overflowing rows are asked for again as Aurora, where the collector CAN run, and the answer must still be
/// an error — a real fault on a capable engine must not be dressed up as a capability gap.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgTopQueriesLiveTests
{
    private const int ServerId = -853977;
    private const string ServerName = "pg-top-queries-e2e";

    /* Out of range for a double by construction (#2548), and negative, which is how most real
       pg_stat_statements ids look. */
    private const long OutOfRangeQueryId = -4185925123159566327L;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheReadExecutes_ReturnsItsQueryIdAsAnExactString_AndAnswersAThrowWithTheCapabilityGate()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_pg_top_queries test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.AuroraPostgres);

            /* ── the whole point: two snapshots, and the query must actually RUN ──

               Two, because every block and WAL figure is differenced across the window and a single sample
               has no interval to difference. delta_* carry the rate columns the way the collector stores
               them. */
            var t0 = MinutesAgo(30);
            var t1 = t0.AddSeconds(60);
            await SeedStatsAsync(connection, ct, t0, OutOfRangeQueryId, deltaCalls: 0, deltaTimeMs: 0, deltaRows: 0, walBytes: 1_000);
            await SeedStatsAsync(connection, ct, t1, OutOfRangeQueryId, deltaCalls: 40, deltaTimeMs: 900, deltaRows: 4_000, walBytes: 5_000);

            /* Text for that id, which is what introduced the join — and therefore the ambiguity. Seeded
               deliberately: an empty pg_statement_text would still exercise the LEFT JOIN, but a POPULATED
               one also proves the join actually matches, so a fix that made the query parse by dropping the
               join would fail here instead of passing quietly. */
            await SeedTextAsync(connection, ct, OutOfRangeQueryId, "SELECT * FROM public.widget WHERE id = $1");

            var json = await DarlingMcpPgTopQueries(dataSource);
            var root = JsonDocument.Parse(json).RootElement;

            /* Before anything else: it did not throw. Named explicitly, because "42702: column reference
               queryid is ambiguous" arriving inside a status:error envelope is exactly what shipped for
               months, and an assertion further down would report it as a missing property instead. */
            Assert.False(
                root.TryGetProperty("status", out var status) && status.GetString() == "error",
                $"the read threw instead of executing: {json}");

            var queries = root.GetProperty("queries").EnumerateArray().ToArray();
            var row = Assert.Single(queries);

            /* #2548: a string, and the EXACT digits. As a JSON number this id comes back
               -4185925123159566300 from any double-decoding parser, which joins to nothing. */
            var queryid = row.GetProperty("queryid");
            Assert.Equal(JsonValueKind.String, queryid.ValueKind);
            Assert.Equal(OutOfRangeQueryId.ToString(CultureInfo.InvariantCulture), queryid.GetString());

            /* The join landed. This is the assertion that separates "the query parses" from "the query does
               what #2219 added it for". */
            Assert.Equal("SELECT * FROM public.widget WHERE id = $1", row.GetProperty("query_text").GetString());

            /* Windowed arithmetic still correct after the qualification: the rate columns come from the
               stored deltas and the WAL figure is differenced across the two snapshots. */
            Assert.Equal(40, row.GetProperty("calls").GetInt64());
            Assert.Equal(900, row.GetProperty("total_exec_time_ms").GetInt64());
            Assert.Equal(4_000, row.GetProperty("rows_returned").GetInt64());
            Assert.Equal(4_000, row.GetProperty("wal_bytes").GetInt64());

            /* database_id stays a NUMBER beside the string id — an oid is unsigned 32-bit and cannot round,
               and a guard that let the whole payload become text would not notice. */
            Assert.Equal(JsonValueKind.Number, row.GetProperty("database_id").ValueKind);

            /* ── the throw path, and the gate that now covers it ──

               delta_calls at the bigint extremes: SUM() widens to numeric, and the read's
               CAST(SUM(delta_calls) AS bigint) overflows at RUNTIME. A deterministic fault that needs no
               DDL against the shared store. */
            await DeleteStatsAsync(connection, ct);
            await SeedStatsAsync(connection, ct, t0, OutOfRangeQueryId, deltaCalls: long.MaxValue, deltaTimeMs: 500, deltaRows: 1, walBytes: 0);
            await SeedStatsAsync(connection, ct, t1, OutOfRangeQueryId, deltaCalls: long.MaxValue, deltaTimeMs: 500, deltaRows: 1, walBytes: 0);

            /* Aurora FIRST, and this is the assertion that keeps the fix honest. The collector CAN run here,
               so a genuine fault must still read as a fault. A catch block that answered not_collected
               unconditionally would pass the stock-PostgreSQL case below while quietly converting every
               Aurora outage into "this engine cannot do that". */
            var auroraFault = JsonDocument.Parse(await DarlingMcpPgTopQueries(dataSource)).RootElement;
            Assert.Equal("error", auroraFault.GetProperty("status").GetString());

            /* Stock PostgreSQL is now a SECOND capable engine, not the gap case (#2625): pg_statement_stats
               reads the vanilla pg_stat_statements view here, so the same induced fault must still read as a
               fault. This assertion used to expect not_collected, and flipping it is the point — it is the
               one place in the suite that would have kept insisting stock PostgreSQL cannot answer this. */
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.Postgres);

            var stockFault = JsonDocument.Parse(await DarlingMcpPgTopQueries(dataSource)).RootElement;
            Assert.Equal("error", stockFault.GetProperty("status").GetString());

            /* And the catch-path GATE still has to speak, or #2532's fix rots the moment its only witness
               moves. With no PostgreSQL flavor left that excludes this collector, the surviving exclusion is
               the DIALECT one: a SQL Server target never receives a PostgreSQL collector at all, and the same
               throw must produce the sentence rather than the SQLSTATE. Different branch of the same gate,
               same guarantee. */
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.SqlServer);

            var dialectFault = JsonDocument.Parse(await DarlingMcpPgTopQueries(dataSource)).RootElement;
            Assert.Equal("not_collected", dialectFault.GetProperty("status").GetString());

            var gapText = dialectFault.GetProperty("message").GetString()!;
            Assert.Contains("pg_statement_stats", gapText, StringComparison.Ordinal);
            Assert.Contains("never will", gapText, StringComparison.Ordinal);

            /* The raw dialect error must not leak into the sentence — the whole complaint was that a caller
               got 42702 where its sibling got an explanation. */
            Assert.DoesNotContain("42702", gapText, StringComparison.Ordinal);
            Assert.DoesNotContain("out of range", gapText, StringComparison.OrdinalIgnoreCase);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7909: a statement that ran in one collection interval of the window has ONE stored row in it, because the
    /// collector stores nothing for a pass with no new calls, and its previous row can be hours earlier. That row's
    /// stored deltas already cover the interval that ended at it, and the block increment is its cumulative figure
    /// less the series' latest row before the window, so the statement ranks with both.
    /// </summary>
    [Fact]
    public async Task AStatementWithOneSampleInTheWindow_RanksWithItsCallsAndItsBlockIncrementSinceItsRowBeforeTheWindow()
    {
        var rows = await TopQueriesOverAsync(async (connection, ct) =>
        {
            /* The tool reads the last four hours. The previous row is two hours before that window began. */
            await SeedStatsAsync(connection, ct, MinutesAgo(6 * 60), OutOfRangeQueryId, deltaCalls: 100, deltaTimeMs: 1_000, deltaRows: 10_000, walBytes: 1_000, calls: 100, sharedBlksRead: 1_000);
            await SeedStatsAsync(connection, ct, MinutesAgo(30), OutOfRangeQueryId, deltaCalls: 10, deltaTimeMs: 100, deltaRows: 1_000, walBytes: 2_000, calls: 110, sharedBlksRead: 1_100);
        });

        var row = Assert.Single(rows);
        Assert.Equal(10, row.GetProperty("calls").GetInt64());
        Assert.Equal(100, row.GetProperty("total_exec_time_ms").GetInt64());
        Assert.Equal(1_000, row.GetProperty("rows_returned").GetInt64());
        Assert.Equal(100, row.GetProperty("shared_blks_read").GetInt64());
        Assert.Equal(1_000, row.GetProperty("wal_bytes").GetInt64());
    }

    /// <summary>
    /// #7909: every counter covers the same intervals. Three samples a minute apart inside the window, with the
    /// series' previous row an hour before the window began: the stored deltas give 100, 10 and 10 calls, and the
    /// block and WAL increments are taken over the same three intervals, the first against the row before the
    /// window. Blocks per call is then one figure over one set of intervals.
    /// </summary>
    [Fact]
    public async Task ThreeSamplesWithARowBeforeTheWindow_CountCallsAndBlocksOverTheSameThreeIntervals()
    {
        var rows = await TopQueriesOverAsync(async (connection, ct) =>
        {
            var t0 = MinutesAgo(30);
            await SeedStatsAsync(connection, ct, MinutesAgo(5 * 60), OutOfRangeQueryId, deltaCalls: 100, deltaTimeMs: 1_000, deltaRows: 10_000, walBytes: 500, calls: 1_000, sharedBlksRead: 900);
            await SeedStatsAsync(connection, ct, t0, OutOfRangeQueryId, deltaCalls: 100, deltaTimeMs: 1_000, deltaRows: 10_000, walBytes: 1_000, calls: 1_100, sharedBlksRead: 1_000);
            await SeedStatsAsync(connection, ct, t0.AddSeconds(60), OutOfRangeQueryId, deltaCalls: 10, deltaTimeMs: 100, deltaRows: 1_000, walBytes: 2_000, calls: 1_110, sharedBlksRead: 1_100);
            await SeedStatsAsync(connection, ct, t0.AddSeconds(120), OutOfRangeQueryId, deltaCalls: 10, deltaTimeMs: 100, deltaRows: 1_000, walBytes: 3_000, calls: 1_120, sharedBlksRead: 1_200);
        });

        var row = Assert.Single(rows);
        Assert.Equal(120, row.GetProperty("calls").GetInt64());
        Assert.Equal(1_200, row.GetProperty("total_exec_time_ms").GetInt64());
        Assert.Equal(12_000, row.GetProperty("rows_returned").GetInt64());
        Assert.Equal(300, row.GetProperty("shared_blks_read").GetInt64());
        Assert.Equal(2_500, row.GetProperty("wal_bytes").GetInt64());

        /* 2.5 blocks per call, numerator and denominator over the same three intervals. */
        Assert.Equal(2.5, (double)row.GetProperty("shared_blks_read").GetInt64() / row.GetProperty("calls").GetInt64(), 3);
    }

    /// <summary>
    /// #7909: a first sighting the collector could date stores <c>delta_calls</c> equal to <c>calls</c>, crediting
    /// the whole accrual to the interval, and its block and WAL figures are the cumulative values for the same
    /// reason. There is no earlier row to difference against.
    /// </summary>
    [Fact]
    public async Task AFirstSightingInsideTheWindow_ReportsItsCumulativeBlocksBesideItsCalls()
    {
        var rows = await TopQueriesOverAsync(async (connection, ct) =>
            await SeedStatsAsync(connection, ct, MinutesAgo(30), OutOfRangeQueryId, deltaCalls: 50, deltaTimeMs: 500, deltaRows: 5_000, walBytes: 4_000, calls: 50, sharedBlksRead: 1_000));

        var row = Assert.Single(rows);
        Assert.Equal(50, row.GetProperty("calls").GetInt64());
        Assert.Equal(500, row.GetProperty("total_exec_time_ms").GetInt64());
        Assert.Equal(1_000, row.GetProperty("shared_blks_read").GetInt64());
        Assert.Equal(4_000, row.GetProperty("wal_bytes").GetInt64());
    }

    /// <summary>
    /// #7909: a row the collector stored with <c>delta_calls = 0</c> is an interval it could not measure (a reset it
    /// could not date, or a gap past its max-gap policy, re-baselined with the cumulative counters as read), so the
    /// block and WAL figures contribute nothing beside it, however far its cumulative counters moved from the
    /// series' row before the window. The next measured sample is differenced against that row, not against the seed.
    /// </summary>
    [Fact]
    public async Task AnIntervalTheCollectorCouldNotMeasure_ContributesNothingOnAnyCounter()
    {
        var rows = await TopQueriesOverAsync(async (connection, ct) =>
        {
            var t0 = MinutesAgo(30);
            await SeedStatsAsync(connection, ct, MinutesAgo(6 * 60), OutOfRangeQueryId, deltaCalls: 100, deltaTimeMs: 1_000, deltaRows: 10_000, walBytes: 1_000, calls: 100, sharedBlksRead: 1_000);
            /* The re-baseline: no deltas, cumulative counters 500 blocks and 500 WAL bytes past the seed row. */
            await SeedStatsAsync(connection, ct, t0, OutOfRangeQueryId, deltaCalls: 0, deltaTimeMs: 0, deltaRows: 0, walBytes: 1_500, calls: 150, sharedBlksRead: 1_500);
            await SeedStatsAsync(connection, ct, t0.AddSeconds(60), OutOfRangeQueryId, deltaCalls: 10, deltaTimeMs: 100, deltaRows: 1_000, walBytes: 1_600, calls: 160, sharedBlksRead: 1_550);
        });

        var row = Assert.Single(rows);
        Assert.Equal(10, row.GetProperty("calls").GetInt64());
        Assert.Equal(100, row.GetProperty("total_exec_time_ms").GetInt64());
        Assert.Equal(50, row.GetProperty("shared_blks_read").GetInt64());
        Assert.Equal(100, row.GetProperty("wal_bytes").GetInt64());
    }

    /// <summary>
    /// #7909: the PG_BAD_ACTOR drill-down shows the block and WAL figures the fact does not carry, and a statement that
    /// ran once in the window reads the same figures there as in <c>get_pg_top_queries</c>: one stored row in the
    /// window, its previous row two hours before it, so both reads difference that row against the row before the window.
    /// </summary>
    [Fact]
    public async Task AStatementThatRanOnceInTheWindow_ReadsTheSameBlocksInItsBadActorDrillDown()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live PG_BAD_ACTOR drill-down parity test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.Postgres);
            await SeedStatsAsync(connection, ct, MinutesAgo(6 * 60), OutOfRangeQueryId, deltaCalls: 100, deltaTimeMs: 1_000, deltaRows: 10_000, walBytes: 1_000, calls: 100, sharedBlksRead: 1_000);
            await SeedStatsAsync(connection, ct, MinutesAgo(30), OutOfRangeQueryId, deltaCalls: 10, deltaTimeMs: 100, deltaRows: 1_000, walBytes: 2_000, calls: 110, sharedBlksRead: 1_100);

            var root = JsonDocument.Parse(await DarlingMcpPgTopQueries(dataSource)).RootElement;
            Assert.False(
                root.TryGetProperty("status", out var status) && status.GetString() == "error",
                $"the read threw instead of executing: {root}");
            var topQueriesRow = Assert.Single(root.GetProperty("queries").EnumerateArray());

            /* The same four-hour window the tool read, through the drill-down for a finding rooted at this statement. */
            var end = DateTime.UtcNow;
            var key = PgTargetFactKeys.BadActorKey(OutOfRangeQueryId);
            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
            };
            var finding = new AnalysisFinding { RootFactKey = key, StoryPath = key, PathKeys = [key], Severity = 1.0 };
            await new PgTargetDrillDownCollector(dataSource).EnrichFindingsAsync([finding], context);
            var detail = JsonSerializer.SerializeToElement(finding.DrillDown!["pg_bad_actor_statements"]).GetProperty(key);

            Assert.Equal(10, detail.GetProperty("calls").GetInt64());
            Assert.Equal(topQueriesRow.GetProperty("calls").GetInt64(), detail.GetProperty("calls").GetInt64());
            Assert.Equal(topQueriesRow.GetProperty("shared_blks_read").GetInt64(), detail.GetProperty("shared_blks_read").GetInt64());
            Assert.Equal(topQueriesRow.GetProperty("shared_blks_hit").GetInt64(), detail.GetProperty("shared_blks_hit").GetInt64());
            Assert.Equal(topQueriesRow.GetProperty("wal_bytes").GetInt64(), detail.GetProperty("wal_bytes").GetInt64());
            Assert.Equal(100, detail.GetProperty("shared_blks_read").GetInt64());
            Assert.Equal(1_000, detail.GetProperty("wal_bytes").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #8054: the page a caller reads, with the statement text looked up per returned row after the ranking and the cap.
    /// Twelve statements with distinct totals, three samples each plus the series' row before the window, text stored for
    /// the odd-numbered ones only, and one of them the service's own: the top eight come back heaviest first with their
    /// differenced figures and their text, a statement with no stored text still ranks and reads null, the service's
    /// statement is flagged, and when it is left out its slot goes to the ninth, with the window total counting every
    /// statement the cap cut.
    /// </summary>
    [Fact]
    public async Task TheTopPage_RanksEveryStatementWithItsFiguresAndItsText_AndTheServiceFilterStillRunsUnderTheCap()
    {
        const int Statements = 12;
        const int ServiceStatement = 11;
        const int Limit = 8;

        var (withService, withoutService) = await ReadOverAsync(
            async (connection, ct) =>
            {
                var t0 = MinutesAgo(30);
                for (var i = 1; i <= Statements; i++)
                {
                    var queryId = OutOfRangeQueryId + i;
                    await SeedStatsAsync(connection, ct, MinutesAgo(6 * 60), queryId, deltaCalls: 100, deltaTimeMs: 1_000, deltaRows: 10_000, walBytes: 1_000, calls: 100, sharedBlksRead: 1_000);
                    for (var k = 1; k <= 3; k++)
                    {
                        await SeedStatsAsync(connection, ct, t0.AddSeconds(60 * (k - 1)), queryId, deltaCalls: 10, deltaTimeMs: 100 * i, deltaRows: 1_000,
                            walBytes: 1_000 + 500 * k, calls: 100 + 10 * k, sharedBlksRead: 1_000 + 100 * k);
                    }

                    if (i % 2 == 1)
                    {
                        await SeedTextAsync(connection, ct, queryId, TextOf(i));
                    }
                }

                await SeedOwnEntriesAsync(connection, ct, OutOfRangeQueryId + ServiceStatement, """[{"u": 10, "d": 16384}]""");
            },
            async (dataSource, ct) =>
            {
                var end = DateTime.UtcNow;
                var start = end.AddHours(-4);
                return (
                    await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, ServerId, start, end, Limit, includeServiceStatements: true, ct),
                    await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, ServerId, start, end, Limit, includeServiceStatements: false, ct));
            });

        /* Heaviest first, the cap honoured, and the window total over all twelve: 300 * (1 + ... + 12). */
        Assert.Equal(new long[] { 12, 11, 10, 9, 8, 7, 6, 5 }, withService.Rows.Select(r => r.QueryId - OutOfRangeQueryId).ToArray());
        Assert.Equal(23_400, withService.WindowTotalExecTimeMs);
        foreach (var row in withService.Rows)
        {
            var i = row.QueryId - OutOfRangeQueryId;
            Assert.Equal(16384, row.DatabaseId);
            Assert.Equal(30, row.Calls);
            Assert.Equal(300 * i, row.TotalExecTimeMs);
            Assert.Equal(3_000, row.RowsReturned);
            /* The seeded differencing: three measured intervals, the first against the row before the window. */
            Assert.Equal(300, row.SharedBlocksRead);
            Assert.Equal(1_500, row.WalBytes);
            Assert.Equal(i % 2 == 1 ? TextOf((int)i) : null, row.QueryText);
            Assert.Equal(i == ServiceStatement, row.IsServiceStatement);
        }

        /* The service's statement leaves before the cap, so the ninth takes its slot; the total drops by its 3,300 ms. */
        Assert.Equal(new long[] { 12, 10, 9, 8, 7, 6, 5, 4 }, withoutService.Rows.Select(r => r.QueryId - OutOfRangeQueryId).ToArray());
        Assert.Equal(20_100, withoutService.WindowTotalExecTimeMs);
        Assert.All(withoutService.Rows, row => Assert.False(row.IsServiceStatement));
        Assert.All(withoutService.Rows, row => Assert.Equal((row.QueryId - OutOfRangeQueryId) % 2 == 1 ? TextOf((int)(row.QueryId - OutOfRangeQueryId)) : null, row.QueryText));
    }

    /// <summary>
    /// #8054: the statement text is read once per returned row, after the ranking and the cap, never once per sample. 120
    /// statements sampled 24 times each inside the window, every one with 6 KB of stored text, and a page of 20: in the
    /// executed plan, the join that brings <c>query_text</c> in emits as many rows as the page holds, not as many as the
    /// window has samples. Read from the plan's shape (a join node that outputs <c>query_text</c> with the text store
    /// beneath it), never from a buffer count or a timing, so it holds on any store and under any join strategy the
    /// planner picks.
    /// </summary>
    [Fact]
    public async Task TheTopPage_ReadsTheStatementTextOncePerReturnedRow_NotOncePerSample()
    {
        const int Statements = 120;
        const int Samples = 24;
        const int Limit = 20;

        var (page, plan) = await ReadOverAsync(
            async (connection, ct) =>
            {
                await SeedStatsSeriesAsync(connection, ct, MinutesAgo(30), Statements, Samples);
                await SeedTextSeriesAsync(connection, ct, Statements);
            },
            async (dataSource, ct) =>
            {
                var end = DateTime.UtcNow;
                var start = end.AddHours(-4);
                var page = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, ServerId, start, end, Limit, includeServiceStatements: true, ct);
                var plan = await ExplainTopQueriesAsync(dataSource, start, end, Limit, ct);
                return (page, plan);
            });

        using (plan)
        {
            Assert.Equal(Limit, page.Rows.Count);
            Assert.All(page.Rows, row => Assert.Equal(6_080, row.QueryText?.Length));

            var root = plan.RootElement[0].GetProperty("Plan");
            var textJoins = new List<JsonElement>();
            CollectTextJoins(root, textJoins);
            Assert.True(textJoins.Count > 0, $"no join in the plan outputs query_text with the text store beneath it: {root}");

            foreach (var join in textJoins)
            {
                var emitted = join.GetProperty("Actual Rows").GetDouble() * join.GetProperty("Actual Loops").GetDouble();
                Assert.True(Math.Abs(emitted - page.Rows.Count) < 0.5,
                    $"the {join.GetProperty("Node Type").GetString()} that joins the statement text emitted {emitted} rows for a page of {page.Rows.Count} over {Statements * Samples} samples: {root}");
            }
        }
    }

    private static string TextOf(int i) => $"SELECT {i} FROM public.widget WHERE id = $1";

    private static readonly Regex QueryTextColumn = new(@"\bquery_text\b", RegexOptions.Compiled);

    private static bool IsJoin(JsonElement node) =>
        node.GetProperty("Node Type").GetString() is "Nested Loop" or "Hash Join" or "Merge Join";

    private static bool ReadsTextStore(JsonElement node) =>
        (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "pg_statement_text")
        || (node.TryGetProperty("Plans", out var children) && children.EnumerateArray().Any(ReadsTextStore));

    private static bool OutputsQueryText(JsonElement node) =>
        node.TryGetProperty("Output", out var output) && output.EnumerateArray().Any(column => QueryTextColumn.IsMatch(column.GetString() ?? ""));

    /// <summary>Every join node that outputs <c>query_text</c> and reads the text store somewhere beneath it: the place the text is attached.</summary>
    private static void CollectTextJoins(JsonElement node, List<JsonElement> found)
    {
        if (IsJoin(node) && OutputsQueryText(node) && ReadsTextStore(node))
        {
            found.Add(node);
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                CollectTextJoins(child, found);
            }
        }
    }

    /// <summary>The executed plan of the read the page came from, bound the way the reader binds it. One process, so each node's row
    /// count is exact rather than a per-worker average; the setting is local to a transaction that is rolled back.</summary>
    private static async Task<JsonDocument> ExplainTopQueriesAsync(NpgsqlDataSource dataSource, DateTime startUtc, DateTime endUtc, int limit, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using (var settings = new NpgsqlCommand("SET LOCAL max_parallel_workers_per_gather = 0", connection, transaction))
        {
            await settings.ExecuteNonQueryAsync(ct);
        }

        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) " + DarlingPgStatementReader.PgTopQueriesSql, connection, transaction);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        command.Parameters.AddWithValue(true);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        await transaction.RollbackAsync(ct);
        return JsonDocument.Parse(json);
    }

    /// <summary>Seeds through <paramref name="seed"/> on a clean store and returns what <paramref name="read"/> reads off it.</summary>
    private static async Task<T> ReadOverAsync<T>(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, CancellationToken, Task<T>> read)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_pg_top_queries page test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.Postgres);
            await seed(connection, ct);

            var result = await read(dataSource, ct);

            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Seeds through <paramref name="seed"/> on a clean store and returns the rows <c>get_pg_top_queries</c> answers for a
    /// stock PostgreSQL target, failing on the error envelope rather than on a missing property.</summary>
    private static async Task<JsonElement[]> TopQueriesOverAsync(Func<NpgsqlConnection, CancellationToken, Task> seed)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_pg_top_queries window test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.Postgres);
            await seed(connection, ct);

            var root = JsonDocument.Parse(await DarlingMcpPgTopQueries(dataSource)).RootElement;
            Assert.False(
                root.TryGetProperty("status", out var status) && status.GetString() == "error",
                $"the read threw instead of executing: {root}");
            var rows = root.GetProperty("queries").EnumerateArray().ToArray();

            bodySucceeded = true;
            return rows;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<string> DarlingMcpPgTopQueries(NpgsqlDataSource dataSource) =>
        await DarlingMcpPgStatementTools.GetPgTopQueries(dataSource, ServerName, 4);

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task SetEngineKindAsync(NpgsqlConnection connection, CancellationToken ct, string kind) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, kind);

    private static async Task SeedStatsAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        long queryId, long deltaCalls, long deltaTimeMs, long deltaRows, long walBytes, long calls = 100, long sharedBlksRead = 5) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned,
     shared_blks_hit, shared_blks_read, storage_blks_read, orcache_blks_hit,
     temp_blks_read, temp_blks_written, wal_bytes, max_exec_peakmem_bytes,
     delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, $5, 16384, 10, TRUE,
        $10, 5000, 91.5, 250,
        10, $11, 3, 2,
        0, 0, $6, 2097152,
        $7, $8, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            queryId, walBytes, deltaCalls, deltaTimeMs, deltaRows, calls, sharedBlksRead);

    private static async Task SeedTextAsync(
        NpgsqlConnection connection, CancellationToken ct, long queryId, string text) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
VALUES ($1, $2, $3, $4, $4)
ON CONFLICT (server_id, queryid) DO UPDATE SET query_text = EXCLUDED.query_text",
            ServerId, queryId, text, DarlingMcpTestData.Naive(DateTime.UtcNow));

    /// <summary>The service-own entry list the hourly text fetch stores for a queryid (#7567), as JSON text.</summary>
    private static async Task SeedOwnEntriesAsync(NpgsqlConnection connection, CancellationToken ct, long queryId, string ownEntriesJson) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE pg_statement_text SET own_entries = $3::jsonb WHERE server_id = $1 AND queryid = $2",
            ServerId, queryId, ownEntriesJson);

    /// <summary>One row per (statement, sample), in one statement: statement i is queryid <see cref="OutOfRangeQueryId"/> + i, its
    /// samples a minute apart back from <paramref name="newestUtc"/>, each a measured ten-call interval of i ms, so the totals rank the
    /// statements by i. The collection ids are drawn from the shared generator so a later row cannot reuse one.</summary>
    private static async Task SeedStatsSeriesAsync(NpgsqlConnection connection, CancellationToken ct, DateTime newestUtc, int statements, int samples)
    {
        long lastCollectionId = 0;
        for (var n = 0; n < statements * samples; n++)
        {
            lastCollectionId = CollectionIdGenerator.Next();
        }

        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned,
     shared_blks_hit, shared_blks_read, storage_blks_read, orcache_blks_hit,
     temp_blks_read, temp_blks_written, wal_bytes, max_exec_peakmem_bytes,
     delta_calls, delta_total_exec_time_ms, delta_rows)
SELECT $1 + (q.i - 1) * $7 + k.i, $2 - make_interval(mins => k.i), $3, $4, $5 + q.i, 16384, 10, TRUE,
       100 + ($7 - k.i) * 10, 5000, 91.5, 250,
       10, 1000 + ($7 - k.i) * 100, 3, 2,
       0, 0, 1000 + ($7 - k.i) * 500, 2097152,
       10, q.i, 1000
FROM generate_series(1, $6) AS q(i)
CROSS JOIN generate_series(0, $7 - 1) AS k(i)",
            lastCollectionId - statements * samples + 1, DarlingMcpTestData.Naive(newestUtc), ServerId, ServerName, OutOfRangeQueryId, statements, samples);
    }

    /// <summary>6,080 characters of text for each of the <paramref name="statements"/> (190 md5 blocks of a per-statement sequence, so
    /// it does not compress): the size at which a stored statement is kept out of line.</summary>
    private static async Task SeedTextSeriesAsync(NpgsqlConnection connection, CancellationToken ct, int statements) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
SELECT $1, $2 + q.i,
       (SELECT string_agg(md5((q.i * 1000 + j)::text), '') FROM generate_series(1, 190) AS j),
       $3, $3
FROM generate_series(1, $4) AS q(i)",
            ServerId, OutOfRangeQueryId, DarlingMcpTestData.Naive(DateTime.UtcNow), statements);

    private static async Task DeleteStatsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM pg_statement_stats WHERE server_id = $1", ServerId);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_text WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
