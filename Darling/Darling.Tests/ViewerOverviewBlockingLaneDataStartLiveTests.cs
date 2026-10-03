/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Overview's blocking chart against a real store (#4966): the chart's two coverage probes and its two trend reads, run the way the
/// control runs them, then the chart's own choice (<c>ViewerBlockingLaneDataStart.ChooseAsync</c>). Own-store: this reaches
/// DARLING_TEST_PG only to create and drop its own database.
/// </summary>
public sealed class ViewerOverviewBlockingLaneDataStartLiveTests
{
    private const int LaterDeadlocksServerId = -496701;
    private const int EarlyBarServerId = -496702;
    private const int OneSeriesServerId = -496703;
    private const int NeitherServerId = -496704;

    /* Added 2 days ago, its blocking rows start then and its deadlocks reach 3 days back (the first collection stores history): the blocking
       series starts later, so the chart names it, true of both series. */
    [Fact]
    public async Task TheLane_NamesTheLaterOfTheTwoSeriesStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var start = await store.LaneStartAsync(LaterDeadlocksServerId, ct);

        Assert.Equal(store.End.AddDays(-2), start);
    }

    /* Blocking rows reach 4 days back and deadlocks 3, both before the server's first collection: each series starts at its earliest bar, and the
       chart names the later, the deadlocks'. */
    [Fact]
    public async Task ABarBeforeTheCoverage_MovesItsSeriesBack_ButTheLaterSeriesIsStillNamed_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        Assert.Equal(store.End.AddDays(-3), await store.LaneStartAsync(EarlyBarServerId, ct));
    }

    /* Added 3 days ago with no blocking and no deadlock row: only the deadlocks collector has run, so the chart names that series' start (the first collection). A server with neither collector names nothing. */
    [Fact]
    public async Task OneSeriesAnswering_IsNamed_AndNeitherNamesNothing_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        Assert.Equal(store.End.AddDays(-3), await store.LaneStartAsync(OneSeriesServerId, ct));
        Assert.Null(await store.LaneStartAsync(NeitherServerId, ct));
    }

    /* A probe fault costs the note and never the bars: the deadlocks table is renamed so its probe and its trend read fail, and the
       blocking series still answers. */
    [Fact]
    public async Task AFailingProbe_CostsOnlyItsSeries_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        await store.RenameDeadlocksAsync(ct);

        Assert.Equal(store.End.AddDays(-2), await store.LaneStartAsync(LaterDeadlocksServerId, ct, deadlockBars: false));
    }

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private Store(ScratchPostgres scratch, ViewerDataService viewer, DateTime end)
        {
            _scratch = scratch;
            Viewer = viewer;
            End = end;
        }

        public ViewerDataService Viewer { get; }

        public DateTime End { get; }

        public DateTime Start => End.AddDays(-7);

        /// <summary>The chart's choice over the viewer's real reads, in the order the control makes them: the trends, then the two probes.</summary>
        public async Task<DateTime?> LaneStartAsync(int serverId, CancellationToken ct, bool deadlockBars = true)
        {
            var blocking = await Viewer.GetBlockingTrendAsync(serverId, Start, End);
            var deadlocks = deadlockBars ? await Viewer.GetDeadlockTrendAsync(serverId, Start, End) : [];
            return await ViewerBlockingLaneDataStart.ChooseAsync(
                Viewer.GetBlockedProcessReportsDataStartAsync(serverId, Start, End, ct),
                Viewer.GetDeadlocksDataStartAsync(serverId, Start, End, ct),
                blocking.Select(b => b.Time),
                deadlocks.Select(d => d.Time));
        }

        public async Task RenameDeadlocksAsync(CancellationToken ct)
        {
            await using var connection = new NpgsqlConnection(_scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using var rename = new NpgsqlCommand("ALTER TABLE collect.deadlocks RENAME TO deadlocks_renamed", connection);
            await rename.ExecuteNonQueryAsync(ct);
        }

        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var scratch = await QueryGridSeed.OpenScratchAsync(ct);
            try
            {
                var end = QueryGridSeed.NowToTheMinute();
                var deadlockCollector = DataWindowFloor.Source.ForCollectorTable("deadlocks").CollectorName!;
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    /* The coverage of both series starts at the server's first collection, or at an earlier row in the window. */
                    await EventGridSeed.AddServerAsync(connection, LaterDeadlocksServerId, "bl-later", end.AddDays(-2), deadlockCollector, end, clockOffsetMinutes: null, ct);
                    await InsertBlockingAsync(connection, LaterDeadlocksServerId, "bl-later", end.AddDays(-2), end, ct);
                    await InsertDeadlockAsync(connection, LaterDeadlocksServerId, "bl-later", end.AddDays(-3), end, ct);

                    await EventGridSeed.AddServerAsync(connection, EarlyBarServerId, "bl-early", end.AddDays(-2), deadlockCollector, end, clockOffsetMinutes: null, ct);
                    await InsertBlockingAsync(connection, EarlyBarServerId, "bl-early", end.AddDays(-4), end, ct);
                    await InsertDeadlockAsync(connection, EarlyBarServerId, "bl-early", end.AddDays(-3), end, ct);

                    await EventGridSeed.AddServerAsync(connection, OneSeriesServerId, "bl-one", end.AddDays(-3), deadlockCollector, end, clockOffsetMinutes: null, ct);

                    await EventGridSeed.AddServerAsync(connection, NeitherServerId, "bl-neither", end.AddDays(-3), "wait_stats", end, clockOffsetMinutes: null, ct);
                }

                return new Store(scratch, new ViewerDataService(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        private static async Task InsertBlockingAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.blocked_process_reports
                    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms)
                SELECT row_number() OVER () + $5, t, $1, $2, t, 'LaneDb', 55, 56, 1000
                FROM generate_series($3::timestamp, $4::timestamp, interval '1 hour') AS t
                """, connection);
            AddRange(insert, serverId, serverName, firstUtc, lastUtc);
            await insert.ExecuteNonQueryAsync(ct);
        }

        private static async Task InsertDeadlockAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.deadlocks
                    (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
                SELECT row_number() OVER () + $5, t, $1, $2, t, 'process1', 'SELECT 1', '<deadlock />'
                FROM generate_series($3::timestamp, $4::timestamp, interval '1 hour') AS t
                """, connection);
            AddRange(insert, serverId, serverName, firstUtc, lastUtc);
            await insert.ExecuteNonQueryAsync(ct);
        }

        private static void AddRange(NpgsqlCommand insert, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc)
        {
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(firstUtc));
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(lastUtc));
            insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 100_000L);
        }

        public async ValueTask DisposeAsync()
        {
            await Viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
