/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4925: an Azure SQL Database master's daily summary counts only its own blocking and deadlocks. The sibling
/// database <c>GP</c> is monitored as its own target, so its events belong to that target's days. Read through
/// the reader, both MCP tools and the fleet sweep's per-server read. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: every row is planted under dedicated server ids and deleted in cleanup. */
[Collection("live-postgres")]
public sealed class DailySummaryAzureMasterScopeLiveTests
{
    private const string Base = "darling-daily-summary-azure-master";
    private const string AzureHost = "dailyscope.database.windows.net";
    private static readonly int MasterId = ServerIdHelper.GetDeterministicHashCode(Base);
    private static readonly int LoneId = MasterId + 1;
    private static readonly int PlainId = MasterId + 2;
    private static readonly int[] AllIds = { MasterId, LoneId, PlainId };
    private static readonly string[] Separate = { "GP" };
    private static readonly DateTime Day1 = DateTime.UtcNow.Date.AddDays(-3);
    private static readonly DateTime Day2 = Day1.AddDays(1);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Graph(params string[] dbs) =>
        "<deadlock><process-list>" + string.Concat(dbs.Select((d, i) => $"<process id=\"p{i}\" currentdbname=\"{d}\" />")) + "</process-list></deadlock>";

    private sealed record Store(NpgsqlDataSource Postgres, MonitoredServerRegistryState Registry);

    private static async Task WithStoreAsync(Func<Store, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        /* #4981: the data source the code under test reads through is pinned to UTC the way every product store
           connection is (DarlingStoreConnection.PinSessionTimeZoneUtc), so this class does not lean on the test
           run's own pin of the connection string. */
        await using var postgres = NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(cs!));
        var bodySucceeded = false;
        try
        {
            async Task Server(int id, string name, int edition)
            {
                await Exec(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, sql_engine_edition, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, $3, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_engine_edition = $3", ct, id, name, edition);
                await Exec(connection, "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, engine_edition) VALUES ($1,$2,$3,$4,$5)",
                    ct, CollectionIdGenerator.Next(), DateTime.UtcNow.AddMinutes(-30), id, name, edition);
                foreach (var day in new[] { Day1, Day2 })
                    await Exec(connection, "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1,$2,$3,'wait_stats',$4,'SUCCESS')",
                        ct, CollectionIdGenerator.Next(), id, name, day.AddHours(1));
            }
            await Server(MasterId, Base + "-master", 5);
            await Server(LoneId, Base + "-lone", 5);
            await Server(PlainId, Base + "-plain", 3);

            async Task Bpr(int id, string name, DateTime day, int waitMs, string db) =>
                await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,60,70,'suspended',$6)",
                    ct, CollectionIdGenerator.Next(), day.AddHours(12), id, name, waitMs, db);
            async Task Dmv(int id, string name, DateTime day, int waitMs) =>
                await Exec(connection, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,'master',70,60,$5,'suspended')",
                    ct, CollectionIdGenerator.Next(), day.AddHours(13), id, name, waitMs);
            async Task Dead(int id, string name, DateTime day, int minute, string? stamp, string xml) =>
                await Exec(connection, "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,$4,$2,$5,$6)",
                    ct, CollectionIdGenerator.Next(), day.AddHours(14).AddMinutes(minute), id, name, xml, (object?)stamp ?? DBNull.Value);

            var master = Base + "-master";
            /* Day 1: three GP reports waiting 600 s, one of the master's own waiting 2 s; day 2: only GP's, so the
               scoped day falls back to the DMV snapshots. Unequal waits, so a peak taken from the wrong set shows. */
            for (var i = 0; i < 3; i++) await Bpr(MasterId, master, Day1, 600_000, "GP");
            await Bpr(MasterId, master, Day1, 2_000, "master");
            for (var i = 0; i < 2; i++) await Bpr(MasterId, master, Day2, 400_000, "GP");
            await Dmv(MasterId, master, Day2, 7_000);
            await Dmv(MasterId, master, Day2, 3_000);
            /* Day 1 deadlocks: an own-database row; a master-stamped graph wholly in GP; a master-stamped mixed
               graph; a NULL-database graph wholly in GP. */
            await Dead(MasterId, master, Day1, 0, "Other", Graph("Other"));
            await Dead(MasterId, master, Day1, 1, "master", Graph("GP", "GP"));
            await Dead(MasterId, master, Day1, 2, "master", Graph("GP", "HS"));
            await Dead(MasterId, master, Day1, 3, null, Graph("gp"));
            /* The same events on a master with no sibling, and on a SQL Server target, must read as ever. */
            await Bpr(LoneId, Base + "-lone", Day1, 5_000, "master");
            await Bpr(PlainId, Base + "-plain", Day1, 5_000, "GP");
            await Dead(PlainId, Base + "-plain", Day1, 0, "GP", Graph("GP"));

            var state = new MonitoredServerRegistryState();
            state.Publish(new List<MonitoredServer>
            {
                new() { Name = "m", Host = AzureHost, Database = "master", StoredServerId = MasterId },
                new() { Name = "g", Host = AzureHost, Database = "GP", StoredServerId = MasterId + 100 },
                new() { Name = "l", Host = "lone.database.windows.net", Database = "master", StoredServerId = LoneId },
                new() { Name = "p", Host = "plain.example.test", Database = "master", StoredServerId = PlainId },
            });

            DarlingHealthReader.ResetRangeCacheForTests();
            await body(new Store(postgres, state), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<List<DarlingHealthReader.DailySummaryReadRow>> RangeAsync(
        NpgsqlDataSource postgres, int id, IReadOnlyList<string>? list, CancellationToken ct)
        => (await DarlingHealthReader.GetDailySummaryRangeAsync(
            postgres, id, Day1, Day2.AddDays(1), separatelyMonitored: list, logger: NullLogger.Instance, cancellationToken: ct)).Rows;

    [Fact]
    public async Task TheReader_CountsOnlyTheMastersOwnEvents_AndTheBandFollows() => await WithStoreAsync(async (s, ct) =>
    {
        var unscoped = await RangeAsync(s.Postgres, MasterId, null, ct);
        var scoped = await RangeAsync(s.Postgres, MasterId, Separate, ct);

        Assert.Equal(4, unscoped[0].DeadlockCount);
        Assert.Equal(4, unscoped[0].BlockingEvents);
        Assert.Equal(600_000, unscoped[0].MaxBlockDurationMs);
        Assert.Equal(2, unscoped[1].BlockingEvents);

        Assert.Equal(2, scoped[0].DeadlockCount);
        Assert.Equal(1, scoped[0].BlockingEvents);
        Assert.Equal(2_000, scoped[0].MaxBlockDurationMs);
        Assert.Equal(2, scoped[1].BlockingEvents);       // GP's reports drop out, the DMV fallback stands
        Assert.Equal(7_000, scoped[1].MaxBlockDurationMs);
        Assert.NotEqual(unscoped[0].HealthBand, scoped[0].HealthBand);
        Assert.Equal(DailyHealthBand.Healthy, scoped[0].HealthBand);

        var single = await DarlingHealthReader.GetDailySummaryAsync(s.Postgres, MasterId, Day1, Separate, NullLogger.Instance, ct);
        Assert.Equal(2, single.DeadlockCount);
        Assert.Equal(1, single.BlockingEvents);
    });

    [Fact]
    public async Task ChangingTheList_ChangesTheCounts_AndALonelyMasterAndSqlServerReadAsEver() => await WithStoreAsync(async (s, ct) =>
    {
        Assert.Equal(1, (await RangeAsync(s.Postgres, MasterId, Separate, ct))[0].BlockingEvents);
        Assert.Equal(4, (await RangeAsync(s.Postgres, MasterId, new[] { "nothing" }, ct))[0].BlockingEvents);
        Assert.Equal(1, (await RangeAsync(s.Postgres, MasterId, Separate, ct))[0].BlockingEvents);

        foreach (var id in new[] { LoneId, PlainId })
        {
            var plain = await RangeAsync(s.Postgres, id, null, ct);
            DarlingHealthReader.ResetRangeCacheForTests();
            var viaTool = await RangeAsync(s.Postgres, id, null, ct);
            static (long, long, long, long) Shape(DarlingHealthReader.DailySummaryReadRow r) => (r.DeadlockCount, r.BlockingEvents, r.MaxBlockDurationMs, r.CollectionRuns);
            Assert.Equal(plain.Select(Shape), viaTool.Select(Shape));
        }

        var json = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummaryRange(
            s.Postgres, Base + "-plain", 5, null, s.Registry, NullLogger.Instance, ct));
        Assert.Equal(1, json.RootElement.GetProperty("days")[0].GetProperty("deadlock_count").GetInt32());
    });

    [Fact]
    public async Task BothMcpTools_AndTheSweepRead_AreScoped_AndAFailedLookupReadsUnscoped() => await WithStoreAsync(async (s, ct) =>
    {
        var name = Base + "-master";
        var range = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummaryRange(s.Postgres, name, 5, null, s.Registry, NullLogger.Instance, ct));
        var day1 = range.RootElement.GetProperty("days").EnumerateArray().Single(d => d.GetProperty("summary_date").GetString() == Day1.ToString("yyyy-MM-dd"));
        Assert.Equal(2, day1.GetProperty("deadlock_count").GetInt32());
        Assert.Equal(1, day1.GetProperty("blocking_events").GetInt32());

        var one = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummary(s.Postgres, name, Day1.ToString("yyyy-MM-dd"), s.Registry, NullLogger.Instance, ct));
        Assert.Equal(2, one.RootElement.GetProperty("deadlock_count").GetInt32());

        DarlingHealthReader.ResetRangeCacheForTests();
        var noRegistry = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummary(s.Postgres, name, Day1.ToString("yyyy-MM-dd"), null, null, ct));
        Assert.Equal(4, noRegistry.RootElement.GetProperty("deadlock_count").GetInt32());

        var span = await FleetSweepEngine.ReadServerSignalsAsync(s.Postgres, MasterId, name, Day1, Day2.AddDays(1), Separate, NullLogger.Instance, ct);
        Assert.Null(span.ReadFault);
        Assert.Equal(3, span.Signals.BlockingEvents);     // day 1: its own report; day 2: the two DMV snapshots
    });

    [Fact]
    public async Task AScopedFailure_ReadsUnscopedRows_WithoutAFault() => await WithStoreAsync(async (s, ct) =>
    {
        /* A null name makes the graph pass throw inside the scoped path; the read must fall back, not fail. */
        var scoped = await RangeAsync(s.Postgres, MasterId, new string[] { null! }, ct);
        var unscoped = await RangeAsync(s.Postgres, MasterId, null, ct);
        Assert.Equal(unscoped[0].DeadlockCount, scoped[0].DeadlockCount);
        Assert.Equal(unscoped[0].BlockingEvents, scoped[0].BlockingEvents);

        var resolved = await DarlingMcpHealthTools.ResolveSeparatelyMonitoredAsync(
            NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Timeout=1"), MasterId, s.Registry, NullLogger.Instance, ct);
        Assert.Null(resolved);
    });

    /* #4981: a DateTime is bound as naive UTC (Kind Unspecified, a plain timestamp). A Kind=Utc value goes out as
       timestamptz, which the server turns back into a naive value in the SESSION's time zone, so seeds written this
       way only held where the session happened to be UTC. */
    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p) cmd.Parameters.AddWithValue(v is DateTime d ? DateTime.SpecifyKind(d, DateTimeKind.Unspecified) : v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", AllIds);
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id IN ({ids}); " +
            $"DELETE FROM dmv_blocking_snapshots WHERE server_id IN ({ids}); " +
            $"DELETE FROM deadlocks WHERE server_id IN ({ids}); " +
            $"DELETE FROM collection_log WHERE server_id IN ({ids}); " +
            $"DELETE FROM server_properties WHERE server_id IN ({ids}); " +
            $"DELETE FROM servers WHERE server_id IN ({ids});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
