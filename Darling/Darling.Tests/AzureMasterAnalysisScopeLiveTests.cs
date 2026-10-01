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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An Azure SQL Database <c>master</c> target's blocking and deadlock findings skip the databases that are
/// monitored as their own targets (<see cref="AnalysisContext.SeparatelyMonitoredDatabases"/>). Gated on
/// DARLING_TEST_PG; the facts and both anomaly spikes are read through the product's own collector and
/// detector, on both the blocked-process arm and the DMV fallback arm.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class AzureMasterAnalysisScopeLiveTests
{
    private const string ServerName = "darling-azure-master-scope";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly string[] Separate = { "GP" };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Graph(params string[] dbs) =>
        "<deadlock><process-list>" +
        string.Concat(dbs.Select((d, i) => $"<process id=\"p{i}\" currentdbname=\"{d}\" />")) +
        "</process-list></deadlock>";

    private sealed record Plan(bool Bpr, string?[] BlockingDbs, string[][] Deadlocks);

    private static async Task<(List<Fact> Facts, List<Fact> Anomalies)> RunAsync(
        Plan plan, IReadOnlyList<string>? separate, bool anomalies, string? drillFact = null, Action<AnalysisFinding>? drilled = null)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await Exec(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);

            var end = new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Unspecified).AddMinutes(-1);
            var start = end.AddHours(-4);
            await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'OLD',1,1,0)",
                ct, CollectionIdGenerator.Next(), end.AddDays(-3), ServerId, ServerName);
            for (var m = 0; m <= 240; m += 15)
                await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'CXPACKET',1,10,0)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(m), ServerId, ServerName);

            for (var i = 0; i < plan.BlockingDbs.Length; i++)
            {
                var at = start.AddMinutes(10 + i);
                if (plan.Bpr)
                    await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,12000,60,$5,'suspended',$6)",
                        ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, 70 + i, (object?)plan.BlockingDbs[i] ?? DBNull.Value);
                else
                    await Exec(connection, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,$5,$6,60,12000,'suspended')",
                        ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, (object?)plan.BlockingDbs[i] ?? DBNull.Value, 70 + i);
            }
            for (var i = 0; i < plan.Deadlocks.Length; i++)
                await Exec(connection, "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,$4,$2,$5,$6)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(20 + i), ServerId, ServerName, Graph(plan.Deadlocks[i]), plan.Deadlocks[i].Length > 0 ? plan.Deadlocks[i][0] : DBNull.Value);

            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = start, TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, SeparatelyMonitoredDatabases = separate, CancellationToken = ct
            };
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
            var spikes = new List<Fact>();
            if (anomalies)
                spikes = await new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres)).DetectAnomaliesAsync(context);
            if (drillFact is not null)
            {
                var finding = new AnalysisFinding { RootFactKey = drillFact, StoryPath = drillFact, PathKeys = [drillFact], Severity = 1.0 };
                await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
                drilled?.Invoke(finding);
            }
            bodySucceeded = true;
            return (facts, spikes);
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* BLOCKING_EVENTS reads blocked_process_reports only; the DMV fallback arm exists in the anomaly read. */
    [Fact]
    public async Task BlockingEvents_SkipSeparatelyMonitoredDatabases()
    {
        var plan = new Plan(true, new string?[] { "GP", "gp", "HS", null, "GP" }, Array.Empty<string[]>());
        var (facts, _) = await RunAsync(plan, Separate, false);
        Assert.Equal(2.0, Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
    }

    [Fact]
    public async Task BlockingEvents_NullOrEmptyList_CountsEverything()
    {
        var plan = new Plan(true, new string?[] { "GP", "HS", null }, Array.Empty<string[]>());
        foreach (var list in new IReadOnlyList<string>?[] { null, Array.Empty<string>() })
        {
            var (facts, _) = await RunAsync(plan, list, false);
            Assert.Equal(3.0, Assert.Single(facts, f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"]);
        }
    }

    [Fact]
    public async Task Deadlocks_AllSeparateIsSkipped_MixedStillCounts()
    {
        var plan = new Plan(true, Array.Empty<string?>(), new[] { new[] { "GP", "GP" }, new[] { "GP", "HS" }, new[] { "gp" } });
        var (facts, _) = await RunAsync(plan, Separate, false);
        Assert.Equal(1.0, Assert.Single(facts, f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
        var (all, _) = await RunAsync(plan, null, false);
        Assert.Equal(3.0, Assert.Single(all, f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
    }

    /* The victim database (the first process) is named and not separately monitored: such a deadlock counts
       without its graph being read, and a victim database that IS separately monitored still goes to the graph. */
    [Fact]
    public async Task Deadlocks_VictimOutsideTheList_CountsWithoutTheGraph_AndSpikeAgrees()
    {
        var plan = new Plan(true, Array.Empty<string?>(), new[] { new[] { "HS", "GP" }, new[] { "GP", "HS" }, new[] { "GP" }, new[] { "hs" }, new[] { "HS" } });
        var (facts, spikes) = await RunAsync(plan, Separate, true);
        Assert.Equal(4.0, Assert.Single(facts, f => f.Key == "DEADLOCKS").Metadata["deadlock_count"]);
        Assert.Contains(spikes, f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
    }

    /* BLOCKING_CHAIN builds from the same pairs: a pair in a separately monitored database is left out. */
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BlockingChain_SkipsSeparatelyMonitoredDatabases(bool bpr)
    {
        var plan = new Plan(bpr, new string?[] { "GP", "gp", "GP" }, Array.Empty<string[]>());
        var (control, _) = await RunAsync(plan, null, false);
        Assert.Contains(control, f => f.Key == "BLOCKING_CHAIN");
        var (filtered, _) = await RunAsync(plan, Separate, false);
        Assert.DoesNotContain(filtered, f => f.Key == "BLOCKING_CHAIN");
        var (mixed, _) = await RunAsync(new Plan(bpr, new string?[] { "GP", "HS", null }, Array.Empty<string[]>()), Separate, false);
        Assert.Contains(mixed, f => f.Key == "BLOCKING_CHAIN");
    }

    /* The service's list (or its per-call resolver) reaches the facts it collects; an explicit list wins. */
    [Fact]
    public async Task Service_ListAndResolver_ReachCollectAndScoreFacts()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await Exec(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);
            var end = new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Unspecified).AddMinutes(-1);
            var start = end.AddHours(-4);
            for (var m = 0; m <= 240; m += 15)
                await Exec(connection, "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms) VALUES ($1,$2,$3,$4,'CXPACKET',1,10,0)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(m), ServerId, ServerName);
            foreach (var (db, i) in new[] { "GP", "GP", "HS" }.Select((d, i) => (d, i)))
                await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,12000,60,$5,'suspended',$6)",
                    ct, CollectionIdGenerator.Next(), start.AddMinutes(10 + i), ServerId, ServerName, 70 + i, db);

            async Task<double> CountAsync(DarlingAnalysisService service)
            {
                var (facts, _, _) = await service.CollectAndScoreFactsAsync(ServerId, ServerName, 4, end, ct);
                return facts.Single(f => f.Key == "BLOCKING_EVENTS").Metadata["event_count"];
            }

            Assert.Equal(3.0, await CountAsync(new DarlingAnalysisService(postgres)));
            Assert.Equal(1.0, await CountAsync(new DarlingAnalysisService(postgres) { SeparatelyMonitoredDatabases = Separate }));
            var asked = new List<int>();
            Assert.Equal(1.0, await CountAsync(new DarlingAnalysisService(postgres) { SeparatelyMonitoredResolver = (id, _) => { asked.Add(id); return Task.FromResult<IReadOnlyList<string>?>(Separate); } }));
            Assert.Equal(new[] { ServerId }, asked.Distinct());
            Assert.Equal(3.0, await CountAsync(new DarlingAnalysisService(postgres)
            { SeparatelyMonitoredDatabases = new[] { "none" }, SeparatelyMonitoredResolver = (_, _) => Task.FromResult<IReadOnlyList<string>?>(Separate) }));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* Both RunAnalysisPassAsync callers pass the list, and the service reads it per call. */
    [Fact]
    public void RunAnalysisPassCallers_PassTheList()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var calls = System.Text.RegularExpressions.Regex.Matches(worker, @"await RunAnalysisPassAsync\((?<args>[^;]*?)\);", System.Text.RegularExpressions.RegexOptions.Singleline);
        Assert.Equal(2, calls.Count);
        foreach (System.Text.RegularExpressions.Match call in calls)
            Assert.Contains("AnalysisSeparatelyMonitoredDatabases(", call.Groups["args"].Value, StringComparison.Ordinal);
        Assert.Contains("SeparatelyMonitoredDatabases = separatelyMonitoredDatabases", worker, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BlockingSpike_MadeOnlyOfSeparateDatabases_GivesNoSpike(bool bpr)
    {
        var plan = new Plan(bpr, new string?[] { "GP", "GP", "GP", "GP", "GP", "GP" }, Array.Empty<string[]>());
        var (_, filtered) = await RunAsync(plan, Separate, true);
        Assert.DoesNotContain(filtered, f => f.Key == "ANOMALY_BLOCKING_SPIKE");
        var (_, control) = await RunAsync(plan, null, true);
        Assert.Contains(control, f => f.Key == "ANOMALY_BLOCKING_SPIKE");
    }

    [Fact]
    public async Task DeadlockSpike_MadeOnlyOfSeparateDatabases_GivesNoSpike()
    {
        var plan = new Plan(true, Array.Empty<string?>(), new[] { new[] { "GP" }, new[] { "GP", "GP" }, new[] { "GP" } });
        var (_, filtered) = await RunAsync(plan, Separate, true);
        Assert.DoesNotContain(filtered, f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
        var (_, control) = await RunAsync(plan, null, true);
        Assert.Contains(control, f => f.Key == "ANOMALY_DEADLOCK_SPIKE");
    }

    /// <summary>The worker's fill: the list for an Azure master runtime, null or empty otherwise.</summary>
    [Fact]
    public void WorkerFill_GivesTheListForAnAzureMaster_AndNothingElse()
    {
        var method = typeof(DarlingWorker).GetMethod("AnalysisSeparatelyMonitoredDatabases",
            BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public,
            new[] { typeof(bool), typeof(string), typeof(string), typeof(string), typeof(IReadOnlyList<MonitoredServer>) });
        Assert.NotNull(method);
        var live = new List<MonitoredServer>
        {
            new() { Name = "m", Host = "srv.database.windows.net", Database = "master", StoredServerId = 1 },
            new() { Name = "g", Host = "srv.database.windows.net", Database = "GP", StoredServerId = 2 },
        };
        IReadOnlyList<string>? Call(bool azure, int id) => (IReadOnlyList<string>?)method!.Invoke(null,
            new object?[] { azure, id.ToString(CultureInfo.InvariantCulture), "srv.database.windows.net", "master", live });
        Assert.Equal(new[] { "GP" }, Call(true, 1));
        Assert.True(Call(false, 1) is null || Call(false, 1)!.Count == 0);
        Assert.True(Call(true, 2) is null || Call(true, 2)!.Count == 0);
    }

    private static bool Drilled(Plan plan, IReadOnlyList<string>? separate, string fact, string key, out object? value)
    {
        object? captured = null;
        var found = false;
        RunAsync(plan, separate, false, fact, f => found = f.DrillDown is not null && f.DrillDown.TryGetValue(key, out captured)).GetAwaiter().GetResult();
        value = captured;
        return found;
    }

    /* The reconstructed-chains evidence leaves out the pairs of separately monitored databases, as the fact does. */
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ReconstructedChains_SkipSeparatelyMonitoredDatabases(bool bpr)
    {
        Assert.True(Drilled(new Plan(bpr, new string?[] { "GP", "gp" }, Array.Empty<string[]>()), null, "BLOCKING_CHAIN", "reconstructed_blocking_chains", out _));
        Assert.False(Drilled(new Plan(bpr, new string?[] { "GP", "gp" }, Array.Empty<string[]>()), Separate, "BLOCKING_CHAIN", "reconstructed_blocking_chains", out _));
        Assert.True(Drilled(new Plan(bpr, new string?[] { "GP", "HS" }, Array.Empty<string[]>()), Separate, "BLOCKING_CHAIN", "reconstructed_blocking_chains", out _));
    }

    /* top_blocking_chains: a GP-only event is not in master's evidence; an HS event is. */
    [Fact]
    public void TopBlockingChains_SkipSeparatelyMonitoredDatabases()
    {
        Assert.False(Drilled(new Plan(true, new string?[] { "GP" }, Array.Empty<string[]>()), Separate, "BLOCKING_EVENTS", "top_blocking_chains", out _));
        Assert.True(Drilled(new Plan(true, new string?[] { "GP", "HS" }, Array.Empty<string[]>()), Separate, "BLOCKING_EVENTS", "top_blocking_chains", out var kept));
        Assert.Single((System.Collections.IEnumerable)kept!.GetType().GetMethod("ToArray")!.Invoke(kept, null)!);
    }

    /* top_deadlocks: a GP-only deadlock is not in master's evidence; an HS one is. */
    [Fact]
    public void TopDeadlocks_SkipSeparatelyMonitoredDatabases()
    {
        Assert.True(Drilled(new Plan(true, Array.Empty<string?>(), new[] { new[] { "GP", "GP" } }), null, "DEADLOCKS", "top_deadlocks", out _));
        Assert.False(Drilled(new Plan(true, Array.Empty<string?>(), new[] { new[] { "GP", "GP" } }), Separate, "DEADLOCKS", "top_deadlocks", out _));
        Assert.True(Drilled(new Plan(true, Array.Empty<string?>(), new[] { new[] { "GP", "GP" }, new[] { "HS", "GP" } }), Separate, "DEADLOCKS", "top_deadlocks", out _));
    }

    /// <summary>The registry-only fill (the MCP and web hosts) reads the STORED engine edition: 5 gets the list
    /// whatever the host spelling, a managed instance (8), a NULL edition and no row do not, and the newest row wins.</summary>
    [Fact]
    public async Task RegistryFill_UsesTheStoredEngineEdition()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            const string privateHost = "srv.privatelink.database.windows.net";
            const string zoneHost = "x.zone.database.windows.net";
            var now = DateTime.UtcNow;
            var servers = new List<MonitoredServer>();
            async Task Seed(int offset, string host, params int?[] editionsOldestFirst)
            {
                var id = ServerId + offset;
                servers.Add(new() { Name = "m" + offset, Host = host, Database = "master", StoredServerId = id });
                servers.Add(new() { Name = "g" + offset, Host = host, Database = "GP", StoredServerId = id + 1000 });
                for (var i = 0; i < editionsOldestFirst.Length; i++)
                    await Exec(connection, "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, engine_edition) VALUES ($1,$2,$3,$4,$5)",
                        ct, CollectionIdGenerator.Next(), now.AddMinutes(i - 10), id, ServerName, (object?)editionsOldestFirst[i] ?? DBNull.Value);
            }
            await Seed(1, privateHost, 5);
            await Seed(2, zoneHost, 8);
            await Seed(3, "srv.database.windows.net");
            await Seed(4, "srv.database.windows.net", new int?[] { null });
            await Seed(5, "srv.database.windows.net", 8, 5);
            await Seed(6, "srv.database.windows.net", 5, 8);
            var state = new PerformanceMonitor.Darling.Service.Mcp.MonitoredServerRegistryState();
            state.Publish(servers);
            var registry = state.Read();
            Task<IReadOnlyList<string>?> Ask(int offset) => DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(ServerId + offset, registry, postgres, ct);
            Assert.Equal(new[] { "GP" }, await Ask(1));
            Assert.Null(await Ask(2));
            Assert.Null(await Ask(3));
            Assert.Null(await Ask(4));
            Assert.Equal(new[] { "GP" }, await Ask(5));
            Assert.Null(await Ask(6));
            Assert.Null(await DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(ServerId + 999, registry, postgres, ct));
            Assert.Null(await DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(ServerId + 1, null, postgres, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p) cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM dmv_blocking_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM deadlocks WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM server_properties WHERE server_id BETWEEN {ServerId} AND {ServerId} + 6; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
