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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The fleet overview counts an Azure SQL Database <c>master</c> target's blocking and deadlock events once: the
/// events of a database that is monitored as its own target belong to that target's card, so the master's card
/// skips them (the same rule the analysis and the alert sweep apply). Gated on DARLING_TEST_PG; read through the
/// product's own <see cref="DarlingFleetReader.GetFleetOverviewAsync"/> with the registry resolver the web and MCP
/// hosts pass.
/// </summary>
/* #1776 own-store: every row is planted under dedicated server ids and deleted in cleanup. */
[Collection("live-postgres")]
public sealed class FleetOverviewAzureMasterScopeLiveTests
{
    private const string Base = "darling-fleet-azure-master-scope";
    private const string AzureHost = "fleetscope.database.windows.net";
    private const string LoneHost = "fleetlone.database.windows.net";
    private static readonly int MasterId = ServerIdHelper.GetDeterministicHashCode(Base);
    private static readonly int GpId = MasterId + 1;
    private static readonly int PlainId = MasterId + 2;
    private static readonly int LoneId = MasterId + 3;
    private static readonly int[] AllIds = { MasterId, GpId, PlainId, LoneId };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Graph(string db) =>
        $"<deadlock><process-list><process id=\"p0\" currentdbname=\"{db}\" /></process-list></deadlock>";

    [Fact]
    public async Task MasterCard_CountsItsOwnEventsOnce_AndTheHeaderFollows()
    {
        var scoped = await RunAsync((postgres, registry, now, ct) => FleetAsync(postgres, registry, now, ct));
        var master = Assert.Single(scoped.Cards, c => c.ServerId == MasterId);
        Assert.Equal(3, master.BlockingCount);
        Assert.Equal(1, master.DeadlockCount);

        var gp = Assert.Single(scoped.Cards, c => c.ServerId == GpId);
        Assert.Equal(3, gp.BlockingCount);
        Assert.Equal(1, gp.DeadlockCount);

        /* Not Azure, and an Azure master with no separately monitored sibling: unchanged. */
        Assert.Equal(2, Assert.Single(scoped.Cards, c => c.ServerId == PlainId).BlockingCount);
        Assert.Equal(2, Assert.Single(scoped.Cards, c => c.ServerId == LoneId).BlockingCount);

        /* Each event once: the header is the sum of the cards, and the master added its three, not its six. */
        Assert.Equal(scoped.Cards.Sum(c => (long)c.BlockingCount), scoped.TotalBlockingEvents);
        Assert.Equal(scoped.Cards.Sum(c => (long)c.DeadlockCount), scoped.TotalDeadlocks);
        Assert.Equal(3 + 3 + 2 + 2, scoped.Cards.Where(c => AllIds.Contains(c.ServerId)).Sum(c => c.BlockingCount));
    }

    /// <summary>
    /// The max wait follows the count: master's own events top out at 12 s, a report for the separately monitored
    /// database waited 50 s, and the master's card reports the 12 s, not the 50 s. Unscoped it would be the 50 s.
    /// </summary>
    [Fact]
    public async Task MasterCard_MaxWait_IsTheLargestWaitOfItsOwnDatabases()
    {
        var scoped = await RunAsync((postgres, registry, now, ct) => FleetAsync(postgres, registry, now, ct));
        Assert.Equal(12000, Assert.Single(scoped.Cards, c => c.ServerId == MasterId).MaxBlockingWaitMs);

        var plain = await RunAsync((postgres, registry, now, ct) => FleetAsync(postgres, null, now, ct));
        Assert.Equal(LargerWaitMs, Assert.Single(plain.Cards, c => c.ServerId == MasterId).MaxBlockingWaitMs);
    }

    /// <summary>
    /// A resolver that throws leaves the master on its unscoped counts and the whole read succeeds: every card is
    /// still there. The scope is a refinement; losing it must not blank the fleet.
    /// </summary>
    [Fact]
    public async Task AResolverThatThrows_KeepsTheUnscopedCounts_AndEveryCard()
    {
        var result = await RunAsync(async (postgres, registry, now, ct) =>
            await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-1), now, now, cancellationToken: ct,
                separatelyMonitored: (id, token) => id == MasterId
                    ? throw new InvalidOperationException("the registry read failed")
                    : DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(id, registry, postgres, token)));

        var master = Assert.Single(result.Cards, c => c.ServerId == MasterId);
        Assert.Equal(6, master.BlockingCount);
        Assert.Equal(2, master.DeadlockCount);
        Assert.Equal(LargerWaitMs, master.MaxBlockingWaitMs);
        foreach (var id in AllIds) Assert.Single(result.Cards, c => c.ServerId == id);

        /* A server that did resolve is still scoped: only the failed lookup falls back. */
        Assert.Equal(3, Assert.Single(result.Cards, c => c.ServerId == GpId).BlockingCount);
    }

    /// <summary>
    /// A resolver that succeeds but a scoped read that throws (the database list holds a NUL, which PostgreSQL
    /// refuses in text) keeps the unscoped row the same way, and logs the server.
    /// </summary>
    [Fact]
    public async Task AScopedReadThatThrows_KeepsTheUnscopedCounts_AndEveryCard()
    {
        var log = new List<string>();
        var result = await RunAsync(async (postgres, registry, now, ct) =>
            await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-1), now, now, cancellationToken: ct,
                separatelyMonitored: (id, token) => Task.FromResult<IReadOnlyList<string>?>(id == MasterId ? new[] { "GP\0" } : null),
                logger: new ListLogger(log)));

        var master = Assert.Single(result.Cards, c => c.ServerId == MasterId);
        Assert.Equal(6, master.BlockingCount);
        Assert.Equal(2, master.DeadlockCount);
        foreach (var id in AllIds) Assert.Single(result.Cards, c => c.ServerId == id);
        Assert.Contains(log, line => line.Contains(MasterId.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal));
    }

    /// <summary>
    /// get_server_summary counts a master's last hour the way its fleet card does (3 blocking, 1 deadlock), leaves
    /// a plain server and a lone master alone, and falls back to the unscoped 6 / 2 when the lookup throws.
    /// </summary>
    [Fact]
    public async Task ServerSummary_CountsAMasterOnce_LikeItsFleetCard()
    {
        var (card, summaries) = await RunAsync(async (postgres, registry, now, ct) =>
        {
            var fleet = await FleetAsync(postgres, registry, now, ct);
            Func<int, CancellationToken, Task<IReadOnlyList<string>?>> resolver =
                (id, token) => DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(id, registry, postgres, token);
            var s = new Dictionary<string, DarlingHealthReader.ServerSummaryReadResult>
            {
                ["master"] = await DarlingHealthReader.GetServerSummaryAsync(postgres, MasterId, resolver, null, ct),
                ["gp"] = await DarlingHealthReader.GetServerSummaryAsync(postgres, GpId, resolver, null, ct),
                ["plain"] = await DarlingHealthReader.GetServerSummaryAsync(postgres, PlainId, resolver, null, ct),
                ["lone"] = await DarlingHealthReader.GetServerSummaryAsync(postgres, LoneId, resolver, null, ct),
                ["none"] = await DarlingHealthReader.GetServerSummaryAsync(postgres, MasterId, null, null, ct),
                ["throws"] = await DarlingHealthReader.GetServerSummaryAsync(
                    postgres, MasterId, (id, token) => throw new InvalidOperationException("the registry read failed"), null, ct),
            };
            return (fleet.Cards.Single(c => c.ServerId == MasterId), s);
        });

        Assert.Equal(card.BlockingCount, summaries["master"].BlockingCount);
        Assert.Equal(card.DeadlockCount, summaries["master"].DeadlockCount);
        Assert.Equal(3, summaries["master"].BlockingCount);
        Assert.Equal(1, summaries["master"].DeadlockCount);
        Assert.Equal(3, summaries["gp"].BlockingCount);
        Assert.Equal(1, summaries["gp"].DeadlockCount);
        Assert.Equal(2, summaries["plain"].BlockingCount);
        Assert.Equal(2, summaries["lone"].BlockingCount);
        Assert.Equal(6, summaries["none"].BlockingCount);
        Assert.Equal(2, summaries["none"].DeadlockCount);
        Assert.Equal(6, summaries["throws"].BlockingCount);
        Assert.Equal(2, summaries["throws"].DeadlockCount);
    }

    /// <summary>The tool resolves the live registry (like get_fleet_overview) and the read-dispatch mirror hands it over.</summary>
    [Fact]
    public void GetServerSummary_ResolvesTheRegistry()
    {
        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHealthTools.cs");
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");

        Assert.Contains("separatelyMonitored: registryState is null", tool, StringComparison.Ordinal);
        Assert.Contains("DarlingMcpHealthTools.GetServerSummary(pg, Server(c), registryState, logger, c.RequestAborted)", web, StringComparison.Ordinal);
    }

    private static Task<FleetOverviewResult> FleetAsync(
        NpgsqlDataSource postgres, MonitoredServerRegistryState.Snapshot? registry, DateTime now, CancellationToken ct) =>
        DarlingFleetReader.GetFleetOverviewAsync(
            postgres, now.AddHours(-1), now, now, cancellationToken: ct,
            separatelyMonitored: registry is null
                ? null
                : (id, token) => DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(id, registry, postgres, token));

    /// <summary>
    /// Every caller of the fleet overview passes the registry: /api/fleet, the MCP tool, and the /api/read mirror
    /// (through BuildReadDispatch's registry seat), so all three scope an Azure master's counts the same way.
    /// </summary>
    [Fact]
    public void EveryFleetOverviewCaller_PassesTheRegistry()
    {
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        var tool = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFleetTools.cs");

        Assert.Contains("Str(c, \"band\"), registryState: registryState, logger: logger, cancellationToken: c.RequestAborted)", web, StringComparison.Ordinal);
        Assert.Contains("BuildReadDispatch(logger, postgresConfig, registryState)", web, StringComparison.Ordinal);
        Assert.Contains("separatelyMonitored: registryState is null", tool, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WithoutAResolver_TheCountsAreTheOldOnes()
    {
        var plain = await RunAsync((postgres, registry, now, ct) => FleetAsync(postgres, null, now, ct));
        var master = Assert.Single(plain.Cards, c => c.ServerId == MasterId);
        Assert.Equal(6, master.BlockingCount);
        Assert.Equal(2, master.DeadlockCount);
    }

    private const long LargerWaitMs = 50000;

    private static async Task<T> RunAsync<T>(
        Func<NpgsqlDataSource, MonitoredServerRegistryState.Snapshot?, DateTime, CancellationToken, Task<T>> body)
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
            async Task Server(int id, string name, int edition)
            {
                await Exec(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, sql_engine_edition, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, $3, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_engine_edition = $3", ct, id, name, edition);
                await Exec(connection, "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, engine_edition) VALUES ($1,$2,$3,$4,$5)",
                    ct, CollectionIdGenerator.Next(), DateTime.UtcNow.AddMinutes(-30), id, name, edition);
            }
            await Server(MasterId, Base + "-master", 5);
            await Server(GpId, Base + "-gp", 5);
            await Server(PlainId, Base + "-plain", 3);
            await Server(LoneId, Base + "-lone", 5);

            var at = DateTime.UtcNow.AddMinutes(-20);
            var seq = 0;
            async Task Bpr(int id, string name, string? db, long waitMs = 12000) =>
                await Exec(connection, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocking_status, blocked_spid, database_name) VALUES ($1,$2,$3,$4,$2,$7,60,'suspended',$5,$6)",
                    ct, CollectionIdGenerator.Next(), at.AddSeconds(seq), id, name, 70 + seq++, (object?)db ?? DBNull.Value, waitMs);
            async Task Dead(int id, string name, string db) =>
                await Exec(connection, "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,$4,$2,$5,$6)",
                    ct, CollectionIdGenerator.Next(), at.AddSeconds(seq++), id, name, Graph(db), db);

            /* The first GP report waited longer than any of master's own: a scoped max wait must not see it. */
            await Bpr(MasterId, Base + "-master", "GP", LargerWaitMs);
            foreach (var db in new[] { "master", "master", "GP", "GP", null }) await Bpr(MasterId, Base + "-master", db);
            foreach (var db in new[] { "GP", "GP", "GP" }) await Bpr(GpId, Base + "-gp", db);
            foreach (var db in new[] { "x", "y" }) await Bpr(PlainId, Base + "-plain", db);
            foreach (var db in new[] { "master", "master" }) await Bpr(LoneId, Base + "-lone", db);
            await Dead(MasterId, Base + "-master", "GP");
            await Dead(MasterId, Base + "-master", "Other");
            await Dead(GpId, Base + "-gp", "GP");

            var state = new MonitoredServerRegistryState();
            state.Publish(new List<MonitoredServer>
            {
                new() { Name = "m", Host = AzureHost, Database = "master", StoredServerId = MasterId },
                new() { Name = "g", Host = AzureHost, Database = "GP", StoredServerId = GpId },
                new() { Name = "p", Host = "plain.example.test", Database = "master", StoredServerId = PlainId },
                new() { Name = "l", Host = LoneHost, Database = "master", StoredServerId = LoneId }
            });
            var registry = state.Read();

            var result = await body(postgres, registry, DateTime.UtcNow, ct);
            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Collects each formatted message, so a test can assert what was logged.</summary>
    private sealed class ListLogger(List<string> lines) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            lines.Add(logLevel + ": " + formatter(state, exception));
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p) cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", AllIds);
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id IN ({ids}); " +
            $"DELETE FROM deadlocks WHERE server_id IN ({ids}); " +
            $"DELETE FROM server_properties WHERE server_id IN ({ids}); " +
            $"DELETE FROM servers WHERE server_id IN ({ids});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
