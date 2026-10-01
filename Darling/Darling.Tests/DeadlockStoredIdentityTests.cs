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
using System.Reflection;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// On an Azure SQL Database registered at master, a database's own ring-buffer item and the server's
/// telemetry read can both return one deadlock. The exact identity (server, microsecond time, full graph
/// text) lets the write skip a copy the store already holds, and never drops a row that merely resembles
/// one. The last fact writes through the collector runner's own batch write, with no manual step standing in
/// for the host.
/// </summary>
[Collection("live-postgres")]
public sealed class DeadlockStoredIdentityTests
{
    private static readonly DateTime T = new(2026, 9, 30, 12, 0, 0, DateTimeKind.Unspecified);
    private const string Graph = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list></deadlock>";

    private static DeadlocksCollector.Row Row(DateTime? time, string? graph, string? database = "GP", string? plan = null) => new()
    {
        DeadlockTime = time,
        GraphXml = graph,
        DatabaseName = database,
        VictimQueryPlanXml = plan,
    };

    private static HashSet<(DateTime Time, string Graph)> Stored(params (DateTime, string)[] identities) =>
        new(identities, new IdentityComparer());

    /* The host loads the stored identities into an ordinal set; the pure drop must hold with the default
       tuple comparer too, which compares the string ordinally. */
    private sealed class IdentityComparer : IEqualityComparer<(DateTime Time, string Graph)>
    {
        public bool Equals((DateTime Time, string Graph) x, (DateTime Time, string Graph) y) =>
            x.Time == y.Time && string.Equals(x.Graph, y.Graph, StringComparison.Ordinal);

        public int GetHashCode((DateTime Time, string Graph) obj) => HashCode.Combine(obj.Time, obj.Graph);
    }

    private static List<DeadlocksCollector.Row> Drop(List<DeadlocksCollector.Row> rows, HashSet<(DateTime Time, string Graph)> stored) =>
        DeadlocksCollector.Instance.DropAlreadyStored(rows, stored);

    [Fact]
    public void ATelemetryCopyOfAStoredRingBufferRow_IsDropped()
    {
        var stored = Stored((T, Graph));
        var telemetry = Row(T, Graph, "GP", plan: null);

        Assert.Empty(Drop(new List<DeadlocksCollector.Row> { telemetry }, stored));
    }

    [Fact]
    public void ABlobOnlyRow_IsKept()
    {
        var stored = Stored((T, Graph));
        var other = Row(T.AddMinutes(1), Graph.Replace("process1", "process9", StringComparison.Ordinal), "HS");

        Assert.Single(Drop(new List<DeadlocksCollector.Row> { other }, stored));
    }

    [Fact]
    public void ARingBufferOnlyRow_IsKept()
    {
        var kept = Drop(new List<DeadlocksCollector.Row> { Row(T, Graph, "GP", plan: "<ShowPlanXML/>") }, Stored());

        Assert.Single(kept);
    }

    [Fact]
    public void TheSameTimeWithAGraphDifferingByOneAttribute_IsKept()
    {
        var stored = Stored((T, Graph));
        var near = Row(T, Graph.Replace("process1", "process2", StringComparison.Ordinal));

        Assert.Single(Drop(new List<DeadlocksCollector.Row> { near }, stored));
    }

    [Fact]
    public void ANullOrEmptyGraphOrANullTime_IsNeverDropped()
    {
        var stored = Stored((T, Graph), (T, ""));
        var rows = new List<DeadlocksCollector.Row>
        {
            Row(T, null),
            Row(T, ""),
            Row(null, Graph),
            Row(T, null),
        };

        Assert.Equal(4, Drop(rows, stored).Count);
        Assert.Null(DeadlocksCollector.Instance.GetIdentity(rows[0]));
        Assert.Null(DeadlocksCollector.Instance.GetIdentity(rows[1]));
        Assert.Null(DeadlocksCollector.Instance.GetIdentity(rows[2]));
    }

    [Fact]
    public void AnInBatchExactRepeat_KeepsOnlyTheFirst_AndAnInBatchNearMissKeepsBoth()
    {
        var first = Row(T, Graph, "master");
        var repeat = Row(T, Graph, "master", plan: "<ShowPlanXML/>");
        var near = Row(T, Graph.Replace("process1", "process2", StringComparison.Ordinal), "master");

        var kept = Drop(new List<DeadlocksCollector.Row> { first, repeat, near }, Stored());

        Assert.Equal(2, kept.Count);
        Assert.Same(first, kept[0]);
        Assert.Same(near, kept[1]);
    }

    [Fact]
    public void ATimeAt100NanosecondsMatchesTheStoredMicrosecond()
    {
        var stored = Stored((T.AddTicks(10), Graph));
        var telemetry = Row(T.AddTicks(10).AddTicks(7), Graph);

        Assert.Empty(Drop(new List<DeadlocksCollector.Row> { telemetry }, stored));
        Assert.Equal(T.AddTicks(10), DeadlocksCollector.Instance.GetIdentity(telemetry)!.Value.Time);
    }

    /* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches
       another test's store. */
    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    [Fact]
    public async Task TheRunnersWrite_StoresAGraphBothReadsReturnOnce_AndStillStoresADifferentOne()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the deadlock identity write pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "dl-identity", Host = "dl-identity" },
            ConnectionString = "Server=dl-identity",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "dl-identity",
            ServerId = -483_200,
            EngineEdition = 5,
        };

        var bodySucceeded = false;
        try
        {
            var deadlockTime = DateTime.UtcNow.AddMinutes(-3);
            deadlockTime = DateTime.SpecifyKind(deadlockTime.AddTicks(-(deadlockTime.Ticks % 10)), DateTimeKind.Unspecified);

            async Task WriteAsync(DateTime collectionTime, params DeadlocksCollector.Row[] rows)
            {
                var context = new CollectorContext
                {
                    ServerId = server.ServerId,
                    ServerName = server.StorageName,
                    CollectionTime = collectionTime,
                    Deltas = new CollectorDeltaCalculator(),
                    Target = server.Target,
                };
                await (Task)WriteBatchMethod.MakeGenericMethod(typeof(DeadlocksCollector.Row)).Invoke(
                    runner,
                    new object?[] { connection, DeadlocksCollector.Instance, rows.ToList(), server, collectionTime, context, ct })!;
            }

            var collected = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await WriteAsync(collected, Row(deadlockTime, Graph, "GP", plan: "<ShowPlanXML/>"));
            await WriteAsync(collected.AddMinutes(5),
                Row(deadlockTime.AddTicks(7), Graph, "GP"),
                Row(deadlockTime.AddSeconds(1), Graph.Replace("process1", "process9", StringComparison.Ordinal), "HS"));

            await using var count = new NpgsqlCommand(
                "SELECT database_name FROM deadlocks WHERE server_id = @id ORDER BY database_name", connection);
            count.Parameters.AddWithValue("id", server.ServerId);
            var stored = new List<string>();
            await using (var reader = await count.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    stored.Add(reader.GetString(0));
                }
            }

            Assert.Equal(new[] { "GP", "HS" }, stored);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM deadlocks WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
