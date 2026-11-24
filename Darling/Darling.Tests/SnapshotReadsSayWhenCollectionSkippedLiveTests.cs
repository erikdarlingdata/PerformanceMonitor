/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9339: a THREADPOOL episode starves the collector's own connection, so the snapshot reads came back empty for it and read as
/// "nothing was waiting". An empty answer for a server that is online now says so when the window holds skipped slots or the
/// snapshot collector's failed runs. An offline server keeps its offline answer, and a gap outside the window adds nothing.
/// </summary>
[Collection("live-postgres")]
public sealed class SnapshotReadsSayWhenCollectionSkippedLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string NameFor(string family, string flavour) => "sql9339-" + flavour + "-" + family.Replace('_', '-');

    public static TheoryData<string> Families() => new() { "get_waiting_tasks", "get_active_queries", "get_load_by_caller" };

    private static Task<string> ReadAsync(string family, NpgsqlDataSource ds, string name) => family switch
    {
        "get_waiting_tasks" => DarlingMcpSessionTools.GetWaitingTasks(ds, name),
        "get_active_queries" => DarlingMcpSessionTools.GetActiveQueries(ds, name),
        "get_load_by_caller" => DarlingMcpWhoDroveItTools.GetLoadByCaller(ds, name),
        _ => throw new ArgumentOutOfRangeException(nameof(family), family, null),
    };

    private static string CollectorOf(string family) => family == "get_waiting_tasks" ? "waiting_tasks" : "query_snapshots";

    private static async Task RunAsync(string name, DateTime registered, Func<NpgsqlConnection, NpgsqlDataSource, int, DateTime, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live snapshot gap tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        async Task CleanAsync(NpgsqlConnection c, CancellationToken token)
        {
            await DarlingMcpTestData.ExecAsync(c, token, "DELETE FROM collection_log WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(c, token, "DELETE FROM servers WHERE server_id = $1", serverId);
        }

        await CleanAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)", serverId, name, DarlingMcpTestData.Naive(registered));
            await using var postgres = NpgsqlDataSource.Create(cs!);
            await body(connection, postgres, serverId, DateTime.UtcNow, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, CleanAsync);
        }
    }

    private static Task LogAsync(NpgsqlConnection c, int serverId, string name, string collector, string status, DateTime at, int? fanout, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, fanout_item_count)
VALUES ($1, $2, $3, $4, $5, $6, 10, 0, $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, name, collector, status, (object?)fanout ?? DBNull.Value);

    /// <summary>Two skipped-slot rows (105 slots) and three failed runs of the snapshot collector inside the one-hour window, and a
    /// successful run a minute ago, so the server is online now.</summary>
    private static async Task SeedGapsAsync(NpgsqlConnection c, int serverId, string name, string collector, DateTime now, CancellationToken ct)
    {
        await LogAsync(c, serverId, name, collector, "SUCCESS", now.AddMinutes(-1), null, ct);
        await LogAsync(c, serverId, name, CollectionCycleRow.CollectorName, CollectionCycleRow.Status, now.AddMinutes(-40), 60, ct);
        await LogAsync(c, serverId, name, CollectionCycleRow.CollectorName, CollectionCycleRow.Status, now.AddMinutes(-30), 45, ct);
        foreach (var minutes in new[] { 35, 33, 31 })
        {
            await LogAsync(c, serverId, name, collector, "ERROR", now.AddMinutes(-minutes), null, ct);
        }
    }

    [Theory]
    [MemberData(nameof(Families))]
    public Task EmptyAnswer_NamesSkippedSlotsAndFailedRuns_InTheWindow(string family)
    {
        var name = NameFor(family, "gap");
        return RunAsync(name, DateTime.UtcNow.AddDays(-40), async (c, ds, serverId, now, ct) =>
        {
            await SeedGapsAsync(c, serverId, name, CollectorOf(family), now, ct);

            var answer = JsonNode.Parse(await ReadAsync(family, ds, name))!;

            Assert.Equal("empty", (string?)answer["status"]);
            var gap = answer["hints"]![CollectionGapNote.HintKey]!;
            Assert.Equal(105, (long)gap["skipped_slots"]!);
            Assert.Equal(2, (long)gap["skipped_records"]!);
            Assert.Equal(3, (long)gap["failed_runs"]!);
            Assert.Equal(CollectorOf(family), (string?)gap["collector"]);
            Assert.NotNull(gap["skipped_first"]);
            Assert.NotNull(gap["failed_last"]);
            Assert.Contains("105 collection slots were skipped", (string?)answer["message"], StringComparison.Ordinal);
            Assert.Contains("3 runs of the " + CollectorOf(family) + " collector did not succeed", (string?)answer["message"], StringComparison.Ordinal);
        });
    }

    [Theory]
    [MemberData(nameof(Families))]
    public Task EmptyAnswer_IgnoresGapsOutsideTheWindow(string family)
    {
        var name = NameFor(family, "outside");
        return RunAsync(name, DateTime.UtcNow.AddDays(-40), async (c, ds, serverId, now, ct) =>
        {
            await LogAsync(c, serverId, name, CollectorOf(family), "SUCCESS", now.AddMinutes(-1), null, ct);
            await LogAsync(c, serverId, name, CollectionCycleRow.CollectorName, CollectionCycleRow.Status, now.AddHours(-5), 60, ct);
            await LogAsync(c, serverId, name, CollectorOf(family), "ERROR", now.AddHours(-5), null, ct);
            /* A failed run of ANOTHER collector inside the window is not this collector's gap. */
            await LogAsync(c, serverId, name, "wait_stats", "ERROR", now.AddMinutes(-20), null, ct);

            var answer = JsonNode.Parse(await ReadAsync(family, ds, name))!;

            Assert.Equal("empty", (string?)answer["status"]);
            Assert.Null(answer["hints"]?[CollectionGapNote.HintKey]);
            Assert.DoesNotContain("did not run cleanly", (string?)answer["message"], StringComparison.Ordinal);
        });
    }

    [Theory]
    [MemberData(nameof(Families))]
    public Task EmptyAnswer_CountsASkippedRunAsHealthy_AndAWarningRunAsFailed(string family)
    {
        var name = NameFor(family, "skiprun");
        return RunAsync(name, DateTime.UtcNow.AddDays(-40), async (c, ds, serverId, now, ct) =>
        {
            await LogAsync(c, serverId, name, CollectorOf(family), "SUCCESS", now.AddMinutes(-1), null, ct);
            /* SKIPPED is healthy everywhere else (EnumeratedCollectorDriver.FreshnessSuccessStatuses), so two of them are no failed runs. */
            await LogAsync(c, serverId, name, CollectorOf(family), "SKIPPED", now.AddMinutes(-20), null, ct);
            await LogAsync(c, serverId, name, CollectorOf(family), "SKIPPED", now.AddMinutes(-10), null, ct);

            var healthy = JsonNode.Parse(await ReadAsync(family, ds, name))!;

            Assert.Equal("empty", (string?)healthy["status"]);
            Assert.Null(healthy["hints"]?[CollectionGapNote.HintKey]);
            Assert.DoesNotContain("did not succeed", (string?)healthy["message"], StringComparison.Ordinal);

            /* A WARNING run partly failed, so it still counts. */
            await LogAsync(c, serverId, name, CollectorOf(family), "WARNING", now.AddMinutes(-5), null, ct);

            var warned = JsonNode.Parse(await ReadAsync(family, ds, name))!;

            Assert.Equal(1, (long)warned["hints"]![CollectionGapNote.HintKey]!["failed_runs"]!);
        });
    }

    [Theory]
    [MemberData(nameof(Families))]
    public Task OfflineServer_KeepsItsOfflineAnswer(string family)
    {
        var name = NameFor(family, "offline");
        return RunAsync(name, DateTime.UtcNow.AddDays(-40), async (c, ds, serverId, now, ct) =>
        {
            /* The newest successful run is twelve days old: offline. Skipped slots in the window do not change that answer. */
            await LogAsync(c, serverId, name, CollectorOf(family), "SUCCESS", now.AddDays(-12), null, ct);
            await LogAsync(c, serverId, name, CollectionCycleRow.CollectorName, CollectionCycleRow.Status, now.AddMinutes(-30), 60, ct);

            var answer = JsonNode.Parse(await ReadAsync(family, ds, name))!;

            Assert.Equal("unavailable", (string?)answer["status"]);
            Assert.Null(answer["hints"]?[CollectionGapNote.HintKey]);
        });
    }
}
