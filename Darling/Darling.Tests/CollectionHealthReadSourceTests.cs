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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6254: collection health shows where the Query Store collector's newest run read its rows from (<c>read_source</c>),
/// and only when it has one. With NULL everywhere, which is every server until an operator sets the Query Store replica,
/// the tool's reply, the Viewer's Note column and the web table are exactly what they were.
/// </summary>
public sealed class CollectionHealthReadSourceTests
{
    private const string Replica = "replica 12, primary 2 (redo_behind: Sales)";

    [Fact]
    public void TheToolReply_WithNoReadSource_IsReturnedAsItIs()
    {
        var reply = """{"server":"s","collectors":[{"collector":"query_store","status":"HEALTHY"}]}""";
        Assert.Same(reply, McpHelpers.WithCollectorReadSource(reply, new Dictionary<string, string>()));
    }

    [Fact]
    public void TheToolReply_PutsTheReadSourceOnTheCollectorThatHasOne_AndNoKeyOnTheOthers()
    {
        var reply = """{"server":"s","collectors":[{"collector":"wait_stats","status":"HEALTHY"},{"collector":"query_store","status":"HEALTHY"}]}""";
        var patched = JsonDocument.Parse(McpHelpers.WithCollectorReadSource(reply, new Dictionary<string, string> { ["query_store"] = Replica })).RootElement;
        var rows = patched.GetProperty("collectors").EnumerateArray().ToList();
        Assert.False(rows[0].TryGetProperty("read_source", out _));
        Assert.Equal(Replica, rows[1].GetProperty("read_source").GetString());
        Assert.Equal("s", patched.GetProperty("server").GetString());
    }

    [Fact]
    public void TheViewerNote_IsUnchangedWithNoReadSource_AndCarriesItWhenTheQueryStoreRowHasOne()
    {
        var queryStore = new CollectorHealthRow { CollectorName = "query_store", TotalRuns = 10, SuccessCount = 10 };
        var waitStats = new CollectorHealthRow { CollectorName = "wait_stats", TotalRuns = 10, SuccessCount = 10 };
        var rows = new List<CollectorHealthRow> { queryStore, waitStats };
        var before = rows.Select(r => r.NoteFormatted).ToList();

        ViewerDataService.ApplyReadSource(rows, null);
        ViewerDataService.ApplyReadSource(rows, "   ");
        Assert.Equal(before, rows.Select(r => r.NoteFormatted).ToList());

        ViewerDataService.ApplyReadSource(rows, Replica);
        Assert.Equal("read source: " + Replica, queryStore.NoteFormatted);
        Assert.Equal(before[1], waitStats.NoteFormatted);
    }

    [Fact]
    public void TheNote_JoinsTheReadSourceToAnExistingNote()
    {
        var plain = CollectorHealthClassifier.FormatCollectionNote("a note", 1, 1);
        Assert.Equal(plain + "; read source: replica 3",
            CollectorHealthClassifier.FormatCollectionNote("a note", 1, 1, readSource: "replica 3"));
        Assert.Equal(CollectorHealthClassifier.FormatCollectionNote("a note", 1, 1),
            CollectorHealthClassifier.FormatCollectionNote("a note", 1, 1, readSource: null));
    }
}

/// <summary>The same, end to end against a store: the tool reads the newest Query Store run's <c>read_source</c>.</summary>
[Collection("live-postgres")]
public sealed class CollectionHealthReadSourceLiveTests
{
    private const string ServerName = "collection-health-read-source-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task CleanAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }

    private static async Task InsertRunAsync(
        NpgsqlConnection connection, string collector, DateTime at, string? readSource, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, read_source)
VALUES ($1,$2,$3,$4,$5,'SUCCESS',100,5,$6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue((object?)readSource ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live collection health read source tests.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(cs!);
            using (var register = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection))
            {
                register.Parameters.AddWithValue(ServerId);
                register.Parameters.AddWithValue(ServerName);
                register.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
                await register.ExecuteNonQueryAsync(ct);
            }

            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, CleanAsync);
        }
    }

    private static async Task<JsonElement> HealthAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    private static JsonElement Row(JsonElement health, string collector) =>
        health.GetProperty("collectors").EnumerateArray().Single(r => r.GetProperty("collector").GetString() == collector);

    /// <summary>The newest run's source is the one shown, on the Query Store row only.</summary>
    [Fact]
    public Task Health_ShowsTheNewestQueryStoreRunsReadSource_OnThatRowOnly() => RunAsync(async (connection, postgres, ct) =>
    {
        var now = DateTime.UtcNow;
        await InsertRunAsync(connection, "query_store", now.AddMinutes(-20), "replica 3, primary 1", ct);
        await InsertRunAsync(connection, "query_store", now.AddMinutes(-5), "replica 12, primary 2", ct);
        await InsertRunAsync(connection, "wait_stats", now.AddMinutes(-2), null, ct);

        var health = await HealthAsync(postgres, ct);
        Assert.Equal("replica 12, primary 2", Row(health, "query_store").GetProperty("read_source").GetString());
        Assert.False(Row(health, "wait_stats").TryGetProperty("read_source", out _));
    });

    /// <summary>A server whose setting was turned off stops showing a source as soon as its runs stop writing one.</summary>
    [Fact]
    public Task Health_NewestRunWithNoSource_ShowsNoSource() => RunAsync(async (connection, postgres, ct) =>
    {
        var now = DateTime.UtcNow;
        await InsertRunAsync(connection, "query_store", now.AddMinutes(-20), "replica 3, primary 1", ct);
        await InsertRunAsync(connection, "query_store", now.AddMinutes(-5), null, ct);

        var health = await HealthAsync(postgres, ct);
        Assert.False(Row(health, "query_store").TryGetProperty("read_source", out _));
    });

    /// <summary>With NULL on every run, which is every server with the setting off, the reply names no read_source anywhere.</summary>
    [Fact]
    public Task Health_WithNoReadSourceAnywhere_CarriesNoReadSourceKey() => RunAsync(async (connection, postgres, ct) =>
    {
        await InsertRunAsync(connection, "query_store", DateTime.UtcNow.AddMinutes(-5), null, ct);
        await InsertRunAsync(connection, "wait_stats", DateTime.UtcNow.AddMinutes(-2), null, ct);

        var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct);
        Assert.DoesNotContain("read_source", json, StringComparison.Ordinal);
    });
}
