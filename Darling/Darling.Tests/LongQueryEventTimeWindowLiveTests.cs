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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7900: get_long_query_completions picks its events by when they HAPPENED (<c>event_time</c>), not by when the collector
/// stored them (<c>collection_time</c>), as get_blocking and get_deadlocks do. The collector stores an event minutes after it
/// happens, so on a narrow window placed on an incident a completion that happened inside the window but was stored after its
/// end is in the window, and one that happened before the window but was stored inside it is not. Skips without
/// <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class LongQueryEventTimeWindowLiveTests
{
    private const string ServerName = "darling-longquery-event-time-window-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live long-query event-time window test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheWindowIsOnTheEventsOwnTime_ALateStoredEventLists_AnEarlierEventStoredInsideDoesNot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* A one-hour window that ended two hours ago, so a row stored after its end is still in the past. */
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-2));
            var start = end.AddHours(-1);

            /* Happened five minutes before the window's end, stored three minutes after it: in the window. */
            await SeedAsync(connection, ct, eventTime: end.AddMinutes(-5), collectionTime: end.AddMinutes(3), seconds: 9, text: "late_stored");
            /* Happened five minutes before the window began, stored two minutes after it began: not in the window. */
            await SeedAsync(connection, ct, eventTime: start.AddMinutes(-5), collectionTime: start.AddMinutes(2), seconds: 8, text: "early_event");
            /* Happened and stored inside the window. */
            await SeedAsync(connection, ct, eventTime: start.AddMinutes(30), collectionTime: start.AddMinutes(31), seconds: 7, text: "inside");

            var rows = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(postgres, ServerId, start, end, 100, cancellationToken: ct);
            Assert.Equal(new[] { "late_stored", "inside" }, rows.Select(r => r.StatementText).ToArray());

            /* The tool, one hour back from as_of at the window's end: the same two events, by their own time. */
            using var tool = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(
                postgres, ServerName, 1, 30, as_of: WebDataStartNote.FormatWindowEnd(DateTime.SpecifyKind(end, DateTimeKind.Utc)), cancellationToken: ct));
            var listed = tool.RootElement.GetProperty("completions").EnumerateArray()
                .Select(r => r.GetProperty("event_time").GetString())
                .ToArray();
            Assert.Equal(
                new[] { Naive(end.AddMinutes(-5)).ToString("o"), Naive(start.AddMinutes(30)).ToString("o") },
                listed);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime Naive(DateTime value) => DarlingMcpTestData.Naive(value);

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, DateTime eventTime, DateTime collectionTime, int seconds, string text) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            CollectionIdGenerator.Next(), Naive(collectionTime), ServerId, ServerName, Naive(eventTime), "rpc_completed", "AppDb", seconds * 1_000_000L, text);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM long_query_completions WHERE server_id = {ServerId}; DELETE FROM config.config_collector_schedules WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
