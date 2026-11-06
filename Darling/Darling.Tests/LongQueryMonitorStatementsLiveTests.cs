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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7416 item 11: on an idle server every long-query completion was one of the monitor's own statements. The read leaves out a row
/// whose stored <c>client_app_name</c> starts with the monitor's program name prefix, keeps a user's application that only contains
/// the words and a row with no program name, and brings them back when asked (<c>include_monitor</c>). Skips without
/// <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class LongQueryMonitorStatementsLiveTests
{
    private const string ServerName = "darling-longquery-monitor-statements-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live long-query monitor-statement test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<DateTime> SeedAsync(string cs, string?[] apps, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        for (var i = 0; i < apps.Length; i++)
        {
            var at = DarlingMcpTestData.Naive(end.AddMinutes(-10 - i));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, client_app_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, at, "rpc_completed", "AppDb", (object?)apps[i] ?? DBNull.Value, (9L - i) * 1_000_000, "EXEC p" + i);
        }

        return end;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM long_query_completions WHERE server_id = {ServerId}; DELETE FROM config.config_collector_schedules WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static string?[] AppsOf(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("completions").EnumerateArray()
            .Select(r => r.TryGetProperty("client_app_name", out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null)
            .ToArray();

    /* Off by default and on when asked, at the reader, the MCP tool and the web dispatch's internal overload. The user rows are a similar
       name (the words inside a longer name), an ordinary application, and a NULL program name. The old read had no program predicate and
       returned all six rows, so the default assertions fail on it. */
    [Fact]
    public async Task TheMonitorsOwnStatements_AreLeftOutByDefault_AndKeptWhenAsked_AUsersSimilarNameStays()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, new string?[]
            {
                "PerformanceMonitorDarling", "PerformanceMonitorLite", "PerformanceMonitorDarling-Remediation",
                "MyPerformanceMonitorTool", "Orders.Api", null,
            }, ct);

            var def = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(postgres, ServerId, end.AddHours(-1), end, 100, cancellationToken: ct);
            Assert.Equal(new string?[] { "MyPerformanceMonitorTool", "Orders.Api", null }, def.Select(r => r.ClientAppName).ToArray());

            var all = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(postgres, ServerId, end.AddHours(-1), end, 100, includeMonitor: true, cancellationToken: ct);
            Assert.Equal(6, all.Count);

            var tool = await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, ServerName, 24, 30, cancellationToken: ct);
            Assert.Equal(new string?[] { "MyPerformanceMonitorTool", "Orders.Api", null }, AppsOf(tool));

            var toolAll = await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, ServerName, 24, 30, include_monitor: true, cancellationToken: ct);
            Assert.Equal(6, AppsOf(toolAll).Length);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* An idle server whose only long completions are the monitor's answers empty, and the empty text says they were left out. */
    [Fact]
    public async Task OnlyMonitorRows_AnswerEmpty_AndTheTextSaysTheyAreLeftOut()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, new string?[] { "PerformanceMonitorDarling" }, ct);

            using var def = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, ServerName, 24, 30, cancellationToken: ct));
            Assert.Equal("empty", def.RootElement.GetProperty("status").GetString());
            Assert.Contains("monitor's own statements are left out", def.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            Assert.Single(AppsOf(await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, ServerName, 24, 30, include_monitor: true, cancellationToken: ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* #7684 item 1: the empty text points at the opt-in switch only when the schedule has the collector off. A server whose own
       schedule row switches it on, with nothing captured in the window, was told to enable it. */
    [Fact]
    public async Task AnEmptyWindow_NamesTheOptInSwitchOnlyWhenTheCollectorIsOff()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, Array.Empty<string?>(), ct);

            foreach (var (enabled, expectSentence) in new[] { (true, false), (false, true) })
            {
                using (var connection = new NpgsqlConnection(cs))
                {
                    await connection.OpenAsync(ct);
                    await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM config.config_collector_schedules WHERE server_id = {ServerId}");
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES ($1,'long_query_completions',1,30,$2)",
                        ServerId, enabled);
                }

                using var doc = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(postgres, ServerName, 24, 30, cancellationToken: ct));
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal(expectSentence, doc.RootElement.GetProperty("message").GetString()!.Contains("opt-in (default OFF)", StringComparison.Ordinal));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
