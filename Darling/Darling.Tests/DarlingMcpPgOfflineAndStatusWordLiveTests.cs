/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #6130: the PostgreSQL reads answer with the five status words the MCP instructions define, say first that a server is offline
/// (the rule <c>list_servers</c> uses) with its last collection time, and name the server by its display name.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPgOfflineAndStatusWordLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] Tables = ["pg_blocking_edges", "pg_wait_stats", "pg_session_states"];

    private static int Id(string name) => ServerIdHelper.GetDeterministicHashCode(name);

    private static Task SetDisplayNameAsync(NpgsqlConnection c, string name, string display) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, "UPDATE servers SET display_name = $2 WHERE server_id = $1", Id(name), display);

    /// <summary>A target last collected ten days ago answers unavailable, and the first words say it is offline since the last
    /// collection time, before any advice to check the collector. The answer names the server by its display name.</summary>
    [Theory]
    [InlineData("get_pg_blocking")]
    [InlineData("get_pg_wait_stats")]
    [InlineData("get_pg_session_states")]
    public async Task AnUnavailableAnswer_FromAServerNotCollectedForDays_SaysSoFirst_WithTheLastCollectionTime_AgainstDevPostgres(string read)
    {
        const string Name = "pg6130-offline.example:appdb:pg";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_blocking"], [Name], Tables, async (c, ds, now) =>
        {
            var lastCollection = now.AddDays(-10);
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "pg_blocking", lastCollection.AddHours(-3), 30, lastCollection, Tables, TestContext.Current.CancellationToken);
            await SetDisplayNameAsync(c, Name, "Orders PG");

            var answer = read switch
            {
                "get_pg_blocking" => await DarlingMcpPgBlockingTools.GetPgBlocking(ds, Name, 1),
                "get_pg_wait_stats" => await DarlingMcpPgWaitTools.GetPgWaitStats(ds, Name, 1),
                _ => await DarlingMcpPgSessionStatesTools.GetPgSessionStates(ds, Name, 1),
            };
            var root = WindowFloorLiveHarness.Parse(answer);

            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            var message = root.GetProperty("message").GetString()!;
            var expectedLead = "Orders PG is offline: the store has had no collection from it since "
                + lastCollection.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC";
            Assert.StartsWith(expectedLead, message, StringComparison.Ordinal);
            Assert.DoesNotContain(Name, message, StringComparison.Ordinal);
        });
    }

    /// <summary>The same read against a server collected a minute ago says nothing about being offline: it is the collector advice only.</summary>
    [Fact]
    public async Task AnUnavailableAnswer_FromAServerCollectedJustNow_DoesNotClaimItIsOffline_AgainstDevPostgres()
    {
        const string Name = "pg6130-fresh.example:appdb:pg";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_wait_stats"], [Name], Tables, async (c, ds, now) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "pg_wait_stats", DateTime.UtcNow.AddMinutes(-10), 1, DateTime.UtcNow.AddMinutes(-1), Tables, TestContext.Current.CancellationToken);

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgBlockingTools.GetPgBlocking(ds, Name, 1));

            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            Assert.DoesNotContain("offline", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
            Assert.Contains("get_collection_health", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
        });
    }

    /// <summary>Captures exist and none held a chain: a true negative, so <c>empty</c>, with the detail in the finding, and the server
    /// named by its display name rather than the connection key.</summary>
    [Fact]
    public async Task CapturesWithNoBlocking_AnswerEmpty_NamingTheServerByItsDisplayName_AgainstDevPostgres()
    {
        const string Name = "pg6130-quiet.example:appdb:pg";
        await WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_blocking"], [Name], Tables, async (c, ds, now) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(
                c, Name, now.AddDays(-40), "pg_blocking", now.AddHours(-3), 1, DateTime.UtcNow.AddMinutes(-1), Tables, TestContext.Current.CancellationToken);
            await SetDisplayNameAsync(c, Name, "Orders PG");

            var root = WindowFloorLiveHarness.Parse(await DarlingMcpPgBlockingTools.GetPgBlocking(ds, Name, 6));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Equal("Orders PG", root.GetProperty("server").GetString());
            Assert.True(root.GetProperty("captures_total").GetInt32() > 0);
            Assert.Equal(0, root.GetProperty("captures_with_blocking").GetInt32());
            Assert.Contains("No blocking was present in any of the", root.GetProperty("finding").GetString()!, StringComparison.Ordinal);
        });
    }

    /// <summary>A server with no display name is still named: by its storage name, the fallback every other read uses.</summary>
    [Fact]
    public void ADisplayNameIsTheFallback_ToTheStorageName_WhenTheRegistryHasNone()
    {
        var servers = new[]
        {
            new RegisteredServer(1, "host-a:appdb:pg", "Orders PG"),
            new RegisteredServer(2, "host-b:appdb:pg", null),
        };

        Assert.Equal("Orders PG", DarlingServerResolver.ResolveOrErrorForAnswer(servers, "host-a").resolved.DisplayName);
        Assert.Equal("host-a:appdb:pg", DarlingServerResolver.ResolveOrErrorForAnswer(servers, "host-a").resolved.ServerName);
        Assert.Equal("host-b:appdb:pg", DarlingServerResolver.ResolveOrErrorForAnswer(servers, "host-b").resolved.DisplayName);
    }

    /// <summary>The offline rule is <c>list_servers</c>'s own: the same instants classify the same way in both.</summary>
    [Fact]
    public void TheOfflineLead_AppliesExactlyWhenListServersSaysOffline()
    {
        var now = new DateTime(2026, 10, 19, 12, 0, 0, DateTimeKind.Utc);
        var created = now.AddDays(-60);

        foreach (var last in new DateTime?[] { now.AddMinutes(-1), now.AddMinutes(-10), now.AddHours(-3), now.AddDays(-2), now.AddDays(-9), null })
        {
            var offline = DarlingMcpDataTools.FreshnessStatus(last, created, now) == "Offline";
            Assert.Equal(offline, DarlingServerOfflineNote.LeadFor("Orders PG", last, created, now) is not null);
        }

        Assert.Null(DarlingServerOfflineNote.LeadFor("Orders PG", now.AddMinutes(-1), created, now));
        Assert.NotNull(DarlingServerOfflineNote.LeadFor("Orders PG", now.AddDays(-9), created, now));
    }
}
