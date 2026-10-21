/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6305: <c>get_pg_xmin_horizon</c> answered <c>holder_present</c> with the "old snapshot / idle in transaction" remedy for the
/// monitor's own collecting session, a holder at xmin age 0. A holder at age 0 holds nothing back, and rows an older build stored
/// for it must not read as a holder either.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPgXminZeroAgeLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SelfDetail = "state=active application=PerformanceMonitorDarling xact_start=(none) query_start=(none)";

    private static Task Exec(NpgsqlConnection c, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, sql, values);

    private static Task SeedHolder(NpgsqlConnection c, string name, DateTime at, string source, long age, string holder, string detail, bool winner) =>
        Exec(c,
            "INSERT INTO pg_xmin_horizon (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerIdHelper.GetDeterministicHashCode(name), name, source, age, holder, detail, winner);

    private static Task RunAsync(string name, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, ["pg_xmin_horizon"], [name], ["pg_xmin_horizon"], async (c, ds, end) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-30), "pg_xmin_horizon", end.AddDays(-2), 30, end, ["pg_xmin_horizon"], TestContext.Current.CancellationToken);
            await body(c, ds, end);
        });

    private static Task<string> Call(NpgsqlDataSource ds, string name, DateTime end) =>
        DarlingMcpPgXminTools.GetPgXminHorizon(ds, name, 24, as_of: WebDataStartNote.FormatWindowEnd(end));

    [Fact]
    public async Task ASelfRowAtAgeZero_IsNotReportedAsAHolder_AgainstDevPostgres()
    {
        await RunAsync("xmin-zero-only", async (c, ds, end) =>
        {
            await SeedHolder(c, "xmin-zero-only", end.AddMinutes(-10), "session", 0, "1234", SelfDetail, true);

            var root = WindowFloorLiveHarness.Parse(await Call(ds, "xmin-zero-only", end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.False(root.TryGetProperty("winning_holder", out _), root.ToString());
            Assert.DoesNotContain("idle in transaction", root.ToString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ASelfRowAtAgeZero_NextToARealHolder_DoesNotWinOrAppearInTheHolderList_AgainstDevPostgres()
    {
        await RunAsync("xmin-zero-beside", async (c, ds, end) =>
        {
            await SeedHolder(c, "xmin-zero-beside", end.AddMinutes(-10), "session", 0, "1234", SelfDetail, true);
            await SeedHolder(c, "xmin-zero-beside", end.AddMinutes(-10), "replication_slot", 9000, "slot_a", "slot detail", false);

            var root = WindowFloorLiveHarness.Parse(await Call(ds, "xmin-zero-beside", end));

            Assert.Equal("holder_present", root.GetProperty("status").GetString());
            Assert.Equal("replication_slot", root.GetProperty("winning_source").GetString());
            var holders = root.GetProperty("holders").EnumerateArray().ToList();
            Assert.Single(holders);
            Assert.Equal("replication_slot", holders[0].GetProperty("source").GetString());
        });
    }

    [Fact]
    public async Task ARealHolder_StillAnswersHolderPresent_AgainstDevPostgres()
    {
        await RunAsync("xmin-real", async (c, ds, end) =>
        {
            await SeedHolder(c, "xmin-real", end.AddMinutes(-10), "session", 5000, "42", "state=idle in transaction application=app", true);

            var root = WindowFloorLiveHarness.Parse(await Call(ds, "xmin-real", end));

            Assert.Equal("holder_present", root.GetProperty("status").GetString());
            Assert.Equal(5000, root.GetProperty("winning_xmin_age").GetInt64());
        });
    }
}
