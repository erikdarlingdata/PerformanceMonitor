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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test creates its own scratch database through ScratchPostgres
   and works entirely inside it, so it cannot race live collection. */
/// <summary>
/// #7892: the <c>top_blocking_chains</c> drill-down lists WAITS, the unit the BLOCKING_EVENTS fact counts, not the blocked-process
/// reports SQL Server writes for a still-blocked session every threshold seconds. A wait reported eight times is one row, carrying
/// its longest report's wait time and the number of reports it collapsed, and the DMV snapshot arm folds its samples the same way.
/// </summary>
public sealed class TopBlockingChainsWaitsLiveTests
{
    private const int ServerId = -7892_01;
    private const string ServerName = "darling-top-chains-waits";
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the live test.";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime WindowStart = new(2026, 3, 4, 10, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(4);
    private static readonly string[] ScopeGp = ["GP"];

    private static async Task Exec(NpgsqlConnection c, CancellationToken ct, string sql, params object[] args)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var a in args) cmd.Parameters.AddWithValue(a);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static Task Bpr(NpgsqlConnection c, CancellationToken ct, DateTime eventTime, long waitMs, int blocked, int blocker, string database) =>
        Exec(c, ct,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocked_spid, blocking_spid, lock_mode, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,$6,$7,'X','suspended',$8)",
            CollectionIdGenerator.Next(), eventTime, ServerId, ServerName, waitMs, blocked, blocker, database);

    private static Task Dmv(NpgsqlConnection c, CancellationToken ct, DateTime eventTime, long waitMs, int blocked, int blocker, string database) =>
        Exec(c, ct,
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status) VALUES ($1,$2,$3,$4,$2,$5,$6,$7,$8,'X','suspended')",
            CollectionIdGenerator.Next(), eventTime, ServerId, ServerName, database, blocked, blocker, waitMs);

    /// <summary>Thirteen rows of three waits. Wait A: session 51 blocked by 60 in GP, reported eight times five seconds apart, each report five
    /// seconds longer than the last, so every report derives the same wait start. Wait B: session 52 blocked by 60 in HS, reported twice the
    /// same way. Wait C: a DMV snapshot sees session 53 blocked by 61 in HS on three consecutive one-minute samples.</summary>
    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await Exec(c, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)", ServerId, ServerName);

        var startA = WindowStart.AddHours(1);
        for (var i = 1; i <= 8; i++)
            await Bpr(c, ct, startA.AddSeconds(5 * i), 5000L * i, blocked: 51, blocker: 60, "GP");
        var startB = WindowStart.AddHours(2);
        for (var i = 1; i <= 2; i++)
            await Bpr(c, ct, startB.AddSeconds(5 * i), 5000L * i, blocked: 52, blocker: 60, "HS");
        var startC = WindowStart.AddHours(3);
        for (var i = 1; i <= 3; i++)
            await Dmv(c, ct, startC.AddMinutes(i), 60000L * i, blocked: 53, blocker: 61, "HS");
    }

    private static async Task<List<JsonElement>> DrillAsync(IReadOnlyList<string>? scope, CancellationToken ct)
    {
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await SeedAsync(c, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var context = new AnalysisContext
        {
            ServerId = ServerId, ServerName = ServerName, TimeRangeStart = WindowStart, TimeRangeEnd = WindowEnd,
            ServerUtcOffset = TimeSpan.Zero, SeparatelyMonitoredDatabases = scope, CancellationToken = ct
        };
        var finding = new AnalysisFinding { RootFactKey = "BLOCKING_EVENTS", StoryPath = "BLOCKING_EVENTS", PathKeys = ["BLOCKING_EVENTS"], Severity = 1.0 };
        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
        Assert.NotNull(finding.DrillDown);
        return JsonSerializer.SerializeToElement(finding.DrillDown["top_blocking_chains"]).EnumerateArray().ToList();
    }

    [Fact]
    public async Task TopBlockingChains_ListEachWaitOnce_WithItsLongestReportAndItsReportCount()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var rows = await DrillAsync(null, TestContext.Current.CancellationToken);

        /* Three waits, worst first: a list of reports would be five escalating reports of waits C and A. */
        Assert.Equal([180000L, 40000L, 10000L], rows.Select(r => r.GetProperty("wait_time_ms").GetInt64()).ToArray());
        Assert.Equal([53, 51, 52], rows.Select(r => r.GetProperty("blocked_spid").GetInt32()).ToArray());
        Assert.Equal([3L, 8L, 2L], rows.Select(r => r.GetProperty("report_count").GetInt64()).ToArray());
    }

    /// <summary>The Azure SQL Database master form: the waits of a database monitored as its own target are left out, and the rest still
    /// fold to one row per wait.</summary>
    [Fact]
    public async Task TopBlockingChains_SkippingSeparatelyMonitored_ListEachRemainingWaitOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipReason);
        var rows = await DrillAsync(ScopeGp, TestContext.Current.CancellationToken);

        Assert.Equal([180000L, 10000L], rows.Select(r => r.GetProperty("wait_time_ms").GetInt64()).ToArray());
        Assert.Equal([53, 52], rows.Select(r => r.GetProperty("blocked_spid").GetInt32()).ToArray());
        Assert.Equal([3L, 2L], rows.Select(r => r.GetProperty("report_count").GetInt64()).ToArray());
    }
}
