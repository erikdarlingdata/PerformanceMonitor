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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6261: <c>get_alert_history</c> with <c>include_streak</c> adds <c>streak_days</c> to each row, read from the alert log in one
/// query for the page; without it the rows carry no streak member. The streak is counted from fires outside the page too (the
/// page is bounded by <c>limit</c>, the streak is not), a muted fire counts, and a recovery row has none.
/// </summary>
public sealed class AlertHistoryStreakLiveTests
{
    private const int ServerId = 7_626_101;
    private const string ServerName = "LcStreakSrv";
    private const string Metric = "Blocking";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task InsertAsync(NpgsqlConnection connection, DateTime at, string metric, bool muted, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 45, 30, $5, 'email', NULL, $6, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(at), ServerId, ServerName, metric, !muted, muted);

    [Fact]
    public async Task IncludeStreak_AddsTheStreakToEachRow_AndTheDefaultReadCarriesNone()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert history streak live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* The metric fired at 02:10 on days 1-4 back and 6 back (day 5 is the gap); day 3 only muted. A fire at 14:10 on
           day 5 is another time of day, so the gap stays, and a recovery row sits under its own title. */
        var today = DateTime.UtcNow.Date;
        DateTime At(int daysBack) => today.AddDays(-daysBack).AddHours(2).AddMinutes(10);
        foreach (var daysBack in new[] { 1, 2, 4, 6 })
            await InsertAsync(connection, At(daysBack), Metric, muted: false, ct);
        await InsertAsync(connection, At(3), Metric, muted: true, ct);
        await InsertAsync(connection, At(5).AddHours(12), Metric, muted: false, ct);  // the gap day, at another time of day
        await InsertAsync(connection, At(1).AddMinutes(5), Metric + " Resolved", muted: false, ct);

        /* A 23:50 alert: its day-1 fire is the 00:10 of its own date, day 2 the 00:10 before. A count by calendar date puts the
           first on the alert's own date and finds no streak. */
        var late = today.AddDays(-1).AddHours(23).AddMinutes(50);
        await InsertAsync(connection, late, "Midnight", muted: false, ct);
        await InsertAsync(connection, today.AddDays(-1).AddMinutes(10), "Midnight", muted: false, ct);
        await InsertAsync(connection, today.AddDays(-2).AddMinutes(10), "Midnight", muted: true, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var plain = await DarlingMcpAlertTools.GetAlertHistory(dataSource, ServerName, 240, 50, metric_name: Metric);
        Assert.Contains("alerts_returned", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("streak", plain, StringComparison.Ordinal);

        var answer = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertHistory(
            dataSource, ServerName, 240, 50, include_streak: true)).RootElement;
        var rows = answer.GetProperty("alerts").EnumerateArray().ToList();
        JsonElement Row(DateTime at, string metric) => rows.Single(a =>
            a.GetProperty("metric_name").GetString() == metric
            && DateTime.Parse(a.GetProperty("alert_time").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind) == at);
        int Streak(DateTime at) => Row(at, Metric).GetProperty("streak_days").GetInt32();

        Assert.Equal(3, Streak(At(1)));   // days 2, 3 (muted only) and 4 back from it, then the gap at day 5
        Assert.Equal(2, Streak(At(2)));
        Assert.Equal(1, Streak(At(3)));   // the muted row has a streak of its own
        Assert.Equal(0, Streak(At(4)));   // day 5 did not fire
        Assert.Equal(0, Streak(At(6)));
        Assert.Equal(0, Streak(At(5).AddHours(12))); // nothing at 14:10 the day before
        Assert.False(Row(At(1), Metric).GetProperty("streak_at_least_days_window").GetBoolean());
        Assert.Equal(JsonValueKind.Null, Row(At(1).AddMinutes(5), Metric + " Resolved").GetProperty("streak_days").ValueKind);

        /* The page is bounded by limit, the streak is not: the newest row still sees the days older than the page. */
        var narrow = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertHistory(
            dataSource, ServerName, 240, 1, metric_name: Metric, include_streak: true)).RootElement;
        var only = narrow.GetProperty("alerts").EnumerateArray().Single();
        Assert.Equal(2, Row(late, "Midnight").GetProperty("streak_days").GetInt32());

        Assert.Equal(1, narrow.GetProperty("alerts").GetArrayLength());
        Assert.Equal(3, only.GetProperty("streak_days").GetInt32());
    }
}
