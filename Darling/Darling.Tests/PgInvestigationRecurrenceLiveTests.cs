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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6000 (#5632 Phase C): the brief's recurrence, read by <see cref="PgInvestigationBriefSourceReader"/> from
/// <c>config_alert_log</c>: how often each of the request's linked metrics fired within 30 minutes either side of the first
/// alert's time of day, in the 7 days before it. Every fact plants history rows around a fixed first alert, reads the
/// sources through the reader and checks the counts, the dates and the composed brief.
/// </summary>
public sealed class PgInvestigationRecurrenceLiveTests
{
    private const int ServerId = 7_563_301;
    private const string ServerName = "LcRecurrenceSrv";
    private const string Metric = "Blocking";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static InvestigationAlert Alert(DateTime at, string metric = Metric) =>
        new(1, ServerId, ServerName, "blocking", metric, at, new[] { ServerId + ":Blocking" }, null,
            20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    private static async Task<Guid> OpenAsync(NpgsqlDataSource dataSource, DateTime at, CancellationToken ct, string metric = Metric) =>
        (await new InvestigationRequestStore(dataSource, 30, () => at).OpenOrJoinAsync(Alert(at, metric), ct)).RequestId!.Value;

    private static Task FireAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct, bool muted = false,
        string metric = Metric, int serverId = ServerId) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 45, 30, $5, 'email', NULL, $6, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(at), serverId, ServerName, metric, !muted, muted);

    private static async Task<InvestigationAlertRecurrence> ReadAsync(
        NpgsqlDataSource dataSource, Guid requestId, DateTime now, CancellationToken ct, string metric = Metric)
    {
        var sources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => now).ReadAsync(requestId, ct);
        return sources!.Recurrence!.Single(r => r.MetricName == metric);
    }

    /// <summary>
    /// A 02:10 alert counts a fire within the window of its clock time on an earlier day, edge minutes included, and no fire
    /// beyond it. The edge minutes come from <see cref="InvestigationAlertRecurrence.WindowMinutes"/>, the constant the brief
    /// publishes, so the SQL and the brief state one window (#3684): one inside either side counts, the window's last minute
    /// either side counts, one past it does not. Another metric, another server, the alert's own row and a fire after it never count.
    /// </summary>
    [Fact]
    public async Task A_0210Alert_CountsToTheWindowEdge_AndNotOneMinutePast()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            const int W = InvestigationAlertRecurrence.WindowMinutes;
            var anchor = new DateTime(2026, 10, 10, 2, 10, 0, DateTimeKind.Utc);
            var requestId = await OpenAsync(dataSource, anchor, ct);

            await FireAsync(connection, anchor, ct);                                              // the alert's own row
            await FireAsync(connection, anchor.AddDays(-1).AddMinutes(-(W - 5)), ct);             // inside, before the clock time, counts
            await FireAsync(connection, anchor.AddDays(-2).AddMinutes(W - 1), ct);                // one inside the edge, after, counts
            await FireAsync(connection, anchor.AddDays(-3).AddMinutes(-(W - 1)), ct);             // one inside the edge, before, counts
            await FireAsync(connection, anchor.AddDays(-4).AddMinutes(W), ct);                    // the edge itself, counts
            await FireAsync(connection, anchor.AddDays(-5).AddMinutes(W + 1), ct);                // one past, does not
            await FireAsync(connection, anchor.AddDays(-6).AddMinutes(-(W + 1)), ct);             // one past, before, does not
            await FireAsync(connection, anchor.AddDays(-1), ct, metric: "Deadlocks");
            await FireAsync(connection, anchor.AddDays(-2), ct, serverId: ServerId + 1);
            await FireAsync(connection, anchor.AddMinutes(5), ct);                                 // this incident, after the alert

            var recurrence = await ReadAsync(dataSource, requestId, anchor, ct);
            Assert.Equal(4, recurrence.Fired);
            Assert.Equal(0, recurrence.Muted);
            Assert.Equal(new[] { new DateOnly(2026, 10, 9), new DateOnly(2026, 10, 8), new DateOnly(2026, 10, 7), new DateOnly(2026, 10, 6) },
                recurrence.FiredDates);
            Assert.Empty(recurrence.MutedDates);
            Assert.Equal(new TimeSpan(2, 10, 0), recurrence.AroundUtc.TimeOfDay);
        }, extraServers: 1);
    }

    /// <summary>The incident's own rise is not its history (#6000 review): a fire 10 minutes before the first alert (the same
    /// rise, as when an episode spent its route cap and opened on a later fire) does not count, nor does one at the window's
    /// length before it; a fire 23 hours 50 minutes before it, the same clock time a day back, does.</summary>
    [Fact]
    public async Task TheIncidentsOwnRise_IsNotCounted_ButTheSameClockTimeADayBackIs()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var anchor = new DateTime(2026, 10, 10, 2, 10, 0, DateTimeKind.Utc);
            var requestId = await OpenAsync(dataSource, anchor, ct);
            await FireAsync(connection, anchor.AddMinutes(-10), ct);                                     // the rise, does not count
            await FireAsync(connection, anchor.AddMinutes(-InvestigationAlertRecurrence.WindowMinutes), ct); // the window's length back, does not
            await FireAsync(connection, anchor.AddHours(-23).AddMinutes(-50), ct);                       // a day back, counts

            var recurrence = await ReadAsync(dataSource, requestId, anchor, ct);
            Assert.Equal(1, recurrence.Fired);
            Assert.Equal(new[] { new DateOnly(2026, 10, 9) }, recurrence.FiredDates);
        });
    }

    /// <summary>The anchor is compared as naive UTC whatever the session time zone says: the same fires read the same with the
    /// session set east of UTC (#6000 review: config.investigation_request_alerts.alert_time is timestamptz).</summary>
    [Fact]
    public async Task TheCounts_DoNotMoveWithTheSessionTimeZone()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var anchor = new DateTime(2026, 10, 10, 2, 10, 0, DateTimeKind.Utc);
            var requestId = await OpenAsync(dataSource, anchor, ct);
            await FireAsync(connection, anchor.AddDays(-1), ct);
            await FireAsync(connection, anchor.AddDays(-2).AddMinutes(InvestigationAlertRecurrence.WindowMinutes), ct);
            await FireAsync(connection, anchor.AddDays(-3).AddMinutes(InvestigationAlertRecurrence.WindowMinutes + 1), ct);

            await using var tokyo = await dataSource.OpenConnectionAsync(ct);
            await using (var set = new NpgsqlCommand("SET timezone = 'Asia/Tokyo'", tokyo))
                await set.ExecuteNonQueryAsync(ct);
            await using (var sql = new NpgsqlCommand(PgInvestigationBriefSourceReader.RecurrenceSql, tokyo))
            {
                sql.Parameters.Add(new NpgsqlParameter { Value = requestId });
                sql.Parameters.Add(new NpgsqlParameter { Value = ServerId });
                await using var reader = await sql.ExecuteReaderAsync(ct);
                var counted = 0L;
                while (await reader.ReadAsync(ct))
                    if (!reader.IsDBNull(2)) counted += reader.GetInt64(4);
                Assert.Equal(2L, counted);
            }
        });
    }

    /// <summary>The 7-day reach: a fire at the clock time 6 days before counts, one 8 days before does not.</summary>
    [Fact]
    public async Task TheSevenDayReach_Counts6DaysBack_AndNot8()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var anchor = new DateTime(2026, 10, 20, 2, 10, 0, DateTimeKind.Utc);
            var requestId = await OpenAsync(dataSource, anchor, ct);
            await FireAsync(connection, anchor.AddDays(-6), ct);
            await FireAsync(connection, anchor.AddDays(-8), ct);

            var recurrence = await ReadAsync(dataSource, requestId, anchor, ct);
            Assert.Equal(1, recurrence.Fired);
            Assert.Equal(new[] { new DateOnly(2026, 10, 14) }, recurrence.FiredDates);
        });
    }

    /// <summary>A 23:50 alert counts a 00:10 fire (20 minutes later by the clock, across midnight) and a 23:35 fire, not a
    /// 00:21 one (31 minutes). The clock test wraps at midnight.</summary>
    [Fact]
    public async Task A_2350Alert_CountsA_0010Fire_AcrossMidnight()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var anchor = new DateTime(2026, 10, 10, 23, 50, 0, DateTimeKind.Utc);
            var requestId = await OpenAsync(dataSource, anchor, ct);
            await FireAsync(connection, new DateTime(2026, 10, 9, 0, 10, 0, DateTimeKind.Utc), ct);   // 00:10, counts
            await FireAsync(connection, new DateTime(2026, 10, 8, 23, 35, 0, DateTimeKind.Utc), ct);  // 23:35, counts
            await FireAsync(connection, new DateTime(2026, 10, 7, 0, 21, 0, DateTimeKind.Utc), ct);   // 00:21, +31, does not
            await FireAsync(connection, new DateTime(2026, 10, 6, 23, 19, 0, DateTimeKind.Utc), ct);  // 23:19, -31, does not

            var recurrence = await ReadAsync(dataSource, requestId, anchor, ct);
            Assert.Equal(2, recurrence.Fired);
            Assert.Equal(new[] { new DateOnly(2026, 10, 9), new DateOnly(2026, 10, 8) }, recurrence.FiredDates);
        });
    }

    /// <summary>Muted fires are counted apart from the ones that were not muted, and so are their dates: a day with only muted
    /// fires is in <c>muted_dates</c> and not in <c>fired_dates</c>.</summary>
    [Fact]
    public async Task MutedFires_AreCountedApartFromUnmutedOnes()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var anchor = new DateTime(2026, 10, 10, 2, 10, 0, DateTimeKind.Utc);
            var requestId = await OpenAsync(dataSource, anchor, ct);
            var day = anchor.Date;
            await FireAsync(connection, day.AddDays(-1).AddHours(2), ct);
            await FireAsync(connection, day.AddDays(-2).AddHours(2), ct);
            await FireAsync(connection, day.AddDays(-3).AddHours(2), ct, muted: true);
            await FireAsync(connection, day.AddDays(-3).AddHours(2).AddMinutes(5), ct, muted: true);

            var recurrence = await ReadAsync(dataSource, requestId, anchor, ct);
            Assert.Equal(2, recurrence.Fired);
            Assert.Equal(2, recurrence.Muted);
            Assert.Equal(new[] { new DateOnly(2026, 10, 9), new DateOnly(2026, 10, 8) }, recurrence.FiredDates);
            Assert.Equal(new[] { new DateOnly(2026, 10, 7) }, recurrence.MutedDates);
        });
    }

    /// <summary>A linked metric with no history says zero, in the reader and in the composed brief; it is not left out. A second
    /// linked metric with history is counted on its own.</summary>
    [Fact]
    public async Task AMetricWithNoHistory_SaysZero_AndEachLinkedMetricIsCountedOnItsOwn()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var anchor = new DateTime(2026, 10, 10, 2, 10, 0, DateTimeKind.Utc);
            var requestId = await OpenAsync(dataSource, anchor, ct);
            Assert.Equal(requestId, await OpenAsync(dataSource, anchor.AddMinutes(1), ct, metric: "Deadlocks"));
            await FireAsync(connection, anchor.Date.AddDays(-1).AddHours(2), ct, metric: "Deadlocks");

            var sources = await new PgInvestigationBriefSourceReader(dataSource, utcNow: () => anchor).ReadAsync(requestId, ct);
            Assert.Equal(new[] { Metric, "Deadlocks" }, sources!.Recurrence!.Select(r => r.MetricName));
            Assert.Equal(0, sources.Recurrence![0].Fired);
            Assert.Equal(0, sources.Recurrence[0].Muted);
            Assert.Empty(sources.Recurrence[0].FiredDates);
            Assert.Empty(sources.Recurrence[0].MutedDates);
            Assert.Equal(1, sources.Recurrence[1].Fired);

            var brief = JsonNode.Parse(InvestigationBriefComposer.Compose(
                sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, anchor))!;
            var node = brief["recurrence"]!.AsArray().Single(n => n!["metric"]!.GetValue<string>() == Metric)!;
            Assert.Equal(0, node["fired"]!.GetValue<int>());
            Assert.Equal(0, node["muted"]!.GetValue<int>());
            Assert.Empty(node["fired_dates"]!.AsArray());
            Assert.Empty(node["muted_dates"]!.AsArray());
            Assert.Equal("02:10", node["around_utc"]!.GetValue<string>());
            Assert.Equal(InvestigationAlertRecurrence.WindowMinutes, node["window_minutes"]!.GetValue<int>());
            Assert.Equal(InvestigationAlertRecurrence.Days, node["days"]!.GetValue<int>());
        });
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body, int extraServers = 0)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation recurrence live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        for (var i = 1; i <= extraServers; i++)
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId + i, ServerName + i, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(dataSource, connection, ct);
    }
}
