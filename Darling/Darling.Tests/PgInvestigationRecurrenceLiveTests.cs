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
    /// A 02:10 alert counts a 01:45 fire and not a 02:41 fire. The edges: 29 minutes either side count, 31 minutes either side
    /// do not, 30 exactly counts. Another metric, another server, the alert's own row and a fire after it never count.
    /// </summary>
    [Fact]
    public async Task A_0210Alert_Counts01_45_AndNot02_41_AndTheEdgesAre29In_31Out()
    {
        await RunAsync(async (dataSource, connection, ct) =>
        {
            var anchor = new DateTime(2026, 10, 10, 2, 10, 0, DateTimeKind.Utc);
            var requestId = await OpenAsync(dataSource, anchor, ct);
            var day = anchor.Date;

            await FireAsync(connection, anchor, ct);                                              // the alert's own row
            await FireAsync(connection, day.AddDays(-1).AddHours(1).AddMinutes(45), ct);          // 01:45, counts
            await FireAsync(connection, day.AddDays(-2).AddHours(2).AddMinutes(39), ct);           // +29, counts
            await FireAsync(connection, day.AddDays(-3).AddHours(1).AddMinutes(41), ct);           // -29, counts
            await FireAsync(connection, day.AddDays(-4).AddHours(2).AddMinutes(40), ct);           // +30, counts
            await FireAsync(connection, day.AddDays(-5).AddHours(2).AddMinutes(41), ct);           // 02:41, +31, does not
            await FireAsync(connection, day.AddDays(-6).AddHours(1).AddMinutes(39), ct);           // -31, does not
            await FireAsync(connection, day.AddDays(-1).AddHours(2).AddMinutes(10), ct, metric: "Deadlocks");
            await FireAsync(connection, day.AddDays(-2).AddHours(2).AddMinutes(10), ct, serverId: ServerId + 1);
            await FireAsync(connection, anchor.AddMinutes(5), ct);                                 // this incident, after the alert

            var recurrence = await ReadAsync(dataSource, requestId, anchor, ct);
            Assert.Equal(4, recurrence.Fired);
            Assert.Equal(0, recurrence.Muted);
            Assert.Equal(new[] { new DateOnly(2026, 10, 9), new DateOnly(2026, 10, 8), new DateOnly(2026, 10, 7), new DateOnly(2026, 10, 6) },
                recurrence.Dates);
            Assert.Equal(new TimeSpan(2, 10, 0), recurrence.AroundUtc.TimeOfDay);
        }, extraServers: 1);
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
            Assert.Equal(new[] { new DateOnly(2026, 10, 14) }, recurrence.Dates);
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
            Assert.Equal(new[] { new DateOnly(2026, 10, 9), new DateOnly(2026, 10, 8) }, recurrence.Dates);
        });
    }

    /// <summary>Muted fires are counted apart from the ones that were not muted, and a day with only muted fires is still a date.</summary>
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
            Assert.Equal(3, recurrence.Dates.Count);
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
            Assert.Empty(sources.Recurrence[0].Dates);
            Assert.Equal(1, sources.Recurrence[1].Fired);

            var brief = JsonNode.Parse(InvestigationBriefComposer.Compose(
                sources, DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, anchor))!;
            var node = brief["recurrence"]!.AsArray().Single(n => n!["metric"]!.GetValue<string>() == Metric)!;
            Assert.Equal(0, node["fired"]!.GetValue<int>());
            Assert.Equal(0, node["muted"]!.GetValue<int>());
            Assert.Empty(node["dates"]!.AsArray());
            Assert.Equal("02:10", node["around_utc"]!.GetValue<string>());
            Assert.Equal(30, node["window_minutes"]!.GetValue<int>());
            Assert.Equal(7, node["days"]!.GetValue<int>());
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
