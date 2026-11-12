/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #8551 (gated, DARLING_TEST_PG): the sidebar badge's read, <c>get_alert_badge_summary</c>. The page used to count the newest 1,000
/// rows of <c>get_alert_history</c>, so a window busier than that lost its oldest actionable alerts, and the reply was megabytes.
/// The seed is that window: more than 1,000 muted and resolution rows newer than the one actionable alert of server A, and negative
/// server ids throughout (the registry's sentinel ids are real ids).
/// </summary>
public sealed class AlertBadgeSummaryLiveTests
{
    private const int ServerA = -815101;
    private const int ServerB = -815102;
    private const int BusyNoise = 600;
    private const int QuietNoise = 100;

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert badge summary live tests.");
        return connectionString!;
    }

    private static Task InsertAsync(NpgsqlConnection connection, int serverId, DateTime at, string metric, bool muted, bool dismissed, string? contextJson, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 45, 30, $5, 'email', NULL, $6, $7, $8, $9)",
            DarlingMcpTestData.Naive(at), serverId, "badge-" + serverId, metric, !muted, muted, dismissed,
            new string('d', 1500), contextJson);

    private static string FiredAt(AlertSeverityLevel level) =>
        AlertContextSerializer.Serialize(new AlertContext { SeverityOverride = level });

    /// <summary>Server A: one actionable alert 20 hours old under <paramref name="noise"/> x 2 newer muted and resolution rows. Server B: the mix the badge rule has
    /// to sort, one row of each kind.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, int noise, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, "badge-" + ServerA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, "badge-" + ServerB, ct);

        await InsertAsync(connection, ServerA, now.AddHours(-20), "High CPU", false, false, null, ct);
        for (var i = 0; i < noise; i++)
        {
            await InsertAsync(connection, ServerA, now.AddMinutes(-(i + 1)), "High CPU", true, false, null, ct);
            await InsertAsync(connection, ServerA, now.AddMinutes(-(i + 1)).AddSeconds(30), "High CPU Resolved", false, false, null, ct);
        }

        await InsertAsync(connection, ServerB, now.AddHours(-5), "Blocking Detected", false, false, null, ct);               // a warning by name
        await InsertAsync(connection, ServerB, now.AddHours(-4), "Deadlocks Detected", false, false, null, ct);             // critical by name
        await InsertAsync(connection, ServerB, now.AddHours(-3), "Poison Wait", false, false, FiredAt(AlertSeverityLevel.Warning), ct); // critical by name, fired Warning
        await InsertAsync(connection, ServerB, now.AddHours(-2), "Low Disk Space", false, false, FiredAt(AlertSeverityLevel.Critical), ct); // fired Critical
        await InsertAsync(connection, ServerB, now.AddHours(-1), "Blocking Detected", true, false, null, ct);               // muted
        await InsertAsync(connection, ServerB, now.AddMinutes(-30), "Blocking Detected", false, true, null, ct);           // dismissed
        await InsertAsync(connection, ServerB, now.AddMinutes(-20), "Server Restored", false, false, null, ct);             // a resolution by name
        await InsertAsync(connection, ServerB, now.AddHours(-30), "Blocking Detected", false, false, null, ct);             // outside the 24 hours
    }

    private static async Task<(NpgsqlDataSource Source, ScratchPostgres Scratch)> OpenSeededAsync(DateTime now, int noise, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, now, noise, ct);
        return (NpgsqlDataSource.Create(scratch.ConnectionString), scratch);
    }

    private static Task<string> BadgeAsync(NpgsqlDataSource source, CancellationToken ct) =>
        DarlingWebEndpoints.BuildReadDispatch()["get_alert_badge_summary"](new DefaultHttpContext { RequestAborted = ct }, source, null!);

    private static Dictionary<int, (long Count, bool Critical, DateTime Latest)> Entries(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("servers").EnumerateArray().ToDictionary(
            s => s.GetProperty("server_id").GetInt32(),
            s => (s.GetProperty("count").GetInt64(), s.GetProperty("critical").GetBoolean(),
                DateTime.Parse(s.GetProperty("latest_alert_time").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)));
    }

    [Fact]
    public async Task TheBadgeCountsAnActionableAlertOlderThanTheNewestThousandRows_InAReplyUnder50KB()
    {
        var ct = TestContext.Current.CancellationToken;
        var now = DateTime.UtcNow;
        var (source, scratch) = await OpenSeededAsync(now, BusyNoise, ct);
        await using var _ = scratch;
        await using var __ = source;

        /* The precondition: the old read, the newest 1,000 rows, does not reach server A's one actionable alert. */
        var history = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertHistory(source, null, 24, 1000, as_of: null, include_dismissed: false, metric_name: null, family: null, include_streak: false, maskSetting: null, full_text: true, responseByteBudget: null, cancellationToken: ct)).RootElement;
        Assert.True(history.GetProperty("truncated").GetBoolean());
        Assert.DoesNotContain(history.GetProperty("alerts").EnumerateArray(), a =>
            a.GetProperty("server_id").GetInt32() == ServerA && !a.GetProperty("muted").GetBoolean() && a.GetProperty("severity").GetString() != "resolution");

        var json = await BadgeAsync(source, ct);
        var entries = Entries(json);
        Assert.Equal(1, entries[ServerA].Count);
        Assert.False(entries[ServerA].Critical);
        Assert.InRange((now.AddHours(-20) - entries[ServerA].Latest).Duration().TotalSeconds, 0, 1);
        Assert.True(Encoding.UTF8.GetByteCount(json) < 50_000, "The reply is " + Encoding.UTF8.GetByteCount(json) + " bytes.");

        /* The fixed window, and one entry per server. */
        Assert.Equal(2, entries.Count);
        Assert.Equal(24, JsonDocument.Parse(json).RootElement.GetProperty("window_hours").GetInt32());
    }

    [Fact]
    public async Task TheBadgeAgreesWithTheRuleTheDeskAppliedToTheHistoryRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var now = DateTime.UtcNow;
        var (source, scratch) = await OpenSeededAsync(now, QuietNoise, ct);
        await using var _ = scratch;
        await using var __ = source;

        /* The old badge, done over a page big enough to hold the window: a row counts when it is not muted, not a resolution and not dismissed
           (the default read leaves dismissed rows out), critical when any such row's severity says so. */
        var history = JsonDocument.Parse(await DarlingMcpAlertTools.GetAlertHistory(source, null, 24, 1000, as_of: null, include_dismissed: false, metric_name: null, family: null, include_streak: false, maskSetting: null, full_text: true, responseByteBudget: null, cancellationToken: ct)).RootElement;
        Assert.False(history.GetProperty("truncated").GetBoolean());
        var expected = history.GetProperty("alerts").EnumerateArray()
            .Where(a => !a.GetProperty("muted").GetBoolean() && a.GetProperty("severity").GetString() != "resolution" && !a.GetProperty("dismissed").GetBoolean())
            .GroupBy(a => a.GetProperty("server_id").GetInt32())
            .ToDictionary(g => g.Key, g => (
                Count: (long)g.Count(),
                Critical: g.Any(a => a.GetProperty("severity").GetString() == "critical"),
                Latest: g.Max(a => DateTime.Parse(a.GetProperty("alert_time").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind))));

        var actual = Entries(await BadgeAsync(source, ct));

        Assert.Equal(expected.Keys.OrderBy(k => k), actual.Keys.OrderBy(k => k));
        foreach (var (id, e) in expected)
        {
            Assert.Equal(e, actual[id]);
        }

        /* Server B's four actionable rows: Blocking, Deadlocks (critical by name), Poison Wait (fired Warning, so not critical by name) and Low Disk
           Space (fired Critical). The muted, dismissed, restored and 30-hour-old rows are none of them. */
        Assert.Equal(4, actual[ServerB].Count);
        Assert.True(actual[ServerB].Critical);
    }

    [Fact]
    public async Task AReaderScopedToOneServerGetsNoEntryForAnother()
    {
        var ct = TestContext.Current.CancellationToken;
        var (source, scratch) = await OpenSeededAsync(DateTime.UtcNow, QuietNoise, ct);
        await using var _ = scratch;
        await using var __ = source;

        async Task<int[]> ReadAs(int[] grant) =>
            Entries(await DarlingServerResolver.ReadWebScopeAsync(grant, Array.Empty<int>(), () => BadgeAsync(source, ct))).Keys.OrderBy(k => k).ToArray();

        Assert.Equal(new[] { ServerA }, await ReadAs(new[] { ServerA }));
        Assert.Equal(new[] { ServerB }, await ReadAs(new[] { ServerB }));
        Assert.Equal(new[] { ServerA, ServerB }.OrderBy(k => k), await ReadAs(new[] { ServerA, ServerB }));
        Assert.Empty(await ReadAs(Array.Empty<int>()));
    }
}
