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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Reports;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7041: the Daily Health Report's renderers and section rules. The text has to fit the smallest channel limit and say how many
/// servers it left out, the HTML has to encode every collected string (a server, database or job name can hold markup), a healthy server
/// with nothing to report takes one line, and a PostgreSQL target never shows the SQL Server sections.
/// </summary>
public sealed class DailyHealthReportTests
{
    private static readonly DateTime End = new(2026, 10, 31, 12, 0, 0, DateTimeKind.Utc);

    private static DailyHealthServerReport Server(
        string name,
        string band = "Healthy",
        IReadOnlyList<DailyHealthAlertCount>? alerts = null,
        IReadOnlyList<DailyHealthFailedJob>? jobs = null,
        IReadOnlyList<DailyHealthProtectionItem>? protection = null,
        IReadOnlyList<DailyHealthLowDisk>? disk = null,
        IReadOnlyList<string>? problems = null,
        IReadOnlyList<string>? reasons = null) =>
        new(1, name, false, band, reasons ?? [], alerts ?? [], jobs ?? [], protection ?? [], disk ?? [], problems ?? []);

    private static DailyHealthReportData Data(params DailyHealthServerReport[] servers) =>
        new("Daily Health Report", End.AddHours(-24), End, new DateOnly(2026, 10, 31), "UTC", servers);

    [Fact]
    public void Html_EncodesEveryCollectedString_AndCarriesNoScriptOrExternalImage()
    {
        const string evil = "<script>alert('x')</script>";
        var server = Server(
            evil,
            "Critical",
            alerts: [new DailyHealthAlertCount("<b>CPU</b>", 2)],
            jobs: [new DailyHealthFailedJob("<img src=x onerror=alert(1)>", 1, End, "<script>boom</script>")],
            protection: [new DailyHealthProtectionItem("full_backup", "<script>db</script>", "<i>never</i>")],
            disk: [new DailyHealthLowDisk("<u>D:</u>", 1, 100, 1)],
            problems: ["<script>offline</script>"],
            reasons: ["<script>why</script>"]);
        var data = Data(server) with { Title = "<script>title</script>" };

        var html = DailyHealthReportRenderer.Html(data);

        Assert.DoesNotContain("<script", html, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("<img", html, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("<b>CPU", html, StringComparison.Ordinal);
        Assert.DoesNotContain("<u>", html, StringComparison.Ordinal);
        Assert.Contains("&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;", html, StringComparison.Ordinal);
        Assert.Contains("&lt;script&gt;db&lt;/script&gt;", html, StringComparison.Ordinal);
        Assert.Contains("&lt;img src=x onerror=alert(1)&gt;", html, StringComparison.Ordinal);
        Assert.DoesNotContain("http", html, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Text_StaysInsideTheLimit_AndCountsTheServersItLeftOut()
    {
        var servers = Enumerable.Range(0, 300)
            .Select(i => Server(
                "server-" + i.ToString("000", CultureInfo.InvariantCulture),
                "Warning",
                alerts: [new DailyHealthAlertCount("High CPU", 3), new DailyHealthAlertCount("Blocking", 1)],
                jobs: [new DailyHealthFailedJob("Nightly load", 2, End, "failed")],
                reasons: ["CPU is high"]))
            .ToArray();

        var text = DailyHealthReportRenderer.Text(Data(servers));

        Assert.True(text.Length <= DailyHealthReportRenderer.TextLimit, "text was " + text.Length);
        var lines = text.Split('\n');
        var last = lines[^1];
        Assert.Matches(@"^and \d+ more servers$", last);
        var shown = lines.Count(l => l.StartsWith("server-", StringComparison.Ordinal));
        var left = int.Parse(last.Split(' ')[1], CultureInfo.InvariantCulture);
        Assert.Equal(300, shown + left);
        Assert.True(shown > 0);
        /* The first lines are the fleet header, so a reader sees the totals even when the servers are cut. */
        Assert.StartsWith("Daily Health Report: 300 servers, 300 warning, 0 critical, 2026-10-31", text, StringComparison.Ordinal);
    }

    [Fact]
    public void EmptyFleet_RendersAHeaderAndSaysNoServers_InBothForms()
    {
        var data = Data();

        Assert.Equal("Daily Health Report: 0 servers, 0 warning, 0 critical, 2026-10-31", DailyHealthReportRenderer.Subject(data));
        Assert.Contains("No servers are registered", DailyHealthReportRenderer.Text(data), StringComparison.Ordinal);
        Assert.Contains("No servers are registered", DailyHealthReportRenderer.Html(data), StringComparison.Ordinal);
    }

    [Fact]
    public void Subject_UsesTheConfiguredTitleAndTheLocalDate()
    {
        var data = Data(Server("a"), Server("b", "Warning", reasons: ["x"]), Server("c", "Critical", reasons: ["y"])) with { Title = "Nightly Fleet" };

        Assert.Equal("Nightly Fleet: 3 servers, 1 warning, 1 critical, 2026-10-31", DailyHealthReportRenderer.Subject(data));
    }

    [Fact]
    public void HealthyServerWithNothingToReport_TakesOneLine_AndLeavesOutEmptySections()
    {
        var quiet = Server("quiet-1");
        Assert.True(quiet.IsQuiet);

        var text = DailyHealthReportRenderer.Text(Data(quiet));
        var serverLines = text.Split('\n').Where(l => l.Contains("quiet-1", StringComparison.Ordinal)).ToArray();
        Assert.Single(serverLines);
        Assert.Equal("quiet-1: healthy, nothing to report", serverLines[0]);

        var html = DailyHealthReportRenderer.Html(Data(quiet));
        Assert.DoesNotContain("<table", html, StringComparison.Ordinal);
        Assert.DoesNotContain("Failed jobs", html, StringComparison.Ordinal);

        /* A server with a single failed job prints that section and none of the others. */
        var busy = Server("busy-1", "Warning", jobs: [new DailyHealthFailedJob("Backup", 1, End, null)], reasons: ["x"]);
        var busyText = DailyHealthReportRenderer.Text(Data(busy));
        Assert.Contains("Failed jobs: Backup (1, last 10-31 12:00 UTC)", busyText, StringComparison.Ordinal);
        Assert.DoesNotContain("Low disk", busyText, StringComparison.Ordinal);
        Assert.DoesNotContain("Alerts:", busyText, StringComparison.Ordinal);
    }

    [Fact]
    public void Servers_AreListedWorstFirst_InTheBuiltOrder()
    {
        var servers = new[]
        {
            Server("w", "Warning", reasons: ["x"]),
            Server("h"),
            Server("c", "Critical", reasons: ["y"]),
            Server("o", "Offline", problems: ["Offline"]),
        };

        var ordered = servers.OrderBy(s => s.BandRank).Select(s => s.Name).ToArray();

        Assert.Equal(new[] { "c", "o", "w", "h" }, ordered);
    }

    [Fact]
    public void PostgresTarget_NeverShowsTheSqlServerSections_EvenWhenRowsReachIt()
    {
        var card = new FleetServerCard
        {
            ServerId = 7,
            DisplayName = "pg-1",
            ServerName = "pg-1",
            IsPostgres = true,
            Band = FleetHealthBand.Warning,
            IsOnline = true,
            DatabaseProtectionOverdue = [new FleetDatabaseProtectionOverdue("full_backup", "appdb", "critical", null, "no full backup")],
        };
        var volumes = new List<VolumeFreeSpaceInfo> { new() { MountPoint = "/data", TotalMb = 1000, FreeMb = 10 } };

        var section = DailyHealthReportBuilder.AssembleServer(
            card,
            "Deadlocks are high",
            [new DailyHealthAlertCount("Deadlocks", 4)],
            [new DailyHealthFailedJob("Agent job", 1, End, null)],
            volumes,
            lowDiskPercent: 10,
            lowDiskGb: 5);

        Assert.True(section.IsPostgres);
        Assert.Empty(section.FailedJobs);
        Assert.Empty(section.Protection);
        Assert.Empty(section.LowDisk);
        Assert.Equal(4, section.AlertTotal);
        Assert.Equal(new[] { "Deadlocks are high" }, section.Reasons);
    }

    [Fact]
    public void SqlServerTarget_ShowsJobsProtectionAndLowDisk_AndAHealthyOneIsQuiet()
    {
        var card = new FleetServerCard
        {
            ServerId = 8,
            DisplayName = "sql-1",
            ServerName = "sql-1",
            Band = FleetHealthBand.Healthy,
            IsOnline = true,
            DatabaseProtectionOverdue =
            [
                new FleetDatabaseProtectionOverdue("log_backup", "b", "warning", 7200, "2 h"),
                new FleetDatabaseProtectionOverdue("full_backup", "a", "critical", null, "never"),
            ],
        };
        var volumes = new List<VolumeFreeSpaceInfo>
        {
            new() { MountPoint = "C:\\", TotalMb = 100_000, FreeMb = 50_000 },
            new() { MountPoint = "D:\\", TotalMb = 100_000, FreeMb = 2_000 },
        };

        var section = DailyHealthReportBuilder.AssembleServer(
            card, null, null, [new DailyHealthFailedJob("Nightly", 3, End, "x")], volumes, lowDiskPercent: 10, lowDiskGb: 5);

        Assert.False(section.IsPostgres);
        Assert.Equal("Nightly", Assert.Single(section.FailedJobs).JobName);
        Assert.Equal(new[] { "full_backup", "log_backup" }, section.Protection.Select(p => p.Kind).ToArray());
        Assert.Equal("D:\\", Assert.Single(section.LowDisk).Volume);

        var quiet = DailyHealthReportBuilder.AssembleServer(
            new FleetServerCard { ServerId = 9, DisplayName = "sql-2", ServerName = "sql-2", Band = FleetHealthBand.Healthy, IsOnline = true },
            null, null, null, null, 10, 5);
        Assert.True(quiet.IsQuiet);
    }

    [Fact]
    public void OfflineServer_ReportsTheOutage_AndFailingCollectors()
    {
        var card = new FleetServerCard
        {
            ServerId = 10,
            DisplayName = "gone",
            ServerName = "gone",
            Band = FleetHealthBand.Offline,
            IsOnline = false,
            LastCollectionTime = new DateTime(2026, 10, 30, 1, 2, 0, DateTimeKind.Utc),
            FailedCollectorCount = 2,
        };

        var section = DailyHealthReportBuilder.AssembleServer(card, "Offline for ages", null, null, null, 10, 5);

        Assert.False(section.IsQuiet);
        Assert.Empty(section.Reasons);
        Assert.Equal(new[] { "Offline, last collection 2026-10-30 01:02 UTC", "2 collectors failing" }, section.CollectionProblems.ToArray());
    }

    [Fact]
    public void Period_EndsAtSendAtInTheZone_AndTheLatestEndedDateFollowsTheClock()
    {
        var zone = TimeZoneInfo.FindSystemTimeZoneById("UTC");
        var sendAt = new TimeOnly(7, 0);

        Assert.Equal(new DateTime(2026, 10, 31, 7, 0, 0, DateTimeKind.Utc), DailyHealthReportPeriod.EndUtcFor(new DateOnly(2026, 10, 31), sendAt, zone));
        Assert.Equal(new DateOnly(2026, 10, 31), DailyHealthReportPeriod.LatestEndedDate(new DateTime(2026, 10, 31, 7, 0, 0, DateTimeKind.Utc), sendAt, zone));
        Assert.Equal(new DateOnly(2026, 10, 30), DailyHealthReportPeriod.LatestEndedDate(new DateTime(2026, 10, 31, 6, 59, 0, DateTimeKind.Utc), sendAt, zone));
    }
}
