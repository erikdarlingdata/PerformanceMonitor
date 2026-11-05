/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7561: the Fleet page showed a red `Identity capacity CRIT ... 100% used, 0 left` tile on a server it counted as Healthy, and the top strip said
/// `0 Critical`. A column at its critical bar is an insert that fails, so it now bands the server Critical through the one classifier the web
/// fleet, the Viewer and the MCP reply share. A WARN identity, Agent alerts or Protection tile stays out of the band, and is listed in
/// Needs attention and sorted above a server with none.
/// </summary>
public sealed class IdentityCapacityCritBandsTheServerTests
{
    private static readonly DateTime Now = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private static FleetServerCard Card(
        int serverId,
        string name,
        FleetIdentityCapacity? identity = null,
        IReadOnlyList<FleetAgentAlertGap>? gaps = null,
        IReadOnlyList<FleetDatabaseProtectionOverdue>? overdue = null) =>
        DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(serverId, name, name, null, null, false),
            default,
            default,
            default,
            default,
            default,
            new DarlingFleetReader.BlockingRow(0, 0, 0, 0),
            new DarlingFleetReader.DeadlockRow(0, null),
            new DarlingFleetReader.PgDeadlockRow(0, null, 0),
            Now.AddSeconds(-30),
            new DarlingFleetReader.CollectorCounts(40, 0, 40, 0, null, null),
            null,
            Now,
            TimeSpan.FromHours(1),
            DeadlockRateThresholds.Default,
            agentAlertGaps: gaps,
            databaseProtectionOverdue: overdue,
            identityCapacity: identity);

    private static FleetIdentityCapacity Crit => new(IdentityCapacityMath.BandCritical, 1, 1, "Sales", "dbo.Orders", "Id", 100.0, "0");
    private static FleetIdentityCapacity Warn => new(IdentityCapacityMath.BandWarning, 2, 0);
    private static readonly IReadOnlyList<FleetAgentAlertGap> OneGap = new[] { new FleetAgentAlertGap("x", "Agent alert 823 is missing.") };

    [Fact]
    public void ACriticalIdentityTile_BandsTheServerCritical_AndTheStripAndTheMcpReplyAgree()
    {
        var healthy = Card(1, "plain");
        var withCrit = Card(2, "full-column", identity: Crit);

        Assert.Equal(FleetHealthBand.Healthy, healthy.Band);
        Assert.Equal(FleetHealthBand.Critical, withCrit.Band);
        Assert.Equal(HealthSeverity.Critical, withCrit.OverallMetricSeverity);
        /* The tile is not a seventh metric: the "N of M measured" qualifier is unchanged. */
        Assert.Equal(healthy.MeasuredMetricCount, withCrit.MeasuredMetricCount);
        Assert.Equal(healthy.MetricCount, withCrit.MetricCount);

        var rollup = DarlingFleetReader.BuildRollup(new[] { healthy, withCrit }, Now, Now.AddHours(-1), Now);
        Assert.Equal(1, rollup.CriticalCount);
        Assert.Equal(1, rollup.HealthyCount);

        /* The MCP reply and the web page read this same serialized card and rollup. */
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(rollup, DarlingFleetReader.JsonOptions));
        var ranked = doc.RootElement.GetProperty("worst_servers")[0];
        Assert.Equal("full-column", ranked.GetProperty("display_name").GetString());
        Assert.Equal("Critical", ranked.GetProperty("band").GetString());
        Assert.Contains("Identity capacity critical", ranked.GetProperty("reason").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, doc.RootElement.GetProperty("critical_count").GetInt32());
    }

    [Fact]
    public void ACriticalIdentityTile_BandsAServerCritical_EvenWhenNoMetricWasMeasured()
    {
        var metrics = new ServerHealthMetrics { IdentityCapacityCritical = true };
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.OverallMetricSeverity(metrics));
        Assert.Equal(FleetHealthBand.Critical, ServerHealthClassifier.ClassifyBand(true, false, false, HealthSeverity.Critical));
    }

    [Fact]
    public void TheViewersCard_BandsCriticalOnTheSameTile()
    {
        var card = new ServerSummaryItem
        {
            ServerName = "t",
            ServerId = 1,
            HealthyCollectorCount = 40,
            CollectorCount = 40,
            DeadlockWindow = TimeSpan.FromHours(1),
            BlockingWindow = TimeSpan.FromHours(1),
            LastCollectionTime = Now.AddSeconds(-30),
            IdentityCapacity = new IdentityCapacityCard(IdentityCapacityMath.BandCritical, 1, 1),
        };
        card.ApplyFreshness(Now);
        Assert.Equal(HealthSeverity.Critical, card.OverallMetricSeverity);
        Assert.Equal(FleetHealthBand.Critical, FleetRollup.ClassifyBand(card));

        card.IdentityCapacity = new IdentityCapacityCard(IdentityCapacityMath.BandWarning, 1, 0);
        Assert.NotEqual(HealthSeverity.Critical, card.OverallMetricSeverity);
    }

    [Fact]
    public void AWarnIdentityAgentOrProtectionTile_KeepsTheBand_ButIsListedInNeedsAttention()
    {
        var plain = Card(1, "plain");
        var warnIdentity = Card(2, "warn-identity", identity: Warn);
        var agent = Card(3, "agent", gaps: OneGap);
        var protection = Card(4, "protection", overdue: new[] { new FleetDatabaseProtectionOverdue("full", "Sales", "overdue", 90000, "Sales full backup overdue") });

        foreach (var c in new[] { warnIdentity, agent, protection })
        {
            Assert.Equal(FleetHealthBand.Healthy, c.Band);
            Assert.True(c.HasAdvisoryTile, c.DisplayName);
        }

        Assert.False(plain.HasAdvisoryTile);

        var rollup = DarlingFleetReader.BuildRollup(new[] { plain, warnIdentity, agent, protection }, Now, Now.AddHours(-1), Now);
        Assert.Equal(4, rollup.HealthyCount);
        Assert.Equal(0, rollup.WarningCount + rollup.CriticalCount + rollup.OfflineCount);
        Assert.Equal(3, rollup.WorstServers.Count);
        Assert.DoesNotContain(rollup.WorstServers, w => w.DisplayName == "plain");
        Assert.Contains("Agent alerts 1 gap", rollup.WorstServers.Single(w => w.DisplayName == "agent").Reason, StringComparison.Ordinal);
        Assert.Contains("Protection 1 overdue item", rollup.WorstServers.Single(w => w.DisplayName == "protection").Reason, StringComparison.Ordinal);

        /* The reply says WHY a Healthy server is listed: the tile names ride on the row, and on the card the web page reads. */
        Assert.Equal(new[] { "Identity capacity" }, rollup.WorstServers.Single(w => w.DisplayName == "warn-identity").AdvisoryTiles);
        Assert.Equal(new[] { "Agent alerts" }, rollup.WorstServers.Single(w => w.DisplayName == "agent").AdvisoryTiles);
        Assert.Equal(new[] { "Protection" }, rollup.WorstServers.Single(w => w.DisplayName == "protection").AdvisoryTiles);
        Assert.Empty(plain.AdvisoryTiles);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(rollup, DarlingFleetReader.JsonOptions));
        var agentRow = doc.RootElement.GetProperty("worst_servers").EnumerateArray().Single(r => r.GetProperty("display_name").GetString() == "agent");
        Assert.Equal("Healthy", agentRow.GetProperty("band_label").GetString());
        Assert.Equal("Agent alerts", agentRow.GetProperty("advisory_tiles")[0].GetString());
    }

    [Fact]
    public void TheViewersNeedsAttention_ListsAndRanksAWarnTileServerAboveAHealthyOne_WithoutChangingItsBand()
    {
        ServerSummaryItem Item(int id, string name, IReadOnlyList<AgentAlertGap>? gaps = null, IdentityCapacityCard? identity = null)
        {
            var item = new ServerSummaryItem
            {
                ServerName = name,
                DisplayName = name,
                ServerId = id,
                HealthyCollectorCount = 40,
                CollectorCount = 40,
                DeadlockWindow = TimeSpan.FromHours(1),
                BlockingWindow = TimeSpan.FromHours(1),
                LastCollectionTime = Now.AddSeconds(-30),
                IdentityCapacity = identity,
                AgentAlertGaps = gaps ?? Array.Empty<AgentAlertGap>(),
            };
            item.ApplyFreshness(Now);
            return item;
        }

        var plain = Item(1, "a-plain");
        var agent = Item(2, "b-agent", gaps: new[] { new AgentAlertGap("x", "Agent alert 823 is missing.", Array.Empty<string>(), Array.Empty<int>()) });
        var warnIdentity = Item(3, "c-warn-identity", identity: new IdentityCapacityCard(IdentityCapacityMath.BandWarning, 2, 0));
        var all = new[] { plain, agent, warnIdentity };

        /* The band and the dot stay Healthy: nothing here re-bands the server. */
        foreach (var s in all)
        {
            Assert.Equal(FleetHealthBand.Healthy, FleetRollup.ClassifyBand(s));
        }

        Assert.False(plain.HasAdvisoryTile);
        Assert.Equal(new[] { "Agent alerts" }, agent.AdvisoryTiles);
        Assert.Equal(new[] { "Identity capacity" }, warnIdentity.AdvisoryTiles);

        /* The Needs attention list and the worst-first ranking keep them and leave the plain one out. */
        Assert.Equal(new[] { "b-agent", "c-warn-identity" }, FleetRollup.NeedsAttention(all).Select(s => s.DisplayName).ToArray());
        var rollup = FleetRollup.Build(all, new FleetTotals(), totalServerCount: 3);
        Assert.Equal(3, rollup.HealthyCount);
        Assert.Equal(0, rollup.WarningCount + rollup.CriticalCount + rollup.OfflineCount);
        Assert.Equal(new[] { "b-agent", "c-warn-identity" }, rollup.WorstServers.Select(w => w.DisplayName).ToArray());
        Assert.All(rollup.WorstServers, w => Assert.Equal(FleetHealthBand.Healthy, w.Band));
        Assert.Contains("Agent alerts 1 gap", rollup.WorstServers[0].Reason, StringComparison.Ordinal);
        Assert.Contains("Identity capacity 2 columns at the warning bar", rollup.WorstServers[1].Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIdentityReason_SaysEachCountTruthfully_OnEverySurface()
    {
        /* One column at its limit and four near it: not "5 columns at the bar". */
        var mixed = new FleetIdentityCapacity(IdentityCapacityMath.BandCritical, 5, 1);
        var service = DarlingFleetReader.BuildReason(Card(1, "mixed", identity: mixed));
        Assert.Contains("Identity capacity critical (1 column at its limit, 4 near it)", service, StringComparison.Ordinal);
        Assert.DoesNotContain("at the bar", service, StringComparison.Ordinal);

        var viewer = ViewerCard(identity: new IdentityCapacityCard(IdentityCapacityMath.BandCritical, 5, 1));
        Assert.Equal(service, FleetRollup.BuildReason(viewer));

        Assert.Equal("Identity capacity critical (2 columns at their limit)", ServerHealthClassifier.IdentityCapacityReason(2, 2));
        Assert.Equal("Identity capacity critical (1 column at its limit)", ServerHealthClassifier.IdentityCapacityReason(1, 1));
        Assert.Equal("Identity capacity 1 column at the warning bar", ServerHealthClassifier.IdentityCapacityReason(1, 0));
    }

    [Fact]
    public void TheProtectionReason_IsTheServicesWords_OnTheServiceAndTheViewer()
    {
        var overdue = new[]
        {
            new FleetDatabaseProtectionOverdue("full", "Sales", "overdue", 90000, "Sales full backup overdue"),
            new FleetDatabaseProtectionOverdue("full", "Hr", "overdue", 90000, "Hr full backup overdue"),
            new FleetDatabaseProtectionOverdue("checkdb", "Hr", "overdue", 90000, "Hr CHECKDB overdue"),
        };
        var service = DarlingFleetReader.BuildReason(Card(1, "p", overdue: overdue));
        Assert.Equal("Protection 3 overdue items", service);

        /* The Viewer's chip says "3 items on 2 databases"; its Needs attention reason must not. */
        var viewer = ViewerCard();
        viewer.DatabaseProtectionChipValue = "3 items on 2 databases";
        viewer.DatabaseProtectionOverdueCount = 3;
        Assert.Equal("Protection 3 overdue items", FleetRollup.BuildReason(viewer));
    }

    private static ServerSummaryItem ViewerCard(IdentityCapacityCard? identity = null)
    {
        var item = new ServerSummaryItem
        {
            ServerName = "v",
            DisplayName = "v",
            ServerId = 1,
            HealthyCollectorCount = 40,
            CollectorCount = 40,
            DeadlockWindow = TimeSpan.FromHours(1),
            BlockingWindow = TimeSpan.FromHours(1),
            LastCollectionTime = Now.AddSeconds(-30),
            IdentityCapacity = identity,
        };
        item.ApplyFreshness(Now);
        return item;
    }

    private const string FleetJsPath = "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/fleet.js";

    /* Reads the shipped sort table and the needs-attention predicate out of fleet.js (no DOM is involved in either), then sorts a three-card
       fleet the way the page does and prints the names in order, plus which cards the Needs attention filter keeps. */
    private const string NodeScript =
        "const fs = require('node:fs');" +
        "const src = fs.readFileSync(process.argv[1], 'utf8');" +
        "const a = src.indexOf('const BAND_RANK');" +
        "const b = src.indexOf('\\n};', src.indexOf('const SORTS')) + 3;" +
        "const c = src.indexOf('function cardNeedsAttention(');" +
        "const d = src.indexOf('\\n}', c) + 2;" +
        "const code = (src.slice(a, b) + src.slice(c, d)).replace(/export /g, '');" +
        "const p = src.indexOf('function problemServerCount(');" +
        "const q = src.indexOf('\\n}', p) + 2;" +
        "const f = new Function(code + src.slice(p, q).replace(/export /g, '') + '\\nreturn { SORTS, cardNeedsAttention, problemServerCount };')();" +
        "const cards = JSON.parse(process.argv[2]);" +
        "console.log(JSON.stringify({ order: cards.slice().sort(f.SORTS.severity).map(x => x.display_name), attention: cards.filter(f.cardNeedsAttention).map(x => x.display_name), problems: f.problemServerCount(JSON.parse(process.argv[3])) }));";

    /* Runs the shipped page script over the cards of one fleet: the sort order, the Needs attention filter and the problem count the all-clear line reads.
       Null when Node is not installed. */
    private static async System.Threading.Tasks.Task<JsonDocument?> RunFleetScriptAsync(IReadOnlyList<FleetServerCard> cards)
    {
        var rollup = DarlingFleetReader.BuildRollup(cards, Now, Now.AddHours(-1), Now);
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add("-e");
        psi.ArgumentList.Add(NodeScript);
        psi.ArgumentList.Add(PathTo(FleetJsPath));
        psi.ArgumentList.Add(JsonSerializer.Serialize(rollup.Cards, DarlingFleetReader.JsonOptions));
        psi.ArgumentList.Add(JsonSerializer.Serialize(rollup, DarlingFleetReader.JsonOptions));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            return null;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = (await proc.StandardOutput.ReadToEndAsync()).Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the fleet script did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the fleet script failed: " + await error);
            return JsonDocument.Parse(output);
        }
    }

    [Fact]
    public async System.Threading.Tasks.Task TheFleetPage_ListsAndSortsAnAdvisoryServerAboveAHealthyOne_WithoutChangingItsBand()
    {
        var cards = new[]
        {
            Card(1, "a-healthy"),
            Card(2, "b-agent-alerts-warn", gaps: OneGap),
            Card(3, "c-identity-crit", identity: Crit),
        };
        using var doc = await RunFleetScriptAsync(cards);
        if (doc is null)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return;
        }

        var order = doc.RootElement.GetProperty("order").EnumerateArray().Select(e => e.GetString()).ToArray();
        var attention = doc.RootElement.GetProperty("attention").EnumerateArray().Select(e => e.GetString()).ToArray();

        /* Critical first, then the Healthy-banded server with a tile, then the plain one (which sorts first by name alone). */
        Assert.Equal(new[] { "c-identity-crit", "b-agent-alerts-warn", "a-healthy" }, order);
        Assert.Equal(new[] { "b-agent-alerts-warn", "c-identity-crit" }, attention);
        Assert.Equal(FleetHealthBand.Healthy, cards[1].Band);
        Assert.Equal(2, doc.RootElement.GetProperty("problems").GetInt32());
    }

    [Fact]
    public async System.Threading.Tasks.Task TheFleetPageAllClear_ShowsOnlyWhenNoServerIsListed_NotWhenAHealthyServerHasAWarnTile()
    {
        /* The all-clear line shows when the page's problem count is 0. One Healthy-banded server with a WARN tile leaves the band counts at
           0 Critical / 0 Warning / 0 Offline, and the page must still not say everything is healthy. */
        using var warnOnly = await RunFleetScriptAsync(new[] { Card(1, "a-healthy"), Card(2, "b-warn-identity", identity: Warn) });
        if (warnOnly is null)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return;
        }

        Assert.Equal(1, warnOnly.RootElement.GetProperty("problems").GetInt32());

        using var clear = await RunFleetScriptAsync(new[] { Card(1, "a-healthy"), Card(2, "b-healthy") });
        Assert.Equal(0, clear!.RootElement.GetProperty("problems").GetInt32());
    }
}
