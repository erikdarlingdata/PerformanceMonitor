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

    private static FleetIdentityCapacity Crit => new(IdentityCapacityMath.BandCritical, 1, "Sales", "dbo.Orders", "Id", 100.0, "0");
    private static FleetIdentityCapacity Warn => new(IdentityCapacityMath.BandWarning, 2);
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
            IdentityCapacity = new IdentityCapacityCard(IdentityCapacityMath.BandCritical, 1),
        };
        card.ApplyFreshness(Now);
        Assert.Equal(HealthSeverity.Critical, card.OverallMetricSeverity);
        Assert.Equal(FleetHealthBand.Critical, FleetRollup.ClassifyBand(card));

        card.IdentityCapacity = new IdentityCapacityCard(IdentityCapacityMath.BandWarning, 1);
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
        "const f = new Function(code + '\\nreturn { SORTS, cardNeedsAttention };')();" +
        "const cards = JSON.parse(process.argv[2]);" +
        "console.log(JSON.stringify({ order: cards.slice().sort(f.SORTS.severity).map(x => x.display_name), attention: cards.filter(f.cardNeedsAttention).map(x => x.display_name) }));";

    [Fact]
    public async System.Threading.Tasks.Task TheFleetPage_ListsAndSortsAnAdvisoryServerAboveAHealthyOne_WithoutChangingItsBand()
    {
        var cards = new[]
        {
            Card(1, "a-healthy"),
            Card(2, "b-agent-alerts-warn", gaps: OneGap),
            Card(3, "c-identity-crit", identity: Crit),
        };
        var rollup = DarlingFleetReader.BuildRollup(cards, Now, Now.AddHours(-1), Now);
        var json = JsonSerializer.Serialize(rollup.Cards, DarlingFleetReader.JsonOptions);

        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add("-e");
        psi.ArgumentList.Add(NodeScript);
        psi.ArgumentList.Add(PathTo(FleetJsPath));
        psi.ArgumentList.Add(json);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return;
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
            using var doc = JsonDocument.Parse(output);
            var order = doc.RootElement.GetProperty("order").EnumerateArray().Select(e => e.GetString()).ToArray();
            var attention = doc.RootElement.GetProperty("attention").EnumerateArray().Select(e => e.GetString()).ToArray();

            /* Critical first, then the Healthy-banded server with a tile, then the plain one (which sorts first by name alone). */
            Assert.Equal(new[] { "c-identity-crit", "b-agent-alerts-warn", "a-healthy" }, order);
            Assert.Equal(new[] { "b-agent-alerts-warn", "c-identity-crit" }, attention);
            Assert.Equal(FleetHealthBand.Healthy, cards[1].Band);
        }
    }
}
