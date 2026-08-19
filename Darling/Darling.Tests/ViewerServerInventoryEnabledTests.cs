/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2359: Server Inventory says whether a server is still monitored, so an old <c>Last Updated</c> is legible.
///
/// <para><b>The bug.</b> The grid lists every REGISTERED server and joined <c>servers</c> without ever reading
/// <c>is_enabled</c>. A decommissioned server therefore kept the <c>collection_time</c> it had when monitoring
/// stopped — accurate, and read by every operator as a broken freshness column. Measured on a 61-server fleet the
/// split was total: 19 disabled servers all last collected within five minutes of each other on the day they were
/// removed, and 42 enabled servers all fresh within a minute of each other. Nothing was stale; nineteen things
/// were finished.</para>
///
/// <para>The rows are kept rather than filtered out. This is the FinOps tab, and a decommissioned server's cost
/// history is exactly what someone opens it to look at — dropping them would trade a confusing grid for a lying
/// one.</para>
/// </summary>
public class ViewerServerInventoryEnabledTests
{
    private static string InventorySource => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Inventory.cs"));

    /// <summary>
    /// The projection carries <c>is_enabled</c>, and it comes from the REGISTRY rather than from the properties
    /// snapshot — <c>server_properties</c> has no such column, and a disabled server's newest snapshot is
    /// precisely the row that cannot tell you it is disabled.
    /// </summary>
    [Fact]
    public void TheInventoryQuery_SelectsIsEnabledFromTheRegistry()
    {
        Assert.Contains("s.is_enabled", InventorySource, StringComparison.Ordinal);
    }

    /// <summary>
    /// The whole fleet a person still runs sorts first. A grid that interleaves nineteen finished servers with
    /// forty-two live ones is the same confusion in a different arrangement.
    /// </summary>
    [Fact]
    public void TheInventoryQuery_SortsLiveServersFirst()
    {
        Assert.Contains("ORDER BY s.is_enabled DESC, server_name", InventorySource, StringComparison.Ordinal);
    }

    /// <summary>
    /// <b>The ordinal shift, pinned.</b> Inserting a column mid-projection moves every ordinal after it, and a
    /// positional reader does not fail when that happens — it keeps reading, one column off, and turns a cost
    /// into a boolean. That exact mistake shipped once already in the Aurora detection query, so the
    /// neighbours are asserted by number rather than trusted.
    ///
    /// <para>#2359's <c>last_collection</c> was appended LAST for the same reason: at ordinal 19 it moves
    /// nothing that came before it.</para>
    /// </summary>
    [Fact]
    public void TheReader_ReadsIsEnabledAt17_MonthlyCostAt18_AndLastCollectedAt19()
    {
        Assert.Contains("IsEnabled = reader.IsDBNull(17)", InventorySource, StringComparison.Ordinal);
        Assert.Contains("MonthlyCost = reader.IsDBNull(18)", InventorySource, StringComparison.Ordinal);
        Assert.Contains("LastCollected = reader.IsDBNull(19)", InventorySource, StringComparison.Ordinal);

        /* And nothing still reads the pre-shift position for the cost. */
        Assert.DoesNotContain("MonthlyCost = reader.IsDBNull(17)", InventorySource, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2359, the actual reported bug: <c>server_properties</c> ships with <c>FrequencyMinutes 0</c> — "collect
    /// once on server load only" — so its timestamp is the last service start, not a heartbeat. Calling it
    /// <c>Last Updated</c> made every actively-monitored server on a long-running install look stale.
    ///
    /// <para>The field is named for what it is, and the value people were actually asking for is carried
    /// alongside it rather than instead of it — a decommissioned server still needs its snapshot time.</para>
    /// </summary>
    [Fact]
    public void TheGrid_SeparatesTheConfigSnapshotFromRealFreshness()
    {
        var xaml = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml"));

        Assert.Contains("{Binding InventoryAsOf", xaml, StringComparison.Ordinal);
        Assert.Contains("Text=\"Inventory As Of\"", xaml, StringComparison.Ordinal);
        Assert.Contains("{Binding LastCollected", xaml, StringComparison.Ordinal);
        Assert.Contains("Text=\"Last Collected\"", xaml, StringComparison.Ordinal);

        /* The misleading label must not come back. */
        Assert.DoesNotContain("Text=\"Last Updated\"", xaml, StringComparison.Ordinal);
        Assert.DoesNotContain("{Binding LastUpdated", xaml, StringComparison.Ordinal);
    }

    /// <summary>
    /// Freshness comes from the collection log, not from the properties snapshot — reading it off
    /// <c>server_properties</c> would just reproduce the bug under a better column name.
    /// </summary>
    [Fact]
    public void FreshnessComesFromTheCollectionLog()
    {
        Assert.Contains("FROM v_collection_log", InventorySource, StringComparison.Ordinal);
        Assert.Contains("AS last_collection", InventorySource, StringComparison.Ordinal);
    }

    /// <summary>
    /// SELECT order and reader order have to agree, so the count is derived from the SQL rather than restated:
    /// <c>is_enabled</c> must sit immediately before <c>monthly_cost_usd</c>, which is what makes 17/18 correct.
    /// </summary>
    [Fact]
    public void TheProjection_PutsIsEnabledImmediatelyBeforeMonthlyCost()
    {
        var enabled = InventorySource.IndexOf("s.is_enabled", StringComparison.Ordinal);
        var cost = InventorySource.IndexOf("COALESCE(s.monthly_cost_usd, 0) AS monthly_cost_usd", StringComparison.Ordinal);

        Assert.True(enabled > 0 && cost > 0, "both columns must be present");
        Assert.True(enabled < cost, "is_enabled must be selected before monthly_cost_usd");

        var between = InventorySource[enabled..cost];
        Assert.DoesNotContain(",", between[(between.IndexOf(',', StringComparison.Ordinal) + 1)..], StringComparison.Ordinal);
    }

    /// <summary>
    /// Disabled rows are KEPT. A future edit that "fixes" the confusing dates by filtering them away has to
    /// argue with this: it would silently drop cost history from the tab that exists to show cost history.
    /// </summary>
    [Fact]
    public void TheInventoryQuery_DoesNotFilterOutDisabledServers()
    {
        Assert.DoesNotContain("WHERE s.is_enabled", InventorySource, StringComparison.Ordinal);
        Assert.DoesNotContain("AND s.is_enabled", InventorySource, StringComparison.Ordinal);
        Assert.DoesNotContain("is_enabled = true", InventorySource, StringComparison.Ordinal);
    }

    /// <summary>The grid actually shows it — a flag nobody can see fixes nothing.</summary>
    [Fact]
    public void TheGrid_ShowsTheMonitoringColumn()
    {
        var xaml = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml"));

        Assert.Contains("{Binding MonitoringStatus}", xaml, StringComparison.Ordinal);

        /* #2181/#2331: a column carrying an explicit Style whose key lives in MainWindow.xaml's window
           resources resolves at parse time and throws when the grid is realized. This one carries none. */
        var at = xaml.IndexOf("{Binding MonitoringStatus}", StringComparison.Ordinal);
        var column = xaml[(xaml.LastIndexOf('<', at))..(xaml.IndexOf("/>", at, StringComparison.Ordinal) + 2)];
        Assert.DoesNotContain("StaticResource", column, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
