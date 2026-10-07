/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the FinOps Server Inventory tab: it reads get_finops_inventory with view server_inventory and shows only keys that read emits.</summary>
public sealed class FinOpsTabServerInventoryPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "server-inventory.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsInventoryTools.cs")
            .ReplaceLineEndings("\n");

    private static string Slice(string startMarker)
    {
        var source = ToolSource();
        var start = source.IndexOf(startMarker, System.StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("\n        };", start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return source.Substring(start, end - start);
    }

    private static string RowSlice() => Slice("internal static object InventoryRow(");

    private static string EnvelopeSlice() => Slice("internal static object Envelope(");

    private static System.Collections.Generic.List<string> Keys() =>
        Regex.Matches(Tab(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();

    [Fact]
    public void TheTabReadsTheFleetInventoryWithTheMaxLimit()
    {
        var tab = Tab();
        Assert.Contains("readTool(\"get_finops_inventory\", { view: \"server_inventory\", limit: LIMIT }, ctx && ctx.signal)", tab);
        var limit = Regex.Match(tab, "(?m)^const LIMIT = (\\d+);$");
        Assert.True(limit.Success);
        Assert.Equal(DarlingMcpFinOpsInventoryTools.MaxLimit, int.Parse(limit.Groups[1].Value));
        Assert.DoesNotContain("{ server,", tab);
    }

    [Fact]
    public void EveryShownColumnKeyIsEmittedByTheInventoryRow()
    {
        var keys = Keys();
        var row = RowSlice();
        foreach (var key in keys)
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", row);
        // engine_edition is emitted but not shown, as on the desktop.
        Assert.Equal(26, Regex.Matches(row, "(?m)^\\s+[a-z_]+ = ").Count);
        Assert.Equal(25, keys.Count);
    }

    [Fact]
    public void TheColumnsAreInTheDesktopGridOrder()
    {
        Assert.Equal(
            "server,edition,product_version,host_os_version,cpu_count,physical_memory_mb,socket_count,cores_per_socket,hardware_note,"
            + "provisioning_status,avg_cpu_pct,storage_total_gb,idle_db_count,sqlserver_start_time_local,inventory_as_of,last_collected,"
            + "monitoring,monthly_cost_usd,annual_cost_usd,health_score,health_band,license_warning,is_hadr_enabled,ag_replica_role,is_clustered",
            string.Join(",", Keys()));
    }

    [Theory]
    [InlineData("servers")]
    [InlineData("truncated")]
    [InlineData("total_servers")]
    [InlineData("hardware_note_legend")]
    [InlineData("cpu_window_hours")]
    [InlineData("idle_window_days")]
    public void TheEnvelopeKeysTheTabReadsAreEmitted(string key)
    {
        Assert.Matches("(?m)^\\s+" + key + " = ", EnvelopeSlice());
        Assert.Contains("data." + key, Tab());
    }

    [Fact]
    public void TheHostScopedCodeShowsTheLegendText()
    {
        var tab = Tab();
        Assert.Contains("legend[r.hardware_note] ?? r.hardware_note", tab);
        Assert.Contains("data.hardware_note_legend", tab);
        Assert.DoesNotContain("host_scoped", tab);
        Assert.DoesNotContain("Azure SQL Database: memory", tab);
    }

    [Fact]
    public void AnOmittedKeyShowsAsADashBecauseTheRowIsSpreadAndNeverFilled()
    {
        var tab = Tab();
        Assert.Contains("return {\n    ...r,", tab);
        Assert.DoesNotContain("r.edition ??", tab);
        Assert.DoesNotContain("?? \"\"", tab.Replace("String(r.ag_replica_role ?? \"\")", ""));
    }

    [Fact]
    public void TheHealthColourComesFromThePayloadBand()
    {
        Assert.Contains("const BAND_SEV = { good: \"Healthy\", fair: \"Warning\", poor: \"Critical\" };", Tab());
        Assert.Equal("good", FinOpsUtilizationFigures.BandGood);
        Assert.Equal("fair", FinOpsUtilizationFigures.BandFair);
        Assert.Equal("poor", FinOpsUtilizationFigures.BandPoor);
    }

    [Fact]
    public void TheStartTimeShowsMinutesAndAnUnknownStatusDropsUnderscores()
    {
        var tab = Tab();
        Assert.Contains("r.sqlserver_start_time_local.slice(0, 16).replace(\"T\", \" \")", tab);
        Assert.Contains("r.provisioning_status.replace(/_/g, \" \")", tab);
    }

    [Fact]
    public void TheBandColumnIsColouredByTheHealthSeverity()
    {
        Assert.Contains("{ key: \"health_band\", label: \"Band\", sevKey: \"health_sev\" }", Tab());
    }

    [Fact]
    public void TheStatusLabelsAreTheProvisioningVerdictTokens()
    {
        var tab = Tab();
        Assert.Contains(ProvisioningVerdict.RightSized + ": [", tab);
        Assert.Contains(ProvisioningVerdict.OverProvisioned + ": [", tab);
        Assert.Contains(ProvisioningVerdict.UnderProvisioned + ": [", tab);
        Assert.Contains(ProvisioningVerdict.NotApplicable + ": [\"" + ProvisioningVerdict.NotApplicableLabel + "\"", tab);
    }

    [Fact]
    public void TheServerClockStartTimeIsNotParsedAsUtc()
    {
        Assert.Contains("{ key: \"sqlserver_start_time_local\", label: \"Start time (server clock)\" }", Tab());
    }

    [Fact]
    public void TheTabKeepsThePayloadOrder()
    {
        Assert.DoesNotContain(".sort(", Tab());
    }

    [Fact]
    public void TheTruncatedNoticeNamesTheShownCountThenTheTotal()
    {
        Assert.Contains("(data.truncated ? \" (the first \" + n + \" of \" + data.total_servers + \")\" : \"\")", Tab());
    }

    [Fact]
    public void TheReadStatesAreHandled()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"aborted\" \\|\\| res\\.kind === \"auth\"\\) return;$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"empty\"\\) return mount\\(body, emptyStrip\\(res\\.message\\)\\);$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"error\"\\) return mount\\(body, readErrorStrip\\(res\\.message\\)\\);$", tab);
        Assert.Contains("Could not render this tab: ", tab);
    }

    [Fact]
    public void TheTabBuildsItsDomFromTextOnly()
    {
        Assert.DoesNotContain("innerHTML", Tab());
    }

    [Fact]
    public void TheTabImportsOnlyTheSharedHelpers()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\";").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../util.js" }));
    }

    [Fact]
    public void TheStubTextIsGone()
    {
        Assert.DoesNotContain("Not on the web yet", Tab());
    }

    [Fact]
    public async System.Threading.Tasks.Task TheTabParsesUnderNode()
    {
        var path = Path.Combine(Path.GetTempPath(), "server-inventory-" + System.Guid.NewGuid().ToString("N") + ".mjs");
        File.WriteAllText(path, Tab());
        try
        {
            var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            psi.ArgumentList.Add("--check");
            psi.ArgumentList.Add(path);
            Process proc;
            try
            {
                proc = Process.Start(psi)!;
            }
            catch (Win32Exception)
            {
                Assert.Skip("Node is not installed, so the tab script cannot be parsed.");
                throw;
            }

            using (proc)
            {
                var error = proc.StandardError.ReadToEndAsync();
                if (!proc.WaitForExit(20000))
                {
                    proc.Kill(entireProcessTree: true);
                    Assert.Fail("node --check did not finish in 20 s");
                }

                Assert.True(proc.ExitCode == 0, "node --check failed: " + await error);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }
}
