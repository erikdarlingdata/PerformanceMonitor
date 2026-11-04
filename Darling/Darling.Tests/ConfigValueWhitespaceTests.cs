/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7416 item 3: a setting whose new value only gained a trailing space (<c>log_line_prefix</c>) drew From and To cells that read
/// the same. When the two differ only in whitespace, both cells show it. The rule is run in C# (the desktop grids) and as shipped in
/// <c>config-values.js</c> under Node (the web grids, skipped when Node is not installed). Lite twin: <c>ConfigValueWhitespaceRowTests</c>.
/// </summary>
public sealed class ConfigValueWhitespaceTests
{
    [Fact]
    public void ATrailingSpace_ShowsOnBothCells()
    {
        Assert.Equal("“%m·[%p]·”", ConfigValueText.Shown("%m [%p] ", "%m [%p]"));
        Assert.Equal("“%m·[%p]”", ConfigValueText.Shown("%m [%p]", "%m [%p] "));
    }

    [Theory]
    [InlineData("FULL", "SIMPLE")]
    [InlineData("on", "on")]
    [InlineData("a b", "a  c")]
    public void AnythingElse_DrawsAsStored(string old, string @new)
    {
        Assert.Equal(old, ConfigValueText.Shown(old, @new));
        Assert.Equal(@new, ConfigValueText.Shown(@new, old));
    }

    [Fact]
    public void ATabAndALineBreakAreNamed_AndANullStaysNull()
    {
        Assert.Equal("“a→b”", ConfigValueText.Shown("a\tb", "a b"));
        Assert.Equal("“a↵b”", ConfigValueText.Shown("a\nb", "ab"));
        Assert.Null(ConfigValueText.Shown(null, " "));
    }

    [Fact]
    public void TheDesktopRow_ShowsTheWhitespace()
    {
        var row = new DatabaseConfigChangeRow(new ConfigChangeDiff.DatabaseConfigChange(DateTime.UtcNow, "Sales", "collation_name", "Latin1 ", "Latin1"));

        Assert.Equal("“Latin1·”", row.OldValue);
        Assert.Equal("“Latin1”", row.NewValue);
    }

    [Fact]
    public void TheWebGrids_ShowTheWhitespace_RunningTheShippedScript()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "config-values-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped web script cannot be run.");
            return;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEnd();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            Assert.True(proc.WaitForExit(30000), "the config values harness did not finish in 30 s");
            Assert.True(proc.ExitCode == 0, "the config values harness failed: " + error);

            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            var cases = doc.RootElement;
            Assert.Equal("“%m·[%p]·”", cases[0][0].GetString());
            Assert.Equal("“%m·[%p]”", cases[0][1].GetString());
            Assert.Equal("“a→b”", cases[1][0].GetString());
            Assert.Equal("FULL", cases[2][0].GetString());
            Assert.Equal("SIMPLE", cases[2][1].GetString());
            Assert.Equal(JsonValueKind.Null, cases[3][0].ValueKind);
            Assert.Equal("“”", cases[4][0].GetString());
            Assert.Equal("“·”", cases[4][1].GetString());
        }
    }
}
