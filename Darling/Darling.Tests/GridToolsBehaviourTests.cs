/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Copy and CSV export on the web dashboard's shared table renderer (#4843), from the shipped <c>panels.js</c> and
/// <c>grid-tools.js</c> run under Node (<c>web-grid-tools-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class GridToolsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-grid-tools-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the grid tools harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the grid tools harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    [Fact]
    public void Csv_UsesRawValues_QuotesPerRfc4180_NeutralisesFormulas_AndLeavesOutListColumns()
    {
        var r = Run("csv");
        var csv = Str(r, "csv");
        // Header is the column labels; the Tags column holds arrays, so it is left out.
        Assert.StartsWith("Name,N,When\r\n", csv);
        // Commas, quotes and a newline in one cell: quoted, quotes doubled, newline kept inside the quotes.
        Assert.Contains("\"b, \"\"quoted\"\"\nline\",1234567,2026-01-02T03:04:05Z\r\n", csv);
        // Raw number (no thousands separator), stored ISO instant (not the local display), empty for null, unicode intact.
        Assert.Contains("naïve — 日本,-3,\r\n", csv);
        // A leading = or - on text gets a leading apostrophe; a negative number is data and is left alone.
        Assert.Contains("'=SUM(A1),5,2026-01-01T00:00:00Z", csv);
        Assert.Contains("'-cmd,40,", csv);
        Assert.StartsWith("slow-queries-", Str(r, "fileName"));
        Assert.EndsWith(".csv", Str(r, "fileName"));
        Assert.Equal("Exported 4 rows.", Str(r, "status"));
    }

    [Fact]
    public void Csv_FollowsTheActiveSort()
    {
        var r = Run("csvSorted");
        string[] Names(string key) => Str(r, key).Split("\r\n").Skip(1).Select(l => l.Split(',')[0]).ToArray();
        Assert.Equal(new[] { "naïve — 日本", "'=SUM(A1)", "'-cmd", "\"b" }, Names("csv"));
        Assert.Equal(new[] { "\"b", "'-cmd", "'=SUM(A1)", "naïve — 日本" }, Names("csvDesc"));
    }

    [Fact]
    public void Copy_PutsTheFormattedTextOnTheClipboard_TabSeparated_AndFollowsTheSort()
    {
        var r = Run("copy");
        Assert.Equal("Click a cell first, then choose Copy cell.", Str(r, "beforePick"));
        var texts = r.GetProperty("texts").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Equal(3, texts.Length);
        Assert.Equal("5", texts[0]);
        Assert.StartsWith("=SUM(A1)\t5\t", texts[1]);
        var all = texts[2].Split('\n');
        Assert.Equal("Name\tN\tWhen\tTags", all[0]);
        // Sorted ascending by N, with the thousands separator the grid shows (the CSV carries 1234567).
        Assert.StartsWith("naïve — 日本\t-3\t", all[1]);
        Assert.Contains("\t1,234,567\t", all[4]);
        // A newline inside a cell must not split the row.
        Assert.StartsWith("b, \"quoted\" line\t", all[4]);
        Assert.Equal(5, all.Length);
        Assert.Equal("Copied the table.", Str(r, "status"));
    }

    [Fact]
    public void WithoutAClipboard_EveryCopySaysSo_AndARefusedWriteSaysWhy()
    {
        var r = Run("noClipboard");
        Assert.Contains("isn't available", Str(r, "all"));
        Assert.Equal(Str(r, "all"), Str(r, "cell"));
        Assert.Equal("Copy failed: denied", Str(r, "denied"));
    }

    [Fact]
    public void TheRowHook_IsCalledOncePerRow_AndRowClassStylesTheRow_AndToolsCanBeLeftOut()
    {
        var r = Run("hook");
        Assert.Equal(new long[] { 1234567, 5, -3, 40 }, r.GetProperty("seen").EnumerateArray().Select(e => e.GetInt64()).ToArray());
        Assert.Equal(new[] { "1234567", "5", "-3", "40" }, r.GetProperty("attrs").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal(new[] { "hot", "", "", "" }, r.GetProperty("classes").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.All(r.GetProperty("stringClass").EnumerateArray(), e => Assert.Equal("flag", e.GetString()));
        Assert.Equal("div", Str(r, "noTools"));
    }

    [Fact]
    public void TheHelpers_QuoteNeutraliseAndNameFiles()
    {
        var r = Run("helpers");
        Assert.Equal(new[] { "\"a,b\"", "\"q\"\"q\"", "'=1+1", "'+x", "'@y", "-5", "", "plain" },
            r.GetProperty("field").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal("wait-stats-top-10-20260105-143007.csv", Str(r, "name"));
        Assert.Equal("table-20260105-143007.csv", Str(r, "nameEmpty"));
    }
}
