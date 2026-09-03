/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2807: the server-detail Daily Summary showed "Health: NaN". Two source pins guard the fix (this repo
/// ships no JS test runner, so these scan the frontend source the same way the composer drift guards do):
/// the numeric formatters must reject a non-finite value to "—", and the Daily Summary must not carry a
/// numeric card for <c>overall_health</c>, which is the band LABEL (== <c>health_band</c>), never a number.
/// </summary>
public class DailySummaryHealthFormatTests
{
    [Fact]
    public void NumericFormatters_GuardNonFinite()
    {
        var util = FrontendSource("util.js");
        // fmtInt / fmtNum / fmtPct / fmtMb must degrade a non-finite value to "—" (the guard fmtMs already
        // carried), so a stray non-numeric value can never render as "NaN" / "NaN%" / "NaN MB".
        foreach (var fn in new[] { "fmtInt", "fmtNum", "fmtPct", "fmtMb" })
        {
            Assert.Contains("isFinite", FunctionBody(util, fn), StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DailySummary_HasNoNumericOverallHealthCard()
    {
        var daily = DailyStatsBlock(FrontendSource("pages/server-tabs.js"));
        // overall_health is DailyHealthBandCalculator.Label(HealthBand) — the band's text label, the same
        // value health_band carries — so it must not be a Daily Summary card (a num1 one rendered it "NaN").
        Assert.DoesNotContain("key: \"overall_health\"", daily, StringComparison.Ordinal);
        // the band is still shown, once, as text:
        Assert.Contains("key: \"health_band\"", daily, StringComparison.Ordinal);
    }

    /// <summary>The brace-balanced body of a top-level <c>function name(...) { ... }</c> in the source.</summary>
    private static string FunctionBody(string src, string fnName)
    {
        var m = Regex.Match(src, @"function\s+" + Regex.Escape(fnName) + @"\s*\([^)]*\)\s*\{");
        Assert.True(m.Success, $"{fnName} not found in util.js (did the formatters move?)");
        int i = m.Index + m.Length, depth = 1;
        while (i < src.Length && depth > 0)
        {
            if (src[i] == '{') depth++;
            else if (src[i] == '}') depth--;
            i++;
        }
        return src.Substring(m.Index, i - m.Index);
    }

    /// <summary>The <c>const DAILY_STATS = [ ... ];</c> array literal text.</summary>
    private static string DailyStatsBlock(string src)
    {
        const string start = "const DAILY_STATS = [";
        int a = src.IndexOf(start, StringComparison.Ordinal);
        Assert.True(a >= 0, "DAILY_STATS not found in server-tabs.js (did the Daily Summary move?)");
        int b = src.IndexOf("];", a, StringComparison.Ordinal);
        Assert.True(b >= 0, "DAILY_STATS close not found");
        return src.Substring(a, b - a);
    }

    private static string FrontendSource(string relPath, [CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Service", "wwwroot", "js", relPath));
        Assert.True(File.Exists(path), $"{relPath} not found at {path} (did the frontend move?)");
        return File.ReadAllText(path);
    }
}
