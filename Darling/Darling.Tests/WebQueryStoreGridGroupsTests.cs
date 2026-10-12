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
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The server page's Query Store Regressions, Long Query Completions and Plan Corrections grids (#4843): their wide
/// columns sit behind column groups that are off at first, the choice survives the page's rebuild, and every
/// timestamp is a UTC instant the browser prints in its own local time. The page runs under Node
/// (<c>web-qs-grids-harness.mjs</c>, TZ pinned to a zone with a non-zero offset); Node is skipped when not installed.
/// </summary>
public sealed class WebQueryStoreGridGroupsTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.Environment["TZ"] = "America/New_York";
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-qs-grids-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the Query Store grids harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the Query Store grids harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string[] Arr(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static string[] Arr(JsonElement r, string grid, string name) => Arr(r.GetProperty(grid), name);

    private static string ServerTabsJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js")).ReplaceLineEndings("\n");

    private static List<(string Key, string Line)> Columns(string constant)
    {
        var js = ServerTabsJs;
        var start = js.IndexOf("const " + constant + " = [", StringComparison.Ordinal);
        Assert.True(start >= 0, constant);
        var end = js.IndexOf("\n];", start, StringComparison.Ordinal);
        return Regex.Matches(js[start..end], @"^\s*\{ key: ""([a-z_.]+)""[^\n]*$", RegexOptions.Multiline)
            .Select(m => (m.Groups[1].Value, m.Value)).ToList();
    }

    [Fact]
    public void EachGrid_StartsOnItsCoreColumns_WithATogglePerGroup()
    {
        var r = Run("defaults");
        Assert.Equal(new[] { "Columns:", "CPU and reads", "Executions and plans" }, Arr(r, "regressions", "toggles"));
        Assert.Equal(new[] { "Severity", "Database", "Query", "QueryID", "ExtraDuration", "Duration+%", "BaselineDuration", "RecentDuration", "CPU+%", "Reads+%", "LastExec" },
            Arr(r, "regressions", "heads"));
        Assert.Equal(new[] { "Columns:", "I/O and rows", "Session" }, Arr(r, "completions", "toggles"));
        Assert.Equal(new[] { "Time", "Statement", "EventType", "Duration", "CPU", "Result", "Database", "Object", "QueryHash" },
            Arr(r, "completions", "heads"));
        Assert.Equal(new[] { "Columns:", "Plans", "Plan metrics", "Lifecycle" }, Arr(r, "corrections", "toggles"));
        Assert.Equal(new[] { "Collected", "Query", "Database", "State", "StateReason", "Reason", "Score", "Est.gain(s)", "QueryID" },
            Arr(r, "corrections", "heads"));
    }

    [Fact]
    public void TurningAGroupOn_ShowsItsColumns_AndOffHidesThemAgain()
    {
        var r = Run("toggling");
        Assert.Contains("ValidSince", Arr(r, "lifecycleOn"));
        Assert.DoesNotContain("RegressedPlan", Arr(r, "lifecycleOn"));
        Assert.Contains("RegressedPlan", Arr(r, "plansOn"));
        Assert.Contains("RevertedAt", Arr(r, "plansOn"));
        Assert.DoesNotContain("ValidSince", Arr(r, "allOff"));
        Assert.DoesNotContain("RegressedPlan", Arr(r, "allOff"));
        Assert.Equal(new[] { "BaseExecs", "RecentExecs", "BaselinePlans", "RecentPlans" },
            Arr(r, "regressionsExecs").Where(h => h.StartsWith("Base", StringComparison.Ordinal) && h != "BaselineDuration" || h.StartsWith("Recent", StringComparison.Ordinal) && h != "RecentDuration").ToArray());
        Assert.Equal(new[] { "SPID", "App", "Login" }, Arr(r, "completionsSession").Skip(8).Take(3).ToArray());
    }

    [Fact]
    public void TheToggles_SurviveTheSixtySecondRebuild_PerGrid()
    {
        var r = Run("repaint");
        Assert.Contains("ValidSince", Arr(r, "corrections"));
        Assert.DoesNotContain("RegressedPlan", Arr(r, "corrections"));
        Assert.Contains("LogicalReads", Arr(r, "completions"));
        Assert.DoesNotContain("SPID", Arr(r, "completions"));
        Assert.DoesNotContain("BaseExecs", Arr(r, "regressions"));
    }

    [Fact]
    public void AUtcInstant_PrintsInTheBrowsersLocalTime_InEveryTimestampColumn()
    {
        var r = Run("localTime");
        var expected = r.GetProperty("expected").GetString()!;
        Assert.Equal("1/15/2026, 12:30:00 PM", expected);
        // Collected, Valid Since, Last Refresh and Executed At (Lifecycle is on in this scenario).
        var cells = Arr(r, "cells");
        foreach (var i in new[] { 0, 9, 10, 12 }) Assert.Equal(expected, cells[i]);
        Assert.Equal(expected, Arr(r, "regressionCells").Last());
        Assert.Equal(expected, Arr(r, "completionCells").First());
    }

    [Theory]
    [InlineData("QUERY_STORE_REGRESSION_COLUMNS", "QUERY_STORE_REGRESSION_GROUPS", "get_query_store_regressions")]
    [InlineData("LONG_QUERY_COLUMNS", "LONG_QUERY_GROUPS", "get_long_query_completions")]
    [InlineData("PLAN_CORRECTION_COLUMNS", "PLAN_CORRECTION_GROUPS", "get_plan_corrections")]
    public void EveryGroupedColumn_NamesAGroupTheGridDeclares_AndNoGroupIsOnByDefault(string columns, string groups, string read)
    {
        var js = ServerTabsJs;
        var decl = Regex.Match(js, "const " + groups + @" = \{ groups: \[([^\]]*)\], defaultGroups: \[([^\]]*)\] \};");
        Assert.True(decl.Success, groups);
        var declared = Regex.Matches(decl.Groups[1].Value, "\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.Equal("", decl.Groups[2].Value.Trim());
        var used = Columns(columns).Select(c => Regex.Match(c.Line, @"group: ""([^""]+)""")).Where(m => m.Success).Select(m => m.Groups[1].Value).Distinct().ToList();
        Assert.Equal(declared.OrderBy(x => x), used.OrderBy(x => x));
        Assert.Contains(read, js);
    }

    [Fact]
    public void EveryTimestampColumn_IsAnInstantTheBrowserRendersLocally_AndTheirSourcesAreUtcInTheCensus()
    {
        foreach (var constant in new[] { "QUERY_STORE_REGRESSION_COLUMNS", "LONG_QUERY_COLUMNS", "PLAN_CORRECTION_COLUMNS" })
            foreach (var (key, line) in Columns(constant))
                if (key.EndsWith("_time", StringComparison.Ordinal) || key is "valid_since" or "last_refresh")
                    Assert.True(line.Contains("format: \"time\"", StringComparison.Ordinal), constant + "." + key);

        // Frame-ambiguity: none. The three sources are stored UTC (collector-written or UTC-reporting), so the reads
        // write them as stored with "o" and the census registers each as Utc.
        var census = ReadRepoFile("Darling", "Darling.Tests", "ConsumedTimestampFrameDisciplineTests.cs");
        Assert.Contains("(\"query_store_stats\", \"last_execution_time\", ClockFrame.Utc", census);
        Assert.Contains("\"long_query_completions.event_time=Utc\"", census);
        foreach (var stamp in new[] { "execute_action_initiated_time", "last_refresh", "revert_action_initiated_time", "valid_since" })
            Assert.Contains("\"plan_correction." + stamp + "=Utc\"", census);
    }
}
