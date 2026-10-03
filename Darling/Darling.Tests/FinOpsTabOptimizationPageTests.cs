/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>Source pins for the FinOps Optimization tab: it reads get_finops with view optimization and each of its five tables shows only keys that section's row function emits.</summary>
public sealed class FinOpsTabOptimizationPageTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "optimization.js")
            .ReplaceLineEndings("\n");

    private static string ToolSource() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFinOpsTools.Optimization.cs")
            .ReplaceLineEndings("\n");

    /// <summary>The keys of one COLUMNS array, in order.</summary>
    private static System.Collections.Generic.List<string> Keys(string constName)
    {
        var tab = Tab();
        var start = tab.IndexOf("const " + constName + " = [", System.StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = tab.IndexOf("\n];", start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return Regex.Matches(tab.Substring(start, end - start), "\\bkey: \"([a-z_0-9]+)\"").Select(m => m.Groups[1].Value).ToList();
    }

    /// <summary>
    /// One row function's object initializer. The slice starts at the first "new" after the method name and ends at the
    /// initializer's close: "\n    };" for an expression-bodied method, "\n        }).ToList();" for one that selects rows.
    /// </summary>
    private static string RowSlice(string method, string close)
    {
        var source = ToolSource();
        var at = source.IndexOf(" " + method + "(", System.StringComparison.Ordinal);
        Assert.True(at >= 0);
        var start = source.IndexOf("new\n", at, System.StringComparison.Ordinal);
        Assert.True(start > at);
        var end = source.IndexOf(close, start, System.StringComparison.Ordinal);
        Assert.True(end > start);
        return source.Substring(start, end - start);
    }

    private static void AssertKeysMatchRow(string constName, string method, string close, int count)
    {
        var keys = Keys(constName);
        var row = RowSlice(method, close);
        foreach (var key in keys)
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", row);
        var emitted = Regex.Matches(row, "(?m)^\\s+[a-z_0-9]+ = ").Count;
        Assert.Equal(count, emitted);
        Assert.Equal(emitted, keys.Count);
    }

    [Fact]
    public void TheTabReadsGetFinOpsWithTheOptimizationViewWindowAndTopTwenty()
    {
        // Pins the exact read: the view's limit is the expensive-query top-N, and 20 matches the desktop's "Top 20 by CPU".
        const string call = "readTool(\"get_finops\", { server, view: \"optimization\", hours: HOURS, limit: LIMIT }, ctx && ctx.signal)";
        var tab = Tab();
        Assert.Contains(call, tab);
        Assert.Matches("(?m)^const HOURS = 24;$", tab);
        Assert.Matches("(?m)^const LIMIT = 20;$", tab);
        Assert.Single(Regex.Matches(tab, "readTool\\("));
    }

    [Fact]
    public void EveryIdleColumnKeyIsEmittedByTheIdleDatabaseRow() =>
        AssertKeysMatchRow("IDLE_COLUMNS", "IdleDatabaseRow", "\n    };", 4);

    [Fact]
    public void EveryTempdbColumnKeyIsEmittedByTheTempdbRow() =>
        AssertKeysMatchRow("TEMPDB_COLUMNS", "TempdbRow", "\n    };", 4);

    [Fact]
    public void EveryWaitColumnKeyIsEmittedByTheWaitCategoryRows() =>
        AssertKeysMatchRow("WAIT_COLUMNS", "WaitCategoryRows", "\n        }).ToList();", 7);

    [Fact]
    public void EveryQueryColumnKeyIsEmittedByTheExpensiveQueryRows() =>
        AssertKeysMatchRow("QUERY_COLUMNS", "ExpensiveQueryRows", "\n        }).ToList();", 9);

    [Fact]
    public void EveryMemoryGrantColumnKeyIsEmittedByTheMemoryGrantRows() =>
        AssertKeysMatchRow("GRANT_COLUMNS", "MemoryGrantRows", "\n        }).ToList();", 10);

    [Fact]
    public void TheColumnsAreInTheDesktopGridOrder()
    {
        // Idle Databases grid: Darling/PerformanceMonitor.Darling.Viewer/FinOpsTab.xaml, FinOpsIdleDatabasesDataGrid (~:758-765).
        Assert.Equal("database_name,total_size_mb,file_count,last_execution_server_local", string.Join(",", Keys("IDLE_COLUMNS")));
        // tempdb Pressure grid: FinOpsTab.xaml (~:771-779).
        Assert.Equal("metric,current_mb,peak_24h_mb,warning", string.Join(",", Keys("TEMPDB_COLUMNS")));
        // Wait Stats Summary grid: FinOpsTab.xaml (~:797-806).
        Assert.Equal("category,total_wait_time_ms,waiting_tasks,pct_of_total,top_wait_type,top_wait_time_ms,est_cost_usd", string.Join(",", Keys("WAIT_COLUMNS")));
        // Expensive Queries grid: FinOpsTab.xaml (~:825-835).
        Assert.Equal("database_name,query_preview,total_cpu_ms,avg_cpu_ms_per_exec,total_reads,avg_reads_per_exec,executions,has_plan,est_cost_usd", string.Join(",", Keys("QUERY_COLUMNS")));
        // Memory Grant Efficiency grid: FinOpsTab.xaml (~:844-855).
        Assert.Equal("day,avg_granted_mb,avg_used_mb,efficiency_pct,peak_granted_mb,wasted_mb,total_grantees,total_waiters,timeout_errors,forced_grants", string.Join(",", Keys("GRANT_COLUMNS")));
        Assert.Contains("{ key: \"est_cost_usd\", label: \"Est. cost ($)\", format: \"num2\" }", Tab());
        Assert.Contains("{ key: \"total_cpu_ms\", label: \"Total CPU\", format: \"ms\" }", Tab());
    }

    [Fact]
    public void TheServerLocalLastExecutionIsPlainTextNeverTheTimeFormatter()
    {
        var tab = Tab();
        Assert.Contains("{ key: \"last_execution_server_local\", label: \"Last execution (server time)\" },", tab);
        Assert.DoesNotContain("last_execution_server_local\", label: \"Last execution (server time)\", format", tab);
    }

    [Fact]
    public void EachSectionReadsItsOwnStatusAndMapsTheThreeStates()
    {
        var tab = Tab();
        Assert.Contains("if (section.status === \"ok\") {", tab);
        Assert.Contains("content = [noticeStrip(notice(section)), VIZ.table(section, { rowsKey: \"rows\", columns, emptyText })];", tab);
        Assert.Contains("} else if (section.status === \"empty\") {", tab);
        Assert.Contains("content = [emptyStrip(emptyText)];", tab);
        Assert.Contains("content = [noticeStrip(section.message ?? \"This section was not collected.\")];", tab);
        foreach (var field in new[] { "idle_databases", "tempdb_pressure", "wait_categories", "expensive_queries", "memory_grant_efficiency" })
            Assert.Contains("data." + field + ",", tab);
    }

    [Fact]
    public void TheSectionsAreInTheDesktopOrder()
    {
        var tab = Tab();
        var titles = new[] { "\"Idle Databases\"", "\"tempdb Pressure\"", "\"Wait Stats Summary\"", "\"Expensive Queries (Top \" + LIMIT + \" by CPU)\"", "\"Memory Grant Efficiency\"" };
        var at = titles.Select(t => tab.IndexOf("sectionView(" + t, System.StringComparison.Ordinal)).ToList();
        Assert.All(at, i => Assert.True(i >= 0));
        Assert.Equal(at.OrderBy(i => i).ToList(), at);
    }

    [Fact]
    public void TheNoticesAndTheCostLineAreExact()
    {
        var tab = Tab();
        Assert.Contains("const n = (s.rows || []).length;", tab);
        Assert.Contains("let text = (n === 1 ? \"1 idle database\" : n + \" idle databases\") + \" over the last \" + (s.window_days ?? \"?\") + \" days\";", tab);
        Assert.Contains("text += s.truncated ? \"; the top \" + n + \" of \" + (s.database_count ?? \"more\") + \".\" : \".\";", tab);
        Assert.Contains("return \"last \" + (s.window_hours ?? HOURS) + \" hours\";", tab);
        Assert.Contains("let text = \"Top \" + (s.rows || []).length + \" by CPU, last \" + (s.window_hours ?? HOURS) + \" hours\";", tab);
        Assert.Contains("if (s.effective_start) text += \", from \" + applyFormat(\"time\", s.effective_start);", tab);
        Assert.Contains("\"Estimated costs are shares of a monthly cost of $\" + applyFormat(\"num2\", data.monthly_cost_usd) + \".\"", tab);
        Assert.Contains(": (data.cost_reason ?? \"monthly cost not set\");", tab);
    }

    [Fact]
    public void TheReadStatesAreHandledAbortedAndAuthReturnAndEmptyShowsItsMessage()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"aborted\" \\|\\| res\\.kind === \"auth\"\\) return;$", tab);
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"empty\"\\) return mount\\(body, emptyStrip\\(res\\.message\\)\\);$", tab);
    }

    [Fact]
    public void TheTabImportsOnlyTheSharedHelpers()
    {
        var imports = Regex.Matches(Tab(), "from \"([^\"]+)\";").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(imports);
        Assert.All(imports, i => Assert.Contains(i, new[] { "../../panels.js", "../../charts.js", "../../util.js" }));
    }

    [Fact]
    public void TheStubTextIsGone()
    {
        Assert.DoesNotContain("Not on the web yet", Tab());
    }

    [Fact]
    public void TheErrorAndRenderFailureStatesAreHandled()
    {
        var tab = Tab();
        Assert.Matches("(?m)^\\s+if \\(res\\.kind === \"error\"\\) return mount\\(body, readErrorStrip\\(res\\.message\\)\\);$", tab);
        Assert.Contains("Could not render this tab: ", tab);
    }
}
