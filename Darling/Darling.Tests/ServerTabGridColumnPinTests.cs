using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Pins the Query Store Regressions, Plan Corrections and Long Queries grids on the server page to the field
/// names their reads return: a column key the read does not emit renders an empty column, so each key is
/// checked against the tool source.
/// </summary>
public sealed class ServerTabGridColumnPinTests
{
    private static string ServerTabsJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js")).ReplaceLineEndings("\n");

    private static string Tool(string file) => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "Mcp", file)).ReplaceLineEndings("\n");

    private static List<(string Key, string Line)> Columns(string constant)
    {
        var js = ServerTabsJs;
        var start = js.IndexOf("const " + constant + " = [", StringComparison.Ordinal);
        Assert.True(start >= 0, constant);
        var end = js.IndexOf("\n];", start, StringComparison.Ordinal);
        return Regex.Matches(js[start..end], @"^\s*\{ key: ""([a-z_.]+)""[^\n]*$", RegexOptions.Multiline)
            .Select(m => (m.Groups[1].Value, m.Value)).ToList();
    }

    private static void EveryKeyIsReturnedBy(string constant, string toolFile, params string[] expected)
    {
        var tool = Tool(toolFile);
        var cols = Columns(constant);
        foreach (var (key, _) in cols)
            Assert.True(Regex.IsMatch(tool, @"\b" + key + @"\s*="), $"{constant}: '{key}' is not a field of {toolFile}");
        foreach (var key in expected)
            Assert.Contains(cols, c => c.Key == key);
        Assert.Equal(cols.Count, cols.Select(c => c.Key).Distinct().Count());
        // Numeric columns use numeric formats, never pre-formatted *_text strings.
        Assert.DoesNotContain(cols, c => c.Key.EndsWith("_text", StringComparison.Ordinal) && c.Key != "query_text");
    }

    [Fact]
    public void QueryStoreRegressions_CarriesBaseExecsAndBothReadsColumns() =>
        EveryKeyIsReturnedBy("QUERY_STORE_REGRESSION_COLUMNS", "DarlingMcpQueryStoreRegressionTools.cs",
            "baseline_exec_count", "baseline_reads", "recent_reads");

    [Fact]
    public void PlanCorrections_CarriesEveryFieldTheReadReturnsExceptTheDuplicatedFlags() =>
        EveryKeyIsReturnedBy("PLAN_CORRECTION_COLUMNS", "DarlingMcpPlanCorrectionTools.cs",
            "regressed_plan_id", "last_good_plan_id", "last_good_plan_forcing_type",
            "last_good_plan_force_failure_reason", "regressed_plan_execution_count",
            "regressed_plan_cpu_time_average_ms", "last_good_plan_execution_count",
            "last_good_plan_cpu_time_average_ms", "valid_since", "last_refresh",
            "execute_action_initiated_by", "execute_action_initiated_time",
            "revert_action_initiated_by", "revert_action_initiated_time", "recommendation_state_reason");

    [Fact]
    public void PlanCorrections_TimestampColumnsAreInstants()
    {
        foreach (var (key, line) in Columns("PLAN_CORRECTION_COLUMNS"))
            if (key is "valid_since" or "last_refresh" || key.EndsWith("_time", StringComparison.Ordinal))
                Assert.Contains("format: \"time\"", line);
    }

    [Fact]
    public void LongQueries_CarriesEventTypeReadsWritesHashAndLoginButNoHost()
    {
        EveryKeyIsReturnedBy("LONG_QUERY_COLUMNS", "DarlingMcpLongQueryTools.cs",
            "event_type", "logical_reads", "physical_reads", "writes", "query_hash", "server_principal_name");
        Assert.DoesNotContain(Columns("LONG_QUERY_COLUMNS"), c => c.Key.Contains("host", StringComparison.Ordinal));
    }
}
