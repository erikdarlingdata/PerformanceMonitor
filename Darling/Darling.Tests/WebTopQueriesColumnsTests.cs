/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins for the web Top Queries grid: it shows the desktop grid's columns in the desktop's order, every column key
/// is a field the read can return, the columns are grouped with the core set always on, the page asks for the full
/// detail, and both aggregate statements select the detail columns the reader maps by position.
/// </summary>
public sealed class WebTopQueriesColumnsTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js")
            .ReplaceLineEndings("\n");

    private static string Tools() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs").ReplaceLineEndings("\n");

    private static string ColumnsBlock()
    {
        var tab = Tab();
        var start = tab.IndexOf("const TOP_QUERY_COLUMNS = [", StringComparison.Ordinal);
        Assert.True(start >= 0, "TOP_QUERY_COLUMNS not found");
        return tab[start..tab.IndexOf("\n];", start, StringComparison.Ordinal)];
    }

    private static List<string> Keys() =>
        Regex.Matches(ColumnsBlock(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();

    /// <summary>Every field name the read can put on a row: the base projection plus the detail fields.</summary>
    private static HashSet<string> ReadFields()
    {
        var src = Tools();
        var start = src.IndexOf("Name = \"get_top_queries_by_cpu\"", StringComparison.Ordinal);
        var end = src.IndexOf("Name = \"get_top_procedures_by_cpu\"", start, StringComparison.Ordinal);
        var seg = src[start..end];
        var sel = seg.IndexOf("rows.Select(r =>", StringComparison.Ordinal);
        var close = seg.IndexOf("\n            }, r.Detail, full)", sel, StringComparison.Ordinal);
        Assert.True(sel >= 0 && close > sel, "row projection not found");
        var fields = Regex.Matches(seg[sel..close], "^                ([a-z_]+) = ", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToHashSet();
        var detail = src[src.IndexOf("private static JsonObject WithDetail", StringComparison.Ordinal)..];
        detail = detail[..detail.IndexOf("return node;\n    }", StringComparison.Ordinal)];
        foreach (Match m in Regex.Matches(detail, "Put\\(\"([a-z_]+)\""))
        {
            fields.Add(m.Groups[1].Value);
        }

        return fields;
    }

    [Fact]
    public void EveryColumnKeyIsAFieldTheReadReturns()
    {
        var keys = Keys();
        var fields = ReadFields();
        Assert.Empty(keys.Except(fields));
        Assert.Equal(keys.Count, keys.Distinct().Count());
    }

    [Fact]
    public void TheGridShowsTheDesktopColumnsInTheDesktopOrder_WithTheQueryRightOfTheDatabase()
    {
        Assert.Equal(new[]
        {
            "database_name", "query_text", "host_object", "last_execution_time", "creation_time", "execution_count",
            "total_cpu_ms", "avg_cpu_ms", "worker_time_per_second", "plan_generation_num", "total_clr_ms",
            "total_elapsed_ms", "avg_elapsed_ms", "total_logical_reads", "avg_reads", "total_logical_writes",
            "total_physical_reads", "total_rows", "total_spills", "min_cpu_ms", "max_cpu_ms", "min_elapsed_ms",
            "max_elapsed_ms", "min_physical_reads", "max_physical_reads", "min_rows", "max_rows", "min_grant_kb",
            "max_grant_kb", "min_used_grant_kb", "max_used_grant_kb", "min_ideal_grant_kb", "max_ideal_grant_kb",
            "min_spills", "max_spills", "min_dop", "max_dop", "min_reserved_threads", "max_reserved_threads",
            "min_used_threads", "max_used_threads", "query_hash", "query_plan_hash", "sql_handle", "plan_handle",
        }, Keys());
    }

    [Fact]
    public void TheColumnsAreGrouped_TheCoreSetIsUngrouped_AndNoGroupIsOnAtFirst()
    {
        var js = Tab();
        var cols = ColumnsBlock();
        var used = Regex.Matches(cols, "group: \"([^\"]+)\"").Select(m => m.Groups[1].Value).Distinct().ToArray();
        var decl = Regex.Match(js, @"const TOP_QUERY_GROUPS = \{\s*groups: \[([^\]]*)\],\s*defaultGroups: \[([^\]]*)\],?\s*\};");
        Assert.True(decl.Success, "TOP_QUERY_GROUPS must declare groups and defaultGroups.");
        var listed = Regex.Matches(decl.Groups[1].Value, "\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(listed.OrderBy(x => x), used.OrderBy(x => x));
        Assert.Empty(Regex.Matches(decl.Groups[2].Value, "\"([^\"]+)\"").Select(m => m.Groups[1].Value));

        var core = Regex.Matches(cols, @"^  \{ key: ""([a-z_]+)""(?![^\n]*group:)", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(new[]
        {
            "database_name", "query_text", "host_object", "execution_count", "total_cpu_ms", "avg_cpu_ms",
            "total_elapsed_ms", "avg_elapsed_ms", "total_logical_reads", "avg_reads", "total_spills", "query_hash",
        }, core);
    }

    [Fact]
    public void BothTopQueriesGridsPassTheGroupsAndTheFullDetailRead()
    {
        var js = Tab();
        Assert.Equal(2, Regex.Matches(js, @"detail: ""full"" \}").Count);
        Assert.Equal(2, Regex.Matches(js, @"get_top_queries_by_cpu"",\s*\{ server, hours: ctx\.hours, top: 20, detail").Count);
        Assert.Contains("groups: TOP_QUERY_GROUPS.groups", js, StringComparison.Ordinal);
        Assert.Matches(@"TOP_QUERY_COLUMNS,[^;]*?TOP_QUERY_GROUPS\s*\)", js);
    }

    [Fact]
    public void TheDetailParameterIsAnAppendedOptionalWhoseDefaultIsSummary()
    {
        var src = Tools();
        Assert.Contains("string detail = \"summary\",\n        CancellationToken cancellationToken = default)", src, StringComparison.Ordinal);
        var web = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Contains("PTextDefault(\"detail\", \"summary\")", web, StringComparison.Ordinal);
        Assert.Contains("detail: Str(c, \"detail\") ?? \"summary\"", web, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("TopQueriesSql")]
    [InlineData("TopQueriesByHostObjectSql")]
    public void BothAggregatesSelectTheDetailColumnsTheReaderMapsByPosition(string which)
    {
        var sql = which == "TopQueriesSql" ? DarlingDataReader.TopQueriesSql : DarlingDataReader.TopQueriesByHostObjectSql;
        var outer = sql[sql.LastIndexOf("r.database_name,", StringComparison.Ordinal)..];
        var select = outer[..outer.IndexOf("FROM ranked", StringComparison.Ordinal)];
        var cols = Regex.Matches(select, @"^\s+(?:r\.|t\.|CAST\()([a-z_0-9]+)", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToList();
        Assert.Equal(new[]
        {
            "last_execution_time", "creation_time", "min_physical_reads", "max_physical_reads", "min_rows", "max_rows",
            "min_grant_kb", "max_grant_kb", "min_used_grant_kb", "max_used_grant_kb", "min_ideal_grant_kb",
            "max_ideal_grant_kb", "min_spills", "max_spills", "min_reserved_threads", "max_reserved_threads",
            "min_used_threads", "max_used_threads", "total_clr_time", "plan_generation_num", "worker_time_per_second",
        }, cols.Skip(cols.IndexOf("last_execution_time")).ToArray());
        Assert.Contains("distinct_query_hashes", select, StringComparison.Ordinal);
        Assert.Contains("MAX(last_execution_time) AS last_execution_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFullRowOmitsNullsAndTheHourlyTierSendsNoDetail()
    {
        var src = Tools();
        Assert.Contains("if (value is not null)", src, StringComparison.Ordinal);
        Assert.Contains("if (!full || d is null)", src, StringComparison.Ordinal);
        var reader = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");
        Assert.Contains("DistinctQueryHashes: 1));", reader, StringComparison.Ordinal);
    }
}
