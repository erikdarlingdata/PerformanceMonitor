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
/// Source pins for the web Top Procedures grid: it shows the desktop grid's columns in the desktop's order, every column
/// key is a field the read can return, the columns are grouped with the core set always on, both grids ask for the full
/// detail, and the aggregate selects the detail columns the reader maps by position.
/// </summary>
public sealed class WebTopProceduresColumnsTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js")
            .ReplaceLineEndings("\n");

    private static string Tools() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs").ReplaceLineEndings("\n");

    private static string ColumnsBlock()
    {
        var tab = Tab();
        var start = tab.IndexOf("const TOP_PROC_COLUMNS = [", StringComparison.Ordinal);
        Assert.True(start >= 0, "TOP_PROC_COLUMNS not found");
        return tab[start..tab.IndexOf("\n];", start, StringComparison.Ordinal)];
    }

    private static List<string> Keys() =>
        Regex.Matches(ColumnsBlock(), "\\bkey: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();

    /// <summary>Every field name the read can put on a row: the base projection plus the detail fields.</summary>
    private static HashSet<string> ReadFields()
    {
        var src = Tools();
        /* #5226: the row projection lives in the ranked sibling the web dispatch calls; the MCP tool delegates to it. */
        var start = src.IndexOf("internal static async Task<string> GetTopProceduresRanked(", StringComparison.Ordinal);
        var seg = src[start..];
        var sel = seg.IndexOf("rows.Select(r =>", StringComparison.Ordinal);
        var close = seg.IndexOf("}, r.Detail,", sel, StringComparison.Ordinal);
        Assert.True(sel >= 0 && close > sel, "row projection not found");
        var fields = Regex.Matches(seg[sel..close], "^                ([a-z_]+) = ", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToHashSet();
        var detail = src[src.IndexOf("private static JsonObject WithProcedureDetail", StringComparison.Ordinal)..];
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
        Assert.Empty(keys.Except(ReadFields()));
        Assert.Equal(keys.Count, keys.Distinct().Count());
    }

    [Fact]
    public void TheGridShowsTheDesktopColumnsInTheDesktopOrder()
    {
        Assert.Equal(new[]
        {
            "database_name", "full_name", "object_type", "last_execution_time", "cached_time", "execution_count",
            "total_cpu_ms", "avg_cpu_ms", "total_elapsed_ms", "avg_elapsed_ms", "total_logical_reads", "avg_reads",
            "total_logical_writes", "total_physical_reads", "total_spills", "avg_spills", "min_cpu_ms", "max_cpu_ms",
            "min_elapsed_ms", "max_elapsed_ms", "min_logical_reads", "max_logical_reads", "min_physical_reads",
            "max_physical_reads", "min_logical_writes", "max_logical_writes", "min_spills", "max_spills",
        }, Keys());
    }

    [Fact]
    public void TheColumnsAreGrouped_TheCoreSetIsUngrouped_AndNoGroupIsOnAtFirst()
    {
        var js = Tab();
        var cols = ColumnsBlock();
        var used = Regex.Matches(cols, "group: \"([^\"]+)\"").Select(m => m.Groups[1].Value).Distinct().ToArray();
        var decl = Regex.Match(js, @"const TOP_PROC_GROUPS = \{\s*groups: \[([^\]]*)\],\s*defaultGroups: \[([^\]]*)\],?\s*\};");
        Assert.True(decl.Success, "TOP_PROC_GROUPS must declare groups and defaultGroups.");
        var listed = Regex.Matches(decl.Groups[1].Value, "\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(listed.OrderBy(x => x), used.OrderBy(x => x));
        Assert.Empty(Regex.Matches(decl.Groups[2].Value, "\"([^\"]+)\"").Select(m => m.Groups[1].Value));

        var core = Regex.Matches(cols, @"^  \{ key: ""([a-z_]+)""(?![^\n]*group:)", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(new[]
        {
            "database_name", "full_name", "object_type", "execution_count", "total_cpu_ms", "avg_cpu_ms",
            "total_elapsed_ms", "avg_elapsed_ms", "total_logical_reads", "avg_reads", "total_spills",
        }, core);
        Assert.Matches(@"key: ""last_execution_time"", label: ""Last Execution"", format: ""time""", cols);
        Assert.Matches(@"key: ""cached_time"", label: ""Cached Time"", format: ""time""", cols);
    }

    [Fact]
    public void BothTopProceduresGridsPassTheGroupsAndTheFullDetailRead()
    {
        var js = Tab();
        Assert.Equal(2, Regex.Matches(js,
            @"""get_top_procedures_by_cpu"",\s*rankedParams\(\{ server, hours: ctx\.hours, top: 20, detail: ""full"" \}, ranking\),\s*""procedures"",\s*\[\.\.\.TOP_PROC_COLUMNS, procedurePlanColumn\(server\)\],").Count);
        /* #5226: the ranking's retention notice is a second note on the grid, and the selector is its last argument. */
        Assert.Equal(2, Regex.Matches(js, @"""truncation_note"",\s*RANKING_NOTE_KEYS,\s*TOP_PROC_GROUPS,\s*picker\s*\)").Count);
    }

    [Fact]
    public void TheDetailParameterIsAnAppendedOptionalWhoseDefaultIsSummary()
    {
        var src = Tools();
        Assert.Contains("string? as_of = null,\n        [Description(\"'summary' (default) or 'full': full adds the remaining desktop columns (times, read/write/spill extremes), omitting nulls.\")] string detail = \"summary\",\n        CancellationToken cancellationToken = default)",
            src, StringComparison.Ordinal);
        var web = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Matches(@"\[""get_top_procedures_by_cpu""\] = R\([^\n]*PAsOf\(\), PTextDefault\(""detail"", ""summary""\)\)", web);
        /* #5226: the web dispatch calls the ranked sibling, so the MCP tool keeps its own signature. */
        Assert.Matches(@"GetTopProceduresRanked\([^\n]*detail: Str\(c, ""detail""\) \?\? ""summary""", web);
    }

    [Fact]
    public void TheAggregateSelectsTheDetailColumnsTheReaderMapsByPosition()
    {
        var sql = DarlingDataReader.TopProceduresSql;
        var select = sql[..sql.IndexOf("FROM procedure_stats", StringComparison.Ordinal)];
        var aliases = Regex.Matches(select, @"AS ([a-z_]+),?\s*$", RegexOptions.Multiline).Select(m => m.Groups[1].Value).ToList();
        Assert.Equal(new[]
        {
            "max_elapsed_time", "last_execution_time", "cached_time", "min_logical_reads", "max_logical_reads",
            "min_physical_reads", "max_physical_reads", "min_logical_writes", "max_logical_writes", "min_spills", "max_spills",
        }, aliases.Skip(aliases.IndexOf("max_elapsed_time")).ToArray());
        var reader = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs").ReplaceLineEndings("\n");
        Assert.Contains("ReadTopProcedureDetail(reader, 17, clock)", reader, StringComparison.Ordinal);
        Assert.Contains("Time(0), Time(1),\n            Long(2), Long(3), Long(4), Long(5), Long(6), Long(7), Long(8), Long(9));", reader, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTimestampsAreConvertedFromTheServerClock_AndTheHourlyTierSaysItCarriesNoDetail()
    {
        var src = Tools();
        Assert.Contains("last_execution_time = d.LastExecutionTime?.ToString(\"o\")", src, StringComparison.Ordinal);
        Assert.Contains("cached_time = d.CachedTime?.ToString(\"o\")", src, StringComparison.Ordinal);
        Assert.Contains("if (value is not null)", src, StringComparison.Ordinal);
        Assert.Contains("if (!full || d is null)", src, StringComparison.Ordinal);
        Assert.Contains("detail=full was asked, but the hourly rollup does not carry the detail fields (last_execution_time, cached_time,", src, StringComparison.Ordinal);
        var reader = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");
        Assert.Contains("DateTime? Time(int i) => DarlingServerClockReader.ToUtc(clock, reader, first + i);", reader, StringComparison.Ordinal);
    }
}
