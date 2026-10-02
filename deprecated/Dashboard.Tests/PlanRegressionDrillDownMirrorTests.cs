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
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// #3902, the frozen Dashboard's mirror of the Darling/Lite drill-down fixes.
///
/// <para>The regressed-queries drill-down re-ran the PLAN_REGRESSION fact's whole detection to list its top
/// five, deduplicating the Query Store slice a second time in the same pass. The fact now stamps the
/// (database, query_id) pairs it reported on <c>AnalysisContext.PlanRegressionOffenders</c>, and the
/// drill-down restricts its dedup to them, reading the whole slice only when there is no fact to follow.</para>
///
/// <para>The parameter-sensitivity drill-down carried each row's compressed varbinary(max) statement through
/// its window sort to print five. It now carries the row's clustered key and fetches the text for the five
/// rows it prints.</para>
///
/// <para>Pinned on the source text, the only idiom this project has for its SQL-Server-side reads (there is no
/// test database). The old and new SQL were also run side by side on a scratch database on SQL Server 2016
/// (compatibility level 130, the oldest the product supports) and returned identical rows for all three reads.</para>
/// </summary>
public class PlanRegressionDrillDownMirrorTests
{
    private const string FactFile = "SqlServerFactCollector.QueryPerf.cs";
    private const string DrillFile = "SqlServerDrillDownCollector.Queries.cs";

    [Fact]
    public void ThePlanRegressionFact_StampsTheQueriesItReported_AfterClearingAnyEarlierStamp()
    {
        var body = Normalize(MethodBody(Read(FactFile), "CollectPlanRegressionFactsAsync"));

        /* Cleared before the read, stamped after it: a read that fails leaves "not known", never a stale list. */
        var cleared = body.IndexOf("context.PlanRegressionOffenders = null;", StringComparison.Ordinal);
        var tried = body.IndexOf("\n        try\n", StringComparison.Ordinal);
        var stamped = body.IndexOf("context.PlanRegressionOffenders = offenders;", StringComparison.Ordinal);
        Assert.True(cleared >= 0 && cleared < tried, "the stamp must be cleared before the read");
        Assert.True(stamped > tried, "the stamp must be written after the read");

        /* The database is appended after the eight columns the reader takes by ordinal, and read at ordinal 8. */
        Assert.Matches(new Regex(@"regression_factor,\s+database_name\s+FROM compared"), StripComments(body));
        Assert.Contains("reader.GetString(8)", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRegressedQueriesDrillDown_RestrictsItsDedupToTheOffenders_AndReadsTheWholeSliceWithoutThem()
    {
        var body = Normalize(MethodBody(Read(DrillFile), "CollectRegressedQueries"));

        /* Spliced into the dedup's own WHERE, right after the window bound and before the CTE closes: a filter
           anywhere later would still deduplicate the whole slice first. */
        Assert.Contains(
            "AND   server_last_execution_time >= @windowStart\" + offenderFilter + @\"\n),\nplan_agg AS",
            body,
            StringComparison.Ordinal);

        /* No stamp, or an empty one, is no filter at all. */
        Assert.Contains("context.PlanRegressionOffenders is { Count: > 0 } reported ? reported : null", body, StringComparison.Ordinal);
        Assert.Contains("var offenderFilter = string.Empty;", body, StringComparison.Ordinal);

        /* One parameter per value, never values spliced into the text. */
        Assert.Contains("\"\\n    AND   database_name IN (\"", body, StringComparison.Ordinal);
        Assert.Contains("\"\\n    AND   query_id IN (\"", body, StringComparison.Ordinal);
        Assert.Contains("new SqlParameter(\"@offenderDatabase\" + i, databases[i])", body, StringComparison.Ordinal);
        Assert.Contains("new SqlParameter(\"@offenderQueryId\" + i, queryIds[i])", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheParameterSensitivityDrillDown_FetchesTextForThePrintedRowsOnly()
    {
        var sql = StripComments(MethodBody(Read(DrillFile), "CollectParameterSensitiveQueries"));

        var windowStart = sql.IndexOf("WITH latest AS", StringComparison.Ordinal);
        var cut = sql.IndexOf("FETCH NEXT 5 ROWS ONLY", StringComparison.Ordinal);
        var decompress = sql.IndexOf("DECOMPRESS(qs.query_text)", StringComparison.Ordinal);
        Assert.True(windowStart >= 0 && cut > windowStart, "the windowed CTE and the cut should both still be there");

        /* The window and the cut carry no text... */
        Assert.DoesNotContain("query_text", sql[windowStart..cut], StringComparison.Ordinal);

        /* ...the text is decompressed after the cut, from the same row, by its clustered key. */
        Assert.True(decompress > cut, "the text must be fetched after FETCH NEXT 5 ROWS ONLY");
        Assert.Contains("qs.collection_time = o.collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("qs.collection_id = o.collection_id", sql, StringComparison.Ordinal);
    }

    /// <summary>The method's text from its signature to the next member's signature (or the end of the file).</summary>
    private static string MethodBody(string src, string method)
    {
        var start = src.IndexOf($"private async Task {method}(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"method not found: {method}");
        var next = src.IndexOf("private async Task ", start + method.Length, StringComparison.Ordinal);
        return next < 0 ? src[start..] : src[start..next];
    }

    /// <summary>SQL line comments out, so a pin cannot pass on prose that names the shape it forbids.</summary>
    private static string StripComments(string text) => Regex.Replace(text, @"--[^\r\n]*", string.Empty);

    private static string Normalize(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string Read(string file)
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, $"Could not locate the repo root (a directory containing deprecated/Dashboard/Dashboard.csproj) by walking up from {AppContext.BaseDirectory}.");
        var path = Path.Combine(root!, "deprecated", "Dashboard", "Analysis", file);
        Assert.True(File.Exists(path), $"Source file not found: {path}");
        return File.ReadAllText(path);
    }

    /// <summary>Walks up from the test output directory to the repo root — the directory holding the Dashboard project. Mirrors DashboardMirrorPinTests.</summary>
    private static string? FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (int i = 0; i < 10 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir.FullName, "deprecated", "Dashboard", "Dashboard.csproj")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        return null;
    }
}
