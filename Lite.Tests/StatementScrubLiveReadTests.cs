/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Lite.Tests.Helpers;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348, the Lite reads that are not the collector's: a row or plan read live from the monitored server never passes
/// through the collection-time filter, so it is judged where it is read. Two sources were left raw: the Live Snapshot
/// button's own read (the statement text and both plans of every running request, which then reach the grid, every
/// copy and CSV of it, and the .sqlplan a row's Download saves) and the "Get Actual Plan" re-run (the plan the
/// monitored server returns). Pinned here: the Live Snapshot row, and a scan that every re-run call site passes its
/// result through <c>LivePlanDisplay.Filter</c> and that no new live read of the monitored server appears unnoticed.
/// </summary>
public sealed class StatementScrubLiveReadTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    private static object[] SnapshotRow(string text, string? estimatedPlan, string? livePlan)
    {
        var row = new object[35];
        Array.Fill(row, DBNull.Value);
        row[0] = 55;                                  /* session_id */
        row[3] = text;                                /* query text */
        row[4] = estimatedPlan ?? (object)DBNull.Value;
        row[5] = livePlan ?? (object)DBNull.Value;
        return row;
    }

    [Fact]
    public void LiveSnapshotRow_WithANamedStatement_ReadsAsTheMarker_InTheTextAndBothPlans()
    {
        var plan = StatementScrubCanary.CanaryPlan();
        using var reader = new FakeCollectorDataReader(SnapshotRow(StatementScrubCanary.CanaryStatement, plan, plan));
        Assert.True(reader.Read());

        var live = ServerTab.ReadLiveSnapshotRow(reader, new DateTime(2026, 10, 8, 12, 0, 0));

        Assert.Equal(Marker, live.QueryText);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, live.QueryText, StringComparison.Ordinal);
            Assert.DoesNotContain(needle, live.QueryPlan ?? string.Empty, StringComparison.Ordinal);
            Assert.DoesNotContain(needle, live.LiveQueryPlan ?? string.Empty, StringComparison.Ordinal);
        }

        /* The plan buttons stay on (the grid's flags come from the in-row plan), and a Download of it is the withheld
           refusal rather than a file with the secret in it. */
        Assert.True(live.HasQueryPlan);
        Assert.True(live.HasLiveQueryPlan);
    }

    [Fact]
    public void LiveSnapshotRow_WithAPlainStatement_IsUnchanged()
    {
        var plain = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"" + StatementScrubCanary.PlainStatement + "\" /></Statements></Batch></BatchSequence></ShowPlanXML>";
        using var reader = new FakeCollectorDataReader(SnapshotRow(StatementScrubCanary.PlainStatement, plain, plain));
        Assert.True(reader.Read());

        var live = ServerTab.ReadLiveSnapshotRow(reader, new DateTime(2026, 10, 8, 12, 0, 0));

        Assert.Equal(StatementScrubCanary.PlainStatement, live.QueryText);
        Assert.Equal(plain, live.QueryPlan);
        Assert.Equal(plain, live.LiveQueryPlan);
    }

    [Fact]
    public void LiveSnapshotRow_WithNoText_AndNoPlans_StaysEmpty()
    {
        using var reader = new FakeCollectorDataReader(SnapshotRow(string.Empty, null, null));
        Assert.True(reader.Read());

        var live = ServerTab.ReadLiveSnapshotRow(reader, new DateTime(2026, 10, 8, 12, 0, 0));

        Assert.Equal(string.Empty, live.QueryText);
        Assert.Null(live.QueryPlan);
        Assert.Null(live.LiveQueryPlan);
        Assert.False(live.HasQueryPlan);
        Assert.False(live.HasLiveQueryPlan);
    }

    /// <summary>
    /// #5554: the live read judges every row, and the judge can spend seconds on a large plan, so the loop runs off the
    /// calling (UI) thread. Driven from a dedicated thread that blocks until the read returns; a loop that ran inline would
    /// read every row on that same thread.
    /// </summary>
    [Fact]
    public void LiveSnapshotRows_AreReadAndJudged_OffTheCallingThread()
    {
        var plan = StatementScrubCanary.CanaryPlan();
        using var reader = new FakeCollectorDataReader(
            SnapshotRow(StatementScrubCanary.CanaryStatement, plan, plan),
            SnapshotRow(StatementScrubCanary.PlainStatement, null, null))
        {
            GetStringThreads = new System.Collections.Concurrent.ConcurrentBag<int>()
        };

        var callerThread = -1;
        List<QuerySnapshotRow>? rows = null;
        var thread = new Thread(() =>
        {
            callerThread = Environment.CurrentManagedThreadId;
            rows = ServerTab.ReadLiveSnapshotRowsAsync(reader, new DateTime(2026, 10, 8, 12, 0, 0)).GetAwaiter().GetResult();
        });
        thread.Start();
        Assert.True(thread.Join(TimeSpan.FromSeconds(60)));

        Assert.NotNull(rows);
        Assert.Equal(2, rows!.Count);
        Assert.Equal(Marker, rows[0].QueryText);
        Assert.Equal(StatementScrubCanary.PlainStatement, rows[1].QueryText);
        Assert.NotEmpty(reader.GetStringThreads);
        Assert.DoesNotContain(callerThread, reader.GetStringThreads);
    }

    /// <summary>#5554: the re-run and fetch display sites filter off the UI thread; the result is the same as the sync filter.</summary>
    [Fact]
    public async Task FilterAsync_GivesTheSameAnswerAsFilter_AndPassesNullAndEmptyThrough()
    {
        var plan = StatementScrubCanary.CanaryPlan();
        Assert.Equal(LivePlanDisplay.Filter(plan), await LivePlanDisplay.FilterAsync(plan));
        Assert.DoesNotContain(StatementScrubCanary.SecretNeedles[0], await LivePlanDisplay.FilterAsync(plan) ?? "", StringComparison.Ordinal);
        Assert.Null(await LivePlanDisplay.FilterAsync(null));
        Assert.Equal(string.Empty, await LivePlanDisplay.FilterAsync(string.Empty));
    }

    private static readonly Regex ActualPlanCall = new(
        @"ActualPlanExecutor\s*\.\s*ExecuteForActualPlanAsync\s*\(", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex WrappedActualPlanCall = new(
        @"await\s+LivePlanDisplay\s*\.\s*FilterAsync\s*\(\s*await\s+ActualPlanExecutor\s*\.\s*ExecuteForActualPlanAsync\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The actual-plan re-run asks the monitored server for a plan: every Lite call site passes it through the helper at
    /// the call. A new call site fails here until it is wrapped.
    /// </summary>
    [Fact]
    public void EveryActualPlanReRun_PassesThePlanThroughTheHelperAtTheCall()
    {
        var liteRoot = Path.Combine(RepoRoot(), "Lite");
        var calls = 0;
        var wrapped = 0;
        var files = new HashSet<string>(StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(liteRoot, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(liteRoot, file).Replace('\\', '/');
            if (relative.StartsWith("obj/", StringComparison.Ordinal) || relative.StartsWith("bin/", StringComparison.Ordinal))
            {
                continue;
            }

            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            var found = ActualPlanCall.Matches(code).Count;
            if (found == 0)
            {
                continue;
            }

            calls += found;
            wrapped += WrappedActualPlanCall.Matches(code).Count;
            files.Add(relative);
        }

        /* Five windows' plan actions and the server tab's own re-run; a scan that found nothing would pass vacuously. */
        Assert.Equal(6, calls);
        Assert.Equal(6, files.Count);
        Assert.Equal(calls, wrapped);
    }

    /// <summary>
    /// The windows and tabs that open their own connection to the monitored server. A read of statement text or a plan
    /// from one of them is judged where it is read (the Live Snapshot above); a new file that opens a connection fails here
    /// until the list is updated, which is the moment to decide what it reads.
    /// </summary>
    [Fact]
    public void TheFilesThatOpenTheirOwnServerConnection_AreTheKnownOnes()
    {
        var expected = new[]
        {
            "Lite/Controls/ServerTab.xaml.cs",            /* Live Snapshot: judged in ReadLiveSnapshotRow */
            "Lite/Windows/AddMultipleServersDialog.xaml.cs", /* SELECT @@VERSION */
            "Lite/Windows/AddServerDialog.xaml.cs",       /* SELECT @@VERSION */
            "Lite/Windows/ExcludedDatabasesDialog.xaml.cs", /* database names */
            /* #5554: the rest of Lite, not just the windows and tabs. Each is judged, or reads no statement text: */
            "Lite/Analysis/SqlPlanFetcher.cs",            /* drill-down plan fetch: judged in DrillDownCollector.Plans before analysis */
            "Lite/Services/LocalDataService.FinOps.IndexAnalysis.cs",    /* index names and sizes, no statement text */
            "Lite/Services/LocalDataService.FinOps.Recommendations.cs",  /* object and setting metadata, no statement text */
            "Lite/Services/LocalDataService.FinOps.ServerProperties.cs", /* server properties, no statement text */
            "Lite/Services/LocalDataService.QueryStats.cs",              /* the live plan fetches: every caller wraps them (LivePlanDisplay) */
            "Lite/Services/LocalDataService.QueryStore.cs",              /* the Query Store live plan fetch: every caller wraps it */
            "Lite/Services/RemoteCollectorService.cs",                   /* the scheduled collectors: judged at collection */
            "Lite/Services/RemoteCollectorService.DefinitionRunner.cs",  /* the definition-driven collectors: judged at collection */
            "Lite/Services/RemoteCollectorService.QueryStoreBackfill.cs", /* Query Store backfill: judged at collection */
            "Lite/Services/ServerManager.cs",                            /* connection test, @@VERSION */
        };

        var actual = new List<string>();
        foreach (var file in Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(RepoRoot(), file).Replace('\\', '/');
            if (relative.Contains("/obj/", StringComparison.Ordinal) || relative.Contains("/bin/", StringComparison.Ordinal))
            {
                continue;
            }

            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            if (Regex.IsMatch(code, @"new\s+SqlConnection\s*\("))
            {
                actual.Add(relative);
            }
        }

        Assert.Equal(expected.OrderBy(x => x, StringComparer.Ordinal), actual.OrderBy(x => x, StringComparer.Ordinal));

        /* And the one that reads statement text judges it: the row reader takes a session and uses it for the text and both plans. */
        var serverTab = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "ServerTab.xaml.cs"));
        var start = serverTab.IndexOf("internal static QuerySnapshotRow ReadLiveSnapshotRow", StringComparison.Ordinal);
        Assert.True(start > 0);
        var body = serverTab[start..Math.Min(serverTab.Length, start + 4000)];
        Assert.Contains("scrub.Text(", body, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(body, @"scrub\.Xml\(").Count);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
