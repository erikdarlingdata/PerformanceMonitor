/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// On Azure SQL Database, Query Store rows were stored with no statement text and no plan. Every scheduled run
/// nulls the payload's inline text and always ships a NULL plan, so text and plans reach the store only through
/// the two by-ids fetches (<c>FetchAndStoreQueryTextAsync</c> and <c>FetchAndStorePlansAsync</c>) - and those
/// were called from exactly one place, the enumerated per-item read. Azure SQL Database does not take that path:
/// <c>QueryStoreCollector.RunsPerDatabase</c> is <c>IsAzureSqlDb</c>, so query_store runs in the per-database
/// loop, which read and flushed each database and never fetched anything.
///
/// <para><b>Why source pins.</b> The loop needs a live Azure SQL Database connection and a store, and no fake
/// reaches it: its connection comes from <c>OpenDatabaseConnectionAsync</c> and the fetches run real SQL. The
/// behaviour that matters - which call sites reach the fetch, on which connection, for which database name, in
/// what order against the flush - is structural, so it is pinned at source the way the loop's other
/// reachability claims are (<c>PerDatabaseFaultPathSplitTests</c>). Every needle is searched in code with
/// comments and string literals blanked, so prose that names the method cannot satisfy a pin. A live pass
/// against an Azure SQL Database proves the rest.</para>
/// </summary>
public sealed class QueryStoreAzureTextAndPlanFetchTests
{
    private const string SharedFetch = "FetchQueryStorePlansAndTextAsync(";

    private const string QueryStoreGuard =
        "if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))";

    [Fact]
    public void ThePerDatabaseBranchRunsTheTextAndPlanFetchForQueryStore_OnItsOwnConnectionAndDatabase()
    {
        var branch = PerDatabaseBranch();

        var call = branch.IndexOf(SharedFetch, StringComparison.Ordinal);
        Assert.True(
            call >= 0,
            "The per-database branch (the one Azure SQL Database takes for query_store) never calls the shared " +
            "plan and text fetch, so its rows are stored with no statement text and no plan.");

        /* Gated on query_store by name: the branch also serves the XE collectors and others that have no plan
           or text to fetch, and the fetch touches the store connection. */
        var guard = branch.LastIndexOf(QueryStoreGuard, call, StringComparison.Ordinal);
        Assert.True(
            guard >= 0 && call - (guard + QueryStoreGuard.Length) < 120,
            "The fetch is not directly under the query_store name guard, so it would run for every collector " +
            "on this branch.");

        var end = branch.IndexOf(';', call);
        Assert.True(end > call, "Could not find the end of the fetch call.");
        var statement = branch[call..end];

        Assert.Contains("dbConnection", statement, StringComparison.Ordinal);
        Assert.Contains("pgConnection", statement, StringComparison.Ordinal);

        /* The loop's database name, not the registration's initial catalog: the by-ids queries run
           [db].sys.sp_executesql, which Azure accepts only for the connection's current database. */
        Assert.Contains("databaseName", statement, StringComparison.Ordinal);
        Assert.DoesNotContain("ConnectedDatabase", statement, StringComparison.Ordinal);
        Assert.DoesNotContain("InitialCatalog", statement, StringComparison.Ordinal);

        /* The per-database budget token, so the wall-clock ceiling bounds the fetch as it bounds the read. */
        Assert.Contains("dbToken", statement, StringComparison.Ordinal);
        Assert.Contains("perDbTimeout", statement, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFetchRunsAfterTheReadIsTimedAndBeforeTheFlush()
    {
        var branch = PerDatabaseBranch();

        var stop = branch.IndexOf("sqlSlice.Stop();", StringComparison.Ordinal);
        var fetch = branch.IndexOf(SharedFetch, StringComparison.Ordinal);
        var flush = branch.IndexOf("rowsWritten += await WriteBatchAsync(", StringComparison.Ordinal);

        Assert.True(stop >= 0 && fetch >= 0 && flush >= 0, "An ordering anchor moved.");

        /* After the stop, so the read's connect/open/drain/other split does not absorb the fetch into other:. */
        Assert.True(
            fetch > stop,
            "The fetch runs before the read's stopwatch is stopped, so its time lands in the per-database " +
            "line's other: residual, which is documented as work in neither database.");

        /* Before the flush, as on the enumerated path: the fetch repairs the shared store connection it
           borrows before touching it, so the flush never meets a connection a previous database broke. */
        Assert.True(
            fetch < flush,
            "The fetch runs after the flush, one database too late to repair the shared store connection a " +
            "previous database's budget expiry broke before this database's write needs it.");
    }

    [Fact]
    public void TheDatabaseConnectionIsStillOpenWhenTheFetchRuns()
    {
        var branch = PerDatabaseBranch();

        /* A `using (var dbConnection = ...)` block closes the connection with the read, before the fetch has
           anything to run on. The declaration form holds it to the end of the iteration. */
        Assert.Contains("using var dbConnection = openedConnection;", branch, StringComparison.Ordinal);
        Assert.DoesNotContain("using (var dbConnection", branch, StringComparison.Ordinal);
    }

    [Fact]
    public void EachDatabaseClearsItsFetchReadingsFirst_AndFeedsTheRunsFetchSums()
    {
        var branch = PerDatabaseBranch();

        /* The loop reuses ONE context. Without the clear, a database that skips or faults its fetch adds the
           previous database's figures to the run's sums as its own. It must precede the watermark read, the
           same rule the loop's other per-iteration clears follow. */
        var clear = branch.IndexOf("ClearFetchPhaseStamps(context);", StringComparison.Ordinal);
        var watermarkRead = branch.IndexOf(
            "context.Watermark = await GetLastCollectedTimeForDatabaseAsync(", StringComparison.Ordinal);
        Assert.True(clear >= 0, "The per-database loop no longer clears the fetch readings per database.");
        Assert.True(watermarkRead >= 0, "The per-database watermark read moved.");
        Assert.True(clear < watermarkRead, "The fetch readings are cleared after the watermark read.");

        var fetch = branch.IndexOf(SharedFetch, StringComparison.Ordinal);
        var observe = branch.IndexOf("fetchPhases.Observe(context);", StringComparison.Ordinal);
        Assert.True(fetch >= 0 && observe > fetch, "The fetch readings are never folded into the run's fetch sums.");

        /* And the fetch's time reaches the run's SQL total and the per-database fan-out figure, because the
           enumerated path counts it inside the item's SQL slice. */
        Assert.Contains("sqlMs += dbFetchMs;", branch, StringComparison.Ordinal);
        Assert.Contains("fanout.Observe(databaseName, dbSqlMs + dbFetchMs + dbStorageMs);", branch, StringComparison.Ordinal);
    }

    [Fact]
    public void BothReadSitesShareOneFetchMethod_SoNeitherCanDriftFromTheOther()
    {
        var code = RunnerCode();

        /* The definition is `FetchQueryStorePlansAndTextAsync<TRow>(`, which the needle (no generic list) does
           not match, so this counts CALLS: the enumerated per-item read and the per-database loop. */
        Assert.Equal(2, Occurrences(code, SharedFetch));

        /* The two raw fetches are called from the shared method and nowhere else - a copy of the block at a
           third site would be a third place for the guards and the phase stamps to drift. */
        Assert.Equal(1, Occurrences(code, "await FetchAndStorePlansAsync("));
        Assert.Equal(1, Occurrences(code, "await FetchAndStoreQueryTextAsync("));

        /* The enumerated site: right after the per-item read, on the per-item connection and budget token. */
        var readItem = code.IndexOf("await definition.ReadItemAsync(item, itemReader, batch, context, ct);", StringComparison.Ordinal);
        Assert.True(readItem >= 0, "The enumerated per-item read moved.");
        var enumerated = code.IndexOf(SharedFetch, readItem, StringComparison.Ordinal);
        Assert.True(enumerated > readItem, "The enumerated read no longer reaches the shared fetch.");

        /* Nothing but the read's own statement sits between them: the fetch follows the read directly. */
        Assert.Equal(1, Occurrences(code[readItem..enumerated], ";"));

        var statement = code[enumerated..code.IndexOf(';', enumerated)];
        Assert.Contains("targetConnection, pgConnection, server, item, definition.Name,", statement, StringComparison.Ordinal);
        Assert.Contains(", ct)", statement, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSharedMethodKeepsEachFetchBehindItsOwnGate_AndStampsTheParentFromAFinally()
    {
        var code = RunnerCode();
        var method = MethodBody(code, "private async Task FetchQueryStorePlansAndTextAsync<TRow>(");

        Assert.Contains("context.CapturePlanXml && targetConnection is SqlConnection planFetchConnection", method, StringComparison.Ordinal);
        Assert.Contains("context.FetchQueryTextSeparately && targetConnection is SqlConnection textFetchConnection", method, StringComparison.Ordinal);

        /* The databaseName parameter reaches both fetches: it is what names the [db].sys.sp_executesql target. */
        Assert.Contains("FetchAndStorePlansAsync(planFetchConnection, storeConnection,", method, StringComparison.Ordinal);
        Assert.Contains("FetchAndStoreQueryTextAsync(textFetchConnection, storeConnection,", method, StringComparison.Ordinal);
        Assert.Equal(2, Occurrences(method, "server, databaseName, collectorName, context, commandTimeoutSeconds,"));

        /* The #2854 rule the shared method inherited: the parent reading is stamped from a finally, so a fetch
           that throws still reports the time it spent. */
        Assert.Contains("context.PerItemPlanFetchMs = planFetchWatch.ElapsedMilliseconds;", method, StringComparison.Ordinal);
        Assert.Contains("context.PerItemTextFetchMs = textFetchWatch.ElapsedMilliseconds;", method, StringComparison.Ordinal);
        Assert.Equal(2, Occurrences(method, "finally"));
    }

    [Fact]
    public void TheUnavailableAnswerForAMissingPlanNoLongerBlamesOnlyTheCaptureMode()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPlanTools.cs"));

        var start = source.IndexOf("public static async Task<string> AnalyzeQueryStorePlan(", StringComparison.Ordinal);
        Assert.True(start >= 0, "AnalyzeQueryStorePlan moved.");
        var end = source.IndexOf("[McpServerTool(Name = \"analyze_plan_xml\")", start, StringComparison.Ordinal);
        Assert.True(end > start, "The end of AnalyzeQueryStorePlan moved.");
        var tool = source[start..end];

        /* A plan that was simply never fetched is a real cause of this answer (the collector stores plans
           through a separate fetch), so the text must allow for it... */
        Assert.Contains("The plan may not have been collected yet", tool, StringComparison.Ordinal);

        /* ...alongside the two causes it already named, without asserting either as the cause. */
        Assert.Contains("capture mode may exclude this query", tool, StringComparison.Ordinal);
        Assert.Contains("or the plan has been purged", tool, StringComparison.Ordinal);

        /* The old sentence named capture as the one cause. */
        Assert.DoesNotContain("plan capture may be disabled for this database", tool, StringComparison.Ordinal);

        /* It is still the "unavailable" status, not an error or an empty: the data could exist and is not here. */
        Assert.Contains("\"unavailable\"", tool, StringComparison.Ordinal);
    }

    private static string RunnerCode() =>
        CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs")));

    /// <summary>
    /// Just the per-database branch, so a count or an ordering cannot be answered by the enumerated path
    /// further down the same method.
    /// </summary>
    private static string PerDatabaseBranch()
    {
        var code = RunnerCode();

        var start = code.IndexOf("if (definition.RunsPerDatabase(context.Target))", StringComparison.Ordinal);
        Assert.True(start >= 0, "The per-database branch gate moved - this slicer is looking at nothing.");

        var end = code.IndexOf("context.CurrentDatabaseName = null;", start, StringComparison.Ordinal);
        Assert.True(end > start, "The per-database branch's closing anchor moved.");

        return code[start..end];
    }

    private static string MethodBody(string code, string signature)
    {
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{signature} is gone from the runner.");

        var open = code.IndexOf('{', start);
        Assert.True(open > start, "Could not find the method's opening brace.");

        return CSharpSourceWalker.BraceBalanced(code, open);
    }

    private static int Occurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal);
             i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
