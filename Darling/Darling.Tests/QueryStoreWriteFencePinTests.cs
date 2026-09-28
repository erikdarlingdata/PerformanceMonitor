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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4659: the Query Store write fence is hooked at the runner's one COPY chokepoint. These pins hold its position:
/// the begin precedes the transaction's open, and the end sits in a <c>finally</c> that follows the commit, so a
/// throw or a cancel after the rows committed (or a rollback) still ends the write.
/// </summary>
public sealed class QueryStoreWriteFencePinTests
{
    private static string Here([CallerFilePath] string thisFile = "") => thisFile;

    private static string CopyBatchOnceBody()
    {
        var path = Path.Combine(Path.GetDirectoryName(Here())!, "..", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        var text = File.ReadAllText(path);
        var start = text.IndexOf("private async Task<int> CopyBatchOnceAsync<TRow>(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var end = text.IndexOf("private static List<string>? DistinctQueryStoreDatabases", start, StringComparison.Ordinal);
        Assert.True(end > start);
        return text[start..end];
    }

    [Fact]
    public void CopyBatchOnce_BeginsTheFenceBeforeTheTransaction_AndEndsItInAFinallyAfterTheCommit()
    {
        var body = CopyBatchOnceBody();
        var begin = body.IndexOf("_queryStoreWriteFence?.BeginWrite(", StringComparison.Ordinal);
        var open = body.IndexOf("BeginTransactionAsync(", StringComparison.Ordinal);
        var commit = body.IndexOf("transaction.CommitAsync(", StringComparison.Ordinal);
        var finallyAt = body.IndexOf("finally", commit, StringComparison.Ordinal);
        var end = body.IndexOf("_queryStoreWriteFence?.EndWrite(", StringComparison.Ordinal);

        Assert.True(begin > 0 && open > begin, "BeginWrite must precede the transaction's open");
        Assert.True(commit > open, "the commit follows the open");
        Assert.True(finallyAt > commit && end > finallyAt, "EndWrite must sit in a finally that follows CommitAsync");
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(body, "EndWrite\\("));
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(body, "BeginWrite\\("));
    }

    [Fact]
    public void EveryQueryStoreBatchWrite_ReachesTheChokepoint()
    {
        var path = Path.Combine(Path.GetDirectoryName(Here())!, "..", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        var text = File.ReadAllText(path);
        /* The two COPY entry points are the shared-connection attempt and the fresh-connection one; nothing else
           in the runner opens a binary import for a collector's rows. */
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(text, "BeginBinaryImportAsync\\("));
    }

    [Fact]
    public void CopyBatchOnce_MarksTheFenceSucceededLast_AfterTheCommit_AndPassesItToEndWrite()
    {
        var body = CopyBatchOnceBody();
        var commit = body.IndexOf("transaction.CommitAsync(", StringComparison.Ordinal);
        var mark = body.IndexOf("fenceSucceeded = true;", StringComparison.Ordinal);
        var finallyAt = body.IndexOf("finally", commit, StringComparison.Ordinal);
        Assert.True(commit > 0 && mark > commit, "the success mark must follow CommitAsync");
        Assert.True(finallyAt > mark, "the success mark sits in the try, before the finally");
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(body, "fenceSucceeded = true;"));
        /* Nothing but whitespace and comments sits between the mark and the finally's closing of the try. */
        var between = body[(mark + "fenceSucceeded = true;".Length)..finallyAt];
        Assert.Equal("}", between.Trim());
        Assert.Contains("EndWrite(endServerId, fenceSucceeded)", body);
    }

    [Fact]
    public void TheFence_AFailedWritePoisonsTheServer_UntilACleanWriteEnds()
    {
        var fence = new PerformanceMonitor.Darling.Service.QueryStoreWriteFence();
        fence.BeginWrite(1);
        fence.EndWrite(1, false);
        Assert.False(fence.Snapshot(1).Quiet);
        Assert.True(fence.Snapshot(2).Quiet, "another server is untouched");

        fence.BeginWrite(1);
        Assert.False(fence.Snapshot(1).Quiet);
        fence.EndWrite(1, true);
        Assert.True(fence.Snapshot(1).Quiet);
    }

    [Fact]
    public void TheFence_WhenOneOfTwoOverlappingWritesFails_TheServerStaysPoisonedAfterTheCleanOneEnds()
    {
        var fence = new PerformanceMonitor.Darling.Service.QueryStoreWriteFence();
        fence.BeginWrite(1);
        fence.BeginWrite(1);
        fence.EndWrite(1, false);
        fence.EndWrite(1, true);
        Assert.False(fence.Snapshot(1).Quiet);

        fence.BeginWrite(1);
        fence.EndWrite(1, true);
        Assert.True(fence.Snapshot(1).Quiet);

        /* The failure ends after the clean overlapping write does. */
        fence.BeginWrite(1);
        fence.BeginWrite(1);
        fence.EndWrite(1, true);
        fence.EndWrite(1, false);
        Assert.False(fence.Snapshot(1).Quiet);
    }
}
