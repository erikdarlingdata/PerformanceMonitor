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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Source and seam pins for the hour-ledger writer (#4605, plan lane 2). The live behaviour is
/// <see cref="QueryStatsHourLedgerWriterLiveTests"/>; these pins hold the SHAPE the live tests cannot see: where the upsert
/// sits in <c>CopyBatchOnceAsync</c> (inside the COPY's transaction, after the COPY and the dimension flush, before the
/// commit, with nothing that could swallow its fault), and that the interval's position is resolved from the schema.
/// </summary>
public sealed class QueryStatsHourLedgerWriterTests
{
    private static string Here([CallerFilePath] string thisFile = "") => thisFile;

    /// <summary>The body of <c>CopyBatchOnceAsync</c> with comments and string literals blanked (character-aligned), so a word in prose is not code.</summary>
    private static string CopyBatchOnceCode()
    {
        var path = Path.Combine(Path.GetDirectoryName(Here())!, "..", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        var text = File.ReadAllText(path);
        var start = text.IndexOf("private async Task<int> CopyBatchOnceAsync<TRow>(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var end = text.IndexOf("private static List<string>? DistinctQueryStoreDatabases", start, StringComparison.Ordinal);
        Assert.True(end > start);
        return CSharpSourceWalker.StripCommentsAndStrings(text[start..end]);
    }

    [Fact]
    public void TheUpsert_SitsInTheCopyTransaction_AfterTheCopyAndTheDimensionFlush_BeforeTheCommit()
    {
        var code = CopyBatchOnceCode();
        var complete = code.IndexOf("importer.CompleteAsync(", StringComparison.Ordinal);
        var flush = code.IndexOf("PayloadDimensionWriter.FlushAsync(", StringComparison.Ordinal);
        var transactionBlock = code.IndexOf("if (transaction is not null)", StringComparison.Ordinal);
        var upsert = code.IndexOf("QueryStatsHourLedgerWriter.AddBatchAsync(", StringComparison.Ordinal);
        var commit = code.IndexOf("transaction.CommitAsync(", StringComparison.Ordinal);

        Assert.True(complete > 0 && flush > complete, "the dimension flush follows the COPY's completion");
        Assert.True(transactionBlock > complete, "the transaction block follows the COPY");
        Assert.True(upsert > flush, "the ledger upsert follows the dimension flush");
        Assert.True(upsert > transactionBlock, "the ledger upsert sits INSIDE the `transaction is not null` block");
        Assert.True(commit > upsert, "the commit follows the ledger upsert, so both commit together or neither does");
        Assert.Single(Regex.Matches(code, @"QueryStatsHourLedgerWriter\.AddBatchAsync\("));

        /* The transaction block closes after the commit and nothing between the upsert and the commit can swallow or
           split its fault: no catch, no savepoint, no rollback. Its fault must abort the batch. */
        var between = code[upsert..commit];
        Assert.DoesNotContain("catch", between, StringComparison.Ordinal);
        Assert.DoesNotContain("SaveAsync", between, StringComparison.Ordinal);
        Assert.DoesNotContain("SAVEPOINT", between, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("RollbackAsync", between, StringComparison.Ordinal);
    }

    [Fact]
    public void TheUpsert_IsOnlyForQueryStats_KeysOnTheStorageNameTheCopyWrites_AndTakesTheWritersTally()
    {
        var code = CopyBatchOnceCode();

        Assert.Contains("var ledgerBatch = definition is QueryStatsCollector;", code, StringComparison.Ordinal);

        var upsert = code.IndexOf("QueryStatsHourLedgerWriter.AddBatchAsync(", StringComparison.Ordinal);
        var call = code[upsert..code.IndexOf(");", upsert, StringComparison.Ordinal)];
        Assert.Contains("server.ServerId", call, StringComparison.Ordinal);
        Assert.Contains("server.StorageName", call, StringComparison.Ordinal);
        Assert.Contains("storedCollectionTime", call, StringComparison.Ordinal);
        Assert.Contains("writer.NonZeroCounted", call, StringComparison.Ordinal);

        /* The COPY writes that same storage name, so the ledger's key is the raw rows' key. */
        Assert.Contains(".Value(server.StorageName)", code, StringComparison.Ordinal);

        /* The batch's one stored time is written for every row, which is why one batch is one hour. */
        Assert.Single(Regex.Matches(code, @"\.Value\(storedCollectionTime\)"));
        Assert.Single(Regex.Matches(code, @"var storedCollectionTime = "));
    }

    [Fact]
    public void TheTally_StartsOnTheAttemptsOwnWriter_BeforeTheFirstRow_AndANamedTransactionTermCoversTheLedger()
    {
        var code = CopyBatchOnceCode();
        var writerBuilt = code.IndexOf("new PgCollectorRowWriter()", StringComparison.Ordinal);
        var tally = code.IndexOf("writer.CountNonZeroAt(QueryStatsHourLedgerWriter.IntervalPayloadIndex(definition))", StringComparison.Ordinal);
        var copyOpen = code.IndexOf("BeginBinaryImportAsync(", StringComparison.Ordinal);
        var rowLoop = code.IndexOf("foreach (var row in rows)", StringComparison.Ordinal);

        Assert.True(writerBuilt > 0 && tally > writerBuilt, "the writer is built per attempt, and the tally starts on it");
        Assert.True(copyOpen > tally && rowLoop > tally, "the tally starts before the first row is written");
        Assert.Single(Regex.Matches(code, @"new PgCollectorRowWriter\(\)"));

        /* The ledger term keeps the transaction non-null for a ledger batch whatever the diversion plan says. */
        Assert.Matches(@"diversionPlan\.Count > 0 \|\| queryStoreDatabases is not null \|\| ledgerBatch\s*\?\s*await pgConnection\.BeginTransactionAsync", code);

        /* A row that wrote the interval through another overload would be untallied: the batch fails before the COPY completes. */
        var guard = code.IndexOf("writer.CountedWrites != rowsWritten", StringComparison.Ordinal);
        Assert.True(guard > rowLoop && guard < code.IndexOf("importer.CompleteAsync(", StringComparison.Ordinal));
    }

    [Fact]
    public void AddBatchAsync_WritesNothingForACountOfZero_BeforeAnyCommandIsBuilt()
    {
        var path = Path.Combine(Path.GetDirectoryName(Here())!, "..", "PerformanceMonitor.Darling.Storage", "QueryStatsHourLedgerWriter.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
        var skip = code.IndexOf("if (count <= 0)", StringComparison.Ordinal);
        var command = code.IndexOf("new NpgsqlCommand(", StringComparison.Ordinal);

        Assert.True(skip > 0 && command > skip, "a count of 0 returns before a command is built (no round trip)");
        Assert.Contains("CommandTimeout = commandTimeoutSeconds", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIntervalPosition_IsResolvedByName_AndIsTheColumnTheCopyNamesAtThatPosition()
    {
        var definition = QueryStatsCollector.Instance;
        var index = QueryStatsHourLedgerWriter.IntervalPayloadIndex(definition);

        Assert.Equal("sample_interval_seconds", definition.PayloadColumns[index].Name);
        Assert.Equal(CollectorColumnType.Integer, definition.PayloadColumns[index].Type);

        /* The COPY lists collection_time, server_id and server_name (and a collection id when the table has one) before the payload. */
        var copy = PgCollectorRowWriter.CopyCommandFor(definition);
        var open = copy.IndexOf('(', StringComparison.Ordinal);
        var close = copy.IndexOf(')', open);
        var columns = copy[(open + 1)..close].Split(',').Select(c => c.Trim()).ToArray();
        var prefix = 3 + (definition.IncludesCollectionId ? 1 : 0);
        Assert.Equal("sample_interval_seconds", columns[prefix + index]);
    }
}
