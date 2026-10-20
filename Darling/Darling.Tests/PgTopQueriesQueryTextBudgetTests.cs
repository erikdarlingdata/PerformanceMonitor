/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6131: <c>get_pg_top_queries</c> returned 59.6 KB at default arguments because every row carried its whole statement. The
/// answer is built by <see cref="DarlingMcpPgStatementTools.BuildTopQueriesJson"/> with no database, so this runs over the real
/// builder: a 400-character preview by default, flagged, and <c>full_text</c> opts back in.
/// </summary>
public sealed class PgTopQueriesQueryTextBudgetTests
{
    private const int Rows = 20;
    private const int TextLength = 3000;

    private static DarlingPgStatementReader.PgTopQueriesPage Page(int rows, string? text)
    {
        var list = Enumerable.Range(0, rows).Select(i => new DarlingPgStatementReader.PgStatementRow(
            QueryId: 7_000_000_000_000_000_000L + i,
            DatabaseId: 16384,
            Calls: 100,
            TotalExecTimeMs: 5000 - i,
            RowsReturned: 250,
            MaxExecTimeMs: 91.5,
            SharedBlocksHit: 10,
            SharedBlocksRead: 5,
            StorageBlocksRead: 3,
            OrcacheBlocksHit: 2,
            TempBlocksRead: 0,
            TempBlocksWritten: 0,
            WalBytes: 4096,
            MaxPeakMemBytes: 2097152,
            QueryText: text)).ToList();
        return new DarlingPgStatementReader.PgTopQueriesPage(list, list.Sum(r => r.TotalExecTimeMs));
    }

    [Fact]
    public void DefaultAnswer_PreviewsEveryStatement_AndStaysUnderTheBudget()
    {
        var text = "SELECT " + new string('c', TextLength);
        var json = DarlingMcpPgStatementTools.BuildTopQueriesJson("pg-target-01", 24, Page(Rows, text), Rows);

        Assert.True(Encoding.UTF8.GetByteCount(json) < McpResponseBudget.DefaultBytes,
            $"{Rows} rows of {TextLength}-character statements answered {Encoding.UTF8.GetByteCount(json)} bytes, over the {McpResponseBudget.DefaultBytes}-byte budget.");

        using var doc = JsonDocument.Parse(json);
        foreach (var row in doc.RootElement.GetProperty("queries").EnumerateArray())
        {
            Assert.True(row.GetProperty("query_text_truncated").GetBoolean());
            var preview = row.GetProperty("query_text").GetString()!;
            Assert.True(preview.Length < text.Length);
            Assert.StartsWith("SELECT ", preview, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void FullText_ReturnsTheWholeStatement_AndSaysNothingWasCut()
    {
        var text = "SELECT " + new string('c', TextLength);
        var json = DarlingMcpPgStatementTools.BuildTopQueriesJson("pg-target-01", 24, Page(2, text), 2, fullText: true);

        using var doc = JsonDocument.Parse(json);
        foreach (var row in doc.RootElement.GetProperty("queries").EnumerateArray())
        {
            Assert.Equal(text, row.GetProperty("query_text").GetString());
            Assert.False(row.GetProperty("query_text_truncated").GetBoolean());
        }
    }

    [Fact]
    public void AShortOrMissingStatement_IsNotFlaggedAsCut()
    {
        using var shortDoc = JsonDocument.Parse(
            DarlingMcpPgStatementTools.BuildTopQueriesJson("pg-target-01", 24, Page(1, "SELECT 1"), 1));
        var shortRow = shortDoc.RootElement.GetProperty("queries")[0];
        Assert.Equal("SELECT 1", shortRow.GetProperty("query_text").GetString());
        Assert.False(shortRow.GetProperty("query_text_truncated").GetBoolean());

        /* No captured text stays null: it is not a blank statement, and it is not a cut one. */
        using var missingDoc = JsonDocument.Parse(
            DarlingMcpPgStatementTools.BuildTopQueriesJson("pg-target-01", 24, Page(1, null), 1));
        var missingRow = missingDoc.RootElement.GetProperty("queries")[0];
        Assert.Equal(JsonValueKind.Null, missingRow.GetProperty("query_text").ValueKind);
        Assert.False(missingRow.GetProperty("query_text_truncated").GetBoolean());
    }
}
