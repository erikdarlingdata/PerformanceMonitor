/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5449: the procedure trend reads the collector's runs as its time axis; the query views (<c>query_stats</c>) store every
/// cycle and keep reading their own rows. Their raw-tier statement is pinned here by SHA-256 of the text the code produced
/// on origin/dev, before the procedure change, line endings normalised, so a change meant for the procedure grain cannot reach them.
/// A deliberate change to the query views updates the hash with its reason in the commit.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ProcedureTrendQueryViewPinTests
{
    /// <summary>The unfiltered statement (the MCP reader's text before the viewer's database filter, #5244).</summary>
    private const string QueryStatsBucketedUnfilteredSha256 = "59C1215D3E171D69285B05D2CF77C6475F9717631C947D1492ACF9D33AF434E0";

    /// <summary>The filtered statement (the viewer's chart and the MCP reader's filtered text).</summary>
    private const string QueryStatsBucketedFilteredSha256 = "1446ACDD0F7148DAF8268C6AB3D38D3E2B6832B2B6FB787EBEBB09807E8BC98B";

    /// <summary>The per-collection CTE alone (<c>RawCollectionsCte</c>'s query branch), unfiltered.</summary>
    private const string QueryStatsCollectionsCteUnfilteredSha256 = "7B58F60A5C376E9782EC7A44A189D461A3A44E0A835A2DF00F6786D83423E756";

    /// <summary>The per-collection CTE alone, filtered.</summary>
    private const string QueryStatsCollectionsCteFilteredSha256 = "59E2C46848E0AD30C83B1556D2DBB155DDB7168C4397DD3A6C8D105B4B4A80B9";

    [Fact]
    public void QueryStatsRawTrend_IsUnchangedByTheProcedureRunModel()
    {
        var unfiltered = Normalize(DurationTrendRouting.BuildBucketedRawTrendSql("query_stats", withDatabaseFilter: false));
        var filtered = Normalize(DurationTrendRouting.BuildBucketedRawTrendSql("query_stats", withDatabaseFilter: true));

        Assert.Equal(QueryStatsBucketedUnfilteredSha256, Sha256(unfiltered));
        Assert.Equal(QueryStatsBucketedFilteredSha256, Sha256(filtered));
        Assert.Equal(QueryStatsCollectionsCteUnfilteredSha256, Sha256(CollectionsCte(unfiltered)));
        Assert.Equal(QueryStatsCollectionsCteFilteredSha256, Sha256(CollectionsCte(filtered)));
    }

    /// <summary>The text from <c>WITH</c> up to the <c>rated</c> CTE: the per-collection CTE the builder embeds.</summary>
    private static string CollectionsCte(string sql)
    {
        var end = sql.IndexOf("rated AS", StringComparison.Ordinal);
        Assert.True(end > 0, "no rated CTE in the statement");
        return sql[..end];
    }

    private static string Normalize(string sql) => sql.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string Sha256(string text) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
}
