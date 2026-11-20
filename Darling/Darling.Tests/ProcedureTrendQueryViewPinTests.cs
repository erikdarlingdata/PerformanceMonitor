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
/// #7161 is one: the query views rate a bucket over the gap to the previous collection, no longer the MAX of the stored intervals,
/// so the four hashes below were updated on purpose.
/// #8073 is another: the two bucketed statements gain a trailing <c>execution_count</c> column (the bucket's summed rated
/// executions), so their two hashes moved; the collections CTE is untouched and its two did not.
/// #9147 is a third: the query views read the per-collection summary for a server whose summary covers the window, so all four hashes moved; the
/// summary route and the raw route are held equal by DurationTrendCollectionSummaryLiveTests.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ProcedureTrendQueryViewPinTests
{
    /// <summary>The unfiltered statement (the MCP reader's text before the viewer's database filter, #5244).</summary>
    private const string QueryStatsBucketedUnfilteredSha256 = "C0DCB9B2115324B1F32AC2D7D03CF0C3B05D8B05509D65AC74FF084BD0744B17";

    /// <summary>The filtered statement (the viewer's chart and the MCP reader's filtered text).</summary>
    private const string QueryStatsBucketedFilteredSha256 = "1D444DCAE26EC971748D35806F253F86CB19339D6B5007E929930F2D945B80F3";

    /// <summary>The per-collection CTE alone (<c>RawCollectionsCte</c>'s query branch), unfiltered.</summary>
    private const string QueryStatsCollectionsCteUnfilteredSha256 = "955583BDFF9FC93D3BB28E464B99F24BD141BE29DD7DFA292C42B9F4974AB90E";

    /// <summary>The per-collection CTE alone, filtered.</summary>
    private const string QueryStatsCollectionsCteFilteredSha256 = "0C7466D1AB88178F94DEC7C9998459ACC3B860CA7913335F0F9050D2F45404C2";

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
