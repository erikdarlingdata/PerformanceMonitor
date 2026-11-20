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
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ProcedureTrendQueryViewPinTests
{
    /// <summary>The unfiltered statement (the MCP reader's text before the viewer's database filter, #5244).</summary>
    private const string QueryStatsBucketedUnfilteredSha256 = "3D4EBE0D0B4B3C5955066B585A9AA7934DD3BDFFD6A1F70FAE1FC7931FFCD1F2";

    /// <summary>The filtered statement (the viewer's chart and the MCP reader's filtered text).</summary>
    private const string QueryStatsBucketedFilteredSha256 = "3D88F208DF2224098FD2432F742A98EF77BCE51BBDDB07AF204A955D601EBE68";

    /// <summary>The per-collection CTE alone (<c>RawCollectionsCte</c>'s query branch), unfiltered.</summary>
    private const string QueryStatsCollectionsCteUnfilteredSha256 = "B2DC435EA3EAA654F12673D839A54DF09E1CFAD41A212A89BC3F2A5971555BBE";

    /// <summary>The per-collection CTE alone, filtered.</summary>
    private const string QueryStatsCollectionsCteFilteredSha256 = "9C3D09BF7B57E2DEC1E639A549B06BEB697C97BB6708EA3DD7777B1A60F2D3F2";

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
