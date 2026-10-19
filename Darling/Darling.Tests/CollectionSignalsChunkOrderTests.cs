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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3496: the collection-signals recent-N subqueries must order by <c>collection_time</c> FIRST.
///
/// <para><c>collection_log</c> is a TimescaleDB hypertable partitioned on <c>collection_time</c> with no
/// index on <c>log_id</c>, so <c>ORDER BY log_id DESC LIMIT $2</c> had exactly one legal plan: append
/// every chunk — decompressing the columnar history — into a top-N sort, measured live at ~389,000 rows
/// heapsorted to return ten, twice per statement, every alert pass, every 30 seconds, per server. The
/// statement's own <c>MAX(collection_time)</c> arm — same table, same predicate — resolved in 0.097 ms
/// because its sort key is chunk-orderable, which is the fix's efficacy measured before the fix existed.
/// <c>ORDER BY collection_time DESC, log_id DESC</c> is semantics-identical (a server's id order and time
/// order agree; the id is the deterministic tiebreak within one collection instant) and
/// horizon-independent (ChunkAppend stops at the newest chunks however many the retention horizon
/// accumulates).</para>
///
/// <para>These pins exist because the regression is QUIET: a revert to <c>log_id</c>-first returns the
/// same rows on any store small enough for a test, passes every behavioral assertion, and only shows up
/// months later as tail-latency deadline breaches on the store whose retention has filled — exactly how
/// the defect presented the first time. So the ORDER the statement asks for is pinned at the source,
/// scoped to the one statement. The capture-down read in the same file was the different shape this pin
/// originally left alone — <c>ROW_NUMBER ... ORDER BY cl.log_id DESC</c>, which no ordering swap could
/// early-stop; #3597 reshaped it into one chunk-orderable <c>LIMIT 1</c> per collector, and
/// <see cref="CaptureDownChunkOrderTests"/> pins that one.</para>
/// </summary>
public sealed class CollectionSignalsChunkOrderTests
{
    private const string ChunkOrderedRecentN = "ORDER BY collection_time DESC, log_id DESC LIMIT $2";

    [Fact]
    public void BothRecentNSubqueries_OrderByThePartitionColumnFirst_WithTheIdTiebreak()
    {
        var sql = CollectionSignalsSql();

        Assert.Equal(2, CountOf(sql, ChunkOrderedRecentN));
    }

    /// <summary>
    /// The negative half: no recent-N arm may quietly go back to id-first ordering. Matched as a shape
    /// (<c>ORDER BY log_id</c>, optionally qualified) rather than the one literal that shipped, so a
    /// re-spelling cannot slip past the pin the way the original slipped past review.
    /// </summary>
    [Fact]
    public void NoArmOfTheStatement_OrdersByLogIdFirst()
    {
        var sql = CollectionSignalsSql();

        Assert.DoesNotMatch(new Regex(@"ORDER\s+BY\s+(?:[A-Za-z_]+\.)?log_id", RegexOptions.IgnoreCase), sql);
    }

    /// <summary>
    /// The MAX arm is the measured 0.097 ms control and stays untouched — a "helpful" rewrite of it
    /// (say, to a windowed read) would discard the one residual it deliberately keeps: a server whose
    /// newest qualifying row is ancient walks deeper before stopping, the rare case and the right cost
    /// to pay exactly then.
    /// </summary>
    [Fact]
    public void TheMaxArm_TheMeasuredControl_IsStillTheUnboundedMax()
    {
        var sql = CollectionSignalsSql();

        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("status IN ('SUCCESS', 'SKIPPED')", sql, StringComparison.Ordinal);
    }

    /* ---------------- extraction ---------------- */

    /// <summary>
    /// The verbatim SQL of <c>ReadCollectionSignalsAsync</c>'s one statement, extracted from the source:
    /// from the method's declaration to its command construction's closing quote. Extraction failures are
    /// loud — a renamed method or a restructured command must fail here rather than silently pinning an
    /// empty string as clean.
    /// </summary>
    private static string CollectionSignalsSql()
    {
        var path = Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs");
        Assert.True(File.Exists(path), $"evaluator source not found: {path}");

        var text = File.ReadAllText(path);

        var method = text.IndexOf("> ReadCollectionSignalsAsync(", StringComparison.Ordinal);
        Assert.True(method >= 0, "ReadCollectionSignalsAsync has moved or been renamed — re-point this pin");

        var open = text.IndexOf("@\"", method, StringComparison.Ordinal);
        Assert.True(open >= 0, "no verbatim SQL literal after ReadCollectionSignalsAsync's declaration");

        var close = text.IndexOf("\", connection)", open, StringComparison.Ordinal);
        Assert.True(close > open, "the statement's closing quote has moved — re-point this pin");

        return text[(open + 2)..close];
    }

    private static int CountOf(string text, string token)
    {
        var count = 0;
        var at = 0;
        while ((at = text.IndexOf(token, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += token.Length;
        }

        return count;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
