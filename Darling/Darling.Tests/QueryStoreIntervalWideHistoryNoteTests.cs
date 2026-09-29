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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4689: the interval-table history note and the grid banner's short reason come from one builder in Storage.
/// Exact strings for every <see cref="QueryStoreIntervalWide.WideStartBound"/> and both scopes, and a census that
/// the note's reason phrase is spelled once across the product source.
/// </summary>
public sealed class QueryStoreIntervalWideHistoryNoteTests
{
    private static readonly DateTime Start = new(2026, 8, 3, 12, 0, 0, DateTimeKind.Utc);
    private const string StartText = "2026-08-03T12:00:00.0000000Z";

    private const string Lead1 = "The window reaches further back than the Query Store history this store holds for ";
    private const string Lead2 = ". Nothing older than " + StartText + " was read. Past the raw tier's retention, intervals are read from the per-interval table (kept 9 days), which holds exactly what raw held for them. ";
    private const string Purge = "The interval table keeps 9 days, and intervals that began before its purge edge are not read.";
    private const string Clamp = "The read is clamped at the raw tier's retention floor: nothing older than it can be shown exactly.";

    [Theory]
    [InlineData(QueryStoreIntervalWide.WideStartBound.FilledSince, false, Lead1 + "this server" + Lead2 + "The interval table began keeping complete history for this server at " + StartText + ".")]
    [InlineData(QueryStoreIntervalWide.WideStartBound.FilledSince, true, Lead1 + "the servers in scope" + Lead2 + "The interval table began keeping complete history for these servers at " + StartText + ".")]
    [InlineData(QueryStoreIntervalWide.WideStartBound.TablePurgeEdge, false, Lead1 + "this server" + Lead2 + Purge)]
    [InlineData(QueryStoreIntervalWide.WideStartBound.TablePurgeEdge, true, Lead1 + "the servers in scope" + Lead2 + Purge)]
    [InlineData(QueryStoreIntervalWide.WideStartBound.RawFloor, false, Lead1 + "this server" + Lead2 + Clamp)]
    [InlineData(QueryStoreIntervalWide.WideStartBound.RawFloor, true, Lead1 + "the servers in scope" + Lead2 + Clamp)]
    [InlineData(QueryStoreIntervalWide.WideStartBound.Window, false, Lead1 + "this server" + Lead2 + Clamp)]
    [InlineData(QueryStoreIntervalWide.WideStartBound.Window, true, Lead1 + "the servers in scope" + Lead2 + Clamp)]
    public void HistoryNote_IsExact_ForEveryBoundAndScope(QueryStoreIntervalWide.WideStartBound bound, bool manyServers, string expected) =>
        Assert.Equal(expected, QueryStoreIntervalWide.HistoryNote(Start, bound, manyServers));

    [Fact]
    public void HistoryNote_NamesTheSettingServer_OnlyForManyServers()
    {
        var expected = Lead1 + "the servers in scope" + Lead2 + "The interval table began keeping complete history for alpha at " + StartText + ".";
        Assert.Equal(expected, QueryStoreIntervalWide.HistoryNote(Start, QueryStoreIntervalWide.WideStartBound.FilledSince, manyServers: true, settingServer: "alpha"));
        Assert.Equal(
            QueryStoreIntervalWide.HistoryNote(Start, QueryStoreIntervalWide.WideStartBound.FilledSince, manyServers: false),
            QueryStoreIntervalWide.HistoryNote(Start, QueryStoreIntervalWide.WideStartBound.FilledSince, manyServers: false, settingServer: "alpha"));
        /* The purge-edge and floor reasons name no server: the same text with or without one. */
        Assert.Equal(
            QueryStoreIntervalWide.HistoryNote(Start, QueryStoreIntervalWide.WideStartBound.TablePurgeEdge, manyServers: true),
            QueryStoreIntervalWide.HistoryNote(Start, QueryStoreIntervalWide.WideStartBound.TablePurgeEdge, manyServers: true, settingServer: "alpha"));
    }

    [Theory]
    [InlineData(QueryStoreIntervalWide.WideStartBound.Window, null)]
    [InlineData(QueryStoreIntervalWide.WideStartBound.RawFloor, null)]
    [InlineData(QueryStoreIntervalWide.WideStartBound.FilledSince, " (interval table complete from then)")]
    [InlineData(QueryStoreIntervalWide.WideStartBound.TablePurgeEdge, " (interval table keeps 9 days)")]
    public void BannerReason_IsExact_ForEveryBound(QueryStoreIntervalWide.WideStartBound bound, string? expected) =>
        Assert.Equal(expected, QueryStoreIntervalWide.BannerReason(bound));

    [Fact]
    public void ComposeForwarder_CarriesTheManyServersText() =>
        Assert.Equal(
            QueryStoreIntervalWide.HistoryNote(Start, QueryStoreIntervalWide.WideStartBound.TablePurgeEdge, manyServers: true),
            PerformanceMonitor.Darling.Service.DarlingWebEndpoints.QueryStoreHistoryNote(Start, QueryStoreIntervalWide.WideStartBound.TablePurgeEdge));

    /// <summary>The reason phrase is built from two halves so this file never matches its own scan, and the scan
    /// skips the test tree and build output.</summary>
    [Fact]
    public void ThePurgeEdgeReason_IsSpelledOnce_AcrossTheProductSource()
    {
        var token = "keeps 9 days, and intervals that began " + "before its purge edge";
        var product = Directory.EnumerateDirectories(RepoFile.PathTo("Darling"), "PerformanceMonitor.Darling.*")
            .SelectMany(d => Directory.EnumerateFiles(d, "*.cs", SearchOption.AllDirectories))
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                     && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .ToList();
        Assert.NotEmpty(product);
        var hits = product.Where(f => File.ReadAllText(f).Contains(token, StringComparison.Ordinal)).ToList();
        Assert.Single(hits);
        Assert.EndsWith("QueryStoreIntervalWide.cs", hits[0], StringComparison.Ordinal);
        Assert.Equal(1, System.Text.RegularExpressions.Regex.Count(File.ReadAllText(hits[0]), System.Text.RegularExpressions.Regex.Escape(token)));
    }
}
