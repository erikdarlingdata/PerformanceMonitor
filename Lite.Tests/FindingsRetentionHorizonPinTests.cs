/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the findings-retention horizon and its wiring, so naming the literal in
/// <see cref="AnalysisRetentionDefaults"/> is not merely a relocation of it.
///
/// <para>The horizon sits at three points down one call chain: the daily tick in
/// <c>CollectionBackgroundService.RunFindingsCleanupIfDueAsync</c>, the
/// <c>AnalysisService.CleanupAsync</c> wrapper it calls, and the
/// <c>FindingStore.CleanupOldFindingsAsync</c> default beneath that. Only the top one decides what is
/// actually purged, so the two below could disagree with delivered behavior indefinitely with nothing
/// failing — which is what made three copies of one number worth collapsing.</para>
///
/// <para><see cref="EverySiteInTheFindingsCleanupChain_ReadsTheSharedHorizon"/> is the assertion that
/// makes this stick. It reads the SOURCE, so it fails when a site is written back to a literal even if
/// that literal still says 30 and the shared constant is untouched — the case a value-equality test
/// cannot see, because the two numbers would agree. That is how a constant-only pin passes against a
/// no-op, and it is the one thing this file exists to prevent.</para>
///
/// <para>Behavior is already covered elsewhere and is not re-asserted here:
/// <c>FindingStoreTests.CleanupOldFindings_RemovesExpiredData</c> and
/// <c>AnalysisServiceCleanup_RemovesExpiredData</c> drive the chain end-to-end with an explicit window,
/// which is the right shape for a purge test. What was missing was a guard on the NUMBER and where it
/// comes from, so that is all this adds.</para>
/// </summary>
public sealed class FindingsRetentionHorizonPinTests
{
    /// <summary>
    /// The ruled window, pinned as a literal in the one place a literal belongs — a test. A symmetric
    /// edit of the constant and every call site would otherwise move the decision with nothing failing.
    /// 30 days is deliberate and deliberately cross-edition; <c>Darling.Tests</c> carries the other half,
    /// asserting this equals the service's own base data-retention window.
    /// </summary>
    [Fact]
    public void TheFindingsHorizon_IsThirtyDays()
    {
        Assert.Equal(30, AnalysisRetentionDefaults.FindingsRetentionDays);
    }

    /// <summary>
    /// The WIRING, read off the source rather than off a compiled value — the only assertion in the
    /// suite that fails if a site goes back to a bare literal while the constant stays exactly as it is.
    /// </summary>
    [Fact]
    public void EverySiteInTheFindingsCleanupChain_ReadsTheSharedHorizon()
    {
        Assert.Contains(
            "retentionDays: AnalysisRetentionDefaults.FindingsRetentionDays",
            ReadRepoSource("Lite", "Services", "CollectionBackgroundService.cs"),
            StringComparison.Ordinal);
        Assert.Contains(
            "public async Task CleanupAsync(int retentionDays = AnalysisRetentionDefaults.FindingsRetentionDays)",
            ReadRepoSource("Lite", "Analysis", "AnalysisService.cs"),
            StringComparison.Ordinal);
        Assert.Contains(
            "public async Task CleanupOldFindingsAsync(int retentionDays = AnalysisRetentionDefaults.FindingsRetentionDays)",
            ReadRepoSource("Lite", "Analysis", "FindingStore.cs"),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The same guard as a category rather than three instances: no file anywhere under <c>Lite</c> may
    /// spell the findings horizon as a literal. Pinning only the three known sites would let a fourth
    /// caller arrive with its own 30 — the drift this replaced, one call site later. Both spellings,
    /// because the chain passes a named argument at the call site and defaults a parameter at the two
    /// declarations. Ordinal on purpose: an unrelated <c>SomethingRetentionDays = 30</c> keeps its
    /// capital R and is correctly not matched.
    /// </summary>
    [Fact]
    public void NoFileUnderLite_SpellsTheFindingsHorizonAsALiteral()
    {
        var scanned = 0;

        foreach (var file in EnumerateLiteSources())
        {
            var source = File.ReadAllText(file);
            scanned++;

            foreach (var literal in new[] { "retentionDays: 30", "retentionDays = 30" })
            {
                Assert.False(
                    source.Contains(literal, StringComparison.Ordinal),
                    $"{file} spells the findings horizon as '{literal}' — read AnalysisRetentionDefaults.FindingsRetentionDays instead");
            }
        }

        /* A sweep that found nothing to read would pass vacuously. */
        Assert.True(scanned > 100, $"only {scanned} Lite sources scanned — the sweep did not find the tree");
    }

    /// <summary>
    /// The findings horizon and the parquet archive horizon are two DIFFERENT retentions, and reading
    /// the README's archive figure as this one is a live confusion — so pin them apart.
    /// <c>RetentionService</c> sweeps archive FILES on a 3-calendar-month window; this sweeps
    /// <c>analysis_findings</c> ROWS in the hot store on a 30-day one, and the findings tick is gated on
    /// the DuckDB handle rather than on the archive being configured, so either runs with the other off.
    /// Different unit, different service, different store. Collapsing them into one knob would silently
    /// change both, and this fails first if someone starts by making the units match.
    /// </summary>
    [Fact]
    public void TheParquetArchiveHorizon_IsASeparateKnob_InADifferentUnit()
    {
        var archive = ReadRepoSource("Lite", "Services", "RetentionService.cs");

        Assert.Contains("int retentionMonths = 3", archive, StringComparison.Ordinal);
        Assert.False(
            archive.Contains("AnalysisRetentionDefaults", StringComparison.Ordinal),
            "RetentionService took on the findings horizon — the parquet archive window is a separate retention");
    }

    private static IEnumerable<string> EnumerateLiteSources()
    {
        var root = FindRepoDirectory("Lite");

        foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            /* Build output under Lite/bin and Lite/obj holds generated and copied sources; scanning them
               reports the same text twice and can flag an artifact of a previous build. */
            var relative = Path.GetRelativePath(root, file);
            if (relative.StartsWith("bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || relative.StartsWith("obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            {
                continue;
            }

            yield return file;
        }
    }

    private static string ReadRepoSource(params string[] relativeParts) =>
        File.ReadAllText(FindRepoPath(Path.Combine(relativeParts), File.Exists))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string FindRepoDirectory(string relative) =>
        FindRepoPath(relative, Directory.Exists);

    private static string FindRepoPath(string relative, Func<string, bool> exists)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relative);
            if (exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relative} walking up from {AppContext.BaseDirectory}");
    }
}
