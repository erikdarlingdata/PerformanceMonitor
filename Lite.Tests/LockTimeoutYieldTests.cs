/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1805: a SQL error 1222 from the one collector that deliberately sets a 1-second
/// <c>SET LOCK_TIMEOUT</c> guard (query_snapshots) is a YIELD, not a collection failure. These pin
/// the three layers that make that true in Lite: the shared catalog flag, the runner's catch
/// (source-level — the classification is try/catch glue around a live SqlException, so the wiring
/// is pinned where it lives), and the health tracking's neutrality. The daily-health exclusion
/// needs no code of its own — the band's collErrors input counts <c>status = 'ERROR'</c>
/// exact-match — so the pin there guards the PREDICATE SHAPE against being broadened to a
/// not-success form, which would silently re-admit yields into the Critical band.
/// </summary>
[Trait("Stage", "Guard")]
public class LockTimeoutYieldTests
{
    private const int ServerId = 42;

    /* ── the shared flag ── */

    [Fact]
    public void QuerySnapshots_Is_The_Only_Flagged_Collector()
    {
        var flagged = CollectorCatalog.All.Where(c => c.YieldsOnLockTimeout).Select(c => c.Name).ToList();
        Assert.Equal(new[] { "query_snapshots" }, flagged);
    }

    [Fact]
    public void Catalog_Helper_Answers_By_Name_And_Defaults_Unknown_To_False()
    {
        Assert.True(CollectorCatalog.YieldsOnLockTimeout("query_snapshots"));
        Assert.False(CollectorCatalog.YieldsOnLockTimeout("wait_stats"));
        /* Unknown names default FALSE — the opposite of AppliesTo's true, deliberately: a 1222
           from a collector the catalog does not know is unexpected and must stay an ERROR. */
        Assert.False(CollectorCatalog.YieldsOnLockTimeout("no_such_collector"));
    }

    [Fact]
    public void The_Flagged_Collector_Really_Carries_The_Guard_In_Both_Query_Variants()
    {
        /* Ties the flag to the guard it stands for: if the LOCK_TIMEOUT lines ever leave the
           query text, the flag is a lie and this fails naming it. Two variants (on-prem + Azure),
           so two occurrences. */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("PerformanceMonitor.Collectors", "QuerySnapshotsCollector.cs")));
        var occurrences = CountOccurrences(source, "SET LOCK_TIMEOUT 1000;");
        Assert.True(occurrences >= 2,
            $"QuerySnapshotsCollector declares YieldsOnLockTimeout but its source carries {occurrences} " +
            "'SET LOCK_TIMEOUT 1000;' guard(s) — expected one per query variant (>= 2).");
    }

    /* ── the runner's classification (source pin — the catch wraps a live SqlException) ── */

    [Fact]
    public void Lite_Runner_Classifies_1222_Through_The_Catalog_Flag()
    {
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "RemoteCollectorService.cs")));

        Assert.Contains("ex.Number == 1222 && CollectorCatalog.YieldsOnLockTimeout(collectorName)", source);
        Assert.Contains("status = \"YIELDED\";", source);
    }

    [Fact]
    public void DailySummary_Error_Count_Stays_ExactMatch_On_ERROR()
    {
        /* The Critical-band collErrors input. Exact-match means YIELDED (and SKIPPED, PERMISSIONS,
           SESSION_MISSING) never count; a broadening to status <> 'SUCCESS' would re-admit them. */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "LocalDataService.DailySummary.cs")));
        Assert.Contains("FILTER (WHERE status = 'ERROR')", source);
    }

    /* ── health tracking neutrality ── */

    private static RemoteCollectorService CreateService() =>
        new(duckDb: null!, serverManager: null!, scheduleManager: null!);

    [Fact]
    public void Yield_Alone_Is_Not_An_Erroring_Collector()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, "query_snapshots", "YIELDED",
            "Lock-timeout yield (SQL error #1222)");

        var summary = service.GetHealthSummary(ServerId);
        Assert.Equal(0, summary.ErroringCollectors);
        Assert.Empty(summary.Errors);
    }

    [Fact]
    public void Yield_Does_Not_Reset_A_Real_Error_Streak()
    {
        /* A yield is not proof the collector works (SUCCESS is), so it must not clear the FAILING
           signal the way a success does. */
        var service = CreateService();

        service.RecordCollectorResult(ServerId, "query_snapshots", "ERROR", "genuine failure");
        service.RecordCollectorResult(ServerId, "query_snapshots", "YIELDED",
            "Lock-timeout yield (SQL error #1222)");

        var summary = service.GetHealthSummary(ServerId);
        Assert.Equal(1, summary.ErroringCollectors);

        /* And the inverse control: an actual success clears it. */
        service.RecordCollectorResult(ServerId, "query_snapshots", "SUCCESS");
        Assert.Equal(0, service.GetHealthSummary(ServerId).ErroringCollectors);
    }

    /* ── helpers ── */

    private static int CountOccurrences(string text, string token)
    {
        var count = 0;
        var index = 0;
        while ((index = text.IndexOf(token, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += token.Length;
        }
        return count;
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
