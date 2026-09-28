/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A live test class that runs <c>EXPLAIN</c> against <c>DARLING_TEST_PG</c> reads the chunk list of a shared
/// database, which other classes leave future-dated or compressed-empty chunks in (#4650). Such a class must mint
/// its own database with <c>ScratchPostgres.CreateAsync</c>. The classes in <see cref="SharedStoreExplainBaseline"/>
/// predate the rule; the baseline may only shrink.
/// <para>This is a plain token scan over the raw file text and deliberately counts comments: the rule is about
/// where a file puts its database, not about executable statements, so no comment filter applies.</para>
/// </summary>
public sealed class LivePlanShapeIsolationCensusTests
{
    /// <summary>These pass in single-process runs today (the failing full runs named only the two classes now
    /// converted). They are listed so no NEW class joins them; converting one means removing it here.</summary>
    private static readonly HashSet<string> SharedStoreExplainBaseline = new(StringComparer.Ordinal)
    {
        "AnomalyObjectStatsLatestSnapshotsLiveTests.cs",
        "DarlingAgStatesReaderLiveEqualityTests.cs",
        "DarlingDeltaSeederTests.cs",
        "DarlingPgIndexBloatCoverageLivePostgresTests.cs",
        "EventWindowedReadsAreBoundedLivePostgresTests.cs",
        "FleetReadsAreBoundedByTheFleetTests.cs",
        "ForcePlanFailuresAccessPathTests.cs",
        "JobHistoryPerServerTopNLiveTests.cs",
        "LatestValueLookbackTests.cs",
        "ParameterSensitiveDrillDownTextLiveTests.cs",
        "PgPlanCaptureCsvLiveTests.cs",
        "PgPlanCaptureLiveTests.cs",
        "PgServerConfigToolBoundTests.cs",
        "PgTargetConfigSnapshotBoundTests.cs",
        "PgTargetSeqScanTests.cs",
        "PlanRegressionDrillDownReuseLiveTests.cs",
        "ServerListAndSummaryPlanShapeTests.cs",
        "StoreMetricsLatestSkipScanLiveTests.cs",
        "TopCpuQueriesTextLiveTests.cs"
    };

    private const string ScratchToken = "ScratchPostgres.CreateAsync(";

    [Fact]
    public void EveryLiveExplainClass_MintsItsOwnDatabase_OrIsInTheFrozenBaseline()
    {
        var offenders = ExplainFiles()
            .Where(f => !File.ReadAllText(f).Contains(ScratchToken, StringComparison.Ordinal))
            .Select(Path.GetFileName)
            .Where(n => !SharedStoreExplainBaseline.Contains(n!))
            .ToList();

        Assert.True(offenders.Count == 0,
            "A live test that runs EXPLAIN against DARLING_TEST_PG must create its own database with "
            + "ScratchPostgres.CreateAsync (#4650). Offenders: " + string.Join(", ", offenders));
    }

    [Fact]
    public void EveryBaselineEntry_StillExists_AndStillSharesTheStore()
    {
        var dir = TestsDirectory();
        var stale = new List<string>();
        foreach (var name in SharedStoreExplainBaseline)
        {
            var path = Path.Combine(dir, name);
            if (!File.Exists(path) || File.ReadAllText(path).Contains(ScratchToken, StringComparison.Ordinal))
            {
                stale.Add(name);
            }
        }

        Assert.True(stale.Count == 0,
            "These baseline entries no longer exist or now mint their own database; remove them from "
            + "SharedStoreExplainBaseline: " + string.Join(", ", stale));
    }

    private static List<string> ExplainFiles()
    {
        var self = Path.GetFileName(SelfPath());
        return Directory.GetFiles(TestsDirectory(), "*.cs")
            .Where(f => Path.GetFileName(f) != self)
            .Where(f =>
            {
                var text = File.ReadAllText(f);
                return text.Contains("DARLING_TEST_PG", StringComparison.Ordinal)
                       && text.Contains("EXPLAIN", StringComparison.Ordinal);
            })
            .ToList();
    }

    private static string SelfPath([CallerFilePath] string thisFile = "") => thisFile;

    private static string TestsDirectory() => Path.GetDirectoryName(SelfPath())!;
}
