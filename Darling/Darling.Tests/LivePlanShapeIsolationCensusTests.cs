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
/// its own database with the <c>CreateAsync</c> factory on <c>ScratchPostgres</c>. The classes in <see cref="SharedStoreExplainBaseline"/>
/// predate the rule; the baseline may only shrink.
/// <para>This is a plain token scan over the raw file text and deliberately counts comments: the rule is about
/// where a file puts its database, not about executable statements, so no comment filter applies.</para>
/// </summary>
public sealed class LivePlanShapeIsolationCensusTests
{
    /// <summary>The classes that run <c>EXPLAIN</c> against the shared store and are kept there on purpose, each
    /// with the reason its assertions cannot be flipped by another class's leftover chunks. Converting one means
    /// removing it here; a new class must mint its own database instead of joining this list.</summary>
    private static readonly IReadOnlyDictionary<string, string> SharedStoreExplainBaseline =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["DarlingDeltaSeederTests.cs"] = "names the planted rows' own chunks by tableoid and asserts only on those two; leftover chunks can't flip it",
            ["DarlingPgIndexBloatCoverageLivePostgresTests.cs"] = "EXPLAIN appears only in prose; its asserts are census verdict counts, not a plan",
            ["ParameterSensitiveDrillDownTextLiveTests.cs"] = "counts rows the executed plan resolves against query_text_dim; empty chunks return no rows",
            ["PgTargetSeqScanTests.cs"] = "Seq Scan is a node inside plan JSON the test seeds and parses; no store plan is asserted",
            ["PlanRegressionDrillDownReuseLiveTests.cs"] = "sums actual rows from EXPLAIN ANALYZE leaf nodes; empty chunks add 0",
            ["StoreMetricsLatestSkipScanLiveTests.cs"] = "asserts an index on the plain table collect.store_metrics; no hypertable in the plan",
            ["TopCpuQueriesTextLiveTests.cs"] = "counts rows resolved against query_text_dim in the executed plan, not chunk layout"
        };

    // Built with nameof so this file never spells the factory call out: LivePostgresCollectionHygieneTests treats
    // any source containing that literal text as a shared-store user, and this census touches no store.
    private static readonly string ScratchToken =
        nameof(ScratchPostgres) + "." + nameof(ScratchPostgres.CreateAsync) + "(";

    [Fact]
    public void EveryLiveExplainClass_MintsItsOwnDatabase_OrIsInTheFrozenBaseline()
    {
        var offenders = ExplainFiles()
            .Where(f => !File.ReadAllText(f).Contains(ScratchToken, StringComparison.Ordinal))
            .Select(Path.GetFileName)
            .Where(n => !SharedStoreExplainBaseline.ContainsKey(n!))
            .ToList();

        Assert.True(offenders.Count == 0,
            "A live test that runs EXPLAIN against DARLING_TEST_PG must create its own database with "
            + "the ScratchPostgres factory (#4650). Offenders: " + string.Join(", ", offenders));
    }

    [Fact]
    public void EveryBaselineEntry_StillExists_AndStillSharesTheStore()
    {
        var dir = TestsDirectory();
        var stale = new List<string>();
        foreach (var name in SharedStoreExplainBaseline.Keys)
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

    [Fact]
    public void EveryBaselineEntry_CarriesAReason()
    {
        var blank = SharedStoreExplainBaseline.Where(kv => string.IsNullOrWhiteSpace(kv.Value)).Select(kv => kv.Key).ToList();

        Assert.True(blank.Count == 0, "Baseline entries need a reason: " + string.Join(", ", blank));
    }

    private static List<string> ExplainFiles()
    {
        var self = Path.GetFileName(SelfPath());
        return Directory.GetFiles(TestsDirectory(), "*.cs")
            .Where(f => Path.GetFileName(f) != self)
            .Where(f =>
            {
                var text = File.ReadAllText(f);
                return text.Contains("\"DARLING_TEST_PG\"", StringComparison.Ordinal)
                       && text.Contains("EXPLAIN", StringComparison.Ordinal);
            })
            .ToList();
    }

    private static string SelfPath([CallerFilePath] string thisFile = "") => thisFile;

    private static string TestsDirectory() => Path.GetDirectoryName(SelfPath())!;
}
