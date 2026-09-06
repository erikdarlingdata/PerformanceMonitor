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
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Darling FinOps tab loads a sub-tab's data by switching on its POSITIONAL index
/// (<c>FinOpsTab.Loaders.cs</c>'s <c>FinOps*SubTabIndex</c> constants), so those constants and the
/// <c>TabItem</c> order in <c>FinOpsTab.xaml</c> are one contract maintained in two files. Nothing enforced
/// it until #1951, which inserted a tab in the MIDDLE and had to hand-shift six constants.
///
/// <para>The failure this pins is silent and awful to diagnose: insert a tab above an existing index and
/// every constant below it points one tab too high, so selecting a sub-tab loads a DIFFERENT tab's data
/// into a grid that renders empty (its rows bind properties the other loader never filled) — no exception,
/// no log line, and the tab that "does not work" is not the one that was changed.</para>
///
/// <para>Source-scanned rather than reflected: the constants are private and the XAML is a WPF resource,
/// so a runtime check would need the app. The two files are parsed the same way the #1949 grid pins parse
/// theirs.</para>
/// </summary>
public sealed class FinOpsSubTabIndexPinTests
{
    /// <summary>Constant name suffix stripped to give the header it must correspond to.</summary>
    private static readonly (string ConstantName, string TabHeader)[] Expected =
    [
        ("FinOpsUtilizationSubTabIndex", "Utilization"),
        ("FinOpsDatabaseResourcesSubTabIndex", "Database Resources"),
        ("FinOpsStorageGrowthSubTabIndex", "Storage Growth"),
        ("FinOpsLockingSubTabIndex", "Locking &amp; Contention"),
        ("FinOpsDatabaseSizesSubTabIndex", "Database Sizes"),
        ("FinOpsPvsStatsSubTabIndex", "Version Store (PVS)"),
        ("FinOpsOptimizationSubTabIndex", "Optimization"),
        ("FinOpsHighImpactSubTabIndex", "High Impact"),
        ("FinOpsApplicationConnectionsSubTabIndex", "Application Connections"),
        ("FinOpsServerInventorySubTabIndex", "Server Inventory"),
        ("FinOpsIndexAnalysisSubTabIndex", "Index Analysis"),
        ("FinOpsRecommendationsSubTabIndex", "Recommendations"),
    ];

    [Fact]
    public void EveryDeclaredSubTabIndex_MatchesItsTabItemPosition()
    {
        var declared = DeclaredIndexes();
        var headers = TabHeadersInOrder();

        foreach (var (constantName, tabHeader) in Expected)
        {
            Assert.True(declared.TryGetValue(constantName, out int index),
                $"{constantName} is gone from FinOpsTab.Loaders.cs. If the sub-tab was removed, drop its row " +
                "from this table in the same commit; otherwise the loader switch has lost an arm.");

            Assert.True(index >= 0 && index < headers.Count,
                $"{constantName} = {index} but FinOpsTab.xaml has {headers.Count} sub-tabs.");

            Assert.True(string.Equals(headers[index], tabHeader, StringComparison.Ordinal),
                $"{constantName} = {index}, which is the '{headers[index]}' tab, not '{tabHeader}'. " +
                "A TabItem was inserted or moved without re-numbering the constants below it: selecting that " +
                "sub-tab now runs another tab's loader and renders an empty grid with no error.");
        }
    }

    [Fact]
    public void TheTableCoversEverySubTab_SoAnAddedTabCannotSlipInUnpinned()
    {
        var headers = TabHeadersInOrder();

        Assert.True(headers.Count == Expected.Length,
            $"FinOpsTab.xaml has {headers.Count} sub-tabs but this pin table holds {Expected.Length}. " +
            "Adding a sub-tab means adding its index constant, its arm in the loader switch, and a row here " +
            $"— in tab order. Current headers: {string.Join(" | ", headers)}");

        /* Scan self-check: a parser that silently returned nothing would make the loop above vacuous. */
        Assert.True(DeclaredIndexes().Count >= Expected.Length,
            "the constant scan found fewer declarations than the pin table — the parser is broken.");
    }

    private static Dictionary<string, int> DeclaredIndexes()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs"));

        var found = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(
            source, @"private\s+const\s+int\s+(FinOps\w*SubTabIndex)\s*=\s*(\d+)\s*;"))
        {
            found[m.Groups[1].Value] = int.Parse(m.Groups[2].Value, System.Globalization.CultureInfo.InvariantCulture);
        }

        return found;
    }

    private static List<string> TabHeadersInOrder()
    {
        var xaml = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml"));

        return Regex.Matches(xaml, @"<TabItem\s+Header=""([^""]*)""")
            .Select(m => m.Groups[1].Value)
            .ToList();
    }

}
