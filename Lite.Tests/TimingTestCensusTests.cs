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
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Darling.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// A Lite.Tests class that starts a clock runs in the <c>timing</c> collection or is on the allow list with its
/// reason (#5602). xunit runs test classes in parallel, so a class that times its work and fails past a limit is
/// judging the whole runner's load. <see cref="TimingCollection"/> is declared with <c>DisableParallelization</c>, so
/// xunit runs it alone, after the parallel classes. The rule, the scanner and its own tests are the Darling.Tests
/// ones (<c>TimingTestScanner</c>, linked into this project); this class applies them to Lite.Tests. The allow list
/// is per file, every entry carries its reason, and an entry whose file no longer starts a clock outside the
/// collection fails, so the list only shrinks.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class TimingTestCensusTests
{
    private const string HangGuard = "hang guard: the bound is a generous ceiling that only stops a stuck test, so load cannot trip it and a regression is not what it measures";

    private const string Diagnostic = "diagnostic: the time is written to the test output and nothing asserts on it";

    private const string OwnCollection = "already in a collection with DisableParallelization (process-wide state), so it runs alone after the parallel classes, which is all the timing collection does";

    /// <summary>Anti-vacuity: the timing collection holds at least this many classes, so a scan that stopped seeing it fails.</summary>
    private const int TimingMemberFloor = 2;

    private static readonly Dictionary<string, string> s_allowed = new(StringComparer.Ordinal)
    {
        ["CollectionResetGateTests.cs"] = OwnCollection + "; the 200 ms bound is a lower bound (the reset waited for the gate), which load only lengthens",
        ["DuckDbSentinelConnectionTests.cs"] = OwnCollection + " (CollectionResetGate)",
        ["PostCollectionCheckpointBudgetTests.cs"] = OwnCollection + " (CollectionResetGate)",
        ["QuerySnapshotPlanOnDemandTests.cs"] = Diagnostic + " (old and new read shapes compared on rows and allocations)",
        ["QueryWindowTruncationTests.cs"] = Diagnostic + " (written to the console)",
        ["TestCpuAccountantTests.cs"] = HangGuard + " (the loops spin until the accountant has moved, with 30 s as the stop)",
    };

    [Fact]
    public void EveryClassThatStartsAClock_RunsInTheTimingCollection_OrIsAllowedWithAReason()
    {
        var rows = TimingTestScanner.Scan(TestsRoot());

        var problems = TimingTestScanner.Problems(rows, s_allowed);
        Assert.True(
            problems.Count == 0,
            "A Lite.Tests class starts a clock outside the `timing` collection (#5602). xunit runs classes in parallel, so a time "
          + "limit judged there is judged against the runner's load. Add [Collection(\"timing\")] above the class (TimingCollection "
          + "runs alone, after the parallel classes), assert on the work instead of the clock, or, for a hang guard, a poll loop or a "
          + "diagnostic, name the file in TimingTestCensusTests.s_allowed with the reason:\n  " + string.Join("\n  ", problems));

        var stale = TimingTestScanner.Stale(rows, s_allowed);
        Assert.True(
            stale.Count == 0,
            "These allow-list entries no longer name a file that starts a clock outside the timing collection; delete them:\n  "
          + string.Join("\n  ", stale));

        var members = rows.Count(r => r.InTimingCollection);
        Assert.True(
            members >= TimingMemberFloor,
            $"only {members} Lite.Tests classes are in the timing collection; the scan has stopped seeing them");
    }

    [Fact]
    public void EveryAllowListEntry_NamesARealFile_AndGivesAReason()
    {
        foreach (var (file, reason) in s_allowed)
        {
            Assert.True(File.Exists(Path.Combine(TestsRoot(), file)), $"allow-list entry {file} is not a file under Lite.Tests");
            Assert.False(string.IsNullOrWhiteSpace(reason), $"allow-list entry {file} has no reason");
        }
    }

    [Fact]
    public void TheTimingCollection_IsNotParallel_AndIsTheOnlyOneOfItsName()
    {
        var definitions = typeof(TimingTestCensusTests).Assembly.GetTypes()
            .Select(t => (Type: t, Attribute: t.GetCustomAttribute<CollectionDefinitionAttribute>()))
            .Where(d => d.Attribute is not null && string.Equals(d.Attribute.Name, TimingTestScanner.CollectionName, StringComparison.Ordinal))
            .ToList();

        var definition = Assert.Single(definitions);
        Assert.True(definition.Attribute!.DisableParallelization, "the timing collection must run alone, or it protects nothing");
    }

    private static string TestsRoot([CallerFilePath] string thisFile = "") => Path.GetDirectoryName(thisFile)!;
}
