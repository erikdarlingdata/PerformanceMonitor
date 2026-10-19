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
/// The guard stage's tagging census for Darling.Tests (#5459, restored by #6099). A class that is a guard carries
/// <c>[Trait("Stage", "Guard")]</c>, or is on the exemption list below with a reason.
///
/// <para><b>Why it is back.</b> #5833 deleted the first census with the other spelling tests, and nothing read
/// <see cref="GuardStageScanner"/> after that. Five no-store source scans then ran only in the PostgreSQL shards and failed
/// pull requests about 25 minutes in (#6099). The scanner and this class are the only thing that notices an untagged
/// guard class, so it is a census on purpose: a class that is missed fails nothing else, and nobody would see it.</para>
///
/// <para><b>The rules.</b> R1: a class named like a guard (Hygiene, Census, Convention, Adoption, Ratchet, Guard, SourcePin,
/// Pin(s), Drift or Inventory plus <c>Tests</c>). R2: a class that walks the repo root recursively. R4 (#5776): a class
/// that reaches a whole catalog (the MCP tools, a product assembly's types, every collector). R5 (#6099): a class that reads
/// repository source files by any route (<c>RepoFile</c>, <c>ParitySource</c>, a repo-root call) and needs no store. Each
/// carries the trait, or is exempt below, or (R4 and R5 only) is on <see cref="GuardStageScanner.SlowCatalogClasses"/>. R3: a
/// tagged class needs no store, because the guard job has no PostgreSQL.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class GuardStageCensusTests
{
    /// <summary>The classes that are guard candidates and stay untagged because they need a store (R3), each with the reason.
    /// They run on the PostgreSQL shards only. Shrinks only.</summary>
    private static readonly Dictionary<string, string> s_exempt = new(StringComparer.Ordinal)
    {
        ["AlertHistoryAdviceLiveTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["AttentionPlainTableTests"] = "R3: reads a DARLING_TEST_ variable; a store run only.",
        ["CollectionHealthPayloadBudgetLiveTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["CollectorStateContractTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["CollectorTableCatalogShadowTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable and reaches the store through a helper; a store run only.",
        ["DarlingAnalysisPipelineTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["DarlingAnalysisStoreTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["DarlingEmptyEnumerationInventoryTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["DarlingMcpFinOpsToolsTests"] = "R3: reads a DARLING_TEST_ variable; a store run only.",
        ["DarlingMcpToolsTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["DarlingOwnedSecretsCensusTests"] = "R3: reads a DARLING_TEST_ variable; a store run only.",
        ["DarlingPgReadSqlParsesLiveTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["DarlingPgRuntimeVersionPinTests"] = "R3: reads a DARLING_TEST_ variable; a store run only.",
        ["DarlingRetentionTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable and reaches the store through a helper; a store run only.",
        ["GatedCollectorRowsLiveTests"] = "R3: reads a DARLING_TEST_ variable; a store run only.",
        ["McpReadToolBudgetLiveTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["PayloadDimensionLiveTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["PgCopyWriterTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["PgFactCollectorTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["PgTargetAnomalyTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["PgTargetBufferDrillDownTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["PgTargetFactCollectorTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["PseudonymKeyLiveTests"] = "R3: reads a DARLING_TEST_ variable; a store run only.",
        ["QueryStatsHourLedgerTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["StoreApplicationNamePinTests"] = "R3: reads a DARLING_TEST_ variable; a store run only.",
        ["StoreSelfMetricsTests"] = "R3: reads a DARLING_TEST_ variable; a store run only.",
        ["StoreStatementHistoryRungTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
        ["TimescaleSupportTests"] = "R3: live-postgres collection and reads a DARLING_TEST_ variable; a store run only.",
    };

    /// <summary>The size of the list above. A new entry is a deliberate edit of this number and a reason.</summary>
    private const int ExemptionCeiling = 28;

    /// <summary>The size of <see cref="GuardStageScanner.SlowCatalogClasses"/>. A new entry is a deliberate edit of this number
    /// and a measured cost.</summary>
    private const int SlowCeiling = 6;

    /// <summary>Anti-vacuity: the suite tags well over this many classes, so a scan that stopped seeing them fails.</summary>
    private const int TaggedFloor = 200;

    [Fact]
    public void EveryGuardCandidate_CarriesTheTrait_OrIsExempt_AndNoTaggedClassNeedsAStore()
    {
        var rows = GuardStageScanner.Scan(TestsRoot(), ProjectFile());

        var problems = GuardStageScanner.Problems(rows, s_exempt, GuardStageScanner.SlowCatalogClasses);

        Assert.True(
            problems.Count == 0,
            $"The Darling.Tests guard-stage tagging is wrong. Add {GuardStageScanner.GuardTraitText} directly above each MISSING "
          + "class (build.yml's guard-tests job then runs it on every pull request), or, when the class needs a store, list it in "
          + "GuardStageCensusTests.s_exempt with the reason:\n  " + string.Join("\n  ", problems));

        Assert.True(
            rows.Count(r => r.Tagged) >= TaggedFloor,
            $"fewer than {TaggedFloor} Darling.Tests classes carry {GuardStageScanner.GuardTraitText}; the scan has stopped seeing them");
        Assert.True(
            rows.Count(r => r.SourceScan) >= 200,
            "fewer than 200 Darling.Tests classes were read as source scans (R5); the scan has stopped seeing them");
    }

    [Fact]
    public void TheTaggedSet_IsTheSetXunitSelects_AndRuleR3HoldsOverIt()
    {
        var rows = GuardStageScanner.Scan(TestsRoot(), ProjectFile());
        var reflected = GuardStageScanner.ReflectGuardClasses(typeof(GuardStageCensusTests).Assembly);

        var problems = GuardStageScanner.ReflectionProblems(rows, reflected);

        Assert.True(
            problems.Count == 0,
            "The source scan and xunit disagree about which Darling.Tests classes carry Stage=Guard, or a class xunit selects "
          + "needs a store (the guard job has no PostgreSQL):\n  " + string.Join("\n  ", problems));
        Assert.True(reflected.Count >= TaggedFloor, $"xunit selects only {reflected.Count} classes for Stage=Guard");
    }

    [Fact]
    public void RuleR5_FlagsAnUntaggedSourceScan_HoweverItIsNamed_AndOnlyThatOne()
    {
        const string text = "namespace Synthetic;\n\n"
          + "public sealed class PlainReadTests\n{\n    [Fact]\n    public void A() { var s = RepoFile.ReadRepoFile(\"Darling\", \"x.cs\"); }\n}\n\n"
          + "public sealed class OldStyleReadTests\n{\n    [Fact]\n    public void B() { var s = File.ReadAllText(Path.Combine(FindRepoRoot(), \"x.cs\")); }\n}\n\n"
          + "[Trait(\"Stage\", \"Guard\")]\npublic sealed class TaggedReadTests\n{\n    [Fact]\n    public void C() { var s = RepoFile.ReadRepoFile(\"x.cs\"); }\n}\n\n"
          + "[Collection(\"live-postgres\")]\npublic sealed class LiveReadTests\n{\n    [Fact]\n    public void D() { var s = RepoFile.ReadRepoFile(\"x.cs\"); var v = Environment.GetEnvironmentVariable(\"DARLING_TEST_PG\"); }\n}\n\n"
          + "public sealed class InMemoryTests\n{\n    [Fact]\n    public void E() { var s = CSharpSourceWalker.StripCommentsAndStrings(\"class X {}\"); }\n}\n";

        var rows = GuardStageScanner.ScanText(text).ToDictionary(r => r.Name);

        Assert.True(rows["PlainReadTests"].SourceScan);
        Assert.True(rows["OldStyleReadTests"].SourceScan);
        Assert.False(rows["LiveReadTests"].SourceScan);
        Assert.False(rows["InMemoryTests"].SourceScan);

        var problems = GuardStageScanner.Problems(rows.Values.ToList(), new Dictionary<string, string>());

        Assert.Contains(problems, p => p.StartsWith("MISSING", StringComparison.Ordinal) && p.Contains("PlainReadTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("MISSING", StringComparison.Ordinal) && p.Contains("OldStyleReadTests", StringComparison.Ordinal));
        Assert.DoesNotContain(problems, p => p.Contains("InMemoryTests", StringComparison.Ordinal));
        Assert.DoesNotContain(problems, p => p.Contains("LiveReadTests", StringComparison.Ordinal));
        Assert.DoesNotContain(problems, p => p.Contains("TaggedReadTests", StringComparison.Ordinal));
        Assert.Equal(2, problems.Count);
    }

    [Fact]
    public void ANestedStoreClassNamedLikeAMember_IsNoStoreHelper()
    {
        /* #6099: a private nested class named Store in a live file made every class that wrote Hop.Store a store user. */
        const string text = "namespace Synthetic;\n\n"
          + "public sealed class LiveStoreTests\n{\n    private sealed class Store\n    {\n        string cs = Environment.GetEnvironmentVariable(\"DARLING_TEST_PG\");\n    }\n\n"
          + "    [Fact]\n    public void A() { }\n}\n\n"
          + "[Trait(\"Stage\", \"Guard\")]\npublic sealed class HopPinTests\n{\n    private enum Hop { Store }\n\n    [Fact]\n    public void B() { var h = Hop.Store; }\n}\n";

        var rows = GuardStageScanner.ScanText(text);

        Assert.DoesNotContain(GuardStageScanner.Problems(rows, new Dictionary<string, string>()), p => p.StartsWith("STORE", StringComparison.Ordinal));
    }

    [Fact]
    public void TheLinkedFiles_AreReadFromTheCsprojItems()
    {
        var links = GuardStageScanner.LinkedCompileFiles(ProjectFile());

        Assert.NotEmpty(links);
        Assert.All(links, l => Assert.True(File.Exists(l.Path), $"the linked file {l.Link} does not exist at {l.Path}"));
    }

    [Fact]
    public void TheExemptionLists_HaveTheirPinnedSize()
    {
        Assert.Equal(ExemptionCeiling, s_exempt.Count);
        Assert.Equal(SlowCeiling, GuardStageScanner.SlowCatalogClasses.Count);
    }

    private static string TestsRoot([CallerFilePath] string thisFile = "") =>
        Path.GetDirectoryName(thisFile)
        ?? throw new InvalidOperationException("The caller file path has no directory.");

    private static string ProjectFile() => Path.Combine(TestsRoot(), "Darling.Tests.csproj");
}
