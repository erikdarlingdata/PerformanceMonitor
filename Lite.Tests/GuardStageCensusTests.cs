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
using Darling.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Every Lite.Tests class that is a guard carries <c>[Trait("Stage", "Guard")]</c>, or is on the exemption
/// list below with a reason (#5459).
///
/// <para><b>Why the trait exists.</b> build.yml's <c>guard-tests</c> job is the first thing a pull request runs.
/// It runs only the tagged classes, in both test suites, so a source pin, a census or a hygiene scan fails in
/// about ten minutes and every shard is skipped, instead of the failure arriving after the whole run. An untagged
/// guard class loses that fast failure and nothing else (the shards still run it), which nobody would notice, so
/// the requirement is derived from the source and this class fails and names the class.</para>
///
/// <para><b>The rules.</b> R1: a class whose name ends in Hygiene, Census, Convention, Adoption, Ratchet, Guard,
/// SourcePin, Pin(s), Drift or Inventory plus <c>Tests</c> carries the trait or is exempt. R2: a class that
/// enumerates the repo root recursively carries it or is exempt. R3: no tagged class is in the
/// <c>live-postgres</c> collection, reads a <c>DARLING_TEST_</c> variable, or refers to a helper that does,
/// because the guard job has no PostgreSQL and every tagged class runs there. The exemption list is the one place
/// a class that is guard-style and cannot be tagged is named; its size is pinned and may only shrink.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class GuardStageCensusTests
{
    /// <summary>The classes that are guard-style and stay untagged, each with the reason. Empty today; it only shrinks, and TheExemptionList_StaysEmpty pins that.</summary>
    private static readonly Dictionary<string, string> s_exempt = new(StringComparer.Ordinal)
    {
    };

    /// <summary>Anti-vacuity: the suite tags well over this many classes, so a scan that stopped seeing them fails.</summary>
    private const int TaggedFloor = 30;

    [Fact]
    public void TheDetector_SeesTheTraitTheCollectionAndARepoRootWalk()
    {
        const string text = "namespace Lite.Tests;\n\n"
          + "[Trait(\"Stage\", \"Guard\")]\npublic sealed class AdoptionTests\n{\n    [Fact]\n    public void A() { }\n}\n\n"
          + "[Collection(\"live-postgres\")]\npublic sealed class LiveThingTests\n{\n    [Fact]\n    public void B() { }\n}\n\n"
          + "public sealed class WholeTreeScanTests\n{\n    [Fact]\n    public void C()\n    {\n"
          + "        var root = RepoRoot();\n"
          + "        var all = Directory.EnumerateFiles(root, \"*.cs\", SearchOption.AllDirectories);\n    }\n}\n";

        var rows = GuardStageScanner.ScanText(text).ToDictionary(r => r.Name);

        Assert.True(rows["AdoptionTests"].Tagged);
        Assert.False(rows["AdoptionTests"].LiveCollection);
        Assert.True(rows["LiveThingTests"].LiveCollection);
        Assert.False(rows["LiveThingTests"].Tagged);
        Assert.True(rows["WholeTreeScanTests"].EnumeratesRepoRoot);
        Assert.False(rows["WholeTreeScanTests"].Tagged);
    }

    [Fact]
    public void TheDetector_IgnoresANarrowerEnumerationAndProse()
    {
        Assert.True(GuardStageScanner.EnumeratesRepoRootRecursively(
            "var f = Directory.GetFiles(ParitySource.RepoRoot(), \"*.csproj\", SearchOption.AllDirectories);"));
        Assert.True(GuardStageScanner.EnumeratesRepoRootRecursively(
            "string root = RepoRootOrFail();\nvar o = new EnumerationOptions { RecurseSubdirectories = true };\n"
          + "var f = Directory.EnumerateFiles(root, \"*.props\", o);"));

        Assert.False(GuardStageScanner.EnumeratesRepoRootRecursively(
            "var root = RepoRoot();\nvar f = Directory.EnumerateFiles(Path.Combine(root, \"Lite\"), \"*.cs\", SearchOption.AllDirectories);"));
        Assert.False(GuardStageScanner.EnumeratesRepoRootRecursively(
            "var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, \"*.cs\", SearchOption.TopDirectoryOnly);"));
        Assert.False(GuardStageScanner.EnumeratesRepoRootRecursively(
            "var root = RepoRoot();\n// Directory.EnumerateFiles(root, \"*.cs\", SearchOption.AllDirectories)\nvar a = 1;"));
    }

    [Fact]
    public void TheCensus_FlagsAnUntaggedCandidateATaggedStoreClassAndAStaleExemption()
    {
        var rows = new List<GuardStageScanner.Row>
        {
            new("SomeCensusTests", "a.cs", true, false, false, false, false, false),
            new("LiveDriftTests", "b.cs", true, true, true, false, false, false),
            new("PlainTests", "c.cs", true, false, false, false, false, false),
            new("GoneInventoryTests", "d.cs", true, true, false, false, false, false),
        };

        var problems = GuardStageScanner.Problems(
            rows,
            new Dictionary<string, string>
            {
                ["GoneInventoryTests"] = "now tagged",
                ["NoSuchPinTests"] = "deleted",
                ["PlainTests"] = "",
            });

        Assert.Contains(problems, p => p.StartsWith("MISSING", StringComparison.Ordinal) && p.Contains("SomeCensusTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("STORE", StringComparison.Ordinal) && p.Contains("LiveDriftTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("STALE", StringComparison.Ordinal) && p.Contains("GoneInventoryTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("STALE", StringComparison.Ordinal) && p.Contains("NoSuchPinTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("REASON", StringComparison.Ordinal) && p.Contains("PlainTests", StringComparison.Ordinal));
        Assert.Equal(5, problems.Count);
    }

    [Fact]
    public void EveryGuardClass_CarriesTheStageGuardTrait_OrIsExempt_AndNoTaggedClassNeedsAStore()
    {
        var rows = GuardStageScanner.Scan(TestsRoot());

        var problems = GuardStageScanner.Problems(rows, s_exempt);

        Assert.True(
            problems.Count == 0,
            $"The Lite.Tests guard-stage tagging is wrong. Add {GuardStageScanner.GuardTraitText} directly above each "
          + "MISSING class (build.yml's guard-tests job then runs it first), or, when the class needs a store, list it in "
          + "GuardStageCensusTests.s_exempt with the reason:\n  " + string.Join("\n  ", problems));

        var tagged = rows.Count(r => r.Tagged);
        Assert.True(
            tagged >= TaggedFloor,
            $"only {tagged} Lite.Tests classes carry {GuardStageScanner.GuardTraitText}; the scan has stopped seeing them");
        Assert.True(
            rows.Count(r => r.EnumeratesRepoRoot) >= 3,
            "fewer than 3 Lite.Tests classes were found enumerating the repo root; the scan has stopped seeing them");
    }

    [Fact]
    public void TheExemptionList_StaysEmpty()
    {
        Assert.Empty(s_exempt);
    }

    private static string TestsRoot() => Path.Combine(ParitySource.RepoRoot(), "Lite.Tests");
}
