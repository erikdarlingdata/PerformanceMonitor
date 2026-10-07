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
/// Every Darling.Tests class that is a guard carries <c>[Trait("Stage", "Guard")]</c>, or is on the exemption
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
    /// <summary>The classes that are guard-style and stay untagged, each with the reason. Shrinks only.</summary>
    private static readonly Dictionary<string, string> s_exempt = new(StringComparer.Ordinal)
    {
        ["AlertDeliveryChannelTests"] = "R2 only: it walks the repo for a source check but also holds tests that reach the shared store through a helper (R3).",
        ["DarlingEmptyEnumerationInventoryTests"] = "R3: live-postgres collection and reads DARLING_TEST_PG; a store run only.",
        ["DarlingOwnedSecretsCensusTests"] = "R3: sets and reads its own DARLING_TEST_OIDC_SECRET_OWNEDSET variable; the shards keep it.",
        ["DarlingPgRuntimeVersionPinTests"] = "R3: gated on DARLING_TEST_PGRUNTIME, which only the darling-pg job sets.",
        ["StoreApplicationNamePinTests"] = "R3: reads DARLING_TEST_PG to create its own scratch database.",
        ["WebExceptionTextCensusTests"] = "R3: live-postgres collection and reads DARLING_TEST_PG; a store run only.",
    };

    /// <summary>The size of the list above. A new entry is a deliberate edit of this number and a reason.</summary>
    private const int ExemptionCeiling = 6;

    /// <summary>Anti-vacuity: the suite tags well over this many classes, so a scan that stopped seeing them fails.</summary>
    private const int TaggedFloor = 50;

    [Fact]
    public void TheDetector_SeesTheTraitTheCollectionAndARepoRootWalk()
    {
        const string text = "namespace Darling.Tests;\n\n"
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
        var rows = GuardStageScanner.Scan(TestsRoot(), ProjectFile());

        var problems = GuardStageScanner.Problems(rows, s_exempt);

        Assert.True(
            problems.Count == 0,
            $"The Darling.Tests guard-stage tagging is wrong. Add {GuardStageScanner.GuardTraitText} directly above each "
          + "MISSING class (build.yml's guard-tests job then runs it first), or, when the class needs a store, list it in "
          + "GuardStageCensusTests.s_exempt with the reason:\n  " + string.Join("\n  ", problems));

        var tagged = rows.Count(r => r.Tagged);
        Assert.True(
            tagged >= TaggedFloor,
            $"only {tagged} Darling.Tests classes carry {GuardStageScanner.GuardTraitText}; the scan has stopped seeing them");
        Assert.True(
            rows.Count(r => r.EnumeratesRepoRoot) >= 10,
            "fewer than 10 Darling.Tests classes were found enumerating the repo root; the scan has stopped seeing them");
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
          + "needs a store (the guard job has no PostgreSQL). Write the trait as the one attribute line above the class, or "
          + "list the project's link in the csproj so the scan reads it:\n  " + string.Join("\n  ", problems));

        Assert.True(
            reflected.Count >= TaggedFloor,
            $"xunit selects only {reflected.Count} Darling.Tests classes for Stage=Guard; the reflection has stopped seeing them");
    }

    [Fact]
    public void TheReflectionCheck_FlagsADisagreementAPhantomAndAStoreClass()
    {
        var rows = new List<GuardStageScanner.Row>
        {
            new("SeenTests", "a.cs", true, true, false, false, false, false),
            new("MethodTaggedTests", "b.cs", true, false, false, false, false, false),
            new("MethodTaggedLiveTests", "c.cs", true, false, false, false, true, false),
            new("PhantomTests", "d.cs", true, true, false, false, false, false),
        };
        var reflected = new List<GuardStageScanner.ReflectedClass>
        {
            new("SeenTests", false),
            new("MethodTaggedTests", false),
            new("MethodTaggedLiveTests", false),
            new("LinkedTests", false),
            new("CollectionTests", true),
        };

        var problems = GuardStageScanner.ReflectionProblems(rows, reflected);

        Assert.Contains(problems, p => p.StartsWith("UNTAGGED", StringComparison.Ordinal) && p.Contains("MethodTaggedTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("UNTAGGED", StringComparison.Ordinal) && p.Contains("MethodTaggedLiveTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("UNSCANNED", StringComparison.Ordinal) && p.Contains("LinkedTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("UNSCANNED", StringComparison.Ordinal) && p.Contains("CollectionTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("PHANTOM", StringComparison.Ordinal) && p.Contains("PhantomTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("STORE", StringComparison.Ordinal) && p.Contains("MethodTaggedLiveTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("STORE", StringComparison.Ordinal) && p.Contains("CollectionTests", StringComparison.Ordinal));
        Assert.DoesNotContain(problems, p => p.Contains("SeenTests", StringComparison.Ordinal) && !p.Contains("MethodTagged", StringComparison.Ordinal));
        Assert.Equal(7, problems.Count);
    }

    /* The probes below are private nested classes, which xunit does not run and the assembly-wide reflection skips.
       Their traits are spelled with constants so the source scan does not read them as tagged (it would otherwise
       report them as phantoms): the very spelling the reflection exists to see. */
    private const string ProbeStage = "Stage";
    private const string ProbeGuard = "Guard";
    private const string ProbeLive = "live-postgres";

    private sealed class ProbeTestAttribute : Attribute
    {
    }

    [Trait(ProbeStage, ProbeGuard)]
    private sealed class ProbeClassTagged
    {
        [ProbeTest]
        public void Probe()
        {
        }
    }

    private sealed class ProbeMethodTagged
    {
        [ProbeTest, Trait(ProbeStage, ProbeGuard)]
        public void Probe()
        {
        }
    }

    [Collection(ProbeLive), Trait(ProbeStage, ProbeGuard)]
    private sealed class ProbeLiveTagged
    {
        [ProbeTest]
        public void Probe()
        {
        }
    }

    private sealed class ProbeUntagged
    {
        [ProbeTest]
        public void Probe()
        {
        }
    }

    [Trait(ProbeStage, ProbeGuard)]
    private abstract class ProbeTaggedBase
    {
        [ProbeTest]
        public void Probe()
        {
        }
    }

    private sealed class ProbeInheritsTag : ProbeTaggedBase
    {
    }

    /* xunit v3 runs static and non-public test methods and static classes (L1 of #5471's second review), so the
       reflection has to see a trait on each of them as well. */
    private sealed class ProbeStaticMethodTagged
    {
        [ProbeTest, Trait(ProbeStage, ProbeGuard)]
        public static void Probe()
        {
        }
    }

    private sealed class ProbePrivateMethodTagged
    {
        [ProbeTest, Trait(ProbeStage, ProbeGuard)]
#pragma warning disable IDE0051, CA1822
        private void Probe()
#pragma warning restore IDE0051, CA1822
        {
        }
    }

    [Trait(ProbeStage, ProbeGuard)]
    private static class ProbeStaticClassTagged
    {
        [ProbeTest]
        public static void Probe()
        {
        }
    }

    private static bool IsProbeTest(System.Reflection.MethodInfo m) => m.IsDefined(typeof(ProbeTestAttribute), inherit: true);

    [Fact]
    public void TheReflection_SeesAClassTraitAMethodTraitAConstantAnInheritedTraitAndALiveCollection()
    {
        var probes = typeof(GuardStageCensusTests).GetNestedTypes(System.Reflection.BindingFlags.NonPublic)
            .Where(t => t.Name.StartsWith("Probe", StringComparison.Ordinal) && t.Name != nameof(ProbeTestAttribute));

        var reflected = GuardStageScanner.ReflectGuardClasses(probes, assemblyTagged: false, isTest: IsProbeTest).ToDictionary(r => r.Name);

        Assert.True(reflected.ContainsKey(nameof(ProbeClassTagged)), "a class-level trait spelled with constants is not seen");
        Assert.True(reflected.ContainsKey(nameof(ProbeMethodTagged)), "a method-level trait is not seen");
        Assert.True(reflected.ContainsKey(nameof(ProbeInheritsTag)), "a trait inherited from a base class is not seen");
        Assert.True(reflected.ContainsKey(nameof(ProbeLiveTagged)) && reflected[nameof(ProbeLiveTagged)].LiveCollection, "a live-postgres collection is not seen");
        Assert.True(reflected.ContainsKey(nameof(ProbeStaticMethodTagged)), "a static test method with a method-level trait is not seen");
        Assert.True(reflected.ContainsKey(nameof(ProbePrivateMethodTagged)), "a non-public test method with a method-level trait is not seen");
        Assert.True(reflected.ContainsKey(nameof(ProbeStaticClassTagged)), "a static class is not seen");
        Assert.False(reflected.ContainsKey(nameof(ProbeUntagged)), "an untagged class was selected");
        Assert.False(reflected.ContainsKey(nameof(ProbeTaggedBase)), "an abstract class was selected");
        Assert.False(reflected[nameof(ProbeClassTagged)].LiveCollection);

        Assert.Contains(
            GuardStageScanner.ReflectGuardClasses(probes.Where(t => t.Name == nameof(ProbeUntagged)), assemblyTagged: true, isTest: IsProbeTest),
            r => r.Name == nameof(ProbeUntagged));
    }

    [Fact]
    public void TheLinkedFiles_AreReadFromTheCsprojItems()
    {
        var links = GuardStageScanner.LinkedCompileFiles(ProjectFile());

        Assert.NotEmpty(links);
        Assert.All(links, l => Assert.True(File.Exists(l.Path), $"the linked file {l.Link} does not exist at {l.Path}"));
    }

    [Fact]
    public void TheExemptionList_HasItsPinnedSize()
    {
        Assert.Equal(ExemptionCeiling, s_exempt.Count);
    }

    private static string TestsRoot([CallerFilePath] string thisFile = "") =>
        Path.GetDirectoryName(thisFile)
        ?? throw new InvalidOperationException("The caller file path has no directory.");

    private static string ProjectFile() => Path.Combine(TestsRoot(), "Darling.Tests.csproj");
}
