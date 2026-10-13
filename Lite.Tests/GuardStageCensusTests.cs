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
///
/// <para><b>Rule R4 (#5776).</b> A class that reaches a whole catalog (the MCP tool catalog, every type of a product
/// assembly, or every collector definition) names no symbol a change touches, so a lane's selection by changed
/// symbol cannot find it, and a new tool or collector is exactly what turns it red. It carries
/// <c>[Trait("Stage", "Guard")]</c> or is exempt with its R3 reason. The scanner's slow list (classes of 50 s or more
/// in the nightly map) names Darling.Tests classes only, so this suite passes none.</para>
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
    public void TheCatalogDetector_SeesTheToolCatalogAProductAssemblyAndTheCollectorSet()
    {
        /* Rule R4 (#5776): each planted reach is flagged. */
        Assert.True(GuardStageScanner.ReachesACatalogInText(
            "var types = typeof(DarlingMcpTools).Assembly.GetTypes().Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null);"));
        Assert.True(GuardStageScanner.ReachesACatalogInText(
            "foreach (var type in assembly.GetTypes()) { }"));
        Assert.True(GuardStageScanner.ReachesACatalogInText(
            "var types = typeof(PgIndexBloatCollector).Assembly\n            .GetExportedTypes();"));
        Assert.True(GuardStageScanner.ReachesACatalogInText(
            "services.AddMcpServer().WithToolsFromAssembly(typeof(DarlingMcpTools).Assembly);"));
        Assert.True(GuardStageScanner.ReachesACatalogInText(
            "var sqlServer = CollectorCatalog.All.Where(c => c.Name.Length > 0).ToList();"));
        Assert.True(GuardStageScanner.ReachesACatalogInText(
            "Assert.Contains(PerformanceMonitor.Collectors.CollectorCatalog . All, c => c.Name == name);"));

        /* And these are not: the test assembly's own types, one named collector, a near-miss member, prose and literals. */
        Assert.False(GuardStageScanner.ReachesACatalogInText(
            "var mine = typeof(SomeCensusTests).Assembly.GetTypes();"));
        Assert.False(GuardStageScanner.ReachesACatalogInText(
            "var mine = typeof(Darling.Tests.SomeCensusTests).Assembly.GetExportedTypes();"));
        Assert.False(GuardStageScanner.ReachesACatalogInText(
            "var mine = Assembly.GetExecutingAssembly().GetTypes();"));
        Assert.False(GuardStageScanner.ReachesACatalogInText(
            "var one = CollectorCatalog.Find(\"wait_stats\"); var ok = CollectorCatalog.AllowsSomething;"));
        Assert.False(GuardStageScanner.ReachesACatalogInText(
            "var attr = method.GetCustomAttribute<McpServerToolAttribute>();"));
        Assert.False(GuardStageScanner.ReachesACatalogInText(
            "// every [McpServerToolType] class, CollectorCatalog.All and assembly.GetTypes()\nvar a = \"McpServerToolTypeAttribute CollectorCatalog.All\";"));
    }

    [Fact]
    public void TheCensus_FlagsAnUntaggedCatalogClassAndATaggedStoreClass()
    {
        const string text = "namespace Synthetic;\n\n"
          + "[Trait(\"Stage\", \"Guard\")]\npublic sealed class CatalogSweepTests\n{\n    [Fact]\n    public void A() { var all = CollectorCatalog.All; }\n}\n\n"
          + "public sealed class ToolListTests\n{\n    [Fact]\n    public void B() { var t = typeof(DarlingMcpTools).Assembly.GetTypes(); }\n}\n\n"
          + "public sealed class PlainReadTests\n{\n    [Fact]\n    public void C() { var one = CollectorCatalog.Find(\"x\"); }\n}\n";

        var rows = GuardStageScanner.ScanText(text).ToDictionary(r => r.Name);

        Assert.True(rows["CatalogSweepTests"].ReachesACatalog);
        Assert.True(rows["CatalogSweepTests"].Tagged);
        Assert.True(rows["ToolListTests"].ReachesACatalog);
        Assert.False(rows["ToolListTests"].Tagged);
        Assert.False(rows["PlainReadTests"].Candidate);

        var withStore = rows.Values.Append(new GuardStageScanner.Row(
            "LiveCatalogTests", "e.cs", true, true, true, false, false, false, ReachesACatalog: true)).ToList();
        var problems = GuardStageScanner.Problems(withStore, new Dictionary<string, string>());

        Assert.Contains(problems, p => p.StartsWith("MISSING", StringComparison.Ordinal) && p.Contains("ToolListTests", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.StartsWith("STORE", StringComparison.Ordinal) && p.Contains("LiveCatalogTests", StringComparison.Ordinal));
        Assert.Equal(2, problems.Count);
    }

    [Fact]
    public void EveryCatalogWideClass_CarriesTheStageGuardTrait_OrIsExempt()
    {
        var rows = GuardStageScanner.Scan(TestsRoot(), ProjectFile());
        var catalog = rows.Where(r => r.ReachesACatalog).ToList();

        /* Rule R4 (#5776): the classes a symbol selection cannot find. Each is tagged or exempt; the full census below
           holds the same rule, and this case names it alone so a red run says which rule broke. */
        var untagged = catalog
            .Where(r => !r.Tagged && !s_exempt.ContainsKey(r.Name))
            .Select(r => $"{r.File}: {r.Name}")
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
        Assert.True(
            untagged.Count == 0,
            $"These Lite.Tests classes reach a whole catalog (the MCP tools, a product assembly's types or every collector) "
          + $"and are not tagged. Add {GuardStageScanner.GuardTraitText} directly above each (build.yml's guard-tests job then "
          + "runs it on every pull request), or, when it needs a store, list it in GuardStageCensusTests.s_exempt with the reason:\n  "
          + string.Join("\n  ", untagged));

        Assert.True(
            catalog.Count >= 35,
            $"only {catalog.Count} Lite.Tests classes were found reaching a whole catalog; the R4 scan has stopped seeing them");

        /* No name rule finds these: #5714 turned them red with a new tool or collector. */
        foreach (var name in new[] { "XeTimeFilterSweepTests" })
        {
            var row = catalog.SingleOrDefault(r => r.Name == name);
            Assert.True(row is not null, $"{name} is no longer read as reaching a whole catalog");
            Assert.True(row!.Tagged, $"{name} is not tagged to always run");
        }
    }

    [Fact]
    public void EveryGuardClass_CarriesTheStageGuardTrait_OrIsExempt_AndNoTaggedClassNeedsAStore()
    {
        var rows = GuardStageScanner.Scan(TestsRoot(), ProjectFile());

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
    public void TheTaggedSet_IsTheSetXunitSelects_AndRuleR3HoldsOverIt()
    {
        var rows = GuardStageScanner.Scan(TestsRoot(), ProjectFile());
        var reflected = GuardStageScanner.ReflectGuardClasses(typeof(GuardStageCensusTests).Assembly);

        var problems = GuardStageScanner.ReflectionProblems(rows, reflected);

        Assert.True(
            problems.Count == 0,
            "The source scan and xunit disagree about which Lite.Tests classes carry Stage=Guard, or a class xunit selects "
          + "needs a store (the guard job has no PostgreSQL). Write the trait as the one attribute line above the class, or "
          + "list the project's link in the csproj so the scan reads it:\n  " + string.Join("\n  ", problems));

        Assert.True(
            reflected.Count >= TaggedFloor,
            $"xunit selects only {reflected.Count} Lite.Tests classes for Stage=Guard; the reflection has stopped seeing them");
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
    public void TheExemptionList_StaysEmpty()
    {
        Assert.Empty(s_exempt);
    }

    private static string TestsRoot() => Path.Combine(ParitySource.RepoRoot(), "Lite.Tests");

    private static string ProjectFile() => Path.Combine(TestsRoot(), "Lite.Tests.csproj");
}
