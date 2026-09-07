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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// That <see cref="RepoFile"/> is the only repo-file reader in <c>Darling.Tests</c> — asserted, rather than
/// stated in prose and left to rot. <c>Darling.Tests</c> and not "the repository": the scope is this one
/// test project, and the paragraph on what the scan cannot see says what that leaves out.
///
/// <para><b>This exists because the prose version failed for thirty-four consecutive classes.</b> Every one
/// of them wanted to read a repo file, none of them found an authority to call, each wrote its own, and the
/// set drifted into EIGHT different resolution semantics — probing for the target versus finding a marker,
/// walking from the pin's source path versus from the test binary's directory, three different markers, and
/// two different answers about newlines. Nothing failed while that happened, because a private helper is
/// invisible to everything except the class holding it. The same failure mode #2913 removed from the source
/// walk and #2938 from the deadline judgement, and the same fix: one authority, and a build that reds when
/// a second one appears.</para>
///
/// <para><b>The LF-adopting set is pinned separately, and it is the half that would fail silently.</b> A
/// missing declaration is a compile error; a pin switched from <see cref="RepoFile.ReadRepoFileLf"/> to
/// <see cref="RepoFile.ReadRepoFile"/> is not. This checkout is CRLF, so an anchor spanning a line break
/// cannot match raw text at all: a positive assertion would fail loudly, but a
/// <c>DoesNotContain</c> would stop being ABLE to fire and report clean forever. Switching a pin off the LF
/// reader is therefore a decision, and it reds here so it gets made on purpose.</para>
///
/// <para><b>What this scan cannot see, stated rather than implied.</b> Three things, and the first is a
/// boundary rather than a limitation.</para>
///
/// <para><b>It sweeps <c>Darling.Tests</c> only</b>, because <see cref="TestDirectory"/> is this file's own
/// directory. <c>Lite.Tests</c> carries five private <c>ReadRepoFile</c> declarations of its own —
/// <c>DetachedCollectorGateTests</c>, <c>GridPayloadColumnOrderPinTests</c>,
/// <c>LiteOverviewCardExplainsItselfTests</c>, <c>LiteSidebarDotRendersTheCardStatusTests</c> and
/// <c>QueryStoreServerGateTests</c> — and this says nothing about them. They are the same pattern in the
/// sibling project, and <see cref="RepoFile"/> is shareable with it by the one <c>Compile Include</c> line
/// that already brings <see cref="CSharpSourceWalker"/> across. Whoever takes that has to widen this sweep
/// or add a sibling pin in <c>Lite.Tests</c>; a reader consolidated there while this scan still looks at one
/// directory would be consolidated with nothing asserting it stayed that way.</para>
///
/// <para><b>It matches a DECLARATION by NAME</b> — <c>ReadRepoFile</c> or <c>ReadRepoFileLf</c> — so a
/// private re-implementation under some other name evades it. The regex anchors on the name, and a name is
/// all one file's text offers.</para>
///
/// <para><b>It says nothing about the fifty-three classes here that carry their own repo-ROOT walk</b>
/// without a reader on top of it. That is the same duplication one layer down, it is a larger population
/// than this one was, and it is deliberately not in scope.</para>
/// </summary>
public sealed class RepoFileAdoptionTests
{
    private const string Authority = "RepoFile.cs";

    /// <summary>A method DECLARATION of the shared reader's name: an accessibility modifier, no <c>;</c> or
    /// <c>=</c> before the name, then the name and its parameter list. Matched against STRIPPED source, so
    /// the doc comments in this family that quote the reader by name are not mistaken for declarations of
    /// it.</summary>
    private static readonly Regex Declaration = new(
        @"^[ \t]*(?:private|internal|public|protected)[^\r\n;=]*?\bReadRepoFile(?:Lf)?\s*\(",
        RegexOptions.Multiline | RegexOptions.Compiled);

    private static readonly Regex RawCall = new(
        @"(?<![\w.])ReadRepoFile\s*\(", RegexOptions.Compiled);

    private static readonly Regex LfCall = new(
        @"(?<![\w.])ReadRepoFileLf\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// The pins whose anchors span a line break, and which therefore read LF-normalised text.
    ///
    /// <para>Membership is a property of a pin's ANCHORS, not of its subject: a pin belongs here exactly
    /// when one of its anchors crosses a newline. The list is compared against the tree rather than trusted,
    /// for the reason in this class's summary — moving a pin between the two readers changes whether its
    /// multi-line assertions can fire at all. Consolidation found six of the thirty-four carrying the
    /// normalising helper and twenty-eight deliberately not; that was a census of one moment, and this is
    /// the live set, so it carries no count of its own to go stale.</para>
    /// </summary>
    private static readonly string[] s_lfReaders =
    {
        "ChartWindowDomainTests.cs",
        "FleetCardCollectionStaleNamesItsPopulationTests.cs",
        "FleetPageAttentionFilterTests.cs",
        "ServerPageTabsTests.cs",
        "StartupFailureTriageTests.cs",
        "StoreCopyPhaseTests.cs",
        "ViewTemplatesTests.cs",
        "ViewerSidebarDotRendersTheCardStatusTests.cs",
    };

    [Fact]
    public void ExactlyOneFile_DeclaresTheSharedRepoFileReader()
    {
        var (files, declarers, rawAdopters, lfAdopters) = Survey();

        /* A floor rather than an equality, and the same one the rest of this family uses: it catches a sweep
           that read the wrong directory and then reported clean on nothing. The claims that matter are the
           set equalities, which move whenever a declaration or an adopter does. */
        Assert.True(
            files.Count >= 400,
            $"the sweep found only {files.Count} .cs files under {TestDirectory()} — it is not reading the "
          + "test project, so every assertion below would pass for a reason unrelated to the defect");

        Assert.Equal(new[] { Authority }, declarers.ToArray());

        /* The authority being alone is satisfied just as well by an authority nothing calls, which is why
           the adopting population is floored on its own. Thirty-four classes were migrated onto it; a floor
           under that catches the reader being quietly abandoned while this file keeps passing. */
        var adopters = rawAdopters.Concat(lfAdopters).Distinct(StringComparer.Ordinal).ToArray();

        Assert.True(
            adopters.Length >= 30,
            $"only {adopters.Length} classes call the shared reader. Thirty-four were migrated onto it, so "
          + "either pins were deleted or they have gone back to reading files some other way — in which "
          + "case the single-declaration assertion above is guarding an authority nobody uses");
    }

    [Fact]
    public void TheLfReadingPins_AreExactlyTheOnesDeclaredHere()
    {
        var (_, _, _, lfAdopters) = Survey();

        Assert.Equal(
            s_lfReaders.OrderBy(f => f, StringComparer.Ordinal).ToArray(),
            lfAdopters.ToArray());

        /* The floor that makes the equality above mean something. An empty tree satisfies an empty
           expectation, and this list is short enough that a scan reading nothing looks plausible. */
        Assert.NotEmpty(s_lfReaders);
    }

    private static (List<string> Files, List<string> Declarers, List<string> RawAdopters, List<string> LfAdopters)
        Survey()
    {
        var directory = TestDirectory();

        /* bin/ and obj/ excluded the way CrossAppGuardCiGateTests excludes them: obj/ carries generated
           sources, and a throwaway verification harness staged under bin/ would otherwise be swept as if it
           were a pin. AllDirectories rather than TopDirectoryOnly so a pin that lands in a future
           subdirectory is still counted — the flat layout today is not an invariant anything asserts. */
        var files = Directory
            .EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
            .Where(f =>
                !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) &&
                !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        var declarers = new List<string>();
        var rawAdopters = new List<string>();
        var lfAdopters = new List<string>();

        foreach (var file in files)
        {
            var name = Path.GetFileName(file);

            /* STRIPPED, for the reason CommandDeadlineScannerAdoptionTests gives: this family discusses the
               shared reader at length in doc comments and names both methods, so read raw, every file in it
               would look like an adopter and several like declarers. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));

            if (Declaration.IsMatch(code))
            {
                declarers.Add(name);
            }

            if (name.Equals(Authority, StringComparison.Ordinal))
            {
                continue;
            }

            if (LfCall.IsMatch(code))
            {
                lfAdopters.Add(name);
            }

            /* RawCall matches the Lf spelling too — its name is a prefix — so a file is a raw adopter only
               once every Lf call is accounted for. Counted by occurrence rather than by presence, so a class
               that legitimately uses both readers lands in both lists instead of one arbitrarily. */
            if (RawCall.Matches(code).Count > LfCall.Matches(code).Count)
            {
                rawAdopters.Add(name);
            }
        }

        declarers.Sort(StringComparer.Ordinal);
        rawAdopters.Sort(StringComparer.Ordinal);
        lfAdopters.Sort(StringComparer.Ordinal);

        return (files, declarers, rawAdopters, lfAdopters);
    }

    private static string TestDirectory([CallerFilePath] string thisFile = "")
        => Path.GetDirectoryName(thisFile)!;
}
