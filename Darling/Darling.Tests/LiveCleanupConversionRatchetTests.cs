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

namespace Darling.Tests;

/// <summary>
/// No live test cleans up on the connection its own body used (#1902).
///
/// <para><b>The defect this closes.</b> A <c>finally</c> that runs its teardown on the BODY's connection and
/// throws straight out of the finally reports the teardown's error instead of the test's — a throw from a
/// finally REPLACES the exception already in flight — and abandons every statement after the throwing one,
/// leaving debris the next run inherits as an unrelated flake. The two halves compound: it is the body's
/// failure that closes the connection, so the teardown fails BECAUSE of the thing it then hides. #1896
/// demonstrated it end to end — with a body failure and a cleanup failure forced into one test, the old shape
/// reported the cleanup's <c>42883</c> and lost the body's exception entirely.</para>
///
/// <para><b>This started as a ratchet and is now an invariant.</b> Through batches one and two it was a
/// ceiling — 126 sites, then 87, then 19 — that had to come DOWN with each batch and could never go up, so the
/// backlog could not regrow behind the conversions. Batch three took the last of them, so the number is zero
/// and the assertion now says what it always meant: there are none. The ceiling constant is gone with it, on
/// the grounds that a number which can only be zero is a worse way of writing zero.</para>
///
/// <para><b>What counts as compliant.</b> Only going through <see cref="LiveStoreCleanup"/> — either
/// <c>RunAsync</c>, which opens its own connection, or <c>RunOwnedAsync</c> for the few teardowns that MUST
/// use resources the test already holds (a session-scoped <c>lock_timeout</c>, a blocking transaction's own
/// rollback, a store that owns its data source). Opening a fresh connection BY HAND is deliberately not
/// accepted: it is half the fix, it still throws from the finally, and an exemption shaped "this one is
/// correct by hand" is one a later incorrect site inherits. The two live classes that had been doing exactly
/// that were converted rather than exempted.</para>
///
/// <para><b>Every textual decision reads CODE, never prose (#3014).</b> Membership, compliance, the
/// teardown-kind exclusion and the block match all run over
/// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output, so no comment and no string literal can
/// decide any of them. Read over raw source all four were decidable by a sentence: a class joined the scan by
/// NAMING the collection attribute in a doc comment, a teardown claimed compliance by naming
/// <see cref="LiveStoreCleanup"/> in prose, a store teardown escaped as file teardown by mentioning
/// <c>File.</c>, and a <c>}</c> written in prose ended the block early — the same blind spot the fixed line
/// window had, arriving through the brace match instead of through the window.
/// <see cref="CSharpSourceWalker.BraceBalanced"/> is only correct over stripped text, which is why the block
/// match is that method rather than a second copy of it here.</para>
///
/// <para><b>There is no token exemption, because there is nothing left for one to exempt.</b> A pair of
/// substrings — the <c>#1776 own-store</c> marker and the scratch-database helper's name — used to skip a
/// whole file, and they existed to undo the prose-driven membership test above: the classes they silenced are
/// the ones whose headers say, in words, that they are deliberately NOT in the collection. No class that
/// APPLIES the attribute is own-store or scratch-database backed, so a membership test that reads code
/// leaves those classes unscanned for the honest reason and needs nothing to exempt them. Nothing enforces
/// that split and nothing needs to: a class that both joins the shared collection and mints its own store is
/// simply scanned, and a teardown on resources the test already holds is what <c>RunOwnedAsync</c> is for —
/// the same answer that converted the two by-hand classes instead of exempting them.</para>
///
/// <para><b>Why the scratch-database classes are out of scope, as facts about the code (#3036).</b> #3014
/// justified the exemption it inherited by saying the exempt files stand up their own throwaway CLUSTER.
/// That is true of the two <c>#1776 own-store</c> classes that bootstrap the bundled runtime, and false of
/// <see cref="ScratchPostgres"/>, whose own summary says it mints a database on the SHARED
/// <c>DARLING_TEST_PG</c> server. Three separate facts put those classes out of this scan's reach, and "it
/// has its own cluster" is not one of them. MEMBERSHIP: not one of the ten classes backed by the scratch
/// helper APPLIES the attribute, so the code-reading test above leaves them unscanned without an exemption
/// existing at all. SHAPE: not one <c>finally</c> in any of them tears down store state — all four delete a
/// temp config file, which is the file-teardown exclusion's own case — so there is nothing here for
/// <see cref="LiveStoreCleanup"/> to wrap. EQUIVALENCE: the scratch drop already holds both properties
/// #1902 is about, and could not be given them by conversion in any case, being an <c>await using</c>
/// rather than a <c>try</c>/<c>finally</c>. It runs on an admin connection it opens itself and could not
/// run on the body's if it tried, because <c>DROP DATABASE</c> cannot be issued from inside the database it
/// drops; and it cannot throw out of its teardown at all, so it can never replace the body's in-flight
/// exception.</para>
///
/// <para><b>An abandoned scratch-database drop is tolerable because of what the code does, not because of
/// the word "throwaway".</b> The database is named from a fresh <c>Guid</c>, so a leaked one collides with
/// nothing a later run creates; the drop is <c>WITH (FORCE)</c> against a <c>Pooling=false</c> connection
/// string, so no lingering connection can wedge it; the one live class that reads <c>pg_database</c>
/// deliberately asserts no total count, BECAUSE these databases come and go, so a leak is invisible to the
/// only test that could have noticed it; and the shared server is itself a cluster the workflow
/// <c>initdb</c>s in the runner's temp directory and stops again around this suite, so a leak cannot
/// outlive the job. What one costs is disk on a cluster that is about to be deleted. This ratchet exists to
/// stop state being stranded for someone ELSE, and a scratch database strands none — but that is a fact
/// about scratch DATABASES and not a licence for the classes that use them: one which joins the shared
/// collection is scanned like anything else, pinned below in both directions.</para>
/// </summary>
public sealed class LiveCleanupConversionRatchetTests
{
    /// <summary>The attribute that puts a class in the shared live collection, and so in this scan.</summary>
    private const string LiveCollectionAttribute = "[Collection(\"live-postgres\")]";

    /// <summary>
    /// The part of <see cref="LiveCollectionAttribute"/> that survives stripping. The collection NAME is a
    /// string literal, so stripped text carries the attribute with its name blanked to spaces, and only this
    /// head can be matched there.
    /// </summary>
    private const string AttributeHead = "[Collection(";

    /// <summary>
    /// A <c>finally</c> that opens a block. Matched over stripped text, where prose cannot spell one, and
    /// written to tolerate both <c>finally</c> alone above its brace and <c>finally {</c> on one line — the
    /// fixed-shape match it replaces read only the first, so the second would have gone unscanned silently.
    /// </summary>
    private static readonly Regex FinallyBlock =
        new(@"(?<![A-Za-z0-9_])finally\s*\{", RegexOptions.Compiled);

    /// <summary>
    /// Teardown that is not store state, and so has nothing to do with a connection and nothing for
    /// <see cref="LiveStoreCleanup"/> to do: deleting a file or a directory, and killing a process the test
    /// started.
    ///
    /// <para><b>Every token is the shape of a CALL, which <c>Kill</c> alone was not (#3036).</b> As a bare
    /// substring it also matched <c>Killed</c> and <c>Killer</c>, so a teardown holding nothing more than a
    /// <c>wasKilled</c> local excluded itself from the sweep. That direction matters: these are EXCLUSIONS,
    /// so an over-broad one fails toward a MISSED offender, which is the half of a guard whose mistakes are
    /// silent. Measured before narrowing rather than after: the bare token excluded 0 of the 237
    /// <c>finally</c> blocks in the 120 files that apply the attribute, and the project's only real
    /// <c>process.Kill(</c> sites sit in classes that are not in the collection at all — so no block in the
    /// tree changes verdict, which is precisely why it was cheap to narrow now instead of on the day it
    /// silenced something.</para>
    ///
    /// <para>Read as a SET by the sweep and asserted as one by
    /// <see cref="EveryNonStoreTeardownTokenIsPinned"/>, so a token added here without a case arrives
    /// unexercised and fails rather than passing quietly.</para>
    /// </summary>
    private static readonly string[] NonStoreTeardownTokens = ["File.", "Directory.", ".Kill("];

    /// <summary>What one sweep found, and the population it actually examined to find it.</summary>
    private readonly record struct Sweep(List<string> Offenders, int Files, int Blocks);

    [Fact]
    public void NoLiveTestCleansUpOnItsOwnBodysConnection()
    {
        var directory = FindTestProjectDirectory();
        Assert.True(directory is not null,
            "could not locate Darling/Darling.Tests by walking up from the test output directory.");

        var sweep = Scan(SourcesUnder(directory!));

        /* A membership test or a block match narrow enough to match NOTHING satisfies the offender assertion
           below without reading a single teardown, and a green ratchet is indistinguishable from a compliant
           tree. Both populations are asserted so that failure is loud rather than green. */
        Assert.True(sweep.Files > 0,
            $"no file under {directory} APPLIES {LiveCollectionAttribute} — the membership test matched "
            + "nothing, so this ratchet examined no teardown at all.");
        Assert.True(sweep.Blocks > 0,
            $"no finally block was found in the {sweep.Files} file(s) that apply {LiveCollectionAttribute} — "
            + "the block match found nothing, so the assertion below held over an empty set.");

        Assert.True(sweep.Offenders.Count == 0,
            $"{sweep.Offenders.Count} live-test teardown(s) do not go through LiveStoreCleanup (#1902). Wrap "
            + "the finally body in LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, ...) — or "
            + "RunOwnedAsync when the cleanup must use connections the test already holds — and set "
            + "bodySucceeded as the last statement of the try. Opening a connection by hand is not enough: it "
            + "leaves the throw-from-finally that replaces the body's exception."
            + Environment.NewLine + string.Join(Environment.NewLine, sweep.Offenders));
    }

    /// <summary>
    /// A doc comment that NAMES an exemption the class does not claim does not silence the class. This is the
    /// #3014 shape: the exemption was a whole-file substring match over raw source, so one occurrence
    /// anywhere — including a sentence saying the class is NOT exempt — skipped every <c>finally</c> in the
    /// file, and the ratchet reported zero offenders over a file it never read.
    /// </summary>
    [Fact]
    public void ProseNamingAnExemptionDoesNotSilenceTheFile()
    {
        var sweep = ScanOne(
            "/// <summary>Not a ScratchPostgres class, and carrying no #1776 own-store marker.</summary>\n"
            + LiveClass(UnconvertedTeardown));

        Assert.Equal(1, sweep.Files);
        Assert.Equal(1, sweep.Blocks);
        Assert.Single(sweep.Offenders);
    }

    /// <summary>
    /// A class that only NAMES the attribute is not in the collection and is not scanned. Own-store classes
    /// record in their headers that they are "deliberately NOT" in it, and a raw substring match read that
    /// sentence as membership — which is the whole reason the scan needed an exemption to undo it.
    /// </summary>
    [Fact]
    public void AClassThatOnlyNamesTheAttributeIsNotScanned()
    {
        var sweep = ScanOne(OwnStoreClass(UnconvertedTeardown));

        Assert.Equal(0, sweep.Files);
        Assert.Equal(0, sweep.Blocks);
        Assert.Empty(sweep.Offenders);
    }

    /// <summary>Naming <see cref="LiveStoreCleanup"/> in a comment does not convert a teardown.</summary>
    [Fact]
    public void ProseNamingLiveStoreCleanupDoesNotMakeATeardownCompliant()
    {
        var sweep = ScanOne(LiveClass(Lines(
            "    [Fact]",
            "    public void T()",
            "    {",
            "        try",
            "        {",
            "            Work();",
            "        }",
            "        finally",
            "        {",
            "            // Not through LiveStoreCleanup yet — this drops the rows the body wrote.",
            "            Drop();",
            "        }",
            "    }")));

        Assert.Equal(1, sweep.Blocks);
        Assert.Single(sweep.Offenders);
    }

    /// <summary>
    /// The file-and-process teardown exclusion cannot be claimed in a comment either. It exists because file
    /// and process teardown is not store state and has nothing to do with a connection, and a store teardown
    /// that merely MENTIONS a file or a kill is still a store teardown.
    /// </summary>
    [Fact]
    public void ProseNamingFileTeardownDoesNotExcludeAStoreTeardown()
    {
        var sweep = ScanOne(LiveClass(Lines(
            "    [Fact]",
            "    public void T()",
            "    {",
            "        try",
            "        {",
            "            Work();",
            "        }",
            "        finally",
            "        {",
            "            /* No File.Delete here and nothing to Kill: this is store state. */",
            "            Drop();",
            "        }",
            "    }")));

        Assert.Equal(1, sweep.Blocks);
        Assert.Single(sweep.Offenders);
    }

    /// <summary>
    /// A closing brace written in prose does not end the block early. The fixed line window this scan
    /// replaced had the same blind spot, and brace-matching RAW text reintroduces it in the false-positive
    /// direction: the block ends at the first <c>}</c> the text contains, so a fully converted teardown whose
    /// comment spells one is reported as unconverted.
    /// </summary>
    [Fact]
    public void ABraceInProseDoesNotEndTheBlockEarly()
    {
        var sweep = ScanOne(LiveClass(Lines(
            "    [Fact]",
            "    public void T()",
            "    {",
            "        try",
            "        {",
            "            Work();",
            "            ok = true;",
            "        }",
            "        finally",
            "        {",
            "            /* The old shape closed the teardown here: } */",
            "            LiveStoreCleanup.RunAsync(ConnectionString, ok, Drop);",
            "        }",
            "    }")));

        Assert.Equal(1, sweep.Blocks);
        Assert.Empty(sweep.Offenders);
    }

    /// <summary>
    /// A converted teardown is accepted through either entry point. The counterweight to the four pins above:
    /// each of those asserts that something stops being exempt, and a scan made strict enough to satisfy all
    /// four while rejecting real conversions would fail a green tree.
    /// </summary>
    [Theory]
    [InlineData("LiveStoreCleanup.RunAsync(ConnectionString, ok, Drop);")]
    [InlineData("await LiveStoreCleanup.RunOwnedAsync(ok, Drop);")]
    public void AConvertedTeardownIsNotAnOffender(string teardown)
    {
        var sweep = ScanOne(LiveClass(Lines(
            "    [Fact]",
            "    public void T()",
            "    {",
            "        try",
            "        {",
            "            Work();",
            "            ok = true;",
            "        }",
            "        finally",
            "        {",
            "            " + teardown,
            "        }",
            "    }")));

        Assert.Equal(1, sweep.Files);
        Assert.Equal(1, sweep.Blocks);
        Assert.Empty(sweep.Offenders);
    }

    /// <summary>
    /// The first test file placed in a SUBDIRECTORY is scanned, and build output under the same root is
    /// not (#3036).
    ///
    /// <para>The enumeration was <c>TopDirectoryOnly</c> while its sibling guard over this same project was
    /// <c>AllDirectories</c> with a written rationale for it. Latent, because the project is flat — which is
    /// what makes it worth a pin rather than a flag flip: the vacuity floor added with #3014's fix fails
    /// when the population reaches zero, and a population missing exactly one subdirectory is not zero.
    /// Nothing about a green run distinguishes "no offenders" from "never looked there".</para>
    ///
    /// <para>Every expectation is COUNTED from <see cref="PlantedTree"/> rather than written as a literal,
    /// so a case added to that table cannot pass against a stale number, and the offenders are compared as
    /// a SET of relative paths rather than by count. Between them the three assertions separate three
    /// regressions: losing the recursion or the build-output filter moves the FILE count, in opposite
    /// directions; reading a nested file without reaching inside it moves the BLOCK count with the file
    /// count intact; and reporting the bare file name again leaves both counts right and only the PATHS
    /// wrong.</para>
    ///
    /// <para>The root is planted under a directory literally named <c>bin</c>, so the exclusion is proven to
    /// be judged relative to the scanned directory. Judged on the absolute path it would classify the whole
    /// tree as build output and this sweep would report no offenders having read nothing at all, which is
    /// the shape of every failure this file is about.</para>
    /// </summary>
    [Fact]
    public void ANestedTestFileIsScannedAndBuildOutputIsNot()
    {
        var temporary = Directory.CreateTempSubdirectory("darling-live-cleanup-ratchet-");
        try
        {
            var root = Path.Combine(temporary.FullName, "bin", "project");

            foreach (var planted in PlantedTree())
            {
                var path = Path.Combine(
                    root, planted.RelativePath.Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, planted.Source);
            }

            var sweep = Scan(SourcesUnder(root));

            var expectedFiles = PlantedTree().Count(planted => planted.Scanned);
            var expectedBlocks = PlantedTree().Where(planted => planted.Scanned).Sum(planted => planted.Blocks);

            Assert.True(sweep.Files == expectedFiles,
                $"the sweep read {sweep.Files} of the {expectedFiles} planted source file(s). Too few means "
                + "a subdirectory never reached the population — the whole defect — and too many means "
                + "build output is being read as source.");

            Assert.True(sweep.Blocks == expectedBlocks,
                $"the sweep found {sweep.Blocks} finally block(s) where the planted files hold "
                + $"{expectedBlocks}. The file count can be right while the blocks inside a nested file go "
                + "unread, and this assertion is the one that separates them.");

            var expected = PlantedTree()
                .Where(planted => planted.Scanned && planted.Offends)
                .Select(planted => planted.RelativePath)
                .Order(StringComparer.Ordinal)
                .ToArray();

            /* Offenders are reported as path:line; the path is what this pin is about. */
            var reported = sweep.Offenders
                .Select(offender => offender[..offender.LastIndexOf(':')])
                .Order(StringComparer.Ordinal)
                .ToArray();

            Assert.True(reported.SequenceEqual(expected, StringComparer.Ordinal),
                "the offenders reported are not the ones planted. Expected ["
                + string.Join(", ", expected) + "], got [" + string.Join(", ", reported)
                + "]. A missing nested path means the recursion is not reaching it; a bin or obj path means "
                + "the build-output filter has come detached from the enumeration; a bare file name means a "
                + "nested offender no longer says where it is.");
        }
        finally
        {
            temporary.Delete(recursive: true);
        }
    }

    /// <summary>
    /// A class that mints its own scratch database is scanned anyway once it JOINS the shared collection
    /// (#3036) — the direction the answer about scratch databases does not exempt.
    ///
    /// <para>No class in the project is in both states today, and that is the point: the reason the scratch
    /// classes go unscanned is that they do not apply the attribute, not that minting a database is a
    /// property that exempts anything. Add the attribute to one of them, or write a new class in both
    /// states, and its teardown is read like any other — this is what stops "it has its own database" from
    /// becoming the by-hand exemption <see cref="LiveStoreCleanup"/>'s two converted classes were denied.</para>
    /// </summary>
    [Fact]
    public void MintingAScratchDatabaseDoesNotExemptAClassInTheSharedCollection()
    {
        var sweep = ScanOne(LiveClass(ScratchDatabaseTeardown("Drop();")));

        Assert.Equal(1, sweep.Files);
        Assert.Equal(1, sweep.Blocks);
        Assert.Single(sweep.Offenders);
    }

    /// <summary>
    /// The counterweight, and the other half of #3036's answer: a genuinely own-store scratch-database
    /// class is not read at all, so its file teardown needs no conversion and its absence from the offender
    /// list is not a near miss.
    ///
    /// <para>Shaped like the real ones — the <c>#1776 own-store</c> header that NAMES the attribute rather
    /// than applying it, a scratch database minted in the body, and a <c>finally</c> that deletes a temp
    /// config file. <c>Files</c> and <c>Blocks</c> are asserted at zero rather than only the offender list
    /// being empty, because those two claims differ: an empty offender list is also what a scan that read
    /// the file and wrongly excluded the block would produce.</para>
    /// </summary>
    [Fact]
    public void AnOwnStoreScratchDatabaseClassIsNotReadAtAll()
    {
        var sweep = ScanOne(OwnStoreClass(ScratchDatabaseTeardown("File.Delete(configPath);")));

        Assert.Equal(0, sweep.Files);
        Assert.Equal(0, sweep.Blocks);
        Assert.Empty(sweep.Offenders);
    }

    /// <summary>
    /// Each non-store teardown token excludes the CALL it is for and leaves its near miss alone (#3036).
    ///
    /// <para>The near miss is the assertion that has teeth. <c>Kill</c> as a bare substring matched
    /// <c>Killed</c>, so a <c>wasKilled</c> local in an otherwise unconverted teardown silenced it — and an
    /// over-broad exclusion fails toward a MISSED offender, so nothing would have said so. Both halves are
    /// asserted per token: without the excluded half, narrowing to the point of matching nothing would pass
    /// the near-miss half on its own.</para>
    ///
    /// <para>The fixtures are checked against the token BEFORE either sweep runs. A near miss that carries
    /// no part of the token, or an excluded case that does not carry the token at all, is reported correctly
    /// for reasons unrelated to the token — this test would then pass on any narrowing whatsoever, including
    /// one that matched nothing, while reading as though it had exercised the token.</para>
    /// </summary>
    [Theory]
    [MemberData(nameof(NonStoreTeardownCases))]
    public void ANonStoreTeardownIsExcludedAndItsNearMissIsNot(
        string token, string stem, string excluded, string nearMiss)
    {
        Assert.Contains(token, excluded, StringComparison.Ordinal);
        Assert.Contains(stem, nearMiss, StringComparison.Ordinal);
        Assert.DoesNotContain(token, nearMiss, StringComparison.Ordinal);

        var excludedSweep = ScanOne(LiveClass(Teardown(excluded)));

        Assert.Equal(1, excludedSweep.Blocks);
        Assert.True(excludedSweep.Offenders.Count == 0,
            $"'{excluded}' is the teardown '{token}' exists to exclude, and it was reported as an offender "
            + "instead. Narrowing the token past the call it is for turns every real file, directory or "
            + "process teardown in a live class into a false failure.");

        var nearMissSweep = ScanOne(LiveClass(Teardown(nearMiss)));

        Assert.Equal(1, nearMissSweep.Blocks);
        Assert.True(nearMissSweep.Offenders.Count == 1,
            $"'{nearMiss}' carries '{stem}' but not the call '{token}', and it tears down store state, so "
            + "it must be reported. It was not, which means the token is matching a bare stem rather than a "
            + "call — an exclusion wide enough to silence an unconverted teardown, in the direction nothing "
            + "complains about.");
    }

    /// <summary>
    /// The tokens the sweep excludes on and the tokens pinned above are the same set.
    ///
    /// <para>A per-token pin list cannot see the set GROW: add a fourth exclusion to
    /// <see cref="NonStoreTeardownTokens"/> and every case above still passes, having never exercised it.
    /// Asserting the two as sets is what makes the new token's arrival the failure.</para>
    /// </summary>
    [Fact]
    public void EveryNonStoreTeardownTokenIsPinned()
    {
        var scanned = NonStoreTeardownTokens.Order(StringComparer.Ordinal).ToArray();
        var pinned = NonStoreTeardownPins.Select(pin => pin.Token).Order(StringComparer.Ordinal).ToArray();

        Assert.True(scanned.SequenceEqual(pinned, StringComparer.Ordinal),
            "the tokens the sweep excludes on and the tokens pinned by the theory above have drifted. "
            + "Sweep: [" + string.Join(", ", scanned) + "]; pinned: [" + string.Join(", ", pinned)
            + "]. Every one of these is an EXCLUSION, so an unexercised one fails toward a missed offender "
            + "and nothing else in this file would report it.");
    }

    /// <summary>
    /// Every <c>*.cs</c> SOURCE file in the test project, as (path relative to
    /// <paramref name="directory"/>, source) pairs.
    ///
    /// <para><b>Recursive, for the reason the sibling guard already wrote down (#3036).</b>
    /// <c>LivePostgresCollectionHygieneTests</c> enumerates this same project with
    /// <see cref="SearchOption.AllDirectories"/> and says why: the project is flat today, so
    /// <c>TopDirectoryOnly</c> is equivalent — and it also means the first test file someone puts in a
    /// subfolder leaves this population silently, which is indistinguishable from a compliant tree and is
    /// the failure mode the ratchet exists to prevent. The vacuity floor in the rule above catches the
    /// population going to ZERO, not one subdirectory's worth of it never arriving.</para>
    ///
    /// <para><b>Build output is not source, and excluding it is load-bearing rather than defensive here.</b>
    /// <c>Darling.Tests.csproj</c> copies three product <c>.cs</c> files into the output directory as
    /// fixtures, so a BUILT tree really does hold <c>.cs</c> under <c>bin</c> — recursing without this would
    /// read product source, generated <c>.AssemblyInfo.cs</c> and <c>.g.cs</c> as though they were tests.
    /// The rule is <c>DocCommentHygieneTests</c>': whole path SEGMENTS, both separator characters, judged
    /// RELATIVE to the scanned directory. A substring test eats <c>Objects</c> and <c>mybin</c>; an
    /// absolute-path test excludes the entire tree whenever the checkout itself sits under a directory
    /// called <c>bin</c>, and this sweep then reports no offenders having read nothing; and the host's
    /// separator alone hands a Windows run and a macOS run different populations.</para>
    ///
    /// <para>The name reported is that relative path with <c>/</c> separators rather than the bare file
    /// name, so a nested offender says where it is and says the same thing on either platform.</para>
    /// </summary>
    private static IEnumerable<(string Name, string Source)> SourcesUnder(string directory) =>
        Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
            .Select(path => (Name: Path.GetRelativePath(directory, path).Replace('\\', '/'), Path: path))
            .Where(found => !IsBuildOutput(found.Name))
            .OrderBy(found => found.Name, StringComparer.Ordinal)
            .Select(found => (found.Name, File.ReadAllText(found.Path)));

    /// <summary>
    /// True when any whole SEGMENT of <paramref name="relativePath"/> is <c>bin</c> or <c>obj</c>, reading
    /// both separator characters whatever the host's is. The path is already relative to the scanned
    /// directory, which is what keeps a <c>bin</c> ABOVE that directory from excluding the whole tree.
    /// </summary>
    private static bool IsBuildOutput(string relativePath) =>
        relativePath.Split('/', '\\')
            .Any(segment => string.Equals(segment, "bin", StringComparison.OrdinalIgnoreCase)
                            || string.Equals(segment, "obj", StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Every <c>finally</c> in a shared-store live class whose block does not go through
    /// <see cref="LiveStoreCleanup"/>, reported as <c>file:line</c>, alongside the counts of files and blocks
    /// the sweep examined.
    ///
    /// <para>Own-store classes are not scanned for the same reason #1776 does not serialize them, which
    /// this class's summary states as properties of their teardown rather than as a claim about what they
    /// mint: an abandoned one of theirs strands nothing another test can read. That is decided by whether
    /// the class APPLIES the collection attribute, not by anything it says about itself. File, directory and
    /// process teardown is excluded because it is not store state and has nothing to do with a
    /// connection.</para>
    ///
    /// <para>Every decision here reads the stripped text. The line numbers reported are the source's own:
    /// stripping preserves newlines, so counting them in either text gives the same answer.</para>
    /// </summary>
    private static Sweep Scan(IEnumerable<(string Name, string Source)> sources)
    {
        var offenders = new List<string>();
        var files = 0;
        var blocks = 0;

        foreach (var (name, source) in sources)
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(source);

            /* Membership below reads the two texts at ONE offset, which only addresses the same character
               while stripping is length-preserving. Checked here rather than assumed: the tree-wide
               alignment pin in CSharpSourceWalkerTests covers the Viewer, Storage and Analysis projects,
               not this one, and a guard reading the wrong offsets would report teardowns that are not
               there. */
            Assert.True(code.Length == source.Length,
                $"{name}: stripping changed the text's length ({source.Length} -> {code.Length}), so the "
                + "source and stripped offsets no longer address the same characters.");

            if (!AppliesTheLiveCollectionAttribute(source, code))
            {
                continue;
            }

            files++;

            foreach (Match found in FinallyBlock.Matches(code))
            {
                blocks++;

                var block = CSharpSourceWalker.BraceBalanced(code, found.Index + found.Length - 1);
                if (block.Contains("LiveStoreCleanup", StringComparison.Ordinal))
                {
                    continue;
                }

                if (NonStoreTeardownTokens.Any(t => block.Contains(t, StringComparison.Ordinal)))
                {
                    continue;
                }

                offenders.Add($"{name}:{code.AsSpan(0, found.Index).Count('\n') + 1}");
            }
        }

        return new Sweep(offenders, files, blocks);
    }

    /// <summary>
    /// Is <see cref="LiveCollectionAttribute"/> APPLIED here, as opposed to merely named in prose?
    ///
    /// <para>Both texts are read at the SAME offset: the stripped text decides whether the occurrence is
    /// code, and the source decides which collection it names, because the name is the one part of the
    /// attribute that stripping blanks. The caller checks the length equality that lets one index address
    /// both.</para>
    ///
    /// <para>Neither text alone is enough. The stripped text alone accepts any collection, of which this
    /// project has four. The source alone accepts a doc comment, and that is not hypothetical: every class
    /// that explains this rule quotes the attribute while explaining it, and ten files that only NAME it were
    /// scanned as though they applied it — the collection's own definition and fixture, the sweep that
    /// enforces it, and seven classes whose headers say they are deliberately NOT in it. #1862 met the same
    /// wall in #1776's sweep and answered it by requiring the attribute to OPEN a line; asking the walk needs
    /// no rule about where an attribute may sit.</para>
    /// </summary>
    private static bool AppliesTheLiveCollectionAttribute(string source, string code)
    {
        for (var i = code.IndexOf(AttributeHead, StringComparison.Ordinal);
             i >= 0;
             i = code.IndexOf(AttributeHead, i + 1, StringComparison.Ordinal))
        {
            if (source.AsSpan(i).StartsWith(LiveCollectionAttribute, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>One fixture source, scanned as though it were the whole project.</summary>
    private static Sweep ScanOne(string source) => Scan(new[] { ("Fixture.cs", source) });

    /// <summary>
    /// Joins fixture lines with <c>\n</c>, so what a fixture means never depends on this file's own line
    /// endings.
    /// </summary>
    private static string Lines(params string[] lines) => string.Join("\n", lines);

    /// <summary>A class that APPLIES the collection attribute, with <paramref name="body"/> as its member.</summary>
    private static string LiveClass(string body) =>
        Lines(LiveCollectionAttribute, "public sealed class Fixture", "{", body, "}", string.Empty);

    /// <summary>
    /// The same class with the attribute NAMED rather than applied — the shape every own-store class in this
    /// project actually has.
    /// </summary>
    private static string OwnStoreClass(string body) =>
        Lines(
            "/* #1776 own-store: deliberately NOT " + LiveCollectionAttribute + ". It mints its own store. */",
            "public sealed class Fixture",
            "{",
            body,
            "}",
            string.Empty);

    /// <summary>A teardown that drops store state without going through <see cref="LiveStoreCleanup"/>.</summary>
    private static readonly string UnconvertedTeardown = Teardown("Drop();");

    /// <summary>The same teardown, converted — the compliant half of every planted tree above.</summary>
    private static readonly string ConvertedTeardown =
        Teardown("LiveStoreCleanup.RunAsync(ConnectionString, ok, Drop);");

    /// <summary>
    /// One planted source file: where it goes under the scanned root, what it holds, how many
    /// <c>finally</c> blocks it holds, whether the sweep is meant to READ it, and whether its teardown is
    /// an offender. The last two are separate on purpose: the build-output entries carry a non-compliant
    /// teardown deliberately, so if the filter ever comes detached from the enumeration they turn up.
    /// </summary>
    private readonly record struct Planted(
        string RelativePath, string Source, int Blocks, bool Scanned, bool Offends);

    /// <summary>
    /// The tree <see cref="ANestedTestFileIsScannedAndBuildOutputIsNot"/> plants, and the only place its
    /// expectations are written down. A method rather than a field so it cannot depend on where the fixture
    /// strings it composes are declared.
    /// </summary>
    private static Planted[] PlantedTree() =>
    [
        new("TopLevel.cs", LiveClass(ConvertedTeardown), 1, Scanned: true, Offends: false),
        new("Nested/Deeper/NestedOffender.cs", LiveClass(UnconvertedTeardown), 1, Scanned: true, Offends: true),
        /* Named LIKE build output and not build output — the half a substring filter gets wrong. */
        new("Objects/NamedLikeOutput.cs", LiveClass(ConvertedTeardown), 1, Scanned: true, Offends: false),
        new("bin/Debug/net10.0/Copied.cs", LiveClass(UnconvertedTeardown), 1, Scanned: false, Offends: true),
        new("obj/Debug/net10.0/Generated.g.cs", LiveClass(UnconvertedTeardown), 1, Scanned: false, Offends: true),
    ];

    /// <summary>
    /// Per token: the bare STEM a looser spelling of it would have matched, the call it exists to exclude,
    /// and a near miss that carries the stem without the call and must still be reported. Read as the
    /// coverage set by <see cref="EveryNonStoreTeardownTokenIsPinned"/>, so this table and
    /// <see cref="NonStoreTeardownTokens"/> cannot drift apart.
    ///
    /// <para>The stem is what makes a near miss NEAR. A string that carries no part of the token is a plain
    /// unconverted teardown, correctly reported for reasons that have nothing to do with the token, and it
    /// would say nothing about the narrowing while looking as though it did — so
    /// <see cref="ANonStoreTeardownIsExcludedAndItsNearMissIsNot"/> asserts the relationship rather than
    /// describing it.</para>
    /// </summary>
    private static readonly (string Token, string Stem, string Excluded, string NearMiss)[] NonStoreTeardownPins =
    [
        ("File.", "File", "File.Delete(configPath);", "var configFile = Drop();"),
        ("Directory.", "Directory", "Directory.Delete(spill, recursive: true);", "var spillDirectory = Drop();"),
        (".Kill(", "Kill", "process.Kill(entireProcessTree: true);", "var wasKilled = Drop();"),
    ];

    /// <summary>The same table as xUnit data, so the theory and the coverage pin read one source.</summary>
    public static TheoryData<string, string, string, string> NonStoreTeardownCases()
    {
        var cases = new TheoryData<string, string, string, string>();

        foreach (var pin in NonStoreTeardownPins)
        {
            cases.Add(pin.Token, pin.Stem, pin.Excluded, pin.NearMiss);
        }

        return cases;
    }

    /// <summary>A test whose <c>finally</c> body is <paramref name="statement"/> and nothing else.</summary>
    private static string Teardown(string statement) => Lines(
        "    [Fact]",
        "    public void T()",
        "    {",
        "        try",
        "        {",
        "            Work();",
        "        }",
        "        finally",
        "        {",
        "            " + statement,
        "        }",
        "    }");

    /// <summary>
    /// The shape the scratch-database classes actually have: a database minted for this test alone, and a
    /// <c>try</c>/<c>finally</c> inside it.
    ///
    /// <para>The helper's factory call is deliberately NOT spelled here.
    /// <c>LivePostgresCollectionHygieneTests</c> keys its own membership rule on that exact string, so a
    /// fixture containing it would enrol THIS class in that guard's population as a shared-store
    /// toucher — a fixture changing what a different guard thinks of the file that holds it.</para>
    /// </summary>
    private static string ScratchDatabaseTeardown(string statement) => Lines(
        "    [Fact]",
        "    public async Task T()",
        "    {",
        "        await using var scratch = await MintAScratchDatabaseAsync(ct);",
        "        try",
        "        {",
        "            Work(scratch.ConnectionString);",
        "        }",
        "        finally",
        "        {",
        "            " + statement,
        "        }",
        "    }");

    /// <summary>
    /// Walks up from the test output directory to the repo root (the directory holding
    /// <c>PerformanceMonitor.sln</c>) and returns this project's source directory. Same walk-up idiom as
    /// <c>LivePostgresCollectionHygieneTests.FindTestProjectDirectory</c>.
    /// </summary>
    private static string? FindTestProjectDirectory()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                var source = Path.Combine(directory.FullName, "Darling", "Darling.Tests");
                return Directory.Exists(source) ? source : null;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
