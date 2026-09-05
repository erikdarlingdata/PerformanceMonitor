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

    /// <summary>Every <c>*.cs</c> in the test project, as (file name, source) pairs.</summary>
    private static IEnumerable<(string Name, string Source)> SourcesUnder(string directory) =>
        Directory.EnumerateFiles(directory, "*.cs")
            .OrderBy(p => p, StringComparer.Ordinal)
            .Select(p => (Path.GetFileName(p), File.ReadAllText(p)));

    /// <summary>
    /// Every <c>finally</c> in a shared-store live class whose block does not go through
    /// <see cref="LiveStoreCleanup"/>, reported as <c>file:line</c>, alongside the counts of files and blocks
    /// the sweep examined.
    ///
    /// <para>Own-store classes are not scanned for the same reason #1776 does not serialize them: they mint
    /// and drop their own database, so an abandoned teardown cannot reach anyone else. That is decided by
    /// whether the class APPLIES the collection attribute, not by anything it says about itself. File and
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

                if (block.Contains("File.", StringComparison.Ordinal)
                    || block.Contains("Directory.", StringComparison.Ordinal)
                    || block.Contains("Kill", StringComparison.Ordinal))
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
    private static readonly string UnconvertedTeardown = Lines(
        "    [Fact]",
        "    public void T()",
        "    {",
        "        try",
        "        {",
        "            Work();",
        "        }",
        "        finally",
        "        {",
        "            Drop();",
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
