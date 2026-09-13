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
/// Every test class that touches the shared <c>DARLING_TEST_PG</c> store must either serialize against the other
/// live classes or say in writing why it does not (#1776).
///
/// <para><b>The failure this prevents is a moving one.</b> Six classes connected to the shared store without
/// <c>[Collection("live-postgres")]</c>, so they ran in parallel with the sixty-odd classes that carry it — and
/// with each other. Three consecutive full-suite runs against one long-lived database failed on a DIFFERENT
/// unrelated class each time, which is the expensive shape of flake: it looks exactly like the change under test
/// broke something it never touched. <b>CI cannot catch this</b>, because it creates a throwaway cluster per run,
/// so the cost lands entirely on local development and shows up as someone else's problem in someone else's pull
/// request. That is precisely why it survived so long, and why the guard has to be a test rather than a habit.</para>
///
/// <para><b>The exemption is real and must stay available.</b> A class that mints its own database (through
/// <c>ScratchPostgres</c>) or stands up its own cluster does not race the shared store, and serializing it would
/// cost suite time for no safety at all. So this does not demand the attribute — it demands a DECISION, recorded
/// either as the attribute or as an <c>#1776 own-store</c> comment explaining the exemption.</para>
///
/// <para><b>Why the match is the quoted literal and not a substring.</b> <c>DARLING_TEST_PGRUNTIME</c> and
/// <c>DARLING_TEST_PGRUNTIME_OLD</c> are DIFFERENT variables that merely share the prefix, naming an assembled
/// runtime directory rather than a store. #1776's original sweep matched on substring and so listed three classes
/// that never read <c>DARLING_TEST_PG</c> at all, reaching the right answer for the wrong reason. Matching the
/// quoted literal keeps those out, and keeps out prose mentions in doc comments (which write the name bare) so a
/// class that only DESCRIBES the variable is not dragged in.</para>
/// </summary>
public sealed class LivePostgresCollectionHygieneTests
{
    /// <summary>The env-var read as it appears in code — quoted, so <c>DARLING_TEST_PGRUNTIME</c> cannot match.</summary>
    private const string SharedStoreVariable = "\"DARLING_TEST_PG\"";

    /// <summary>
    /// The scratch-database helper's factory call. Reaching the store THROUGH the helper counts as reaching the
    /// store, because that is what it does: <c>ScratchPostgres.CreateAsync</c> connects to the
    /// <c>DARLING_TEST_PG</c> server and issues <c>CREATE DATABASE</c> on it.
    ///
    /// <para>Without this the guard would only work by ACCIDENT — it would be detecting the env-var read that
    /// each consumer happens to duplicate at its own call site, not the store access. A routine DRY refactor
    /// moving that read inside the helper would blind the rule for every consumer at once, silently, which is
    /// exactly the failure it exists to prevent. Keying on the helper as well means the rule follows REACHING
    /// the store rather than the spelling of how you got there, so that refactor stays legal instead of being
    /// quietly forbidden.</para>
    ///
    /// <para>Matched with the trailing <c>.CreateAsync(</c> for the same reason the variable is matched quoted:
    /// prose says "through <c>ScratchPostgres</c>" and a test method is named <c>…AgainstScratchPostgres</c>,
    /// neither of which is a store access. The helper's own file declares <c>Task&lt;ScratchPostgres&gt;
    /// CreateAsync(</c> and <c>new ScratchPostgres(</c>, so it does not match itself either.</para>
    /// </summary>
    private const string ScratchStoreFactory = "ScratchPostgres.CreateAsync(";

    private const string LiveCollectionName = "live-postgres";

    private const string LiveCollectionAttribute = "[Collection(\"" + LiveCollectionName + "\")]";

    /// <summary>The recorded-exemption marker. Prose, deliberately: the point is that a human wrote down why.</summary>
    private const string OwnStoreMarker = "#1776 own-store";

    /// <summary>
    /// True when any whole SEGMENT of <paramref name="relativePath"/> is <c>bin</c> or <c>obj</c>, reading
    /// both separator characters on every platform.
    ///
    /// <para>Same shape and the same reasoning as <c>DocCommentHygieneTests.HasBuildOutputSegment</c>, which
    /// records why at length: a substring test for a backslash-delimited segment matches nothing where the
    /// separator is <c>/</c>, so the sweep above reads every generated <c>.AssemblyInfo.cs</c> and
    /// <c>.g.cs</c> off Windows while skipping them on it — and a guard whose scope depends on the host is
    /// two guards. Whole segments rather than a substring, because <c>Objects</c>, <c>obj-cache</c> and
    /// <c>mybin</c> are source directory names.</para>
    /// </summary>


    /// <summary>
    /// The build-output skip is a property of the PATH, not of the host it runs on.
    ///
    /// <para>Before this pin the skip was <c>Contains(@"\bin\")</c>, which matches nothing where the
    /// separator is <c>/</c> — so off Windows the sweep read generated sources out of <c>bin</c> and
    /// <c>obj</c> and could report a generated file as an offender, while on Windows it skipped them. The
    /// suite targets <c>net10.0-windows</c> so CI never saw it, which is exactly why a pin rather than a
    /// comment: the defect is invisible on the only platform that runs it.</para>
    /// </summary>
    [Theory]
    [InlineData(@"bin\Debug\net10.0\Foo.AssemblyInfo.cs", true)]
    [InlineData("bin/Debug/net10.0/Foo.AssemblyInfo.cs", true)]
    [InlineData(@"obj\Debug\Foo.g.cs", true)]
    [InlineData("obj/Debug/Foo.g.cs", true)]
    [InlineData("sub/obj/Debug/Foo.g.cs", true)]
    [InlineData("LivePostgresStoreFixture.cs", false)]
    [InlineData("mybin/Thing.cs", false)]
    [InlineData("obj-cache/Thing.cs", false)]
    [InlineData("Objects/Thing.cs", false)]
    public void TheBuildOutputSkip_ReadsBothSeparators_AndWholeSegmentsOnly(string relativePath, bool skipped)
    {
        Assert.Equal(skipped, HasBuildOutputSegment(relativePath));
    }

    /// <summary>
    /// The sweep routes its skip through <see cref="HasBuildOutputSegment"/> rather than testing the path
    /// inline, so the behaviour the theory above pins is the behaviour the sweep actually gets.
    /// </summary>
    [Fact]
    public void TheSweep_SkipsBuildOutputThroughTheSeparatorAwareHelper()
    {
        var source = File.ReadAllText(ThisSourceFile());

        Assert.Contains("HasBuildOutputSegment(Path.GetRelativePath(", source, StringComparison.Ordinal);
        Assert.DoesNotContain(@"file.Contains(@""\bin\""", source, StringComparison.Ordinal);
        Assert.DoesNotContain(@"file.Contains(@""\obj\""", source, StringComparison.Ordinal);
    }

    /// <summary>This file's own path, for the source pin above.</summary>
    private static string ThisSourceFile([CallerFilePath] string? path = null) => path!;    private static bool HasBuildOutputSegment(string relativePath) =>
        relativePath.Split('/', '\\')
            .Any(segment => string.Equals(segment, "bin", StringComparison.OrdinalIgnoreCase)
                            || string.Equals(segment, "obj", StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// The collection's own fixture (<see cref="LivePostgresStoreFixture"/>) reaches the shared store and
    /// cannot carry <c>[Collection]</c> — a fixture is not a test class. It needs no exemption comment either,
    /// because it is not merely serialized against the live classes: xUnit initializes it BEFORE any of them
    /// runs, which is a stronger guarantee than the attribute buys and is the entire reason it exists (#1862).
    ///
    /// <para>Resolved by REFLECTION off the <c>[CollectionDefinition]</c> rather than accepted as a third
    /// prose marker, so the exemption cannot be claimed by pasting a comment: it belongs to exactly the type
    /// the collection is actually wired to. That also makes this rule guard the wiring — unhook the fixture
    /// from the definition and the exemption evaporates, so the fixture file itself turns up as an offender.
    /// Null when nothing is wired, which exempts nobody.</para>
    /// </summary>
    private static string? LiveCollectionFixtureTypeName()
    {
        foreach (var type in typeof(LivePostgresCollectionHygieneTests).Assembly.GetTypes())
        {
            var definition = (CollectionDefinitionAttribute?)Attribute.GetCustomAttribute(
                type, typeof(CollectionDefinitionAttribute));

            if (definition is null || !string.Equals(definition.Name, LiveCollectionName, StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var contract in type.GetInterfaces())
            {
                if (contract.IsGenericType
                    && contract.GetGenericTypeDefinition() == typeof(ICollectionFixture<>))
                {
                    return contract.GetGenericArguments()[0].Name;
                }
            }
        }

        return null;
    }

    /// <summary>Top-level class declarations. Nested types are indented and correctly not matched.</summary>
    private static readonly Regex ClassDeclaration =
        new(@"^(?:public|internal)\s+(?:sealed\s+|static\s+|abstract\s+|partial\s+)*class\s+(\w+)",
            RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>
    /// How far above a class declaration to look for its attribute or exemption marker. Generous enough for an
    /// attribute sitting above a long doc comment, tight enough that it cannot reach into the previous class's body.
    /// </summary>
    private const int HeaderLookbackLines = 25;

    [Fact]
    public void EveryClassUsingTheSharedStore_IsSerializedOrDocumentsWhyNot()
    {
        var directory = FindTestProjectDirectory();

        /* FAIL rather than skip when the source tree cannot be found — the DocCommentHygieneTests rule: a guard
           that silently skips is a guard that silently stops guarding. */
        Assert.True(directory is not null,
            "Could not locate the Darling.Tests source directory (walked up from the test binary looking for "
            + "PerformanceMonitor.sln). This test scans source, so it cannot run without it — fix the walk-up "
            + "rather than skipping, or the rule stops being enforced without anyone noticing.");

        var offenders = new List<string>();
        var fixtureTypeName = LiveCollectionFixtureTypeName();

        /* Recurse, and skip build output. The project is flat today, so TopDirectoryOnly would be equivalent —
           but it would also mean the first test file someone puts in a subfolder escapes the rule silently, which
           is the failure mode this whole test exists to prevent. Scoping to THIS project is correct rather than
           lazy: [Collection] groups classes within an assembly, so a class elsewhere could not join the live
           collection even if it wanted to, and the quoted literal appears nowhere else in the repo. */
        foreach (var file in Directory.EnumerateFiles(directory!, "*.cs", SearchOption.AllDirectories))
        {
            if (HasBuildOutputSegment(Path.GetRelativePath(directory!, file)))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (!TouchesSharedStore(text))
            {
                continue;
            }

            var declarations = ClassDeclaration.Matches(text);
            for (var i = 0; i < declarations.Count; i++)
            {
                var declaration = declarations[i];
                var bodyEnd = i + 1 < declarations.Count ? declarations[i + 1].Index : text.Length;
                var body = text[declaration.Index..bodyEnd];

                if (!TouchesSharedStore(body))
                {
                    continue;
                }

                if (string.Equals(declaration.Groups[1].Value, fixtureTypeName, StringComparison.Ordinal))
                {
                    continue;
                }

                var header = HeaderAbove(text, declaration.Index);
                if (CarriesLiveCollectionAttribute(header)
                    || header.Contains(OwnStoreMarker, StringComparison.Ordinal))
                {
                    continue;
                }

                var line = text.AsSpan(0, declaration.Index).Count('\n') + 1;
                offenders.Add($"{Path.GetFileName(file)}:{line}  {declaration.Groups[1].Value}");
            }
        }

        Assert.True(offenders.Count == 0,
            "These classes reach the shared DARLING_TEST_PG store — directly, or through ScratchPostgres, which "
            + "mints its database on that server — but neither serialize against the other live classes nor "
            + "record why they do not:\n\n"
            + string.Join("\n", offenders)
            + "\n\nPick one, deliberately:\n"
            + $"  - Add {LiveCollectionAttribute} if the class touches the SHARED database. Rolling the writes "
            + "back does not exempt it: an uncommitted write still holds its row locks and still fires triggers.\n"
            + $"  - Add an \"{OwnStoreMarker}\" comment if the class mints its own database or cluster and so "
            + "cannot race the shared one. Serializing those is pure slowdown.\n\n"
            + "If the class is mostly pure with one live test, prefer SPLITTING the live test into its own "
            + "...LivePostgresTests class (the shape more than forty files here already use) over serializing "
            + "every pure test alongside it.");
    }

    /// <summary>
    /// Does this source reach the shared store — directly by reading <c>DARLING_TEST_PG</c>, or through the
    /// scratch-database helper, which mints its database ON that server? Either counts.
    /// </summary>
    private static bool TouchesSharedStore(string source) =>
        source.Contains(SharedStoreVariable, StringComparison.Ordinal)
        || source.Contains(ScratchStoreFactory, StringComparison.Ordinal);

    /// <summary>
    /// Is the attribute APPLIED here, as opposed to merely talked about? It counts only when it OPENS a line,
    /// which every one of the seventy-odd real ones does — alone above the class, or ahead of it on one line.
    ///
    /// <para>A plain "does the header contain it" was not enough, and the hole was not hypothetical: every
    /// prose mention in this project sits behind <c>///</c> or inside the <c>/* #1776 own-store */</c> blocks,
    /// so ANY class whose doc comment quoted the attribute while explaining the rule exempted itself from it.
    /// <see cref="LivePostgresStoreFixture"/>'s summary mentions it nineteen lines above its declaration —
    /// comfortably inside the lookback — and that alone made the fixture read as compliant when nothing had
    /// been decided about it at all (#1862). Matching on position rather than on comment prefixes is what
    /// makes it robust: it covers the <c>///</c> case, the block-comment case whose continuation lines carry
    /// no marker of their own, and prose that merely names the attribute mid-sentence, with no comment-state
    /// tracking that a SQL string containing a comment delimiter could throw off.</para>
    ///
    /// <para>Deliberately NOT applied to <see cref="OwnStoreMarker"/>, which is prose in a comment BY DESIGN:
    /// the exemption's whole value is that a human wrote down why, so the check for it must read comments.
    /// The two live in the same header and are read by different rules on purpose.</para>
    ///
    /// <para>The narrowing can only DISCARD a match, so its failure mode is a compliant class wrongly
    /// REPORTED, which is loud and gets looked at. That is the right direction for a guard whose silent
    /// misses are what let the #1776 family of moving flakes survive in the first place.</para>
    /// </summary>
    private static bool CarriesLiveCollectionAttribute(string header)
    {
        foreach (var line in header.Split('\n'))
        {
            if (line.TrimStart().StartsWith(LiveCollectionAttribute, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The class's own attribute/comment region: the <see cref="HeaderLookbackLines"/> lines immediately above the
    /// declaration. Bounded so it cannot reach back into the previous class's body and read ITS attribute.
    /// </summary>
    private static string HeaderAbove(string text, int declarationIndex)
    {
        var start = declarationIndex;
        for (var lines = 0; lines < HeaderLookbackLines && start > 0; lines++)
        {
            var previous = text.LastIndexOf('\n', start - 1);
            if (previous < 0)
            {
                start = 0;
                break;
            }

            start = previous;
        }

        return text[start..declarationIndex];
    }

    /// <summary>
    /// Walks up from the test output directory to the repo root (the directory holding
    /// <c>PerformanceMonitor.sln</c>) and returns this project's source directory. Same walk-up idiom as
    /// <c>DocCommentHygieneTests.FindRepoRoot</c>.
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
