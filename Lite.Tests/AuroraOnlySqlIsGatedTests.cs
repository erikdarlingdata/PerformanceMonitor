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
using Darling.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Every reference to an Aurora-only SQL surface is accounted for — either the code path is gated to
/// Aurora, or it has a vanilla alternative chosen by flavor.
///
/// <para>
/// This class exists because the same defect shipped twice. #2625: <c>pg_statement_stats</c> read
/// <c>aurora_stat_statements()</c> behind an <c>AppliesTo =&gt; IsAurora</c> gate, so self-hosted
/// PostgreSQL had no answer at all to "which queries cost the most" — the question a database monitor
/// exists for. #2651: the statement TEXT store read the same function with no gate and no alternative, so
/// off Aurora it silently never populated, which made <c>get_pg_top_queries</c> return a null
/// <c>query_text</c> forever and made <c>test_hypothetical_index</c> unable to work at all.
/// </para>
///
/// <para>
/// The second one survived the audit that found the first, and the reason is structural rather than
/// careless: <c>pg_statement_text</c> is not a collector. It is refreshed directly by the worker, so it
/// was in neither the catalog sweep nor the <c>AppliesTo</c> audit. A guard that only walks
/// <c>ICollectorDefinition</c> implementations cannot see it. This one walks the SOURCE.
/// </para>
///
/// <para>
/// <b>The allow-list is the point.</b> Each entry is a claim that somebody looked at that file and decided
/// what its Aurora dependency means for a self-hosted target. A new file appearing here is not
/// necessarily a bug — but it IS a decision, and the failure message asks for it rather than letting the
/// file arrive unexamined.
/// </para>
/// </summary>
public class AuroraOnlySqlIsGatedTests
{
    /// <summary>The Aurora-extended surfaces. Community PostgreSQL has none of them under any version.</summary>
    private static readonly string[] AuroraSurfaces =
    {
        "aurora_stat_statements",
        "aurora_stat_system_waits",
        "aurora_stat_wait_type",
        "aurora_stat_wait_event",
    };

    /// <summary>
    /// Every file allowed to name one, and WHY it is allowed. Lowering an entry to "gated" or "paired" is
    /// a decision someone made; adding a file is one someone must make.
    /// </summary>
    private static readonly Dictionary<string, string> Accounted = new(StringComparer.OrdinalIgnoreCase)
    {
        ["PgWaitStatsCollector.cs"] =
            "GATED. AppliesTo => IsAurora, and correctly: it reads Aurora's own wait instrumentation, which " +
            "community PostgreSQL has no equivalent of. pg_wait_sampling answers the same question there, and " +
            "since #2625 the permanent-gap message names it.",

        ["PgStatementStatsCollector.cs"] =
            "PAIRED (#2625). Aurora reads aurora_stat_statements(); every other PostgreSQL reads the vanilla " +
            "pg_stat_statements view with the Aurora-only columns as typed NULLs. Chosen in BuildQuery, not by " +
            "AppliesTo, because the source differs and the capability does not.",

        ["PgStatementText.cs"] =
            "PAIRED (#2651). Same split, one layer down: FetchSqlFor(isAurora, major) picks Aurora's function " +
            "or the vanilla view. This is the one that shipped ungated and made test_hypothetical_index dead " +
            "on self-hosted PostgreSQL.",

        ["CollectorEngineCapability.cs"] =
            "PROSE. Names the surfaces in the sentence shown to an operator when a collector cannot run here. " +
            "It describes the dependency rather than depending on it.",
    };

    [Fact]
    public void EveryFileNamingAnAuroraOnlySurface_IsAccountedFor()
    {
        var found = FilesNamingAuroraSurfaces();

        Assert.NotEmpty(found);

        var unaccounted = found.Where(f => !Accounted.ContainsKey(Path.GetFileName(f)))
                               .OrderBy(f => f, StringComparer.Ordinal)
                               .ToArray();

        Assert.True(
            unaccounted.Length == 0,
            "These files reference an Aurora-only SQL surface and nothing records what that means for a " +
            "self-hosted PostgreSQL target:\n  " + string.Join("\n  ", unaccounted) +
            "\n\nAurora-only SQL is fine. Aurora-only SQL that NOBODY DECIDED ABOUT is how #2651 shipped: the " +
            "statement-text store read aurora_stat_statements() with no gate and no alternative, so off Aurora " +
            "it silently never populated and a feature that depends on it could not work at all.\n\n" +
            "Decide which this is, then add it to Accounted:\n" +
            "  GATED  - the collector's AppliesTo excludes non-Aurora targets, and the capability really is absent there.\n" +
            "  PAIRED - there is a vanilla path chosen by flavor, so the SOURCE differs and the capability does not.\n" +
            "  PROSE  - it only names the surface in a message.");
    }

    /// <summary>
    /// The allow-list cannot outlive what it describes. An entry naming a file that no longer references
    /// any Aurora surface is a note about a decision that has been undone — and it would silently permit
    /// that file to reacquire one later.
    /// </summary>
    [Fact]
    public void TheAllowListHasNoStaleEntries()
    {
        var found = FilesNamingAuroraSurfaces().Select(Path.GetFileName).ToHashSet(StringComparer.OrdinalIgnoreCase);

        var stale = Accounted.Keys.Where(k => !found.Contains(k))
                                  .OrderBy(k => k, StringComparer.Ordinal)
                                  .ToArray();

        Assert.True(stale.Length == 0,
            "These allow-list entries name no Aurora surface any more — remove them, or the file could " +
            "reacquire one unexamined: " + string.Join(", ", stale));
    }

    /// <summary>
    /// The two PAIRED files must actually branch. An entry claiming a vanilla alternative while the code
    /// has none would be worse than no entry: it records a decision that was never implemented.
    /// </summary>
    [Theory]
    [InlineData("PgStatementStatsCollector.cs")]
    [InlineData("PgStatementText.cs")]
    public void APairedFileReallyReadsTheVanillaViewToo(string fileName)
    {
        var path = FilesNamingAuroraSurfaces().Single(f => string.Equals(Path.GetFileName(f), fileName, StringComparison.OrdinalIgnoreCase));
        var source = File.ReadAllText(path);

        Assert.Contains("pg_stat_statements", source, StringComparison.Ordinal);

        /* Case-INSENSITIVE: the collector branches on context.Target.IsAurora and the text store on a
           parameter named isAurora, and a guard that only accepted one spelling would fail on correct
           code. It did, on exactly this file, the first time this test ran in CI. */
        Assert.Contains("isaurora", source, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The control for how <see cref="NamesAnAuroraSurfaceOutsideAComment"/> reads a file, on an arranged
    /// input rather than on the tree — because both of the plausible one-line filters get this wrong, in
    /// opposite directions, and neither failure is visible in a passing sweep.
    ///
    /// <para>The fixture puts the surface name in a SQL literal (which IS a dependency) and in a block
    /// comment whose continuation lines carry no asterisk (which is not). A line-prefix filter finds the
    /// comment; <c>StripCommentsAndStrings</c> alone finds neither, because the dependency lives in the
    /// literal it blanks. Only asking both non-comment views gives the answer, and the second assertion
    /// below is the one that stops someone "simplifying" this to the walker's stripper on the grounds that
    /// #3052 named it as the fix.</para>
    /// </summary>
    [Fact]
    public void TheReadFindsASurfaceInSql_AndNotOneInABlockComment()
    {
        const string fixture = """
            internal static class Fixture
            {
                /* Aurora exposes aurora_stat_statements, which community PostgreSQL does not have; the
                   vanilla path reads pg_stat_statements instead and types the missing columns as NULL. */
                private const string Sql = "SELECT calls FROM aurora_stat_wait_event";
            }
            """;

        var path = Path.Combine(Path.GetTempPath(), $"pm-3052-{Guid.NewGuid():N}.cs");

        try
        {
            File.WriteAllText(path, fixture);

            /* The literal is found. */
            Assert.True(NamesAnAuroraSurfaceOutsideAComment(path));

            /* And the reason the read cannot be the stripper alone: it hides exactly that literal. */
            Assert.DoesNotContain(
                "aurora_stat_wait_event",
                CSharpSourceWalker.StripCommentsAndStrings(fixture),
                StringComparison.Ordinal);

            /* The comment half, asked the other way round: the walk removes the paragraph a line-prefix
               filter would have handed over as code, continuation line and all. The prefix filter's own
               failure on this shape is pinned in Darling.Tests/CommentFilterAdoptionTests. */
            Assert.DoesNotContain(
                "aurora_stat_statements",
                CSharpSourceWalker.StripCommentsAndStrings(fixture),
                StringComparison.Ordinal);

            Assert.Contains(
                "aurora_stat_wait_event",
                string.Join("\n", CSharpSourceWalker.StringLiteralBodies(fixture).Select(b => b.Text)),
                StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static string[] FilesNamingAuroraSurfaces()
    {
        var root = RepoRoot();

        return Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}.claude{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            /* Tests are excluded deliberately: a test naming the surface is asserting ABOUT it, which is
               the opposite of depending on it. */
            .Where(f => !f.Contains(".Tests", StringComparison.OrdinalIgnoreCase))
            .Where(NamesAnAuroraSurfaceOutsideAComment)
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>
    /// Comments are skipped, or every explanatory paragraph about why Aurora is different would count as a
    /// dependency on it — which would make the guard so noisy that the allow-list stopped being read.
    ///
    /// <para><b>Asked of code and of string LITERALS, and never of prose.</b> That combination is the whole
    /// subtlety, because the two halves fail in opposite directions here. A line-prefix filter under-strips:
    /// a block comment's continuation lines in this codebase carry no asterisk, so the second line of a
    /// paragraph about <c>aurora_stat_statements</c> reads as a dependency and the file arrives unaccounted
    /// (#3052). But <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> alone over-strips for THIS
    /// question, and catastrophically: every Aurora surface name in this repo is SQL inside a string
    /// literal, so blanking literal text finds nothing at all — four accounted files become zero, which
    /// fails <c>Assert.NotEmpty</c> loudly today and would otherwise be a guard that had quietly stopped
    /// looking.</para>
    ///
    /// <para>So the walk is used for what it is exact about — where the comments are — and both of the
    /// non-comment views it exposes are asked: the code, and the literal bodies. Blanking interpolation
    /// holes inside those bodies costs nothing, because a surface NAME is never assembled out of a
    /// hole.</para>
    ///
    /// <para><b>The raw-text gate is not an optimisation to taste.</b> This read enumerates every
    /// <c>*.cs</c> file in the repository — 1,345 of them — once per test, four times over, and the walk is a
    /// per-character pass with a <c>StringBuilder</c> behind it. Measured: walking unconditionally took the
    /// class from 0.55s to 5.53s, a 10x regression on a guard that is not the thing under test. The gate is
    /// EXACT rather than approximate, which is what makes it safe to skip the walk on: blanking only ever
    /// replaces a character with a space or a newline, and no surface name contains either, so a needle
    /// absent from the raw text is absent from every view of it. Roughly six files in the tree get past it,
    /// and the class is back to 0.49s.</para>
    /// </summary>
    private static bool NamesAnAuroraSurfaceOutsideAComment(string path)
    {
        var text = File.ReadAllText(path);

        if (!AuroraSurfaces.Any(s => text.Contains(s, StringComparison.Ordinal)))
        {
            return false;
        }

        var code = CSharpSourceWalker.StripCommentsAndStrings(text);

        if (AuroraSurfaces.Any(s => code.Contains(s, StringComparison.Ordinal)))
        {
            return true;
        }

        return CSharpSourceWalker.StringLiteralBodies(text)
            .Any(body => AuroraSurfaces.Any(s => body.Text.Contains(s, StringComparison.Ordinal)));
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
