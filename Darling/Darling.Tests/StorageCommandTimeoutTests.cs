/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every command in <c>PerformanceMonitor.Darling.Storage</c> must carry an EXPLICIT deadline
/// (#2874). Sixty-nine of the project's 124 command sites ran on Npgsql's undocumented 30 s
/// default — a value nobody chose, and the defect class behind three production failures
/// (#2810, #2871, #2796): exceeding the ceiling fails in a way that looks like a legitimate result.
///
/// <para><b>Directory-scoped where the alert-pass pin is name-scoped, deliberately.</b>
/// <c>AlertPassCommandTimeoutTests</c> enumerates six files because its claim is "runs inside
/// <c>EvaluateAlertsAsync</c>", a budget boundary no filename expresses. This pin's claim is
/// different: EVERY command this project creates must have had its deadline chosen on purpose —
/// whichever regime's constant that is. That is a property of the project, so the sweep globs the
/// project and a future file is covered the day it appears rather than when someone remembers to
/// enlist it.</para>
///
/// <para>One value is NOT asserted here: the regimes bound themselves with their own deliberate
/// constants (<c>PgMigrations.MigrationCommandTimeoutSeconds</c>,
/// <c>TimescaleSupport.SetupTimeoutSeconds</c> / <c>JobCatalogReadTimeoutSeconds</c>, the
/// self-test's per-layer budget, <c>PlanDimRecompression</c>'s maintenance bound, and the VACUUM
/// FULL site's explicit <c>CommandTimeout = 0</c>, where unlimited is the choice rather than the
/// omission). Freezing those numbers here would couple every deliberate value to one test; the
/// structural claim — a deadline was SET — is what this file owns. The one constant this change
/// introduced, <see cref="StorageCommandDeadlines.McpReadSeconds"/>, gets its band pinned below.</para>
/// </summary>
public sealed class StorageCommandTimeoutTests
{
    /// <summary>
    /// Both ways a command is constructed in this codebase — <c>new NpgsqlCommand(</c> and
    /// <c>.CreateCommand(</c>. The second is the shape #2874's original census missed entirely
    /// (367 of its 371 sites were untimed), and 49 of this project's 69 were that shape.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new NpgsqlCommand\s*\(|\.CreateCommand\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryStorageCommand_SetsAnExplicitDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var path in StorageSources())
        {
            var text = File.ReadAllText(path);

            foreach (Match ctor in s_commandCtor.Matches(text))
            {
                total++;

                var span = StatementSpanFrom(text, ctor.Index, statements: 2);

                if (!s_setsTimeout.IsMatch(span))
                {
                    var line = text.Take(ctor.Index).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(path)}:{line}");
                }
            }
        }

        /* 124 sites at the time this pin landed; the floor guards against the sweep silently
           reading an empty or wrong directory, not against refactors that change the count. */
        Assert.True(total >= 100, $"the storage scan matched only {total} command constructions — the sweep is not reading the project");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} storage command(s) inherit Npgsql's 30s default instead of an explicit deadline: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The MCP-read constant, bounded on both sides — full derivation on
    /// <see cref="StorageCommandDeadlines.McpReadSeconds"/> itself. Short form: every verified read
    /// in the family measured 97–685 ms against both production stores, a deliberately harder
    /// unfiltered superset measured 35.2 s, and nothing encloses or restarts these calls, so the
    /// deadline must sit strictly under the 60 s a budget-rescued pass could tolerate.
    /// </summary>
    [Fact]
    public void TheMcpReadDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = StorageCommandDeadlines.McpReadSeconds;

        Assert.True(
            seconds >= 5,
            $"MCP-read deadline {seconds}s leaves under ~7x headroom over the 685 ms worst verified read — "
            + "a modest stall would fail healthy tools");

        Assert.True(
            seconds < 60,
            $"MCP-read deadline {seconds}s is not meaningfully under the inherited default it replaces: "
            + "these reads have no enclosing budget and nothing restarts them, so a hung tool call and a "
            + "held pool slot are the cost of every second granted here");
    }

    /// <summary>
    /// Scanner blind spots, pinned — a false positive here fails a green build on correct code.
    ///
    /// <para>The last case is the one this group's own tooling got wrong: a construction inside a
    /// <c>using (...) { }</c> STATEMENT, where the deadline legally sits inside the block and the
    /// enclosing method's brace closes before any semicolon. A scanner that clamps its nesting depth
    /// at zero extends the span into the NEXT member and reads a neighbour's deadline — it judged
    /// exactly one of the 69 real sites (PgMigrations' version read) already-timed when it was not.
    /// The <c>depth &lt;= 0</c> termination below is what keeps that from recurring.</para>
    /// </summary>
    [Theory]
    [InlineData(
        "var command = _postgres.CreateCommand(Sql);\n"
        + "/* set separately here; a method result cannot take an initializer. */\n"
        + "command.CommandTimeout = 10;\n",
        true)]
    [InlineData(
        "var command = new NpgsqlCommand(@\"\nSELECT 1;\nSELECT 2;\n\", connection) { CommandTimeout = 10 };\n",
        true)]
    [InlineData(
        "var command = _postgres.CreateCommand(Sql);\n"
        + "await command.ExecuteNonQueryAsync();\n",
        false)]
    [InlineData(
        "using (var read = new NpgsqlCommand(\"SELECT 1\", connection))\n"
        + "{\n"
        + "    value = (int)await read.ExecuteScalarAsync();\n"
        + "}\n"
        + "}\n"
        + "private void Next()\n"
        + "{\n"
        + "    using var other = new NpgsqlCommand(OtherSql, connection) { CommandTimeout = 10 };\n",
        false)]
    public void TheScanner_JudgesTheSiteItself_NotItsNeighbours(string source, bool expectedTimed)
    {
        var ctor = s_commandCtor.Match(source);
        Assert.True(ctor.Success, "the fixture did not contain a command construction");

        var span = StatementSpanFrom(source, ctor.Index, statements: 2);

        Assert.Equal(expectedTimed, s_setsTimeout.IsMatch(span));
    }

    /* The span walker below is the CI-proven copy from AlertPassCommandTimeoutTests — string- and
       comment-aware, terminating on the Nth semicolon at depth <= 0 so a span can never leak out of
       the member it started in. Kept as a private copy the way the three sibling pins keep theirs;
       extracting a shared helper across four test files is a refactor those lanes should take
       together or not at all. */

    private static string StatementSpanFrom(string text, int start, int statements)
    {
        var depth = 0;
        var seen = 0;
        var i = start;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                i = SkipVerbatimString(text, i + 2);
                continue;
            }

            if (c == '"')
            {
                i = SkipRegularString(text, i + 1);
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                var nl = text.IndexOf('\n', i);
                i = nl < 0 ? text.Length : nl + 1;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var end = text.IndexOf("*/", i + 2, System.StringComparison.Ordinal);
                i = end < 0 ? text.Length : end + 2;
                continue;
            }

            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                depth--;
            }
            else if (c == ';' && depth <= 0 && ++seen >= statements)
            {
                return text[start..(i + 1)];
            }

            i++;
        }

        return text[start..];
    }

    private static int SkipVerbatimString(string text, int i)
    {
        while (i < text.Length)
        {
            if (text[i] == '"')
            {
                if (i + 1 < text.Length && text[i + 1] == '"')
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return i;
    }

    private static int SkipRegularString(string text, int i)
    {
        while (i < text.Length)
        {
            if (text[i] == '\\')
            {
                i += 2;
                continue;
            }

            if (text[i] == '"')
            {
                return i + 1;
            }

            i++;
        }

        return i;
    }

    private static IEnumerable<string> StorageSources()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Storage");

        Assert.True(Directory.Exists(dir), $"storage project directory not found: {dir}");

        var paths = Directory.EnumerateFiles(dir, "*.cs", SearchOption.TopDirectoryOnly)
            .OrderBy(p => p, System.StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 30, $"the storage sweep found only {paths.Length} files — the project has moved");

        return paths;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
