/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4316: three notes that ride inside a 200 response body — <c>/api/ping</c>'s <c>detail</c>, and the
/// force-plan and fact-collector notes two analysis tools attach to their payload — used to carry
/// <c>ex.Message</c> verbatim. An exception's message is not vetted for that audience: a connection string, a
/// file path or a driver's inner-exception chain can all land in it. Every one of those sites now uses a fixed,
/// non-message text instead, built by <see cref="CollectionFailure.Describe"/> for the collection-failure
/// records and the two force-plan notes alike, and by <c>CollectorRuntimeState.FailureDetailFor</c> for ping.
/// The full exception text still reaches the service log everywhere it did before, plus one site (the
/// force-plan reader) that logged nothing at all and now does.
///
/// <para>Round 1 B1 retired <c>PgFactCollector.DescribeFailureForPayload</c>: <c>AnalysisContext.RecordCollectionFailure</c>
/// now takes the <see cref="Exception"/> itself rather than a caller-built string (so a fact collector cannot
/// pass <c>ex.Message</c> even by mistake — there is no string parameter left to compile against), and the one
/// remaining string-building consumer, the two force-plan notes, share <see cref="CollectionFailure.Describe"/>
/// with it instead of carrying their own copy of the same wording.</para>
///
/// <para>Pinned two ways: a unit test of the shared formatter, and a structural census over the two force-plan
/// call sites — over comment/string-stripped source, so a re-introduced <c>ex.Message</c> fails here rather
/// than in a support ticket that pastes a ping response into a chat.</para>
/// </summary>
public sealed class FailureNoteRedactionTests
{
    // ── the formatter ────────────────────────────────────────────────────────────────────

    [Fact]
    public void Describe_CarriesTypeAndSqlState_NeverTheMessage()
    {
        var pg = new PostgresException("role \"x\" does not exist", "ERROR", "ERROR", "42501");
        var pgText = CollectionFailure.Describe(pg, CollectionFailureOutcome.Error);
        Assert.Equal("PostgresException, SQLSTATE 42501; the log has the full error", pgText);
        Assert.DoesNotContain("role", pgText, StringComparison.Ordinal);

        /* #4316 round 1 (L1): MissingSchema gets its own wording — the log has nothing for this arm at the
           DEFAULT level (it logs at Debug, the expected pre-migration case), so the note says that rather
           than pointing at an empty log. */
        var missingSchema = new PostgresException("relation \"x\" does not exist", "ERROR", "ERROR", "42P01");
        var missingSchemaText = CollectionFailure.Describe(missingSchema, CollectionFailureOutcome.MissingSchema);
        Assert.Equal(
            "PostgresException, SQLSTATE 42P01; a table or column the read needs is missing, logged only at Debug level",
            missingSchemaText);

        var plain = new InvalidOperationException("Host=secret");
        var plainText = CollectionFailure.Describe(plain, CollectionFailureOutcome.Error);
        Assert.Equal("InvalidOperationException; the log has the full error", plainText);
        Assert.DoesNotContain("secret", plainText, StringComparison.Ordinal);
    }

    // ── only AnalysisContext may construct a CollectionFailure ─────────────────────────

    /// <summary>
    /// #4316 round 2 L1-r2 b, the minimum: <see cref="CollectionFailure"/> stays a public positional record
    /// and <see cref="AnalysisContext.CollectionFailures"/> stays a public <c>List&lt;&gt;</c> — narrowing
    /// either is a bigger change than this round makes — so nothing stops a collector from writing
    /// <c>context.CollectionFailures.Add(new CollectionFailure(family, read, outcome, ex.Message))</c> and
    /// putting an exception's own message straight back into a collection-failure record. This is the
    /// backstop instead: a structural census, over every production <c>.cs</c> file this repository ships
    /// under <c>PerformanceMonitor.Analysis</c>, <c>PerformanceMonitor.Common</c>, <c>Darling</c> and
    /// <c>Lite</c> (every test project, and every <c>bin</c>/<c>obj</c>, excluded — over
    /// comment/string-stripped source, so a remark that quotes the pattern cannot trip it), that only
    /// <c>AnalysisContext.cs</c> ever spells <c>new CollectionFailure(</c> or <c>CollectionFailures.Add(</c>.
    /// </summary>
    [Fact]
    public void OnlyAnalysisContext_ConstructsOrAppendsACollectionFailure()
    {
        var root = RepoRoot();
        var sanctioned = Path.Combine(root, "PerformanceMonitor.Analysis", "AnalysisContext.cs");

        var offenders = (
            from scanRoot in new[] { "PerformanceMonitor.Analysis", "PerformanceMonitor.Common", "Darling", "Lite" }
            from file in Directory.EnumerateFiles(Path.Combine(root, scanRoot), "*.cs", SearchOption.AllDirectories)
            where !string.Equals(file, sanctioned, StringComparison.OrdinalIgnoreCase)
            let relative = Path.GetRelativePath(root, file)
            where !relative.Split('/', '\\').Any(segment =>
                string.Equals(segment, "bin", StringComparison.OrdinalIgnoreCase)
                || string.Equals(segment, "obj", StringComparison.OrdinalIgnoreCase)
                || segment.EndsWith(".Tests", StringComparison.OrdinalIgnoreCase))
            let code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file))
            where code.Contains("new CollectionFailure(", StringComparison.Ordinal)
                  || code.Contains("CollectionFailures.Add(", StringComparison.Ordinal)
            select relative
        ).ToArray();

        Assert.True(
            offenders.Length == 0,
            "Only PerformanceMonitor.Analysis/AnalysisContext.cs may construct a CollectionFailure or append "
          + "to a CollectionFailures list — anywhere else is a seat a collector could put ex.Message into. "
          + "Found the pattern outside it in: " + string.Join(", ", offenders));
    }

    // ── the force-plan target-state reader, and the bot's own copy of the same read ────

    [Theory]
    [InlineData("Mcp/DarlingForcePlanTargetStateReader.cs")]
    [InlineData("PgPlanForceActionStore.cs")]
    public void TheForcePlanReader_LogsOnceAndNeverPutsExMessageInItsReturnedNote(string relativePath)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", relativePath.Replace('/', Path.DirectorySeparatorChar))));

        /* No other ex.Message use exists anywhere in either file (confirmed by reading them) — comments are
           stripped above, so this catches the code, not a remark about the fix. PgPlanForceActionStore's own
           TryGetTargetStatesAsync (#4316 round 1, M1) is the bot's copy of the same read and used to carry the
           same raw ex.Message the reader was fixed to drop. Round 1 B1 moved both off their own
           PgFactCollector.DescribeFailureForPayload copy onto the shared CollectionFailure.Describe, so the
           two notes cannot drift into two different wordings. */
        Assert.DoesNotContain("ex.Message", code, StringComparison.Ordinal);
        Assert.Contains("CollectionFailure.Describe(ex, CollectionFailureOutcome.Error)", code, StringComparison.Ordinal);

        /* #4316: this failure logged nowhere before. It must now, at Warning, with the exception itself
           (not just its message) so the service log keeps the full text and stack. */
        Assert.Contains(".LogWarning(ex,", code, StringComparison.Ordinal);
    }

    // ── DarlingWorker.cs' collection-blocking startup publishes ────────────────────────

    /// <summary>
    /// #4316 round 1 B1: <c>PublishRetrying</c>/<c>PublishStopped</c> no longer take a detail parameter at
    /// all — the reflection test in <c>CollectorRuntimeStateTests</c> pins that no public
    /// <see cref="CollectorRuntimeState"/> <c>Publish*</c> method takes a <see cref="string"/>, so a caller
    /// has no parameter left to put <c>ex.Message</c> into. That is a stronger guarantee than any grep census
    /// here could give, which is why the census this test replaced is gone rather than updated. What is
    /// still worth pinning here is that the FIXED sentences those methods build from actually read as fixed:
    /// no stray <c>{</c> that would mean some template placeholder never got filled in and shipped as a
    /// literal brace in a ping body.
    /// </summary>
    [Fact]
    public void EveryFixedFailureSentence_HoldsNoPlaceholder()
    {
        foreach (CollectorRuntimeState.StartupStep step in Enum.GetValues(typeof(CollectorRuntimeState.StartupStep)))
        {
            Assert.DoesNotContain("{", CollectorRuntimeState.FailureDetailFor(step), StringComparison.Ordinal);
        }

        Assert.DoesNotContain("{", CollectorRuntimeState.ManagedStoreNeedsWindowsDetail, StringComparison.Ordinal);
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
