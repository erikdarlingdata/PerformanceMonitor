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
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4316: three notes that ride inside a 200 response body — <c>/api/ping</c>'s <c>detail</c>, and the
/// force-plan and fact-collector notes two analysis tools attach to their payload — used to carry
/// <c>ex.Message</c> verbatim. An exception's message is not vetted for that audience: a connection string, a
/// file path or a driver's inner-exception chain can all land in it. Every one of those sites now uses a fixed,
/// non-message text instead, built by <see cref="PgFactCollector.DescribeFailureForPayload"/> for the two
/// analysis notes and by <c>CollectorRuntimeState.FailureDetailFor</c> for ping. The full exception text still
/// reaches the service log everywhere it did before, plus one site (the force-plan reader) that logged nothing
/// at all and now does.
///
/// <para>Pinned two ways: a unit test of the shared formatter, and a structural census per call site — over
/// comment/string-stripped source, so a re-introduced <c>ex.Message</c> fails here rather than in a support
/// ticket that pastes a ping response into a chat.</para>
/// </summary>
public sealed class FailureNoteRedactionTests
{
    // ── the formatter ────────────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeFailureForPayload_CarriesTypeAndSqlState_NeverTheMessage()
    {
        const string sentinel = "sentinel-text-that-must-never-leave-the-service-log";

        var pg = new PostgresException(sentinel, "ERROR", "ERROR", "42P01");
        var pgText = PgFactCollector.DescribeFailureForPayload(pg);
        Assert.Contains("42P01", pgText, StringComparison.Ordinal);
        Assert.Contains(nameof(PostgresException), pgText, StringComparison.Ordinal);
        Assert.DoesNotContain(sentinel, pgText, StringComparison.Ordinal);

        var plain = new InvalidOperationException(sentinel);
        var plainText = PgFactCollector.DescribeFailureForPayload(plain);
        Assert.Contains(nameof(InvalidOperationException), plainText, StringComparison.Ordinal);
        Assert.DoesNotContain(sentinel, plainText, StringComparison.Ordinal);
    }

    // ── the two fact collectors ─────────────────────────────────────────────────────────

    [Theory]
    [InlineData("PgFactCollector.cs")]
    [InlineData("PgTargetFactCollector.cs")]
    public void TheFactCollector_RecordsTheFormattersTextNotExMessage(string file)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis", file)));

        var site = Regex.Match(code, @"context\.RecordCollectionFailure\(");
        Assert.True(site.Success, $"expected exactly one context.RecordCollectionFailure(...) call in {file}");

        var call = CSharpSourceWalker.ConstructionSpanFrom(code, site.Index);
        Assert.DoesNotContain("ex.Message", call, StringComparison.Ordinal);
        Assert.Contains("DescribeFailureForPayload(ex)", call, StringComparison.Ordinal);
    }

    // ── the force-plan target-state reader ──────────────────────────────────────────────

    [Fact]
    public void TheForcePlanReader_LogsOnceAndNeverPutsExMessageInItsReturnedNote()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingForcePlanTargetStateReader.cs")));

        /* No other ex.Message use exists anywhere in this file (confirmed by reading it) — comments are
           stripped above, so this catches the code, not a remark about the fix. */
        Assert.DoesNotContain("ex.Message", code, StringComparison.Ordinal);
        Assert.Contains("DescribeFailureForPayload(ex)", code, StringComparison.Ordinal);

        /* #4316: this failure logged nowhere before. It must now, at Warning, with the exception itself
           (not just its message) so the service log keeps the full text and stack. */
        Assert.Contains("logger?.LogWarning(ex,", code, StringComparison.Ordinal);
    }

    // ── DarlingWorker.cs' six collection-blocking startup publishes ────────────────────

    [Fact]
    public void NeitherPublishCall_InDarlingWorker_EverPassesExMessage()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs")));

        var sites = Regex.Matches(code, @"_collectorState\.(?:PublishRetrying|PublishStopped)\(").ToList();
        Assert.True(sites.Count >= 6, $"expected at least the six collection-blocking startup publishes; found {sites.Count}");

        foreach (Match site in sites)
        {
            var span = CSharpSourceWalker.ConstructionSpanFrom(code, site.Index);
            Assert.DoesNotContain("ex.Message", span, StringComparison.Ordinal);
        }

        /* The six collection-blocking sites source their Detail from CollectorRuntimeState.FailureDetailFor
           instead — one call per site, so a count below six means one quietly went back to ex.Message and a
           count above just means the helper gained a caller nobody reviewed for this. */
        Assert.Equal(6, Regex.Matches(code, @"CollectorRuntimeState\.FailureDetailFor\(").Count);
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
