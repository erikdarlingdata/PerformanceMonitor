/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A source-text census pin for the DuckDB/Postgres watermark and prior-success reads that used to catch
/// every exception unconditionally and fall back to "no watermark / no prior success" — the same shape a
/// cancelled request's caller cannot tell apart from an ordinary read failure. Moved here from
/// <c>Lite.Tests</c> (which cannot run in-process on macOS; discovery dies on <c>WindowsBase</c>) because
/// reading source text needs nothing Windows-only. This runs on macOS and in CI alike.
/// </summary>
public sealed class RemoteCollectorServiceCancellationCensusTests
{
    /// <summary>
    /// Lite's five reads (<c>GetLastCollectedTimeAsync</c>, <c>GetLastCollectedTimeWithFrameAsync</c>,
    /// <c>GetLastCollectedTimeForDatabaseAsync</c>, <c>GetLastCollectedInstanceIdAsync</c>,
    /// <c>HasPriorCollectorSuccessAsync</c>) each declare a method-signature <c>CancellationToken</c> and are
    /// called with the caller's own token — the only source of a cancelled token here is the caller's own
    /// cancellation, exactly the reasoning Darling's twin already states on
    /// <c>DarlingCollectorRunner.GetLastCollectedTimeAsync</c>. Every one of their catch blocks must exclude
    /// <see cref="OperationCanceledException"/>, or a cancelled shutdown reads as "DuckDB query failed, use
    /// the fallback window" instead of propagating.
    /// </summary>
    [Fact]
    public void EveryLiteWatermarkOrStateReadCatch_ExcludesOperationCanceledException()
    {
        var source = ReadRepoFile("Lite", "Services", "RemoteCollectorService.cs");

        string[] methods =
        {
            "GetLastCollectedTimeAsync",
            "GetLastCollectedTimeWithFrameAsync",
            "GetLastCollectedTimeForDatabaseAsync",
            "GetLastCollectedInstanceIdAsync",
            "HasPriorCollectorSuccessAsync",
        };

        AssertEveryMethodCatchExcludesCancellation(source, methods, "\n    protected async Task");
    }

    /// <summary>
    /// Darling's <c>HasPriorCollectorSuccessAsync</c> has the same shape as the Lite reads above and is
    /// called with the collector's own <c>cancellationToken</c> too — a swallowed cancellation here returns
    /// <c>false</c> and is read by the caller as "no prior success, first run" rather than propagating; it
    /// is NOT actually caught one level up, since <c>false</c> never throws.
    /// </summary>
    [Fact]
    public void DarlingHasPriorCollectorSuccessAsyncCatch_ExcludesOperationCanceledException()
    {
        var source = ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

        AssertEveryMethodCatchExcludesCancellation(
            source, new[] { "HasPriorCollectorSuccessAsync" }, "\n    public void OnServerReconnected");
    }

    private static void AssertEveryMethodCatchExcludesCancellation(
        string source, string[] methods, string nextMemberMarker)
    {
        foreach (var method in methods)
        {
            /* "async Task<...>" / "async ValueTask<...>" locates the DECLARATION, never a call site —
               HasPriorCollectorSuccessAsync is also invoked earlier in DarlingCollectorRunner.cs, and a
               bare method-name search would slice from that call, not the method it calls. */
            var signatureMatch = Regex.Match(
                source, $@"\basync\s+(Task|ValueTask)(<[^>]*>)?\s+{Regex.Escape(method)}\(");
            Assert.True(signatureMatch.Success, $"Could not find the declaration of {method} in the source");
            var signatureIndex = signatureMatch.Index;

            var bodyEnd = source.IndexOf(nextMemberMarker, signatureIndex + method.Length, StringComparison.Ordinal);
            if (bodyEnd < 0)
            {
                bodyEnd = source.Length;
            }

            var body = source.Substring(signatureIndex, bodyEnd - signatureIndex);

            var catchMatch = Regex.Match(body, @"catch\s*(\(Exception\s+\w+\)\s*(when\s*\([^)]*\))?)?\s*\{");
            Assert.True(catchMatch.Success, $"{method}: no catch block found in its body slice");

            var filter = catchMatch.Groups[2].Value;
            Assert.Contains("OperationCanceledException", filter, StringComparison.Ordinal);
        }
    }
}
