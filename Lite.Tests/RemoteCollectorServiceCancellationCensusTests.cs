/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A code-reading census pin for the five DuckDB watermark/state reads in
/// <c>RemoteCollectorService.cs</c> that used to catch every exception unconditionally and fall back to
/// "no watermark / no state" — the same shape a cancelled request's caller cannot tell apart from an
/// ordinary DuckDB read failure. This class cannot RUN in-process on macOS (Lite.Tests discovery dies on
/// <c>WindowsBase</c>; CI decides the runtime behavior). It instead reads the shipped source and asserts
/// each of the five catch blocks names the <see cref="OperationCanceledException"/> exclusion, so a
/// regression that removes the filter fails this pin even where the real xUnit case cannot run locally.
/// </summary>
public sealed class RemoteCollectorServiceCancellationCensusTests
{
    private static string SourcePath() =>
        Path.Combine(FindRepoRoot(), "Lite", "Services", "RemoteCollectorService.cs");

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("Could not find repo root from " + AppContext.BaseDirectory);
    }

    /// <summary>
    /// The five reads (<c>GetLastCollectedTimeAsync</c>, <c>GetLastCollectedTimeWithFrameAsync</c>,
    /// <c>GetLastCollectedTimeForDatabaseAsync</c>, <c>GetLastCollectedInstanceIdAsync</c>,
    /// <c>HasPriorCollectorSuccessAsync</c>) each declare a method-signature <c>CancellationToken</c> and are
    /// called with the caller's own token — the only source of a cancelled token here is the caller's own
    /// cancellation, exactly the reasoning Darling's twin already states on
    /// <c>DarlingCollectorRunner.GetLastCollectedTimeAsync</c>. Every one of their catch blocks must exclude
    /// <see cref="OperationCanceledException"/>, or a cancelled shutdown reads as "DuckDB query failed, use
    /// the fallback window" instead of propagating.
    /// </summary>
    [Fact]
    public void EveryWatermarkOrStateReadCatch_ExcludesOperationCanceledException()
    {
        var source = File.ReadAllText(SourcePath());

        string[] methods =
        {
            "GetLastCollectedTimeAsync",
            "GetLastCollectedTimeWithFrameAsync",
            "GetLastCollectedTimeForDatabaseAsync",
            "GetLastCollectedInstanceIdAsync",
            "HasPriorCollectorSuccessAsync",
        };

        foreach (var method in methods)
        {
            var signatureIndex = source.IndexOf($" {method}(", StringComparison.Ordinal);
            Assert.True(signatureIndex >= 0, $"Could not find method {method} in RemoteCollectorService.cs");

            // The method body: from the signature to the next top-level "protected"/"internal" method
            // declaration at the same indentation, which is a reliable enough boundary for this file's style.
            var bodyEnd = source.IndexOf("\n    protected async Task", signatureIndex + method.Length, StringComparison.Ordinal);
            if (bodyEnd < 0)
            {
                bodyEnd = source.Length;
            }

            var body = source.Substring(signatureIndex, bodyEnd - signatureIndex);

            var catchMatch = Regex.Match(body, @"catch\s*(\(Exception\s+\w+\)\s*(when\s*\([^)]*\))?)?\s*\{");
            Assert.True(catchMatch.Success, $"{method}: no catch block found in its body slice");

            var filter = catchMatch.Groups[2].Value;
            Assert.Contains("OperationCanceledException", filter,
                StringComparison.Ordinal);
        }
    }
}
