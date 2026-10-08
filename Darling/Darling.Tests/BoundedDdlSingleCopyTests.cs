/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The bounded compression DDL has one copy: <c>TimescaleSupport.TryRunBoundedDdlAsync</c> is the only place that
/// sets the hourly lock timeout, and every compression-enable ALTER goes through it (#4970).
/// </summary>
public sealed class BoundedDdlSingleCopyTests
{
    private static string Storage() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.cs").Replace("\r\n", "\n");

    private static string[] CodeLines() =>
        CSharpSourceWalker.StripCommentsAndStrings(Storage()).Split('\n');

    [Fact]
    public void TheHourlyLockTimeout_IsSetInOneMethodOnly()
    {
        var text = Storage();
        const string setLocal = "SET LOCAL lock_timeout = '{HourlyDdlLockTimeout}'";
        var first = text.IndexOf(setLocal, StringComparison.Ordinal);
        Assert.True(first >= 0, "could not find the helper's SET LOCAL lock_timeout");
        Assert.Equal(-1, text.IndexOf(setLocal, first + 1, StringComparison.Ordinal));
        var helper = text.IndexOf("internal static async Task<BoundedDdlOutcome> TryRunBoundedDdlAsync(", StringComparison.Ordinal);
        Assert.True(helper >= 0 && helper < first && first - helper < 1200,
            "the one SET LOCAL lock_timeout must sit inside TryRunBoundedDdlAsync");
        Assert.Equal(1, text.Split("BoundedDdlOutcome> TryRun", StringSplitOptions.None).Length - 1);
    }

    [Fact]
    public void EveryCompressionEnableAlter_IsAStatementPassedToTheHelper()
    {
        var lines = CodeLines();
        var uses = lines.Select((l, i) => (l, i))
            .Where(x => (x.l.Contains("EnableCompressionSql(", StringComparison.Ordinal) || x.l.Contains("EnableAggregateCompressionSql(", StringComparison.Ordinal))
                && !x.l.Contains("public static string", StringComparison.Ordinal)
                && !x.l.Contains("return EnableCompressionSql(", StringComparison.Ordinal))
            .ToArray();
        Assert.Equal(3, uses.Length);
        foreach (var (line, index) in uses)
        {
            Assert.Contains("new[] {", line, StringComparison.Ordinal);
            Assert.Contains("TryRunBoundedDdlAsync(", lines[index - 1], StringComparison.Ordinal);
        }

        Assert.DoesNotContain("new NpgsqlCommand(EnableCompressionSql(", Storage(), StringComparison.Ordinal);
        Assert.DoesNotContain("new NpgsqlCommand(EnableAggregateCompressionSql(", Storage(), StringComparison.Ordinal);
    }

    [Fact]
    public void ThePrivateCollectionLogCopy_IsGone()
    {
        var text = Storage();
        Assert.DoesNotContain("TrySetCollectionLogCompressionAsync", text, StringComparison.Ordinal);
        Assert.DoesNotContain("CollectionLogSettingsLockTimeout", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailedSettingsRead_SkipsThePolicyCall_OnTheHourlyPass()
    {
        var text = Storage();
        var start = text.IndexOf("public static async Task<int> ApplyCompressionPolicyAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var skip = text.IndexOf("if (skipEnable)\n                {\n                    continue;", start, StringComparison.Ordinal);
        var policy = text.IndexOf("new NpgsqlCommand(AddCompressionPolicySql(schema)", start, StringComparison.Ordinal);
        Assert.True(skip > start && policy > skip,
            "the per-table policy call must be skipped when the settings read failed on the hourly pass");
    }
}
