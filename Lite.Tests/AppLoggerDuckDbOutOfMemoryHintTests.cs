/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5457: about 85 catch blocks only log the raw exception message, and the reporter found the bug through exactly
/// those lines ("[JobHistory] Failed to load job history: Out of Memory Error: failed to pin block ..."). So
/// <see cref="AppLogger"/> itself appends <see cref="DuckDbMemoryLimitSetting.OutOfMemoryHint"/> once to a WARN or
/// ERROR line that holds DuckDB's exact "Out of Memory Error:" text, and only to that.
/// </summary>
[Collection("app-logger-statics")]
public sealed class AppLoggerDuckDbOutOfMemoryHintTests : IDisposable
{
    private const string DuckDbMessage =
        "Out of Memory Error: failed to pin block of size 256.0 KiB (1.8 GiB/1.8 GiB used)";

    private static readonly string s_hintStart = DuckDbMemoryLimitSetting.HintMarker;

    public AppLoggerDuckDbOutOfMemoryHintTests()
    {
        AppLogger.SetMinimumLevel(LogLevel.Information);
        AppLogger.DrainBufferedLines();
    }

    public void Dispose() => AppLogger.SetMinimumLevel(AppLogger.DefaultMinimumLevel);

    private static int HintCount(string text)
    {
        var n = 0;
        for (var i = text.IndexOf(s_hintStart, StringComparison.Ordinal); i >= 0;
            i = text.IndexOf(s_hintStart, i + 1, StringComparison.Ordinal))
        {
            n++;
        }

        return n;
    }

    [Fact]
    public void Error_with_a_DuckDB_out_of_memory_exception_gets_the_hint_once()
    {
        var ex = new InvalidOperationException("outer", new InvalidOperationException(DuckDbMessage));

        AppLogger.Error("JobHistory", "Failed to load job history", ex);

        var all = string.Join("\n", AppLogger.DrainBufferedLines());
        Assert.Equal(1, HintCount(all));
        Assert.Contains("Raise \"DuckDB memory limit\"", all);
    }

    [Fact]
    public void Error_with_an_aggregate_holding_the_error_gets_the_hint_once()
    {
        var ex = new AggregateException(new InvalidOperationException("x"), new InvalidOperationException(DuckDbMessage));

        AppLogger.Error("Collector", "Failed", ex);

        Assert.Equal(1, HintCount(string.Join("\n", AppLogger.DrainBufferedLines())));
    }

    [Fact]
    public void Error_without_an_exception_whose_text_carries_the_prefix_gets_the_hint()
    {
        AppLogger.Error("JobHistory", $"Failed to load job history: {DuckDbMessage}");

        Assert.Equal(1, HintCount(string.Join("\n", AppLogger.DrainBufferedLines())));
    }

    [Fact]
    public void Warn_whose_text_carries_the_prefix_gets_the_hint()
    {
        AppLogger.Warn("JobHistory", $"Failed to load job history: {DuckDbMessage}");

        var lines = AppLogger.DrainBufferedLines();
        Assert.Single(lines);
        Assert.EndsWith("takes effect after Lite restarts.", lines[0]);
        Assert.Equal(1, HintCount(lines[0]));
    }

    [Theory]
    [InlineData("There is insufficient system memory in resource pool 'default' to run this query.")]
    [InlineData("out of memory")]
    [InlineData("Out of Memory")]
    [InlineData("could not free up enough memory")]
    public void SQL_Server_style_memory_messages_get_no_hint(string text)
    {
        AppLogger.Warn("Collector", text);
        AppLogger.Error("Collector", text);
        AppLogger.Error("Collector", "Failed", new InvalidOperationException(text));

        Assert.Equal(0, HintCount(string.Join("\n", AppLogger.DrainBufferedLines())));
    }

    [Fact]
    public void A_line_that_already_holds_the_hint_is_not_hinted_twice()
    {
        var described = DuckDbMemoryLimitSetting.Describe(new InvalidOperationException(DuckDbMessage));

        AppLogger.Warn("JobHistory", $"Failed to load job history: {described}");
        AppLogger.Error("JobHistory", $"Failed to load job history: {described}");
        AppLogger.Error("JobHistory", "Failed", new InvalidOperationException(described));

        var lines = AppLogger.DrainBufferedLines();
        /* Describe's own Warn line carries the hint once; each of the three logged lines carries it once more. */
        Assert.All(lines, l => Assert.True(HintCount(l) <= 1, l));
        Assert.Equal(4, lines.Count(l => HintCount(l) == 1));
    }

    [Fact]
    public void The_hint_line_itself_does_not_recurse()
    {
        AppLogger.Warn("DuckDB", DuckDbMemoryLimitSetting.OutOfMemoryHint(2));

        var lines = AppLogger.DrainBufferedLines();
        Assert.Single(lines);
        Assert.Equal(1, HintCount(lines[0]));
    }

    [Fact]
    public void Info_and_Debug_lines_are_left_alone()
    {
        AppLogger.SetMinimumLevel(LogLevel.Debug);
        AppLogger.Info("Collector", DuckDbMessage);
        AppLogger.Debug("Collector", DuckDbMessage);

        Assert.Equal(0, HintCount(string.Join("\n", AppLogger.DrainBufferedLines())));
    }
}
