/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The line the service logs when saved passwords have to be entered again (#5366), and what it says instead while the
/// old-format passwords are only waiting for the one-time pin step (#5456).
/// </summary>
public sealed class ReenterLineTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void NothingToEnter_SaysNothing(int count)
    {
        Assert.Null(StoreConfigProvider.ReenterLine(count, waiting: false));
        Assert.Null(StoreConfigProvider.ReenterLine(count, waiting: true));
    }

    [Fact]
    public void WhenNotWaiting_TheLineStaysTheEnterAgainText()
    {
        var one = StoreConfigProvider.ReenterLine(1, waiting: false);
        Assert.NotNull(one);
        Assert.StartsWith("1 saved password needs to be entered again.", one, StringComparison.Ordinal);

        var many = StoreConfigProvider.ReenterLine(3, waiting: false);
        Assert.NotNull(many);
        Assert.StartsWith("3 saved passwords need to be entered again.", many, StringComparison.Ordinal);
    }

    [Fact]
    public void WhileWaiting_SaysWait()
    {
        var one = StoreConfigProvider.ReenterLine(1, waiting: true);
        Assert.NotNull(one);
        Assert.Contains("for the one-time pin step. Do not enter it again yet.", one, StringComparison.Ordinal);
        Assert.Contains("Do not enter", one, StringComparison.Ordinal);
        Assert.DoesNotContain("needs to be entered again", one, StringComparison.Ordinal);

        var many = StoreConfigProvider.ReenterLine(3, waiting: true);
        Assert.NotNull(many);
        Assert.StartsWith("3 saved old-format passwords wait for the one-time pin step.", many, StringComparison.Ordinal);
        Assert.Contains("Do not enter them again yet.", many, StringComparison.Ordinal);
        Assert.NotEqual(StoreConfigProvider.ReenterLine(3, waiting: false), many);
    }
}
