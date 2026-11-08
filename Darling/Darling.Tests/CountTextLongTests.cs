/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The 64-bit forms of <see cref="CountText.Plural(int, string)"/> and <see cref="CountText.Plural(int, string, string)"/>, for
/// the counts the store returns as bigint, say the same words as the 32-bit forms: "1 event", "3 events", "12,345 events",
/// "1 index", "3 indexes".
/// </summary>
public sealed class CountTextLongTests
{
    [Theory]
    [InlineData(0L, "0 events")]
    [InlineData(1L, "1 event")]
    [InlineData(3L, "3 events")]
    [InlineData(12_345L, "12,345 events")]
    [InlineData(5_000_000_000L, "5,000,000,000 events")]
    public void LongCount_AddsAnSUnlessTheCountIsOne(long count, string expected)
    {
        Assert.Equal(expected, CountText.Plural(count, "event"));
    }

    [Theory]
    [InlineData(1L, "1 index")]
    [InlineData(3L, "3 indexes")]
    [InlineData(1_000L, "1,000 indexes")]
    public void LongCount_UsesTheGivenPluralForAnIrregularNoun(long count, string expected)
    {
        Assert.Equal(expected, CountText.Plural(count, "index", "indexes"));
    }

    [Fact]
    public void LongAndIntForms_SayTheSameWords()
    {
        Assert.Equal(CountText.Plural(7, "run"), CountText.Plural(7L, "run"));
        Assert.Equal(CountText.Plural(1, "query", "queries"), CountText.Plural(1L, "query", "queries"));
    }
}
