/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="FactAdvice.Plural"/> (#4478): Recommendations advice text used to pluralize "query" as
/// "querys" ("3 querys regressed", "60 querys ran") because the rule was a bare trailing "s". A noun ending in a
/// consonant immediately before a trailing "y" ("query") pluralizes to "ies"; every other noun (including one
/// ending in a VOWEL + "y", like "day") keeps the plain "s" rule.
/// </summary>
public sealed class FactAdvicePluralTests
{
    [Fact]
    public void Query_SingularStaysQuery()
    {
        Assert.Equal("1 query", FactAdvice.Plural(1, "query"));
    }

    [Fact]
    public void Query_PluralIsQueries_NotQuerys()
    {
        Assert.Equal("3 queries", FactAdvice.Plural(3, "query"));
        Assert.Equal("60 queries", FactAdvice.Plural(60, "query"));
    }

    [Fact]
    public void RegularNoun_StillTakesPlainS()
    {
        /* "day" ends in a VOWEL + "y" (unlike "query"'s consonant + "y"), so it must keep the plain "s" rule —
           the fix must not over-generalize the "y" \u2192 "ies" swap onto every noun ending in "y". */
        Assert.Equal("1 day", FactAdvice.Plural(1, "day"));
        Assert.Equal("2 days", FactAdvice.Plural(2, "day"));
    }

    [Fact]
    public void RegularNoun_WithNoTrailingY_TakesPlainS()
    {
        Assert.Equal("1 deadlock", FactAdvice.Plural(1, "deadlock"));
        Assert.Equal("47 deadlocks", FactAdvice.Plural(47, "deadlock"));
    }
}
