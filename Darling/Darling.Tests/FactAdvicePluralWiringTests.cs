/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the "querys" fix (#4478) through the PUBLIC <see cref="FactAdvice.Compose"/> entry point — the path a
/// real reader's advice text goes through — rather than only the private <c>Plural</c> helper. The previous
/// RED for this issue was a COMPILE failure (<c>Plural</c> was private), which the review does not accept: a
/// text-shape bug must be caught by rendering the actual text a fact produces, not only by calling the
/// formatter directly. <see cref="FactAdvicePluralTests"/> keeps the direct unit coverage of the formatter's
/// irregular-noun rule; this file is the wiring proof.
/// </summary>
public sealed class FactAdvicePluralWiringTests
{
    private static Fact HighDopFact(double count) => new()
    {
        Key = "QUERY_HIGH_DOP",
        Severity = 1,
        Metadata = new Dictionary<string, double> { ["high_dop_query_count"] = count },
    };

    [Fact]
    public void HighDopAdvice_ThreeQueries_SaysQueriesNotQuerys()
    {
        var facts = new Dictionary<string, Fact> { ["QUERY_HIGH_DOP"] = HighDopFact(3) };

        var advice = FactAdvice.Compose("QUERY_HIGH_DOP", facts);

        Assert.NotNull(advice);
        Assert.Contains("3 queries ran at a degree of parallelism above 8", advice!.Investigation);
        Assert.DoesNotContain("querys", advice.Investigation, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void HighDopAdvice_OneQuery_StaysSingular()
    {
        var facts = new Dictionary<string, Fact> { ["QUERY_HIGH_DOP"] = HighDopFact(1) };

        var advice = FactAdvice.Compose("QUERY_HIGH_DOP", facts);

        Assert.NotNull(advice);
        Assert.Contains("1 query ran at a degree of parallelism above 8", advice!.Investigation);
    }
}
