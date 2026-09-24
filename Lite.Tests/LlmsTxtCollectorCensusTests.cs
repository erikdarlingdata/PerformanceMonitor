/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// <c>llms.txt</c>'s collector counts against the catalog that defines the collectors. This is the guard
/// <see cref="CrossAppMcpToolInventoryPinTests.LlmsTxtToolCensus_SpansTheTwoCurrentEditions"/> puts on the
/// same file's tool range, and the choice #3072 made for <c>Darling/README.md</c>'s counts
/// (<c>Darling.Tests.ReadmeDerivedCountPinTests</c>): a number in prose is asserted, or it is deleted.
///
/// <para>Nothing held this one. It read "41 T-SQL collectors" against a catalog of 42 SQL Server collectors,
/// and it did not mention the PostgreSQL collectors at all.</para>
///
/// <para>Every match is checked, not only the first, so a second mention of a count cannot go stale behind
/// the first one.</para>
/// </summary>
public sealed class LlmsTxtCollectorCensusTests
{
    [Theory]
    [InlineData(CollectorTargetEngine.SqlServer, "SQL Server")]
    [InlineData(CollectorTargetEngine.PostgreSql, "PostgreSQL")]
    public void LlmsTxtCollectorCount_MatchesTheCatalog(CollectorTargetEngine engine, string label)
    {
        var llms = ParitySource.ReadFile("llms.txt");
        var pattern = new Regex(@"(\d+) " + Regex.Escape(label) + " collectors");
        var matches = pattern.Matches(llms);

        Assert.True(matches.Count > 0,
            $"llms.txt no longer states the {label} collector count in the pinned shape ({pattern}). Keep it "
            + "parseable so this pin can hold it to the catalog, or delete the number and this pin with it (#3072).");

        var expected = CollectorCatalog.All.Count(c => c.TargetEngine == engine);

        foreach (Match match in matches)
        {
            var actual = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);

            Assert.True(actual == expected,
                $"llms.txt reads {actual} {label} collectors, but the collector catalog defines {expected}. "
                + "Update the sentence.");
        }
    }
}
