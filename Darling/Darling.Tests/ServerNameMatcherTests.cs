/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store-row name match the service's server resolver and the Viewer share (#6390 S2b): a name matches a row exactly
/// (storage or display name) before it matches any row partially, and a name that several rows answer to comes back as the
/// candidate list, whose sentence a caller can pass one entry of back.
/// </summary>
public sealed class ServerNameMatcherTests
{
    private static readonly RegisteredServer[] Rows =
    {
        new(1, "sql-prod", "Production"),
        new(2, "sql-prod-replica", "Production replica"),
        new(3, "sql-test", null),
        new(4, "sql-test-2", "sql-test-2"),
    };

    [Fact]
    public void AnExactNameBeatsAPartialMatch()
    {
        // "sql-prod" is a partial match for the replica too, but it is the first row's own storage name.
        var match = ServerNameMatcher.Match(Rows, "sql-prod");

        Assert.Equal("exact", match.MatchedBy);
        Assert.Equal(new[] { 1 }, match.Candidates.Select(c => c.ServerId));
    }

    [Fact]
    public void ADisplayNameIsAnExactMatchAndNeedsNoCase()
    {
        var match = ServerNameMatcher.Match(Rows, "  production REPLICA ");

        Assert.Equal("exact", match.MatchedBy);
        Assert.Equal(new[] { 2 }, match.Candidates.Select(c => c.ServerId));
    }

    [Fact]
    public void TwoPartialMatchesAreTheAmbiguityList()
    {
        var match = ServerNameMatcher.Match(Rows, "test");

        Assert.Equal("partial", match.MatchedBy);
        Assert.Equal(new[] { 3, 4 }, match.Candidates.Select(c => c.ServerId));
        Assert.Equal(
            "'test' matches 2 monitored servers (as part of their names). Pass one server's full name from this list:\n" +
            "sql-test\n" +
            "sql-test-2",
            ServerNameMatcher.AmbiguitySentence("test", match));
    }

    [Fact]
    public void TheListShowsADisplayNameBesideTheStorageNameWhenTheyDiffer()
    {
        var match = ServerNameMatcher.Match(Rows, "replica");

        Assert.Equal("partial", match.MatchedBy);
        Assert.Equal("sql-prod-replica (Production replica)", ServerNameMatcher.ListCandidates(match.Candidates));
    }

    [Fact]
    public void ANameNoRowCarriesMatchesNothing()
    {
        var match = ServerNameMatcher.Match(Rows, "no-such-server");

        Assert.Equal("none", match.MatchedBy);
        Assert.Empty(match.Candidates);
        Assert.Equal("none", ServerNameMatcher.Match(Rows, "   ").MatchedBy);
    }
}
