/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the xmin-horizon read: current holder per source joined to its persistence across the window,
/// and a distinct remedy for every source the collector can emit.
/// </summary>
public class DarlingPgXminReaderTests
{
    private static string Sql => DarlingPgXminReader.PgXminHorizonSql;

    /// <summary>Current state per source — a level, so latest rather than aggregated.</summary>
    [Fact]
    public void TakesTheLatestHolderPerSource()
    {
        Assert.Contains("DISTINCT ON (source)", Sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY source, collection_time DESC", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Persistence is what separates a chronic holder from a query that ran long, and that distinction
    /// changes the response — so the window figures are not optional decoration.
    /// </summary>
    [Fact]
    public void CarriesPersistenceFiguresAcrossTheWindow()
    {
        Assert.Contains("COUNT(*) FILTER (WHERE is_winner)", Sql, StringComparison.Ordinal);
        Assert.Contains("MAX(xmin_age)", Sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY source", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// An inner join is safe here only because both branches read the same window, so every source in one
    /// is in the other. Worth pinning: widening one branch's predicate later without the other would
    /// silently drop holders.
    /// </summary>
    [Fact]
    public void JoinsTheTwoBranchesOnSource()
    {
        Assert.Contains("JOIN window_stats AS w ON w.source = l.source", Sql, StringComparison.Ordinal);
        Assert.Equal(2, Sql.Split("server_id = $1").Length - 1);
        Assert.Equal(2, Sql.Split("collection_time >= $2").Length - 1);
    }

    /// <summary>Oldest holder first — that is the one setting the horizon.</summary>
    [Fact]
    public void OrdersByAgeDescending()
    {
        Assert.Contains("ORDER BY l.xmin_age DESC", Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadsTheXminHorizonTable()
    {
        Assert.Contains("FROM pg_xmin_horizon", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every source the collector can emit needs its own remedy — a generic message would defeat the
    /// point of attributing the cause in the first place.
    /// </summary>
    [Theory]
    [InlineData("session", "idle in transaction")]
    [InlineData("replication_slot", "dropped")]
    [InlineData("replication_slot_catalog", "catalog_xmin")]
    [InlineData("standby_feedback", "hot_standby_feedback")]
    [InlineData("prepared_transaction", "ROLLBACK PREPARED")]
    public void EverySourceHasItsOwnRemedy(string source, string expectedFragment)
    {
        var remedy = DarlingMcpPgXminTools.RemedyFor(source);

        Assert.Contains(expectedFragment, remedy, StringComparison.Ordinal);
        Assert.DoesNotContain("Unrecognized", remedy, StringComparison.Ordinal);
    }

    /// <summary>The five remedies are genuinely distinct, not one message with the noun swapped.</summary>
    [Fact]
    public void RemediesAreDistinctPerSource()
    {
        var remedies = new[]
        {
            DarlingMcpPgXminTools.RemedyFor("session"),
            DarlingMcpPgXminTools.RemedyFor("replication_slot"),
            DarlingMcpPgXminTools.RemedyFor("replication_slot_catalog"),
            DarlingMcpPgXminTools.RemedyFor("standby_feedback"),
            DarlingMcpPgXminTools.RemedyFor("prepared_transaction"),
        };

        Assert.Equal(remedies.Length, remedies.Distinct().Count());
    }

    [Fact]
    public void UnknownSourceIsReportedRatherThanGuessedAt()
    {
        Assert.Contains("Unrecognized", DarlingMcpPgXminTools.RemedyFor("something_new"), StringComparison.Ordinal);
    }
}
