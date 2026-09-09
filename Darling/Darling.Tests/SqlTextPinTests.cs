/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="SqlTextPin"/> is the instrument several files' pins now run through, so it is pinned in BOTH
/// directions. A normaliser that had quietly stopped normalising would turn every caller back into the
/// brittle form it replaces; one that normalised too much would make a removed predicate pass, which is
/// strictly worse than the brittleness. Neither shows up as a failure anywhere else.
/// </summary>
public sealed class SqlTextPinTests
{
    /// <summary>The exact #3212 rewrite: a table alias added, then the column qualified with it.</summary>
    [Fact]
    public void ItAcceptsTheQualificationThatBrokeSixPins()
    {
        SqlTextPin.AssertExpresses(
            "($2::text IS NULL OR database_name = $2::text)",
            """
            SELECT database_name
            FROM v_index_object_stats AS ios
            WHERE ios.server_id = $1
            AND   ($2::text IS NULL OR ios.database_name = $2::text)
            """,
            "the reporter cannot ask about one database");

        /* And the reverse, since the pin is now insensitive rather than merely tolerant: a needle written
           qualified must match an unqualified statement too, or the next contributor who writes the pin
           the other way round gets the same failure from the other side.

           What is NOT interchangeable: an `AS alias` DECLARATION is text like any other, so a needle
           carrying one only matches a statement that declares it. Alias-insensitivity covers
           REFERENCES. */
        SqlTextPin.AssertExpresses(
            "AND ios.collection_time = (SELECT MAX(ios.collection_time) FROM v_index_object_stats)",
            "AND   collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats)",
            "the read is no longer pinned to one capture");
    }

    /// <summary>Whitespace is the other neutral rewrite in the same family — a reflowed predicate, or the
    /// column-per-line style this codebase's readers use.</summary>
    [Fact]
    public void ItAcceptsAReflow()
    {
        SqlTextPin.AssertExpresses(
            "GROUP BY database_name, schema_name, table_name",
            """
            GROUP BY
                database_name,
                schema_name,
                table_name
            """,
            "the rollup grain changed");
    }

    /// <summary>
    /// The direction that matters: a REMOVED clause must still red. Every case here is a real regression
    /// someone could ship, not a mangled string — an unscoped read, a filter dropped, an ordering key
    /// dropped, a cap hardcoded back.
    /// </summary>
    [Theory]
    [InlineData("WHERE server_id = $1", "SELECT database_name FROM v_pvs_stats AS p ORDER BY p.database_name")]
    [InlineData(
        "($2::text IS NULL OR database_name = $2::text)",
        "SELECT database_name FROM v_index_object_stats AS ios WHERE ios.server_id = $1")]
    [InlineData(
        "ORDER BY collection_time DESC",
        "SELECT collection_time FROM plan_correction WHERE server_id = $1 ORDER BY database_name")]
    [InlineData("LIMIT $3", "SELECT 1 FROM v_index_object_stats AS ios LIMIT 200")]
    [InlineData("recommendation_name IS NOT NULL", "SELECT recommendation_name FROM plan_correction AS pc")]
    public void ItStillRedsOnARemovedClause(string clause, string sql)
    {
        Assert.Throws<Xunit.Sdk.TrueException>(
            () => SqlTextPin.AssertExpresses(clause, sql, "the behaviour is gone"));
    }

    /// <summary>
    /// The two things it must NOT normalise away, because both change which rows come back.
    ///
    /// <para>A schema qualifier selects between relations — <c>collect.servers</c> and a search_path
    /// <c>servers</c> are not interchangeable, and the migrate session's search_path puts <c>collect</c>
    /// first, which is exactly how a bare name lands somewhere unintended. Case belongs to values: a
    /// classification literal the tool emits is compared by callers.</para>
    /// </summary>
    [Fact]
    public void ItKeepsSchemaQualifiersAndCase()
    {
        Assert.Equal("FROM collect.server_properties", SqlTextPin.Normalise("FROM collect.server_properties"));
        Assert.Equal("FROM config.custom_views", SqlTextPin.Normalise("FROM config.custom_views"));

        Assert.Throws<Xunit.Sdk.TrueException>(
            () => SqlTextPin.AssertExpresses("FROM collect.servers", "FROM servers", "the schema was dropped"));

        Assert.Throws<Xunit.Sdk.TrueException>(
            () => SqlTextPin.AssertExpresses("THEN 'Unused'", "THEN 'unused'", "the classification changed"));
    }

    /// <summary>
    /// T-SQL's schema qualifiers survive too, so the same pin shape can be taken to the collectors' queries
    /// without silently loosening them. <c>sys.</c> and <c>dbo.</c> select relations exactly as
    /// <c>collect.</c> does; stripped, a needle on <c>sys.dm_xe_database_sessions</c> would be satisfied by
    /// a query that dropped the schema, and nothing would go red to say so.
    /// </summary>
    [Fact]
    public void ItKeepsTsqlSchemaQualifiers_SoTheSamePinShapeTravels()
    {
        Assert.Equal(
            "JOIN sys.dm_xe_database_sessions AS xes",
            SqlTextPin.Normalise("JOIN sys.dm_xe_database_sessions AS xes"));
        Assert.Equal("EXEC dbo.usp_Thing", SqlTextPin.Normalise("EXEC dbo.usp_Thing"));

        Assert.Throws<Xunit.Sdk.TrueException>(
            () => SqlTextPin.AssertExpresses(
                "FROM sys.dm_exec_requests",
                "FROM dm_exec_requests",
                "the schema was dropped"));

        /* And an alias on the same statement still normalises, so the pin stays insensitive to the thing it
           is meant to be insensitive to. */
        Assert.Equal("WHERE rn = 1", SqlTextPin.Normalise("WHERE rs.rn = 1"));
    }

    /// <summary>
    /// The normaliser reads identifiers, not every dot. A numeric literal, a chained qualifier and a
    /// <c>::</c> cast all sit next to the pattern it matches, and mangling any of them would make a pin
    /// pass by comparing two equally-mangled strings — the failure mode a one-directional test cannot see.
    /// </summary>
    [Theory]
    [InlineData("p.pvs_mb / p.data_mb * 100.0", "pvs_mb / data_mb * 100.0")]
    [InlineData("$2::text IS NULL", "$2::text IS NULL")]
    [InlineData("CAST(reserved_mb AS double precision)", "CAST(reserved_mb AS double precision)")]
    [InlineData("GREATEST(last_user_seek, last_user_scan)", "GREATEST(last_user_seek, last_user_scan)")]
    [InlineData("count(*)", "count(*)")]
    [InlineData("collect.wait_stats AS f", "collect.wait_stats AS f")]
    public void ItLeavesEverythingThatIsNotAnAliasQualifier(string input, string expected)
    {
        Assert.Equal(expected, SqlTextPin.Normalise(input));
    }
}
