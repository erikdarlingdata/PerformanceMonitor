/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2651: the statement-text store read Aurora's function and had no vanilla path, so off Aurora it was
/// never populated at all.
///
/// <para>
/// Two things failed silently on every self-hosted target. <c>get_pg_top_queries</c> returned
/// <c>query_text: null</c> on every row forever — while that field's own documentation says null means
/// "not captured YET", which is true on Aurora and a lie here. And <c>test_hypothetical_index</c> (#2612)
/// could not resolve a statement at all, so it answered "no statement text is stored" and blamed a refresh
/// cadence for a missing source — on the one platform it can be tested against.
/// </para>
///
/// <para>
/// Found by running that command end to end against the verification rig rather than by reading the code:
/// the experiment class had been tested directly and passed, and the path from a queryid to a statement
/// had not.
/// </para>
/// </summary>
public sealed class PgStatementTextSourceTests
{
    [Fact]
    public void AuroraStillReadsItsOwnFunction()
    {
        var sql = PgStatementText.FetchSqlFor(isAurora: true, postgresMajorVersion: 17);

        Assert.Contains("aurora_stat_statements(true)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_stat_statements", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryOtherPostgresReadsTheVanillaView()
    {
        var sql = PgStatementText.FetchSqlFor(isAurora: false, postgresMajorVersion: 17);

        Assert.Contains("public.pg_stat_statements", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("aurora_", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The second defect, and it only appeared once the first was fixed.
    ///
    /// <para><c>pg_stat_statements</c> keys on <c>(queryid, userid, dbid, toplevel)</c>, so ONE queryid
    /// comes back once per user and database that ran it. The upsert keys on <c>(server_id, queryid)</c>
    /// and meets those duplicates as <c>21000: ON CONFLICT DO UPDATE command cannot affect row a second
    /// time</c>, abandoning the whole batch. Measured: the first cut shipped without the dedupe and the
    /// refresh failed every cycle, storing nothing — quietly, because the caller deliberately treats a
    /// text-refresh error as non-fatal.</para>
    /// </summary>
    [Fact]
    public void TheVanillaFetchDeduplicatesByQueryId_OrTheUpsertAbandonsTheBatch()
    {
        var sql = PgStatementText.FetchSqlFor(isAurora: false, postgresMajorVersion: 17);

        Assert.Contains("DISTINCT ON (s.queryid)", sql, StringComparison.Ordinal);

        /* DISTINCT ON requires its expression to lead the ORDER BY; without that PostgreSQL rejects the
           statement outright, so this pins the pairing rather than the keyword alone. */
        Assert.Contains("ORDER BY s.queryid, s.total_exec_time DESC", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The cap still selects the statements anyone would look at. Deduplicating inside and ranking outside
    /// is what keeps that true — ranking before the dedupe would spend the cap on duplicates of the same
    /// costly statement.
    /// </summary>
    [Fact]
    public void TheCostliestStatementsStillWinTheCap()
    {
        var sql = PgStatementText.FetchSqlFor(isAurora: false, postgresMajorVersion: 17);

        var dedupe = sql.IndexOf("DISTINCT ON (s.queryid)", StringComparison.Ordinal);
        var outerOrder = sql.IndexOf("ORDER BY d.total_exec_time DESC", StringComparison.Ordinal);
        var limit = sql.IndexOf("LIMIT $1", StringComparison.Ordinal);

        Assert.True(dedupe >= 0 && outerOrder > dedupe && limit > outerOrder,
            "The cap must be applied AFTER the dedupe, or it is spent on duplicates of one statement.");
    }

    /// <summary>
    /// <c>toplevel</c> arrived in <c>pg_stat_statements</c> 1.9 (PostgreSQL 14). Before that nested
    /// tracking did not exist, so every row IS top level and <c>true</c> is the correct predicate rather
    /// than a fallback — the same guard the statement-stats collector already applies. Without it the
    /// query fails on a 13 target instead of degrading.
    /// </summary>
    [Theory]
    [InlineData(13, "true")]
    [InlineData(14, "s.toplevel")]
    [InlineData(17, "s.toplevel")]
    public void ToplevelIsGuardedForPostgresBefore14(int major, string expected)
    {
        var sql = PgStatementText.FetchSqlFor(isAurora: false, postgresMajorVersion: major);

        Assert.Matches(new Regex($@"AND\s+{Regex.Escape(expected)}\b"), sql);
        Assert.DoesNotContain("{TOPLEVEL}", sql, StringComparison.Ordinal);
    }
}
