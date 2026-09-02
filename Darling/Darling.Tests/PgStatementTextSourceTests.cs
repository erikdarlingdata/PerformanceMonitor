/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
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
    /// <para>Pinned over EVERY arm rather than the vanilla one, which is the whole lesson of the recurrence:
    /// the original pin named the arm it was written for, so the arm that already existed stayed broken and
    /// the suite stayed green. <c>FetchSqlFor</c> switches on a bool, so iterating both values is exhaustive
    /// over the sources that can reach the upsert — a derived guard, not an enumerated one.</para>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void EveryFetchDeduplicatesByQueryId_OrTheUpsertAbandonsTheBatch(bool isAurora)
    {
        var sql = PgStatementText.FetchSqlFor(isAurora, postgresMajorVersion: 17);

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
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheCostliestStatementsStillWinTheCap(bool isAurora)
    {
        var sql = PgStatementText.FetchSqlFor(isAurora, postgresMajorVersion: 17);

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

/// <summary>
/// Gated (DARLING_TEST_PG) proof of the fact the dedupe exists for, which no string pin can make: what
/// PostgreSQL actually DOES to a batch carrying one queryid twice.
///
/// <para>The severity lives here rather than in the error code. <c>21000</c> aborts the STATEMENT, so the
/// batch's other rows — the statements that were NOT duplicated — are lost along with it. A reader of the
/// upsert would reasonably expect the conflicting row to be skipped or overwritten; it stores nothing at
/// all. Measured on the pgmon fleet: 50 of 50 Aurora servers failing on every attempt, zero succeeding,
/// and <c>get_pg_top_queries</c> returning <c>query_text: null</c> on every row of every server.</para>
///
/// <para>It compounds, too, and the cadence guard is what compounds it. <see cref="PgStatementText.IsDueSql"/>
/// asks the store when text last landed and COALESCEs a missing answer to TRUE — correct for a first fetch,
/// and indistinguishable from a server whose every write has failed. So a broken server is due on every
/// sweep instead of hourly, and each of those attempts is the <c>showtext = true</c> call this table's whole
/// design exists to ration.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgStatementTextUpsertLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>A server_id of this run's own, so a shared rig and a repeat run cannot collide.</summary>
    private static readonly int ServerId = Random.Shared.Next(2_000_000, int.MaxValue);

    [Fact]
    public async Task ADuplicateQueryIdAbandonsTheWholeBatch_WhichIsWhyBothFetchesDeduplicate()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live statement-text upsert test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        await using (var ddl = new NpgsqlCommand("CREATE SCHEMA IF NOT EXISTS collect;", connection))
        {
            await ddl.ExecuteNonQueryAsync(ct);
        }

        await using (var ddl = new NpgsqlCommand(PgStatementText.CreateTableSql, connection))
        {
            await ddl.ExecuteNonQueryAsync(ct);
        }

        try
        {
            /* One queryid twice — exactly the shape aurora_stat_statements and pg_stat_statements hand back
               when a statement was run in two databases — plus one that is NOT duplicated. */
            var duplicated = new long[] { 42, 42, 99 };

            var abandoned = await Assert.ThrowsAsync<PostgresException>(
                () => UpsertAsync(connection, duplicated, ct));
            Assert.Equal("21000", abandoned.SqlState);

            /* The half that makes this data loss rather than a rejected row: queryid 99 was unique in the
               batch and is gone too. */
            Assert.Equal(0, await CountAsync(connection, ct));

            /* Deduplicated the way both fetch arms now do it, the same batch stores every distinct row. */
            await UpsertAsync(connection, duplicated.Distinct().ToArray(), ct);
            Assert.Equal(2, await CountAsync(connection, ct));
        }
        finally
        {
            await using var cleanup = new NpgsqlCommand(
                "DELETE FROM collect.pg_statement_text WHERE server_id = $1", connection);
            cleanup.Parameters.AddWithValue(ServerId);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task UpsertAsync(NpgsqlConnection connection, long[] queryIds, System.Threading.CancellationToken ct)
    {
        var stamp = PgStatementText.Naive(DateTime.UtcNow);
        await using var upsert = new NpgsqlCommand(PgStatementText.UpsertSql, connection);
        upsert.Parameters.AddWithValue(Enumerable.Repeat(ServerId, queryIds.Length).ToArray());
        upsert.Parameters.AddWithValue(queryIds);
        upsert.Parameters.AddWithValue(queryIds.Select(id => "select " + id).ToArray());
        upsert.Parameters.AddWithValue(Enumerable.Repeat(stamp, queryIds.Length).ToArray());
        await upsert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var count = new NpgsqlCommand(
            "SELECT count(*) FROM collect.pg_statement_text WHERE server_id = $1", connection);
        count.Parameters.AddWithValue(ServerId);
        return (long)(await count.ExecuteScalarAsync(ct))!;
    }
}
