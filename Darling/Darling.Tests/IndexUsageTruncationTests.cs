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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2636, reported from the field: <c>get_index_usage</c> returned 200 rows, all "Unused", from a handful of
/// databases — and zero rows for the database the reporter actually asked about, whose collection was
/// healthy with full retention and no errors.
///
/// <para>
/// Nothing was broken in collection. Two decisions combined into a third nobody made: the ordering puts
/// unused indexes ahead of everything SERVER-WIDE, and the cap was a hardcoded 200 the caller could not
/// change or scope. On an instance with 200+ unused indexes in one legacy database, that database consumes
/// the entire answer and every Active index everywhere else is invisible.
/// </para>
///
/// <para>
/// The reporter's own words are the reason this is a bug and not a limit: an operator "can easily conclude
/// collection is broken, stale, or scoped to exclude that database — when in fact the data exists but was
/// truncated out". A capped answer that cannot say it was capped is indistinguishable from an empty one.
/// </para>
/// </summary>
public sealed class IndexUsageTruncationTests
{
    private static string ReaderSql => DarlingObjectStatsReaderSource.IndexUsageSql;

    /// <summary>
    /// The STRUCTURE that carries the filter: a NULL-tolerant database predicate and a bound cap, both
    /// present in the statement. What the filter DOES — that null answers about every database and a name
    /// answers about exactly that one — is asserted by running the query, in
    /// <see cref="IndexUsageTruncationLivePostgresTests.NullDatabaseNameAnswersAboutEveryDatabase_ANameAnswersAboutOne"/>.
    ///
    /// <para>The split is #3217's point: this assertion is a substring search, so it can only ever claim
    /// that the clause is spelled here, and the doc comment says no more than that. Through
    /// <see cref="SqlTextPin"/> it is insensitive to alias qualification and reflow — the neutral rewrites
    /// that reddened it once — while a REMOVED predicate still reds. It runs everywhere, including where
    /// no PostgreSQL is available, which is why it stays rather than being replaced by the live test.</para>
    /// </summary>
    [Fact]
    public void TheQueryCarriesANullTolerantDatabasePredicate_AndABoundCap()
    {
        SqlTextPin.AssertExpresses(
            "($2::text IS NULL OR database_name = $2::text)",
            ReaderSql,
            "there is no way to ask about one database");
        SqlTextPin.AssertExpresses("LIMIT $3", ReaderSql, "the cap is no longer the caller's to set");
    }

    /// <summary>
    /// The count is a SEPARATE statement, and that is the load-bearing detail.
    ///
    /// <para><c>COUNT(*) OVER ()</c> inside the capped query would return the count of rows that survived the
    /// LIMIT — a number that always equals what was returned, reported as though it were the total. That is
    /// the exact mistake this whole issue is about, reimplemented one layer down.</para>
    /// </summary>
    [Fact]
    public void TheMatchCountIsTakenBeforeTheCap_NotOverTheReturnedRows()
    {
        var countSql = DarlingObjectStatsReaderSource.IndexUsageMatchCountSql;

        SqlTextPin.AssertExpresses("SELECT count(*)", countSql, "the count is not a count");
        SqlTextPin.AssertExpresses(
            "($2::text IS NULL OR database_name = $2::text)",
            countSql,
            "the count is over a different population than the rows");

        /* No cap on the count, or it would count the page again. */
        Assert.DoesNotMatch(new Regex(@"\bLIMIT\b", RegexOptions.IgnoreCase), countSql);
        Assert.DoesNotContain("OVER ()", countSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The count query has to apply the SAME filter as the rows, or the ratio it feeds is between two
    /// different questions and "truncated" becomes noise on every call.
    /// </summary>
    [Fact]
    public void TheCountAndTheRowsShareTheirFilter()
    {
        foreach (var clause in new[]
        {
            "WHERE server_id = $1",
            "collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)",
            "($2::text IS NULL OR database_name = $2::text)",
        })
        {
            Assert.Contains(clause, ReaderSql, StringComparison.Ordinal);
            Assert.Contains(clause, DarlingObjectStatsReaderSource.IndexUsageMatchCountSql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The unused-first ordering STAYS. It is right for the question the tool was built for — which indexes
    /// can I drop — and the defect was never the ordering on its own. Changing it would trade the reporter's
    /// problem for its mirror image: every drop candidate pushed off the end by active indexes.
    ///
    /// <para>This half asserts the ordering KEY is in the statement; that an unused index actually comes
    /// back ahead of a used one is asserted by running the query, in
    /// <see cref="IndexUsageTruncationLivePostgresTests.AnUnusedIndexComesBackAheadOfAUsedOne"/>. A key
    /// present in the ORDER BY and a row order are different claims, and only the second is the one the
    /// reporter would notice.</para>
    /// </summary>
    [Fact]
    public void TheUnusedFirstOrderingKeyIsStillThere_BecauseTheOrderingWasNotTheDefect()
    {
        SqlTextPin.AssertExpresses(
            "CASE WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0 THEN 0 ELSE 1 END",
            ReaderSql,
            "drop candidates are no longer ranked ahead of active indexes");
    }
}

/// <summary>Reaches the internal reader's public SQL constants from the test assembly.</summary>
internal static class DarlingObjectStatsReaderSource
{
    private static readonly Type s_reader =
        typeof(DarlingCommandExecutor).Assembly.GetType("PerformanceMonitor.Darling.Service.Mcp.DarlingObjectStatsReader")
        ?? throw new InvalidOperationException("DarlingObjectStatsReader was not found — this pin needs re-anchoring.");

    public static string IndexUsageSql => Field(nameof(IndexUsageSql));

    public static string IndexUsageMatchCountSql => Field(nameof(IndexUsageMatchCountSql));

    private static string Field(string name)
        => s_reader.GetField(name)?.GetValue(null) as string
           ?? throw new InvalidOperationException($"{name} was not found on DarlingObjectStatsReader.");
}

/// <summary>
/// The behavioural half of <see cref="IndexUsageTruncationTests"/>, executed against a real PostgreSQL
/// (#3217). Its two claims — that the database filter answers the question it exists for, and that a drop
/// candidate outranks an active index — are properties of the ROWS the statement returns, and #3217's
/// remedy for a pin whose doc comment promises a behaviour is to assert the behaviour where a store can be
/// asked about it. No rewrite that preserves meaning can red these, and no rewrite that removes the filter
/// or the ordering can pass them.
///
/// <para>Both plant into two databases in the SAME capture, deliberately: a fixture with one database per
/// snapshot cannot tell "filtered to the asked-about database" from "only one database was collected", and
/// a fixture with one index per table cannot tell "unused sorts first" from "there was nothing to sort".</para>
/// </summary>
[Collection("live-postgres")]
public sealed class IndexUsageTruncationLivePostgresTests
{
    private const string ServerName = "darling-index-usage-truncation-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Asked = "AskedAbout";
    private const string Louder = "LouderNeighbour";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The claim its text pin cannot make: null <c>database_name</c> answers about every database, a name
    /// answers about exactly that one, and the match count follows the rows rather than the whole server.
    /// </summary>
    [Fact]
    public async Task NullDatabaseNameAnswersAboutEveryDatabase_ANameAnswersAboutOne()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live index-usage filter test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PlantTwoDatabasesAsync(connection, ct);

            var everyDatabase = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, ServerId, top: 100, databaseName: null);
            Assert.Equal(
                new[] { Asked, Louder },
                everyDatabase.Select(r => r.DatabaseName).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());

            var oneDatabase = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, ServerId, top: 100, databaseName: Asked);
            Assert.Equal(new[] { Asked }, oneDatabase.Select(r => r.DatabaseName).Distinct().ToArray());
            Assert.NotEmpty(oneDatabase);

            /* The row sets differ, which is the whole claim — a filter that silently did nothing would
               return the same set for both calls and satisfy every assertion above except this one. */
            Assert.True(
                oneDatabase.Count < everyDatabase.Count,
                $"the filtered read returned {oneDatabase.Count} of {everyDatabase.Count} rows — the "
                + "database filter is not narrowing anything");

            /* And the count the truncation notice divides by is the count of the SAME question. */
            Assert.Equal(
                oneDatabase.Count,
                await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, ServerId, databaseName: Asked));
            Assert.Equal(
                everyDatabase.Count,
                await DarlingObjectStatsReader.GetIndexUsageMatchCountAsync(postgres, ServerId, databaseName: null));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The ordering, as row order rather than as an ORDER BY key. The two databases are named so that
    /// alphabetical order and reserved size both DISAGREE with the answer: the unused index is in the
    /// alphabetically-first database and is the SMALLEST row, so an ordering that had lost its
    /// unused-first key would put it last on either remaining tiebreak.
    /// </summary>
    [Fact]
    public async Task AnUnusedIndexComesBackAheadOfAUsedOne()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live index-usage ordering test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PlantTwoDatabasesAsync(connection, ct);

            var rows = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, ServerId, top: 100, databaseName: null);

            Assert.Equal("Unused", rows[0].Classification);
            Assert.Equal("IX_NeverRead", rows[0].IndexName);

            /* The cap is what made the reporter's database invisible, so assert it truncates to the
               unused end rather than to whatever the store happened to return first. */
            var capped = await DarlingObjectStatsReader.GetIndexUsageAsync(postgres, ServerId, top: 1, databaseName: null);
            Assert.Equal("IX_NeverRead", Assert.Single(capped).IndexName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// One capture, two databases, three indexes: a never-read index in the alphabetically-first database
    /// (the smallest, so size cannot be what puts it first), and two actively-read ones in the other.
    /// </summary>
    private static async Task PlantTwoDatabasesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        var capture = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
        var collectionId = CollectionIdGenerator.Next();

        await PlantIndexAsync(connection, ct, collectionId, capture, Asked, "IX_NeverRead", objectId: 100, indexId: 2, reservedMb: 1m, seeks: 0);
        await PlantIndexAsync(connection, ct, collectionId, capture, Louder, "IX_HotOne", objectId: 200, indexId: 1, reservedMb: 5000m, seeks: 900_000);
        await PlantIndexAsync(connection, ct, collectionId, capture, Louder, "IX_HotTwo", objectId: 201, indexId: 1, reservedMb: 4000m, seeks: 800_000);
    }

    private static Task PlantIndexAsync(
        NpgsqlConnection connection,
        CancellationToken ct,
        long collectionId,
        DateTime capture,
        string databaseName,
        string indexName,
        int objectId,
        int indexId,
        decimal reservedMb,
        long seeks) =>
        DarlingMcpTestData.ExecAsync(
            connection,
            ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
            collectionId, capture, ServerId, ServerName, databaseName, "dbo", objectId, "T" + objectId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            indexId, indexName, "NONCLUSTERED", reservedMb, reservedMb, 1_000_000L, seeks, 0L, 0L, 0L);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM index_object_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
