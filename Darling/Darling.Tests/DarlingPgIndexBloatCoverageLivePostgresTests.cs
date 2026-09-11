/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #3278 coverage census EXECUTES, and it answers over the population rather than over a page.
///
/// <para><b>Why this needs a live store, and it is the only pin that can say so.</b>
/// <see cref="DarlingPgIndexBloatReader.GetCoverageVerdictAsync"/> catches every exception and answers
/// <see cref="PgIndexBloatCoverageArm.Undetermined"/> — deliberately, because the census runs to EXPLAIN a
/// result the caller already has and must not turn that into a read error. The cost of that decision is that
/// a census which does not PARSE degrades to a polite sentence, on every server, forever, with nothing that
/// fails. Every other pin in <c>PgIndexBloatCoverageTests</c> is a pure function or a source scan, and not
/// one of them can tell a working query from a syntactically broken one. This can.</para>
///
/// <para><b>Three properties only a real store can settle.</b> The census reduces the window to one row per
/// index, so a collector that writes every btree on every cycle cannot inflate the denominator — the failure
/// that flatters coverage. It applies the row read's measured-row-wins tie-break, so an index whose NEWEST
/// row is a label and whose earlier row is an answer counts as answered in both places. And the run probe is
/// what separates "no indexes" from "no evidence", which needs a <c>collection_log</c> row to exist and then
/// not to.</para>
///
/// <para><b>The seeded shape is the fleet's, not a convenient one.</b> The two suppression buckets are
/// weighted so the ROW ranking and the BYTE ranking disagree, because that is the condition the whole issue
/// turns on and a fixture where the two agree would pass whichever ordering shipped.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgIndexBloatCoverageLivePostgresTests
{
    /// <summary>
    /// NEGATIVE, the convention the live classes here follow and not decoration: teardown deletes from
    /// <c>servers</c> by id, and a real store assigns ids from a sequence, so a positive sentinel is one
    /// collision away from removing an operator's own registry row.
    /// </summary>
    private const int ServerId = -993_278;

    private const string ServerName = "pg-index-bloat-coverage-probe";

    private const long Mib = 1024L * 1024L;
    private const long Gib = Mib * 1024L;

    /// <summary>
    /// The collector's own prose for the two buckets, PREFIXED with the markers
    /// <see cref="PgIndexBloatCoverage.Markers"/> keys on. Quoted rather than generated so this test seeds
    /// what a real collector writes; <c>PgIndexBloatCoverageTests</c> is what pins the markers against the
    /// shipped query, so a drift there fails loudly rather than turning this fixture into two
    /// <see cref="PgIndexBloatSuppression.Unrecognized"/> rows that still add up.
    /// </summary>
    private const string WidthsReason =
        "column widths are not visible for every key: pg_stats filters on has_column_privilege, so a "
        + "monitoring role without SELECT sees nothing, and a never-analyzed parent has no rows either. "
        + "Grant pg_read_all_data, or ANALYZE the parent.";

    private const string NeverAnalyzedReason =
        "the parent table has never been analyzed (reltuples = -1), so there is no row count to model from. "
        + "An ANALYZE of the parent makes this index estimable.";

    /// <summary>
    /// The census executes, counts the POPULATION once per index, applies the row read's tie-break, and
    /// ranks its buckets by bytes.
    /// </summary>
    [Fact]
    public async Task TheCensusExecutesAndAnswersOverThePopulation()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3278 coverage census test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var newest = now.AddHours(-2);
            var older = now.AddHours(-26);

            /* TWO cycles, both inside the 48h evidence window, both writing every index - which is what
               this collector does. A census without DISTINCT ON would report every figure below DOUBLED. */
            foreach (var cycle in new[] { older, newest })
            {
                await SeedEstimateAsync(connection, ct, cycle, "orders", "orders_pkey", 40 * Gib);
                await SeedSuppressedAsync(
                    connection, ct, cycle, "orders", "orders_wide_ix", 300 * Gib, WidthsReason);
                await SeedSuppressedAsync(
                    connection, ct, cycle, "audit", "audit_a_ix", 4 * Mib, NeverAnalyzedReason);
                await SeedSuppressedAsync(
                    connection, ct, cycle, "audit", "audit_b_ix", 4 * Mib, NeverAnalyzedReason);
                await SeedSuppressedAsync(
                    connection, ct, cycle, "audit", "audit_c_ix", 4 * Mib, NeverAnalyzedReason);
            }

            /* THE TIE-BREAK'S OWN CASE: measured on the OLDER cycle, labelled on the NEWER one. The row read
               keeps the measurement, so the census has to as well - otherwise it publishes a trusted count
               the grid beside it contradicts, for this index, with nothing that fails. */
            await SeedMeasuredAsync(connection, ct, older, "shipments", "shipments_pkey", 10 * Gib);
            await SeedSuppressedAsync(
                connection, ct, newest, "shipments", "shipments_pkey", 10 * Gib, WidthsReason);

            await LogRunAsync(connection, ct, newest, "SUCCESS");

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            var verdict = await DarlingPgIndexBloatReader.GetCoverageVerdictAsync(
                postgres, ServerId, DarlingMcpTestData.Naive(now), returnedRows: 2, ct);

            /* ARM 1: it PARSED. Undetermined here is what a broken query looks like, so this assertion is
               the whole reason the class exists - and it names that, because the message is what somebody
               reads when it goes red. */
            Assert.False(
                verdict.Arm == PgIndexBloatCoverageArm.Undetermined,
                "the coverage census answered Undetermined over a store that was just seeded with a "
                + "SUCCESS run and six indexes. GetCoverageVerdictAsync swallows read failures by design, "
                + "so this is what a census that does not PARSE looks like from the outside: check "
                + "CoverageEvidenceSql against the live store. Census: " + verdict.Census);

            Assert.Equal(PgIndexBloatCoverageArm.PartialCoverage, verdict.Arm);

            /* ARM 2: ONE ROW PER INDEX. Six distinct indexes, eleven rows across two cycles, and the
               population is SIX - the denominator error that flatters coverage would report eleven.

               Derived below as well as stated: the literal is what a reader checks the fixture against, and
               the identity is what catches a fixture edit that changes the population without changing the
               literal. Writing the literal from memory got it wrong once already. */
            Assert.Equal(6, verdict.Candidates.IndexCount);
            Assert.Equal(
                DistinctSeededIndexes, verdict.Candidates.IndexCount);

            /* ARM 3: the tie-break. shipments_pkey's newest row carries a reason and its older row carries
               an exact measurement, and it counts as EXACTLY MEASURED - the same row the grid shows. */
            Assert.Equal(1, verdict.ExactlyMeasured.IndexCount);
            Assert.Equal(10 * Gib, verdict.ExactlyMeasured.IndexBytes);

            Assert.Equal(1, verdict.Estimated.IndexCount);
            Assert.Equal(40 * Gib, verdict.Estimated.IndexBytes);
            Assert.Equal(2, verdict.Trusted.IndexCount);
            Assert.Equal(50 * Gib, verdict.Trusted.IndexBytes);

            /* ARM 4: est_tuple_bytes is populated on every suppressed row seeded here, exactly as it is on
               100% of the fleet's, and none of them counts as trusted. This is incident four's route. */
            Assert.Equal(4, verdict.Suppressed.Sum(bucket => bucket.IndexCount));
            Assert.Equal(
                verdict.Trusted.IndexCount + verdict.Suppressed.Sum(bucket => bucket.IndexCount),
                verdict.Candidates.IndexCount);

            /* ARM 5: ranked BY BYTES, over a fixture where the row ranking disagrees - three
               never-analyzed indexes at 4 MB each against one permission index at 300 GB. Counts alone
               would put the 12 MB bucket first. */
            Assert.Equal(
                new[] { PgIndexBloatSuppression.ColumnWidthsNotVisible, PgIndexBloatSuppression.ParentNeverAnalyzed },
                verdict.Suppressed.Select(bucket => bucket.Reason).ToArray());

            Assert.Equal(1, verdict.Suppressed[0].IndexCount);
            Assert.Equal(300 * Gib, verdict.Suppressed[0].IndexBytes);
            Assert.Equal(3, verdict.Suppressed[1].IndexCount);
            Assert.Equal(12 * Mib, verdict.Suppressed[1].IndexBytes);

            /* And the fixture really is one where the two rankings disagree, or ARM 5 asserts nothing. */
            Assert.NotEqual(
                verdict.Suppressed.OrderByDescending(b => b.IndexCount).Select(b => b.Reason).ToArray(),
                verdict.Suppressed.Select(b => b.Reason).ToArray());

            /* The stored prose reaches the bucket ONCE rather than per row, which is the point of keying on
               a short identifier - the never-analyzed bucket is three rows carrying one paragraph. */
            Assert.Equal(NeverAnalyzedReason, verdict.Suppressed[1].Detail);

            /* ARM 6: no evidence is not no indexes. With the run gone the same rows answer Undetermined and
               publish NO population figure - not a zero, and not the figures it could still read. */
            await DarlingMcpTestData.ExecAsync(
                connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);

            var unevidenced = await DarlingPgIndexBloatReader.GetCoverageVerdictAsync(
                postgres, ServerId, DarlingMcpTestData.Naive(now), returnedRows: 2, ct);

            Assert.Equal(PgIndexBloatCoverageArm.Undetermined, unevidenced.Arm);
            Assert.Equal(default, unevidenced.Candidates);
            Assert.Empty(unevidenced.Suppressed);

            /* ARM 7: and an ERRORED run is not evidence either. Measured on a live Aurora target, 3 of this
               collector's 7 runs in a week errored - one a 300-second client-side deadline that stored
               nothing - so an unfiltered probe would report "it ran" over whatever the window happens to
               hold and the classifier would answer over a population no run in it established. */
            await LogRunAsync(connection, ct, newest, "ERROR");

            var erroredOnly = await DarlingPgIndexBloatReader.GetCoverageVerdictAsync(
                postgres, ServerId, DarlingMcpTestData.Naive(now), returnedRows: 2, ct);

            Assert.Equal(PgIndexBloatCoverageArm.Undetermined, erroredOnly.Arm);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(
                bodySucceeded,
                async () =>
                {
                    await using var cleanup = new NpgsqlConnection(connectionString);
                    await cleanup.OpenAsync(CancellationToken.None);
                    await DeleteRowsAsync(cleanup, CancellationToken.None);
                });
        }
    }

    /// <summary>
    /// Distinct (database, schema, table, index) tuples the fixture seeds: five written on both cycles plus
    /// <c>shipments_pkey</c>, which is written once per cycle with a DIFFERENT outcome each time. Named so
    /// the population assertion above has something to check besides a number somebody typed.
    /// </summary>
    private const int DistinctSeededIndexes = 6;

    /// <summary>A trusted ESTIMATE row: no reason, and a modelled tuple width, which is the provenance
    /// discriminator the reader keys <c>measurement_kind</c> on.</summary>
    private static Task SeedEstimateAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string tableName, string indexName, long indexBytes) =>
        SeedAsync(
            connection, ct, collectionTimeUtc, tableName, indexName, indexBytes,
            estTupleBytes: 48L, estBloatPct: 12.5, skippedReason: null);

    /// <summary>A historical EXACT row: no reason and NO modelled width, which is what a pre-#3234
    /// <c>pgstatindex</c> measurement still inside retention looks like.</summary>
    private static Task SeedMeasuredAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string tableName, string indexName, long indexBytes) =>
        SeedAsync(
            connection, ct, collectionTimeUtc, tableName, indexName, indexBytes,
            estTupleBytes: null, estBloatPct: null, skippedReason: null);

    /// <summary>
    /// A SUPPRESSED row, carrying <c>est_tuple_bytes</c> and no <c>est_bloat_pct</c> — the fleet's measured
    /// shape on 100% of suppressed rows in every bucket, and the reason a census keyed on the intermediate
    /// would count this row as covered.
    /// </summary>
    private static Task SeedSuppressedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string tableName, string indexName, long indexBytes, string skippedReason) =>
        SeedAsync(
            connection, ct, collectionTimeUtc, tableName, indexName, indexBytes,
            estTupleBytes: 48L, estBloatPct: null, skippedReason: skippedReason);

    private static Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string tableName, string indexName, long indexBytes, long? estTupleBytes, double? estBloatPct,
        string? skippedReason) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_index_bloat
    (collection_id, collection_time, server_id, server_name, database_name, schema_name,
     table_name, index_name, index_bytes, skipped_reason, est_tuple_bytes, est_bloat_pct)
VALUES ($1::bigint, $2::timestamp, $3::integer, $4::text, $5::text, $6::text,
        $7::text, $8::text, $9::bigint, $10::text, $11::bigint, $12::double precision)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            "appdb", "public", tableName, indexName, indexBytes, skippedReason, estTupleBytes, estBloatPct);

    private static Task LogRunAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string status) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1::bigint, $2::integer, $3::text, 'pg_index_bloat', $4::timestamp, $5::text)",
            CollectionIdGenerator.Next(), ServerId, ServerName,
            DarlingMcpTestData.Naive(collectionTimeUtc), status);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM pg_index_bloat WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}
