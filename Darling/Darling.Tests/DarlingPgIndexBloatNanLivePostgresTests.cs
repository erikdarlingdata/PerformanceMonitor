/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An EMPTY b-tree index does not take <c>get_pg_index_bloat</c> down, and it does not disappear from it
/// either (#3121).
///
/// <para><b>What was wrong.</b> <c>pgstatindex</c> has no leaf pages to average over on an empty index, so it
/// reports <c>avg_leaf_density</c> and <c>leaf_fragmentation</c> as NaN. The store column is
/// <c>double precision</c>, so the NaN is representable and PERSISTS — one such row is re-read on every later
/// call until it ages out. The read guarded the column with <c>IS NULL</c>, which NaN passes, and the
/// surviving expression cast NaN to <c>bigint</c> and raised <c>22003</c>. That failed the WHOLE read, not
/// the row, so a fresh partition or a table whose rows were all deleted was enough to make the surface serve
/// an error instead of the index data.</para>
///
/// <para><b>Why BOTH assertions, and why the second is the one that earns its keep.</b> The cheapest fix that
/// turns the read green is a <c>WHERE</c> that drops the row, and it would pass any test that only asked
/// whether the read succeeded — while hiding a real index from the only surface that reports index bloat.
/// The row-is-still-there assertion is the one that can tell those two fixes apart, so it is written as an
/// accounting invariant (every seeded index appears in the output) rather than as a lookup of the row this
/// test happens to know about.</para>
///
/// <para><b>The NaN is INDUCED, not typed in.</b> An empty index is one <c>CREATE INDEX</c> away, so the
/// value under test is the one a real server produces rather than a literal that resembles it — which also
/// makes the premise itself falsifiable here (<see cref="PgstatindexReportsNaN_ForARealEmptyBtreeIndex"/>)
/// instead of quoted from the issue. <c>'NaN'::double precision</c> is the fallback for a rig where
/// pgstattuple cannot be created, and the assertion message says which route produced the value, because a
/// pin that cannot say what it measured is one round of confusion away from being believed about the wrong
/// thing.</para>
///
/// <para><b>The float sweep is over the ROW TYPE, not over the two columns named in the issue.</b> A scan's
/// blind spot is a property of its detector: asserting on <c>AvgLeafDensity</c> and
/// <c>LeafFragmentation</c> by name would pass unchanged if a third <c>double?</c> column were added to this
/// read without the normalisation. Reflecting over the record's <c>double?</c> properties makes the next
/// column arrive already covered.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgIndexBloatNanLivePostgresTests
{
    /// <summary>Distinctive so a stray row cannot be confused with another live class's seed data.</summary>
    private const int ServerId = 993_121;

    private const string ServerName = "pg-index-bloat-nan-probe";

    /* public, not a throwaway schema: LivePostgresStoreFixture's residue diff scans collect/config/public,
       so a probe table that leaked would be ACCUSED rather than quietly surviving in a schema nothing
       looks at. */
    private const string ProbeTable = "public.pg_index_bloat_nan_probe_3121";

    private const string ProbeIndexName = "pg_index_bloat_nan_probe_3121_ix";

    private const string ProbeIndex = "public." + ProbeIndexName;

    /* The empty index sorts LAST by design (nothing to reclaim), so the limit has to clear the seeded set
       or arm 2 would be measuring the limit instead of the fix. */
    private const int ReadLimit = 25;

    /// <summary>
    /// The premise, measured rather than quoted: <c>pgstatindex</c> really does report NaN — for BOTH float
    /// columns — on a real empty b-tree index. Skips where pgstattuple cannot be created, which is a gap in
    /// this test's provenance and not in the coverage below; the acceptance test still runs there.
    /// </summary>
    [Fact]
    public async Task PgstatindexReportsNaN_ForARealEmptyBtreeIndex()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live pgstatindex NaN premise test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        var created = await TryCreatePgstattupleAsync(connection, ct);
        Assert.SkipWhen(!created,
            "pgstattuple could not be created on this cluster, so pgstatindex cannot be called here. The "
            + "acceptance test in this class still runs, sourcing the NaN from 'NaN'::double precision.");

        var bodySucceeded = false;
        try
        {
            var measurement = await MeasureEmptyIndexAsync(connection, ct);

            Assert.True(double.IsNaN(measurement.Density),
                "pgstatindex is expected to report avg_leaf_density as NaN for an index with no leaf pages; "
                + $"it reported {measurement.Density.ToString(CultureInfo.InvariantCulture)}. If this cluster "
                + "returns NULL or 0 instead, the read's normalisation is still correct but this issue's "
                + "premise is version-specific and the SQL comment should say so.");

            /* The issue names avg_leaf_density only. leaf_fragmentation is NaN on the same row and is also
               double precision in the store, so it reaches System.Text.Json by the same route - found by
               running this rather than by reading the issue. */
            Assert.True(double.IsNaN(measurement.Fragmentation),
                "leaf_fragmentation is NaN on the same row, and is the second float column the read has to "
                + $"normalise; it reported {measurement.Fragmentation.ToString(CultureInfo.InvariantCulture)}.");

            /* An empty b-tree is the metapage and nothing else, which is why 0 reclaimable is a measurement
               rather than a guess. */
            Assert.True(measurement.Bytes > 0 && measurement.Bytes <= 64 * 1024,
                $"an empty b-tree index should be a page or two; this one is {measurement.Bytes} bytes");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DropProbeIndexAsync);
        }
    }

    /// <summary>
    /// Both acceptance arms, through the shared reader AND through the <c>get_pg_index_bloat</c> tool the MCP
    /// surface and the web page both call — the read succeeds, and the empty index is still in the answer.
    ///
    /// <para>Going through the tool as well as the reader is not belt-and-braces. The reader hands back a
    /// <c>double?</c>, and a NaN that clears the SQL is only halfway out: <c>JsonSerializer</c>'s default
    /// number handling REJECTS NaN, so the tool would catch it and return an error envelope having read the
    /// data perfectly. The reader assertion cannot see that, and it is the same defect one layer up.</para>
    ///
    /// <para>Three unrelated indexes share the sample on purpose — empty, churned, and too-large-to-measure —
    /// with names chosen so an alphabetical or insertion-order accident cannot look like correct ranking. A
    /// fixture holding only the interesting row cannot catch a guard that damages the ordinary ones, and the
    /// guard here sits inside the expression the ranking is built on.</para>
    /// </summary>
    [Fact]
    public async Task TheReadServesAStoreCarryingNaN_AndTheEmptyIndexStillAppears()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live pg_index_bloat NaN test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            /* Induced where possible; the message on every assertion below names which route ran. */
            var measurement = await NanSourceAsync(connection, ct);

            var capturedAt = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-5));

            /* aaa_ sorts first alphabetically and the empty index must still come LAST, so an ordering that
               fell back to any name or insertion order fails rather than coincidentally passing. */
            await SeedAsync(connection, ct, capturedAt, "aaa_fresh_partition", "fresh_partition_pkey",
                measurement.Bytes, treeLevel: 0, density: measurement.Density,
                fragmentation: measurement.Fragmentation, skippedReason: null);

            await SeedAsync(connection, ct, capturedAt, "zzz_churned", "churned_ix",
                1_000_000_000L, treeLevel: 3, density: 45.0, fragmentation: 30.0, skippedReason: null);

            await SeedAsync(connection, ct, capturedAt, "mmm_over_ceiling", "over_ceiling_ix",
                90_000_000_000L, treeLevel: null, density: null, fragmentation: null,
                skippedReason: "index is larger than the measurement ceiling; pgstatindex reads every page, "
                             + "so it is recorded but not measured");

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            // ── ARM 1: the read serves at all ────────────────────────────────────────────────────────
            var rows = await DarlingPgIndexBloatReader.GetPgIndexBloatAsync(
                postgres, ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-24)),
                DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(1)), ReadLimit, ct);

            // ── ARM 2: nothing was dropped to achieve arm 1 ──────────────────────────────────────────
            /* Stated as accounting over the whole seeded set rather than as a lookup of the NaN row: a
               filter written to make arm 1 pass has to survive THIS, and "every stored row is accounted
               for by some read" is the invariant a scenario-shaped assertion keeps missing. */
            Assert.Equal(
                new[] { "aaa_fresh_partition", "mmm_over_ceiling", "zzz_churned" },
                rows.Select(r => r.TableName).OrderBy(n => n, StringComparer.Ordinal).ToArray());

            var empty = rows.Single(r => r.TableName == "aaa_fresh_partition");

            /* A sensible reclaimable figure or an explicit null. 0 is the sensible one and is what this
               reports: the index was measured, and what it holds is nothing. */
            Assert.Equal(0L, empty.EstimatedReclaimableBytes);

            /* It was MEASURED. A read that reached the second arm by labelling the row skipped would have
               hoisted it to the top of the grid, above every real bloat candidate. */
            Assert.Null(empty.SkippedReason);
            Assert.Equal("mmm_over_ceiling", rows[0].TableName);
            Assert.Equal("aaa_fresh_partition", rows[^1].TableName);

            /* NO NaN leaves the read, on any float column of the row type - see the class summary for why
               this is over the type and not over the two columns the issue names. */
            foreach (var row in rows)
            {
                foreach (var property in NullableDoubleProperties)
                {
                    var value = (double?)property.GetValue(row);
                    Assert.False(value is double d && double.IsNaN(d),
                        $"{property.Name} came back NaN on {row.TableName}. Every double precision column of "
                        + "collect.pg_index_bloat has to be normalised on the way out: NaN survives the SQL "
                        + "quietly and is then rejected by System.Text.Json, which fails the MCP read and "
                        + $"the web page. NaN source for this run: {measurement.Provenance}.");
                }
            }

            /* The neighbours are untouched. (90 - 45) / 90 * 1e9 = 500,000,000. */
            Assert.Equal(500_000_000L, rows.Single(r => r.TableName == "zzz_churned").EstimatedReclaimableBytes);
            Assert.Null(rows.Single(r => r.TableName == "mmm_over_ceiling").EstimatedReclaimableBytes);

            // ── ARM 1, one layer up: the surface the operator actually calls ─────────────────────────
            var json = await DarlingMcpPgIndexTools.GetPgIndexBloat(
                postgres, ServerName, hours_back: 24, limit: ReadLimit);

            using var document = JsonDocument.Parse(json);
            Assert.False(
                document.RootElement.TryGetProperty("status", out var status)
                && string.Equals(status.GetString(), "error", StringComparison.Ordinal),
                $"get_pg_index_bloat returned an error envelope for a store carrying a NaN density "
                + $"(NaN source: {measurement.Provenance}): {json}");

            var payload = document.RootElement.GetProperty("indexes").EnumerateArray().ToList();
            Assert.Equal(3, payload.Count);

            var emptyPayload = payload.Single(
                i => i.GetProperty("table_name").GetString() == "aaa_fresh_partition");
            Assert.Equal(JsonValueKind.Null, emptyPayload.GetProperty("avg_leaf_density").ValueKind);
            Assert.Equal(0L, emptyPayload.GetProperty("estimated_reclaimable_bytes").GetInt64());
            Assert.Equal(JsonValueKind.Null, emptyPayload.GetProperty("skipped_reason").ValueKind);

            /* And the payload SAYS what that null density means, because nothing else in it can: a null
               density with no skipped_reason is the one row where the note's "a null measurement is absence
               of data" lesson is wrong. */
            var note = document.RootElement.GetProperty("note").GetString();
            Assert.Contains("EMPTY", note!, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                await DropProbeIndexAsync(cleanup, cleanupCt);
            });
        }
    }

    /* ── helpers ─────────────────────────────────────────────────────────────────────────────────── */

    /// <summary>
    /// The <c>double?</c> columns of the row the surfaces consume, read off the type so a column added later
    /// is covered without anyone remembering to add it here.
    /// </summary>
    private static readonly PropertyInfo[] NullableDoubleProperties =
        [.. typeof(DarlingPgIndexBloatReader.PgIndexBloatRow)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(double?))];

    private readonly record struct NanMeasurement(
        double Density, double Fragmentation, long Bytes, string Provenance);

    /// <summary>
    /// A real NaN if pgstatindex can produce one here, and a literal one otherwise — carrying which, so an
    /// assertion that fires can say what it was measuring.
    /// </summary>
    private static async Task<NanMeasurement> NanSourceAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        if (await TryCreatePgstattupleAsync(connection, ct))
        {
            return await MeasureEmptyIndexAsync(connection, ct);
        }

        /* PostgreSQL raises division_by_zero rather than yielding NaN, so a literal is the only other route
           to the value. Same IEEE bits, weaker provenance - hence the label. */
        return new NanMeasurement(double.NaN, double.NaN, 8_192L,
            "'NaN'::double precision (pgstattuple is not creatable on this cluster)");
    }

    /// <summary>
    /// Whether <c>public.pgstatindex</c> can be CALLED here, which is a different question from whether the
    /// CREATE succeeded.
    ///
    /// <para><b>WITH SCHEMA public is load-bearing, and the store's own search_path is why.</b> pgstattuple
    /// is relocatable, so a bare <c>CREATE EXTENSION</c> installs into the first schema on the path — which
    /// on a migrated store is <c>collect</c>, not <c>public</c>. The CREATE then succeeds and
    /// <c>public.pgstatindex(...)</c> fails with 42883, which is how this presented. The collector qualifies
    /// the call <c>public.</c> for its own documented reasons, so <c>public</c> is the placement under test
    /// rather than an arbitrary choice.</para>
    ///
    /// <para>The catalog probe is the verdict, not the CREATE's exit: <c>IF NOT EXISTS</c> is a no-op on a
    /// store where the extension already sits somewhere else, and reporting success off that would send the
    /// caller at a function it cannot reach.</para>
    /// </summary>
    private static async Task<bool> TryCreatePgstattupleAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        try
        {
            await using var create = new NpgsqlCommand(
                "CREATE EXTENSION IF NOT EXISTS pgstattuple WITH SCHEMA public", connection);
            await create.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException)
        {
            /* Not in this cluster's share/extension, or the login cannot create it, or it already exists
               elsewhere. All are legitimate rigs and none is this test's subject - the probe below decides. */
        }

        await using var probe = new NpgsqlCommand("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_proc AS p
                JOIN pg_catalog.pg_namespace AS n
                  ON n.oid = p.pronamespace
                WHERE p.proname = 'pgstatindex'
                AND   n.nspname = 'public')
            """, connection);
        return await probe.ExecuteScalarAsync(ct) is true;
    }

    /// <summary>Creates a real empty b-tree index and returns what <c>pgstatindex</c> says about it.</summary>
    private static async Task<NanMeasurement> MeasureEmptyIndexAsync(
        NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var ddl = new NpgsqlCommand($"""
            DROP TABLE IF EXISTS {ProbeTable} CASCADE;
            CREATE TABLE {ProbeTable} (probe_id bigint);
            CREATE INDEX {ProbeIndexName} ON {ProbeTable} (probe_id);
            """, connection))
        {
            await ddl.ExecuteNonQueryAsync(ct);
        }

        await using var command = new NpgsqlCommand($"""
            SELECT s.avg_leaf_density, s.leaf_fragmentation, pg_catalog.pg_relation_size('{ProbeIndex}')
            FROM public.pgstatindex('{ProbeIndex}') AS s
            """, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "pgstatindex returned no row for the probe index");

        return new NanMeasurement(
            reader.GetDouble(0), reader.GetDouble(1), reader.GetInt64(2),
            "pgstatindex on a real empty b-tree index");
    }

    private static Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        string tableName, string indexName, long indexBytes, int? treeLevel,
        double? density, double? fragmentation, string? skippedReason) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_index_bloat
    (collection_id, collection_time, server_id, server_name, database_name, schema_name,
     table_name, index_name, index_bytes, tree_level, internal_pages, leaf_pages,
     empty_pages, deleted_pages, avg_leaf_density, leaf_fragmentation, skipped_reason)
VALUES ($1::bigint, $2::timestamp, $3::integer, $4::text, $5::text, $6::text,
        $7::text, $8::text, $9::bigint, $10::integer, $11::bigint, $12::bigint,
        $13::bigint, $14::bigint, $15::double precision, $16::double precision, $17::text)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            "appdb", "public", tableName, indexName, indexBytes, treeLevel, null, null, 0L, 0L,
            density, fragmentation, skippedReason);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM pg_index_bloat WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(
            connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }

    /// <summary>
    /// Through <see cref="LiveCleanupBatch"/> rather than a bare DROP: the probe table lives in a schema the
    /// fixture's residue diff scans, so a removal that lost is an accusation with this test's name on it
    /// instead of an anonymous leftover the next run inherits.
    ///
    /// <para>pgstattuple is deliberately LEFT in place. Dropping it is a database-level change racing
    /// nothing, and a teardown that can fail for a reason unrelated to the subject buys a red run rather
    /// than a clean store — the extension itself creates no relation the diff can see.</para>
    /// </summary>
    private static Task DropProbeIndexAsync(NpgsqlConnection connection, CancellationToken ct) =>
        new LiveCleanupBatch(connection).RemoveAsync(
            $"probe table {ProbeTable}",
            $"DROP TABLE IF EXISTS {ProbeTable} CASCADE",
            $"SELECT to_regclass('{ProbeTable}') IS NOT NULL",
            ct);
}
