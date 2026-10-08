/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another
   database; the scratch store goes with the test. */

/// <summary>
/// #5521: the hole scan's materialization probe is a range pair (<c>bucket &gt;= b AND bucket &lt;= b</c>), not
/// <c>bucket = b</c>. On a chunk whose rows all carry one bucket value the equality is estimated to match every
/// row, so the planner Seq Scans the whole chunk to learn that a MISSING bucket is missing; a range with two
/// non-constant bounds gets PostgreSQL's flat default estimate, so the probe is an index seek. The first test
/// proves the answer is unchanged (the pre-#3933 text is the oracle) across compressed and uncompressed chunks,
/// a bucket only one server holds, and a server whose rows are only in old chunks; the second proves the plan.
/// </summary>
[Collection("live-postgres")]
public sealed class MaterializationHoleProbeLiveTests
{
    private const int ServerA = -552101;
    private const int ServerB = -552102;
    private const int ServerC = -552103;

    /// <summary>The pre-#3933 scan, verbatim: an equality on the bucket, no fences.</summary>
    private static string OldScanSql(TimescaleSupport.MaterializationHoleTarget target, (string Schema, string Name) materialization)
    {
        var filter = TimescaleSupport.MaterializationHoleSourceFilterFor(target.CreateSql);
        var sourceFilter = filter.Length == 0 ? string.Empty : $"\n    AND   {filter}";

        return $@"
SELECT b.bucket
FROM generate_series($1::timestamp, $2::timestamp, $3::interval) AS b(bucket)
WHERE NOT EXISTS (SELECT 1 FROM ""{materialization.Schema}"".""{materialization.Name}"" AS m WHERE m.bucket = b.bucket)
AND   EXISTS (
    SELECT 1 FROM collect.{target.Source} AS s
    WHERE s.{target.SourceTimeColumn} >= b.bucket
    AND   s.{target.SourceTimeColumn} < b.bucket + $3::interval{sourceFilter})
ORDER BY b.bucket";
    }

    [Fact]
    public async Task TheRangePairProbe_FindsExactlyTheOldScansBuckets_AcrossCompressedAndUncompressedChunksAndServers_AgainstDevPostgres()
    {
        await using var store = await OpenAsync("the hole-probe oracle test");
        var (connection, h0, target, materialization) = (store.Connection, store.H0, store.Target, store.Materialization);
        var ct = TestContext.Current.CancellationToken;
        DateTime H(int n) => h0.AddHours(n);

        /* Eight UTC days of hourly raw rows for servers A and B; server C only in the first two days, so its rows
           live only in the oldest chunks. Hour 100 has no raw row at all (a candidate the source probe rejects). */
        var all = Enumerable.Range(0, 192).Where(h => h != 100).ToArray();
        await PlantAsync(connection, h0, all, ServerA, ct);
        await PlantAsync(connection, h0, all, ServerB, ct);
        await PlantAsync(connection, h0, Enumerable.Range(0, 48).ToArray(), ServerC, ct);

        await using (var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{target.View}'::regclass, $1::timestamp, $2::timestamp)", connection))
        {
            refresh.Parameters.AddWithValue(h0);
            refresh.Parameters.AddWithValue(H(192));
            await refresh.ExecuteNonQueryAsync(ct);
        }

        /* Holes: hour 30 (a day that is compressed below) and hour 150 (a day that stays uncompressed), deleted
           from the materialization for every server. Hour 10: servers A and B deleted, server C's row kept, so
           the bucket is covered by the one server that holds it and is NOT a hole. */
        await using (var punch = new NpgsqlCommand(
            $"DELETE FROM \"{materialization.Schema}\".\"{materialization.Name}\" WHERE bucket = ANY($1) OR (bucket = $2 AND server_id <> $3)", connection))
        {
            punch.Parameters.AddWithValue(new[] { H(30), H(150) });
            punch.Parameters.AddWithValue(H(10));
            punch.Parameters.AddWithValue(ServerC);
            await punch.ExecuteNonQueryAsync(ct);
        }

        await using (var compress = new NpgsqlCommand(
            $"SELECT count(compress_chunk(c)) FROM show_chunks('\"{materialization.Schema}\".\"{materialization.Name}\"'::regclass, older_than => $1::timestamp) AS c", connection))
        {
            compress.Parameters.AddWithValue(H(5 * 24));
            Assert.True((long)(await compress.ExecuteScalarAsync(ct))! >= 4, "the oldest days must be compressed for the test to mean anything");
        }

        await using (var kinds = new NpgsqlCommand(
            @"SELECT count(*) FILTER (WHERE is_compressed), count(*) FILTER (WHERE NOT is_compressed)
              FROM timescaledb_information.chunks WHERE hypertable_schema = $1 AND hypertable_name = $2", connection))
        {
            kinds.Parameters.AddWithValue(materialization.Schema);
            kinds.Parameters.AddWithValue(materialization.Name);
            await using var reader = await kinds.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.True(reader.GetInt64(0) >= 4 && reader.GetInt64(1) >= 2, "both chunk kinds must be present");
        }

        var current = await ScanAsync(connection, TimescaleSupport.MaterializationHoleScanSql(target, materialization), h0, H(191), target.BucketWidth, ct);
        var oracle = await ScanAsync(connection, OldScanSql(target, materialization), h0, H(191), target.BucketWidth, ct);

        Assert.Equal(oracle, current);
        Assert.Equal(new[] { H(30), H(150) }, current);
    }

    [Fact]
    public async Task AMissingBucketInAChunkWhoseRowsShareOneBucket_IsAnIndexSeek_NotASeqScan_AgainstDevPostgres()
    {
        await using var store = await OpenAsync("the hole-probe plan test");
        var (connection, h0, target, materialization) = (store.Connection, store.H0, store.Target, store.Materialization);
        var ct = TestContext.Current.CancellationToken;
        var relation = $"\"{materialization.Schema}\".\"{materialization.Name}\"";

        /* One bucket (hour 0 of a day) with 60,000 rows straight into the materialization: the chunk's bucket
           column has n_distinct 1, which is what a freshly refreshed field chunk looks like. The chunk is not
           vacuumed (no all-visible pages), as a chunk the refresh keeps rewriting is not. */
        await using (var seed = new NpgsqlCommand($@"
INSERT INTO {relation} (server_id, server_name, database_name, query_hash, sql_handle, bucket)
SELECT 1 + g % 3, 'plan-5521', 'db' || (g % 5), md5(g::text), 'h', $1::timestamp
FROM generate_series(1, 60000) AS g", connection))
        {
            seed.Parameters.AddWithValue(h0);
            await seed.ExecuteNonQueryAsync(ct);
        }

        string chunk;
        await using (var find = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks WHERE hypertable_schema = $1 AND hypertable_name = $2", connection))
        {
            find.Parameters.AddWithValue(materialization.Schema);
            find.Parameters.AddWithValue(materialization.Name);
            chunk = (string)(await find.ExecuteScalarAsync(ct))!;
        }

        await using (var quiet = new NpgsqlCommand($"ALTER TABLE {chunk} SET (autovacuum_enabled = false); ANALYZE {chunk};", connection))
        {
            await quiet.ExecuteNonQueryAsync(ct);
        }

        var scan = TimescaleSupport.MaterializationHoleScanSql(target, materialization);
        var equality = scan.Replace("m.bucket >= b.bucket AND m.bucket <= b.bucket", "m.bucket = b.bucket", StringComparison.Ordinal);
        Assert.NotEqual(scan, equality);

        /* The control: the equality probe seq-scans the chunk for the missing hours. If a planner version stops
           doing that the premise is gone and there is nothing to pin. */
        var control = await ExecutedChunkScansAsync(connection, equality, h0, h0.AddHours(2), target.BucketWidth, ct);
        Assert.SkipWhen(!control.Contains("Seq Scan"), "this planner no longer seq-scans a one-bucket chunk for a missing bucket; the #5521 premise does not hold here");

        var probe = await ExecutedChunkScansAsync(connection, scan, h0, h0.AddHours(2), target.BucketWidth, ct);
        Assert.DoesNotContain("Seq Scan", probe);
        Assert.Contains(probe, n => n.Contains("Index", StringComparison.Ordinal));
    }

    /* ───────────────────────────── helpers ───────────────────────────── */

    private sealed class Store(
        ScratchPostgres scratch, NpgsqlConnection connection, DateTime h0,
        TimescaleSupport.MaterializationHoleTarget target, (string Schema, string Name) materialization) : IAsyncDisposable
    {
        public NpgsqlConnection Connection { get; } = connection;

        public DateTime H0 { get; } = h0;

        public TimescaleSupport.MaterializationHoleTarget Target { get; } = target;

        public (string Schema, string Name) Materialization { get; } = materialization;

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await scratch.DisposeAsync();
        }
    }

    /// <summary>A scratch store with the aggregates and their compression settings, background workers stopped so
    /// no policy runs under the test.</summary>
    private static async Task<Store> OpenAsync(string what)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            $"Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run {what}.");
        var ct = TestContext.Current.CancellationToken;

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
                $"{what} needs TimescaleDB: a materialization chunk exists only with it.");

            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);
            await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
            {
                await stop.ExecuteNonQueryAsync(ct);
            }

            var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == TimescaleSupport.QueryStatsIntervalHourlyView);
            var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, target.View, ct);
            Assert.NotNull(materialization);

            /* Midnight UTC twenty days back: below every refresh policy's window. */
            var h0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-20), DateTimeKind.Unspecified);
            return new Store(scratch, connection, h0, target, materialization.Value);
        }
        catch
        {
            /* A skip is an exception too: the scratch database goes with it either way. */
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
    }

    /// <summary>Three collections an hour, two hashes each, for one server.</summary>
    private static async Task PlantAsync(NpgsqlConnection connection, DateTime h0, int[] hours, int serverId, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT h * 1000 + m * 10 + q, $1 + make_interval(hours => h, mins => m), $2, 'probe-5521-' || $2, 'ProbeDb', '0xHASH' || q, '0xHANDLE' || q, 1000, 1000, 10, 1200
FROM unnest($3::int[]) AS h, (VALUES (0), (20), (40)) AS c(m), (VALUES (1), (2)) AS k(q)", connection);
        insert.Parameters.AddWithValue(h0);
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(hours);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime[]> ScanAsync(NpgsqlConnection connection, string sql, DateTime from, DateTime to, TimeSpan width, CancellationToken ct)
    {
        var buckets = new List<DateTime>();
        await using var scan = new NpgsqlCommand(sql, connection);
        scan.Parameters.AddWithValue(from);
        scan.Parameters.AddWithValue(to);
        scan.Parameters.AddWithValue(width);
        await using var reader = await scan.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            buckets.Add(reader.GetDateTime(0));
        }

        return buckets.ToArray();
    }

    /// <summary>The node types that RAN (Actual Loops above 0) against a materialization chunk, from the plan.</summary>
    private static async Task<List<string>> ExecutedChunkScansAsync(
        NpgsqlConnection connection, string sql, DateTime from, DateTime to, TimeSpan width, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection);
        explain.Parameters.AddWithValue(from);
        explain.Parameters.AddWithValue(to);
        explain.Parameters.AddWithValue(width);
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var plan = JsonDocument.Parse(json);

        return Walk(plan.RootElement[0].GetProperty("Plan"))
            .Where(n => n.TryGetProperty("Relation Name", out var rel) && rel.GetString()!.StartsWith("_hyper_", StringComparison.Ordinal)
                && n.TryGetProperty("Actual Loops", out var loops) && loops.GetInt32() > 0)
            .Select(n => n.GetProperty("Node Type").GetString()!)
            .ToList();
    }

    private static IEnumerable<JsonElement> Walk(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                foreach (var descendant in Walk(child))
                {
                    yield return descendant;
                }
            }
        }
    }
}
