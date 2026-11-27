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
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres, so its seed is the only data the plan fetch sees. */

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #9495: the plan fetch of <c>get_analysis_facts</c> (<see cref="PgFactCollector.PlanAdvisorySql"/>) groups a window's rows by plan key, inline plan text and
/// digest before it ranks them, because a week of history put every snapshot row of the server into the rank and spilled to disk. The group keeps each
/// plan's costliest cost and takes NULL when any row has none (a NULL cost sorts first under DESC). The tests pin the cases the grouping could change by what
/// the read returns, and hold the new statement to the statement as it was before #9495 on one seed.
/// </summary>
public sealed class PlanAdvisoryGroupLiveTests
{
    private const int ServerId = 9495;

    /// <summary>The statement as it was before #9495 (rank over every row), kept here only so the new read is held to its replies.</summary>
    private const string RowRankedSql = @"
/* #8570: the dimension is read only for the rows the fetch may return. query_stats holds one row per query per
   snapshot, so a four-hour window carries tens of thousands of rows and as many distinct plan digests, and a probe
   of query_plan_dim's primary key per row or per digest is that many random reads of a table of tens of millions of
   rows on a cold cache (30,000 probes and 25 s of a 26 s read). The rank orders each plan's rows without the
   dimension; the probes then walk the plans' costliest rows in cost order and stop at the tenth that has a plan.
   A row has a plan when its own column or its dimension row carries one (the dimension's text column or its gzip
   bytes, #2069), and a digest whose dimension row is gone has none. Each plan is represented by its costliest row
   that has a plan, so a plan whose costliest row has none is ranked again over all its rows (misses, demoted). */
WITH cand AS MATERIALIZED
(
    SELECT
        f.query_plan_xml,
        f.query_plan_digest,
        f.delta_worker_time,
        /* #7886: one row per plan. query_stats holds one row per query per snapshot, and every snapshot row of a
           cached plan resolves to the same plan XML. Without this rank the hottest plan fills all ten slots and is
           parsed once per snapshot, so PLAN_WARNING's warning_count and critical_count multiply by its snapshot
           count and the two facts cover one to three plans instead of ten. The plan's costliest row with a plan
           stands for it, and the limit counts distinct plans. A plan is a statement's plan in one database:
           query_plan_hash is a shape hash that two statements with one shape share, as does one statement in two
           databases, and a hashless row's plan_handle is shared by every statement in its batch, so database_name
           and query_hash stay in the key. plan_id numbers the key so the steps below can match on it: the key's
           columns can be NULL, and an equality join on them would not match a NULL to a NULL. */
        DENSE_RANK() OVER (ORDER BY f.database_name, f.query_hash, COALESCE(f.query_plan_hash, f.plan_handle)) AS plan_id,
        ROW_NUMBER() OVER (PARTITION BY f.database_name, f.query_hash, COALESCE(f.query_plan_hash, f.plan_handle) ORDER BY f.delta_worker_time DESC) AS plan_rank
    /* #8570: the rank sorts the table's own thin columns and the plan's digest, not the plan. A plan under the 2 KB
       TOAST limit sits inline in the dimension row, so a join to the dimension here would sort every snapshot row
       with its whole plan (on 67,200 rows of 1.4 KB plans, 26 MB and 21 MB written to disk against 4.5 MB). */
    FROM query_stats AS f
    WHERE f.server_id = $1
    AND   f.collection_time >= $2
    AND   f.collection_time <= $3
    AND   (f.query_plan_xml IS NOT NULL OR f.query_plan_digest IS NOT NULL)
    /* #8570: the own column stays in this filter. 'query_plan_digest IS NOT NULL' alone implies the partial index
       ix_query_stats_digest_floor (collection_time only, rows with a digest), and the planner takes it over
       idx_query_stats_time (server_id, collection_time): it walks the window's rows of every server and fetches
       each one's heap page to test server_id (25,400 pages on a store of 18 servers, against 5,600 through the
       server index, for one server's 86,000-row window). */
),
hits AS MATERIALIZED
(
    /* Each plan's costliest row, in cost order, probed one at a time until ten have a plan. The sorted, fenced
       subquery feeds the lateral lookup row by row, so the limit stops the probes at the tenth hit. The LIMIT 1
       keeps each probe a primary-key lookup: without it the planner may hash the whole dimension. */
    SELECT t.plan_id, t.query_plan_xml, t.query_plan_digest, t.delta_worker_time
    FROM
    (
        SELECT c.plan_id, c.query_plan_xml, c.query_plan_digest, c.delta_worker_time
        FROM cand AS c
        WHERE c.plan_rank = 1
        ORDER BY c.delta_worker_time DESC
        OFFSET 0
    ) AS t
    LEFT JOIN LATERAL
    (
        SELECT 1 AS found
        FROM query_plan_dim AS p
        WHERE t.query_plan_xml IS NULL
        AND   p.digest = t.query_plan_digest
        AND   (p.query_plan_xml IS NOT NULL OR p.query_plan_gz IS NOT NULL)
        LIMIT 1
    ) AS has_plan ON true
    WHERE t.query_plan_xml IS NOT NULL OR has_plan.found IS NOT NULL
    ORDER BY t.delta_worker_time DESC
    LIMIT 10
),
misses AS MATERIALIZED
(
    /* The plans that are not hits and may still place: every one when there are fewer than ten hits, else those
       whose costliest row costs at least the tenth hit or has no cost. Such a plan may still place through a cheaper
       row that has a plan, or through a costliest row tied with the tenth hit that the walk did not reach. A plan
       whose costliest row costs less cannot place: none of its rows costs more. A NULL cost sorts first, so the
       tenth hit's cost is the hits' lowest that is not NULL (min skips NULLs), and a plan whose costliest row has no
       cost was probed before it. When the tenth hit has no cost either, a missed plan can at best tie it, and the
       per-row order breaks that tie arbitrarily too: none qualifies, so rows with no cost, however many, are not
       probed again. */
    SELECT c.plan_id
    FROM cand AS c
    CROSS JOIN (SELECT count(*) AS hit_count, min(h.delta_worker_time) AS floor_cost FROM hits AS h) AS w
    WHERE c.plan_rank = 1
    AND   c.plan_id NOT IN (SELECT h.plan_id FROM hits AS h)
    AND   (w.hit_count < 10
           OR (w.floor_cost IS NOT NULL AND (c.delta_worker_time IS NULL OR c.delta_worker_time >= w.floor_cost)))
),
demoted AS
(
    /* Each missed plan's costliest row that has a plan, over all its rows. */
    SELECT DISTINCT ON (c.plan_id) c.query_plan_xml, c.query_plan_digest, c.delta_worker_time
    FROM
    (
        SELECT c.plan_id, c.query_plan_xml, c.query_plan_digest, c.delta_worker_time
        FROM cand AS c
        WHERE c.plan_id IN (SELECT m.plan_id FROM misses AS m)
        OFFSET 0
    ) AS c
    LEFT JOIN LATERAL
    (
        SELECT 1 AS found
        FROM query_plan_dim AS p
        WHERE c.query_plan_xml IS NULL
        AND   p.digest = c.query_plan_digest
        AND   (p.query_plan_xml IS NOT NULL OR p.query_plan_gz IS NOT NULL)
        LIMIT 1
    ) AS has_plan ON true
    WHERE c.query_plan_xml IS NOT NULL OR has_plan.found IS NOT NULL
    ORDER BY c.plan_id, c.delta_worker_time DESC
)
SELECT COALESCE(r.query_plan_xml, d.query_plan_xml) AS query_plan_xml, d.query_plan_gz
FROM
(
    SELECT u.query_plan_xml, u.query_plan_digest, u.delta_worker_time
    FROM
    (
        SELECT h.query_plan_xml, h.query_plan_digest, h.delta_worker_time FROM hits AS h
        UNION ALL
        SELECT m.query_plan_xml, m.query_plan_digest, m.delta_worker_time FROM demoted AS m
    ) AS u
    ORDER BY u.delta_worker_time DESC
    LIMIT 10
) AS r
/* #2069: a plan in gzip bytes (query_plan_gz) has the text column NULL, so the projection carries BOTH forms and
   the C# side resolves text-else-gz (PayloadDimensions.ResolveContent). The ten winners' content is read here, by
   their digest, after the limit. */
LEFT JOIN query_plan_dim AS d ON d.digest = r.query_plan_digest
ORDER BY r.delta_worker_time DESC";

    [Fact]
    public Task APlan_WhoseCostliestRowHasNoCost_StillRanksFirst() =>
        WithSeedAsync(async (connection, ct) =>
        {
            /* Plan 1 has rows costing 500, NULL and 700. Eleven other plans cost 1,000 and up, so a group that took max() (700) would not place at all. */
            await SeedDimensionAsync(connection, ct, Enumerable.Range(1, 12).ToArray());
            await InsertAsync(connection, ct, key: 1, tag: 1, costs: [500, null, 700]);
            for (var key = 2; key <= 12; key++)
            {
                await InsertAsync(connection, ct, key, tag: key, costs: [1000 + (key * 10), 1000 + (key * 10) + 1]);
            }

            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, ct);
            Assert.Equal(10, actual.Count);
            Assert.Equal("<ShowPlanXML>plan 1</ShowPlanXML>|<null>", actual[0]);
            Assert.Equal(await RowsAsync(connection, RowRankedSql, ct), actual);
        });

    [Fact]
    public Task APlan_WhoseCostliestDigestHasNoDimensionRow_IsRepresentedByItsOtherDigest() =>
        WithSeedAsync(async (connection, ct) =>
        {
            /* Eleven plans cost 11 to 121, so ten hits fill the walk and the tenth hit's cost is 22. Plan 100 has two digests: its costliest rows (900, 800) point at
               a digest the dimension no longer holds, cheaper rows (95, 90) at digest 100, which has text. The plan is a miss of the walk, and its other digest
               stands for it, ahead of the plan costing 22. */
            await SeedDimensionAsync(connection, ct, Enumerable.Range(1, 11).Append(100).ToArray());
            for (var key = 1; key <= 11; key++)
            {
                await InsertAsync(connection, ct, key, tag: key, costs: [key * 11]);
            }

            await InsertAsync(connection, ct, key: 100, tag: 9999, costs: [900, 800]);
            await InsertAsync(connection, ct, key: 100, tag: 100, costs: [95, 90]);

            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, ct);
            Assert.Equal(10, actual.Count);
            Assert.Contains("<ShowPlanXML>plan 100</ShowPlanXML>|<null>", actual);
            Assert.DoesNotContain("<ShowPlanXML>plan 2</ShowPlanXML>|<null>", actual);
            Assert.Equal(await RowsAsync(connection, RowRankedSql, ct), actual);
        });

    [Fact]
    public Task RowsWithInlinePlanText_GroupByTheirOwnText() =>
        WithSeedAsync(async (connection, ct) =>
        {
            /* One plan key, two inline texts and no digest. The text of the costliest row (500, in the second snapshot of text A) represents the key, once, and the
               other text, whose rows cost less, is not returned. A second key's one text over several snapshots returns once. */
            await InsertAsync(connection, ct, key: 1, tag: 0, costs: [300, 500], inlineXml: "<ShowPlanXML>inline A</ShowPlanXML>");
            await InsertAsync(connection, ct, key: 1, tag: 0, costs: [450, 400], inlineXml: "<ShowPlanXML>inline B</ShowPlanXML>");
            await InsertAsync(connection, ct, key: 2, tag: 0, costs: [100, 120, 110, 105], inlineXml: "<ShowPlanXML>inline C</ShowPlanXML>");

            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, ct);
            Assert.Equal(["<ShowPlanXML>inline A</ShowPlanXML>|<null>", "<ShowPlanXML>inline C</ShowPlanXML>|<null>"], actual);
            Assert.Equal(await RowsAsync(connection, RowRankedSql, ct), actual);
        });

    [Fact]
    public Task TenDistinctPlansComeBack_WhenOneHotPlanHasManySnapshots() =>
        WithSeedAsync(async (connection, ct) =>
        {
            /* Plan 1 is the hottest by far and has 150 snapshots. Fourteen other plans have three each. */
            await SeedDimensionAsync(connection, ct, Enumerable.Range(1, 15).ToArray());
            await InsertAsync(connection, ct, key: 1, tag: 1, costs: Enumerable.Range(0, 150).Select(n => (long?)(10_000_000 + n)).ToArray());
            for (var key = 2; key <= 15; key++)
            {
                await InsertAsync(connection, ct, key, tag: key, costs: [key * 100, (key * 100) + 1, (key * 100) + 2]);
            }

            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, ct);
            Assert.Equal(10, actual.Count);
            Assert.Equal(10, actual.Distinct().Count());
            Assert.Equal("<ShowPlanXML>plan 1</ShowPlanXML>|<null>", actual[0]);
            Assert.Equal(await RowsAsync(connection, RowRankedSql, ct), actual);
        });

    [Fact]
    public Task TheGroupedStatement_ReturnsTheRowRankedStatementsReply_OnOneSeed() =>
        WithSeedAsync(async (connection, ct) =>
        {
            /* Twenty-two plans, several snapshots each, distinct costs. Plan 3 also has a NULL-cost row. Plan 5 has a second digest whose rows cost most and that has
               no dimension row. Plan 7 carries inline text on its costliest rows and a digest on the others. Plan 9's costliest digest has no dimension row at all.
               Plan 30 has two rows that cost 1,000,000 and 1,000,050. */
            await SeedDimensionAsync(connection, ct, Enumerable.Range(1, 22).Append(30).ToArray());
            for (var key = 1; key <= 22; key++)
            {
                await InsertAsync(connection, ct, key, tag: key, costs: Enumerable.Range(0, 4 + (key % 3)).Select(n => (long?)((key * 1000) + (n * 37))).ToArray());
            }

            await InsertAsync(connection, ct, key: 3, tag: 3, costs: [null]);
            await InsertAsync(connection, ct, key: 5, tag: 9999, costs: [900_000]);
            await InsertAsync(connection, ct, key: 7, tag: 0, costs: [2_000_000, 1_900_000], inlineXml: "<ShowPlanXML>inline seven</ShowPlanXML>");
            await InsertAsync(connection, ct, key: 9, tag: 9998, costs: [5_000_000, 4_000_000]);
            await InsertAsync(connection, ct, key: 30, tag: 30, costs: [1_000_000, 1_000_050]);

            var expected = await RowsAsync(connection, RowRankedSql, ct);
            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, ct);
            Assert.Equal(10, expected.Count);
            Assert.Equal(expected, actual);
        });

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var end = LiveClock.Now();
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end.AddDays(-7), DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end.AddMinutes(5), DateTimeKind.Unspecified) });
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var text = reader.IsDBNull(0) ? "<null>" : reader.GetString(0);
            var gz = reader.IsDBNull(1) ? "<null>" : Convert.ToHexString(reader.GetFieldValue<byte[]>(1));
            rows.Add(text + "|" + gz);
        }

        return rows;
    }

    /// <summary>One dimension row per tag: digest sha256('a9495-' + tag), text plan 'plan tag'.</summary>
    private static async Task SeedDimensionAsync(NpgsqlConnection connection, CancellationToken ct, int[] tags)
    {
        foreach (var tag in tags)
        {
            await ExecAsync(connection, ct,
                "INSERT INTO query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen) VALUES (sha256(convert_to('a9495-' || $1::text, 'UTF8')), '<ShowPlanXML>plan ' || $1::text || '</ShowPlanXML>', NULL, now() AT TIME ZONE 'UTC')",
                tag);
        }
    }

    private static int _collectionOffset;

    /// <summary>
    /// One snapshot row per cost for plan key <paramref name="key"/>, on the digest of <paramref name="tag"/> (tag 0 carries none), newest snapshot first.
    /// A tag with no dimension row is a digest the dimension no longer holds.
    /// </summary>
    private static async Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, int key, int tag, long?[] costs, string? inlineXml = null)
    {
        var end = LiveClock.Now();
        for (var snapshot = 0; snapshot < costs.Length; snapshot++)
        {
            await using var command = new NpgsqlCommand(
                @"INSERT INTO query_stats
                      (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                       query_text, query_plan_xml, query_plan_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                       delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                       min_dop, max_dop, sample_interval_seconds)
                  VALUES ($1, $2, $3, 'a9495', 'PlanDb', '0xH' || $4::text, '0xP' || $4::text, '0xS' || $4::text, '0xL' || $4::text,
                          NULL, $5, CASE WHEN $6::int = 0 THEN NULL ELSE sha256(convert_to('a9495-' || $6::text, 'UTF8')) END,
                          10, $7, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600)",
                connection) { CommandTimeout = 120 };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = CollectionIdGenerator.Next() + Interlocked.Increment(ref _collectionOffset) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end.AddMinutes(-30 - snapshot), DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = key });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)inlineXml ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = tag });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)costs[snapshot] ?? DBNull.Value });
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task WithSeedAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live plan tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var succeeded = false;
        try
        {
            await body(connection, ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await ExecAsync(cleanupConnection, cleanupCt, "DELETE FROM query_stats WHERE server_id = 9495"));
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        foreach (var value in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
