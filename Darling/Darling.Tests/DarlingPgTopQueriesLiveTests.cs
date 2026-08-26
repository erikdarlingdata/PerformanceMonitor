/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
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
/// <c>get_pg_top_queries</c> EXECUTED against a real PostgreSQL (#2554), plus the wire shape of the id it
/// returns (#2548).
///
/// <para><b>Why this class had to exist at all.</b> Every other test on this read asserted against the SQL
/// STRING. Meanwhile the shipped query did not PARSE: #2219's <c>LEFT JOIN collect.pg_statement_text AS t</c>
/// put <c>t.queryid</c> in scope beside <c>differenced.queryid</c>, so the unqualified references became
/// ambiguous and PostgreSQL raised 42702 — a parse-time error, before a single row is examined. The read
/// therefore threw on EVERY call, on every engine including the Aurora one it exists for, from #2219 until
/// this fix, and a store with zero rows failed exactly the same way a full one did. Not one string assertion
/// could see it, and none did. **The deliverable here is the execution, not the assertions.**</para>
///
/// <para><b>The one-word fix would not have worked.</b> Qualifying only the <c>GROUP BY</c> — the obvious
/// repair, and the one the report proposed — still fails, on the select-list <c>queryid</c>, which is the
/// reference PostgreSQL actually names first. Measured before fixing, not guessed: both needed qualifying.
/// </para>
///
/// <para><b>And the capability gate on the THROW path.</b> The gate sat inside <c>if (rows.Count == 0)</c>,
/// so it could only speak when the query succeeded and returned nothing. On a target that excludes this
/// collector, a throw produced a raw SQL error where the sibling <c>get_pg_wait_stats</c> gives the honest
/// "does not run on that engine, and never will". (The excluded target used to be stock PostgreSQL; #2625
/// gave <c>pg_statement_stats</c> a vanilla <c>pg_stat_statements</c> path, so the gate is now exercised on
/// the DIALECT branch — a SQL Server target — and stock PostgreSQL joined Aurora as a capable engine whose
/// faults must still read as faults.) Guarding that is the awkward half of this class: once the ambiguity is fixed the read no
/// longer throws, so an assertion written against the ordinary empty path would pass with or WITHOUT the
/// catch-path fix and prove nothing. So the throw is INDUCED, deterministically and without touching shared
/// DDL, by seeding <c>delta_calls</c> at <c>bigint</c> extremes: <c>SUM()</c> over them widens to numeric and
/// the read's <c>CAST(... AS bigint)</c> overflows at runtime.</para>
///
/// <para><b>The discriminating half of that test is the Aurora case.</b> A "fix" that simply swallowed
/// exceptions into <c>not_collected</c> would satisfy the stock-PostgreSQL assertion perfectly. So the same
/// overflowing rows are asked for again as Aurora, where the collector CAN run, and the answer must still be
/// an error — a real fault on a capable engine must not be dressed up as a capability gap.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgTopQueriesLiveTests
{
    private const int ServerId = -853977;
    private const string ServerName = "pg-top-queries-e2e";

    /* Out of range for a double by construction (#2548), and negative, which is how most real
       pg_stat_statements ids look. */
    private const long OutOfRangeQueryId = -4185925123159566327L;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheReadExecutes_ReturnsItsQueryIdAsAnExactString_AndAnswersAThrowWithTheCapabilityGate()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live get_pg_top_queries test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.AuroraPostgres);

            /* ── the whole point: two snapshots, and the query must actually RUN ──

               Two, because every block and WAL figure is differenced across the window and a single sample
               has no interval to difference. delta_* carry the rate columns the way the collector stores
               them. */
            var t0 = MinutesAgo(30);
            var t1 = t0.AddSeconds(60);
            await SeedStatsAsync(connection, ct, t0, OutOfRangeQueryId, deltaCalls: 0, deltaTimeMs: 0, deltaRows: 0, walBytes: 1_000);
            await SeedStatsAsync(connection, ct, t1, OutOfRangeQueryId, deltaCalls: 40, deltaTimeMs: 900, deltaRows: 4_000, walBytes: 5_000);

            /* Text for that id, which is what introduced the join — and therefore the ambiguity. Seeded
               deliberately: an empty pg_statement_text would still exercise the LEFT JOIN, but a POPULATED
               one also proves the join actually matches, so a fix that made the query parse by dropping the
               join would fail here instead of passing quietly. */
            await SeedTextAsync(connection, ct, OutOfRangeQueryId, "SELECT * FROM public.widget WHERE id = $1");

            var json = await DarlingMcpPgTopQueries(dataSource);
            var root = JsonDocument.Parse(json).RootElement;

            /* Before anything else: it did not throw. Named explicitly, because "42702: column reference
               queryid is ambiguous" arriving inside a status:error envelope is exactly what shipped for
               months, and an assertion further down would report it as a missing property instead. */
            Assert.False(
                root.TryGetProperty("status", out var status) && status.GetString() == "error",
                $"the read threw instead of executing: {json}");

            var queries = root.GetProperty("queries").EnumerateArray().ToArray();
            var row = Assert.Single(queries);

            /* #2548: a string, and the EXACT digits. As a JSON number this id comes back
               -4185925123159566300 from any double-decoding parser, which joins to nothing. */
            var queryid = row.GetProperty("queryid");
            Assert.Equal(JsonValueKind.String, queryid.ValueKind);
            Assert.Equal(OutOfRangeQueryId.ToString(CultureInfo.InvariantCulture), queryid.GetString());

            /* The join landed. This is the assertion that separates "the query parses" from "the query does
               what #2219 added it for". */
            Assert.Equal("SELECT * FROM public.widget WHERE id = $1", row.GetProperty("query_text").GetString());

            /* Windowed arithmetic still correct after the qualification: the rate columns come from the
               stored deltas and the WAL figure is differenced across the two snapshots. */
            Assert.Equal(40, row.GetProperty("calls").GetInt64());
            Assert.Equal(900, row.GetProperty("total_exec_time_ms").GetInt64());
            Assert.Equal(4_000, row.GetProperty("rows_returned").GetInt64());
            Assert.Equal(4_000, row.GetProperty("wal_bytes").GetInt64());

            /* database_id stays a NUMBER beside the string id — an oid is unsigned 32-bit and cannot round,
               and a guard that let the whole payload become text would not notice. */
            Assert.Equal(JsonValueKind.Number, row.GetProperty("database_id").ValueKind);

            /* ── the throw path, and the gate that now covers it ──

               delta_calls at the bigint extremes: SUM() widens to numeric, and the read's
               CAST(SUM(delta_calls) AS bigint) overflows at RUNTIME. A deterministic fault that needs no
               DDL against the shared store. */
            await DeleteStatsAsync(connection, ct);
            await SeedStatsAsync(connection, ct, t0, OutOfRangeQueryId, deltaCalls: long.MaxValue, deltaTimeMs: 500, deltaRows: 1, walBytes: 0);
            await SeedStatsAsync(connection, ct, t1, OutOfRangeQueryId, deltaCalls: long.MaxValue, deltaTimeMs: 500, deltaRows: 1, walBytes: 0);

            /* Aurora FIRST, and this is the assertion that keeps the fix honest. The collector CAN run here,
               so a genuine fault must still read as a fault. A catch block that answered not_collected
               unconditionally would pass the stock-PostgreSQL case below while quietly converting every
               Aurora outage into "this engine cannot do that". */
            var auroraFault = JsonDocument.Parse(await DarlingMcpPgTopQueries(dataSource)).RootElement;
            Assert.Equal("error", auroraFault.GetProperty("status").GetString());

            /* Stock PostgreSQL is now a SECOND capable engine, not the gap case (#2625): pg_statement_stats
               reads the vanilla pg_stat_statements view here, so the same induced fault must still read as a
               fault. This assertion used to expect not_collected, and flipping it is the point — it is the
               one place in the suite that would have kept insisting stock PostgreSQL cannot answer this. */
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.Postgres);

            var stockFault = JsonDocument.Parse(await DarlingMcpPgTopQueries(dataSource)).RootElement;
            Assert.Equal("error", stockFault.GetProperty("status").GetString());

            /* And the catch-path GATE still has to speak, or #2532's fix rots the moment its only witness
               moves. With no PostgreSQL flavor left that excludes this collector, the surviving exclusion is
               the DIALECT one: a SQL Server target never receives a PostgreSQL collector at all, and the same
               throw must produce the sentence rather than the SQLSTATE. Different branch of the same gate,
               same guarantee. */
            await SetEngineKindAsync(connection, ct, MonitoredEngineKind.SqlServer);

            var dialectFault = JsonDocument.Parse(await DarlingMcpPgTopQueries(dataSource)).RootElement;
            Assert.Equal("not_collected", dialectFault.GetProperty("status").GetString());

            var gapText = dialectFault.GetProperty("message").GetString()!;
            Assert.Contains("pg_statement_stats", gapText, StringComparison.Ordinal);
            Assert.Contains("never will", gapText, StringComparison.Ordinal);

            /* The raw dialect error must not leak into the sentence — the whole complaint was that a caller
               got 42702 where its sibling got an explanation. */
            Assert.DoesNotContain("42702", gapText, StringComparison.Ordinal);
            Assert.DoesNotContain("out of range", gapText, StringComparison.OrdinalIgnoreCase);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<string> DarlingMcpPgTopQueries(NpgsqlDataSource dataSource) =>
        await DarlingMcpPgStatementTools.GetPgTopQueries(dataSource, ServerName, 4);

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task SetEngineKindAsync(NpgsqlConnection connection, CancellationToken ct, string kind) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, kind);

    private static async Task SeedStatsAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        long queryId, long deltaCalls, long deltaTimeMs, long deltaRows, long walBytes) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned,
     shared_blks_hit, shared_blks_read, storage_blks_read, orcache_blks_hit,
     temp_blks_read, temp_blks_written, wal_bytes, max_exec_peakmem_bytes,
     delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, $5, 16384, 10, TRUE,
        100, 5000, 91.5, 250,
        10, 5, 3, 2,
        0, 0, $6, 2097152,
        $7, $8, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            queryId, walBytes, deltaCalls, deltaTimeMs, deltaRows);

    private static async Task SeedTextAsync(
        NpgsqlConnection connection, CancellationToken ct, long queryId, string text) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
VALUES ($1, $2, $3, $4, $4)
ON CONFLICT (server_id, queryid) DO UPDATE SET query_text = EXCLUDED.query_text",
            ServerId, queryId, text, DarlingMcpTestData.Naive(DateTime.UtcNow));

    private static async Task DeleteStatsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM pg_statement_stats WHERE server_id = $1", ServerId);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_text WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
