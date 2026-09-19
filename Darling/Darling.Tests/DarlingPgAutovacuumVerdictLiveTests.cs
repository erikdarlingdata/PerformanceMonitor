/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// get_pg_autovacuum_health end to end against a real store — the #3534 verdict seam.
///
/// <para><b>The test that carries this class is the append-only table.</b> The read ranks by
/// GREATEST(dead ratio, insert ratio), so a table ten times past its INSERT threshold with zero dead
/// tuples arrives as worst_table — and the tool used to classify it from the dead ratio alone, handing
/// the #1-ranked table severity "ok" with a 0 threshold_ratio. An agent reads that as "worst thing here
/// is fine" and moves on from the classic wraparound route the collector gathers inserts_since_vacuum
/// for in the first place. The projection arithmetic lives inline in the tool, so only a real round-trip
/// exercises it.</para>
///
/// <para><b>And the growth claim a single sample cannot make.</b> One reading means first == latest, and
/// dead_tuples_growing = false from it converts into "autovacuum blocked or not running" territory the
/// window cannot support. Null, with first_seen_at published so the caller can see how much history
/// stands behind the claim.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgAutovacuumVerdictLiveTests
{
    private const int ServerId = -853917;
    private const string ServerName = "pg-autovacuum-verdict-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task InsertDrivenWorstTableCarriesItsRankSeverity_AndOneSampleGrowthIsUnknown()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live autovacuum verdict test.");

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

            var t0 = MinutesAgo(30);
            var t1 = t0.AddMinutes(10);

            /* appendonly: zero dead tuples, 10x past its INSERT threshold. GREATEST ranks it #1; the
               dead-only severity called it "ok" (#3534's scenario, verbatim). */
            await SeedAsync(connection, ct, t0, "appendonly", deadTuples: 0, vacuumThreshold: 1_050,
                insertsSinceVacuum: 100_000, insertVacuumThreshold: 10_000);
            await SeedAsync(connection, ct, t1, "appendonly", deadTuples: 0, vacuumThreshold: 1_050,
                insertsSinceVacuum: 100_000, insertVacuumThreshold: 10_000);

            /* churny: past its dead threshold and climbing — the case the dead axis already handled,
               kept as the control that the insert axis widens the verdict rather than replacing it. */
            await SeedAsync(connection, ct, t0, "churny", deadTuples: 1_500, vacuumThreshold: 1_000,
                insertsSinceVacuum: 0, insertVacuumThreshold: 10_000);
            await SeedAsync(connection, ct, t1, "churny", deadTuples: 2_500, vacuumThreshold: 1_000,
                insertsSinceVacuum: 0, insertVacuumThreshold: 10_000);

            /* onesample: a single reading. Growth is unmeasurable, which is not the same as "not
               growing". */
            await SeedAsync(connection, ct, t1, "onesample", deadTuples: 3_000, vacuumThreshold: 1_000,
                insertsSinceVacuum: 0, insertVacuumThreshold: 10_000);

            var payload = JsonDocument.Parse(
                await DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(dataSource, ServerName, 4)).RootElement;

            Assert.Equal("tables_with_pending_maintenance", payload.GetProperty("status").GetString());

            var tables = payload.GetProperty("tables").EnumerateArray().ToArray();

            /* THE assertion: worst_table and its severity come from the SAME axis. Ten times past the
               insert threshold is critical, and the insert-side ratio is published beside the dead one
               so the figure the verdict came from is visible. */
            Assert.Equal("public.appendonly", payload.GetProperty("worst_table").GetString());
            Assert.Equal("critical_far_past_threshold", payload.GetProperty("worst_severity").GetString());

            var append = Row(tables, "public.appendonly");
            Assert.Equal("critical_far_past_threshold", append.GetProperty("severity").GetString());
            Assert.Equal(10.0, append.GetProperty("insert_threshold_ratio").GetDouble(), 2);
            Assert.Equal(0.0, append.GetProperty("threshold_ratio").GetDouble(), 2);

            /* The control: a dead-driven table classifies exactly as it always did. */
            var churny = Row(tables, "public.churny");
            Assert.Equal("warning_past_threshold_and_growing", churny.GetProperty("severity").GetString());
            Assert.True(churny.GetProperty("dead_tuples_growing").GetBoolean());
            Assert.Equal(1_000, churny.GetProperty("dead_tuple_change").GetInt64());

            /* One sample: growth and the change figure are NULL, and first_seen_at == measured_at says
               why — the window holds one reading, not a flat line. The band's plain label carries it. */
            var oneSample = Row(tables, "public.onesample");
            Assert.Equal(JsonValueKind.Null, oneSample.GetProperty("dead_tuples_growing").ValueKind);
            Assert.Equal(JsonValueKind.Null, oneSample.GetProperty("dead_tuple_change").ValueKind);
            Assert.Equal(
                oneSample.GetProperty("measured_at").GetDateTime(),
                oneSample.GetProperty("first_seen_at").GetDateTime());
            Assert.Equal("warning_past_threshold", oneSample.GetProperty("severity").GetString());

            /* The summary counts read both axes, growing counts only MEASURED growth, and the
               undersized call discloses its page scope (the get_pg_database_stats pattern). */
            Assert.Equal(3, payload.GetProperty("past_threshold_count").GetInt32());
            Assert.Equal(1, payload.GetProperty("growing_count").GetInt32());
            Assert.False(payload.GetProperty("truncated").GetBoolean());

            /* #3653: the cut is OBSERVED off a limit + 1 fetch (#3594), so a limit of exactly the population
               is complete and one below it is truncated with the page under its *_returned name. */
            var whole = JsonDocument.Parse(
                await DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(dataSource, ServerName, 4, tables.Length)).RootElement;
            Assert.False(whole.GetProperty("truncated").GetBoolean());
            Assert.Equal(tables.Length, whole.GetProperty("tables_returned").GetInt32());
            var truncated = JsonDocument.Parse(
                await DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(dataSource, ServerName, 4, 2)).RootElement;
            Assert.Equal(2, truncated.GetProperty("tables_returned").GetInt32());
            Assert.True(truncated.GetProperty("truncated").GetBoolean());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The row for one table, asserted present with a message naming what DID come back.</summary>
    private static JsonElement Row(JsonElement[] rows, string tableName)
    {
        var row = rows.FirstOrDefault(r => r.GetProperty("table_name").GetString() == tableName);

        Assert.True(
            row.ValueKind == JsonValueKind.Object,
            $"no row for '{tableName}' — the read returned [{string.Join(", ", rows.Select(r => r.GetProperty("table_name").GetString()))}]");

        return row;
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string tableName,
        long deadTuples, long vacuumThreshold, long insertsSinceVacuum, long insertVacuumThreshold) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_autovacuum_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name,
     live_tuples, dead_tuples, vacuum_threshold, mods_since_analyze, analyze_threshold,
     inserts_since_vacuum, insert_vacuum_threshold, autovacuum_disabled, total_bytes, autovacuum_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc),
            ServerId, ServerName, "appdb", "public", tableName,
            10_000L, deadTuples, vacuumThreshold, 0L, 500L,
            insertsSinceVacuum, insertVacuumThreshold, false, 1_000_000L, 1L);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_autovacuum_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
