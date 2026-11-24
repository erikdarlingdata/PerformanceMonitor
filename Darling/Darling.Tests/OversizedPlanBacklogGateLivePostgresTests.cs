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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8789: <see cref="OversizedPlanBacklog.RecordSightingsSql"/> skips the ranking of the server's whole pending set when no new row
/// can rank past the cap, which is every cycle in normal running. The skip must change nothing: the same rows in the table, the same
/// number left out and the same return value as the statement that always ranks. The ungated statement is the product's own text with
/// the gate's second test made the same as its first.
/// </summary>
public sealed class OversizedPlanBacklogGateLivePostgresTests
{
    private const int GatedServer = 4701;
    private const int UngatedServer = 4702;
    private const int Cap = 300;
    private const int Limit = 200;
    private const string Database = "gate_db";

    /// <summary>
    /// The gate's second test counts the pending rows seen at or after the sighting time. Counting all of them instead makes it the
    /// first test again, so the statement ranks on every cycle over the cap, as it did before the gate.
    /// </summary>
    private static readonly string UngatedSql = Ungated();

    private static string Ungated()
    {
        const string gated = "count(*) FILTER (WHERE last_seen_at >= $10) AS recent_rows";
        Assert.Contains(gated, OversizedPlanBacklog.RecordSightingsSql, StringComparison.Ordinal);
        return OversizedPlanBacklog.RecordSightingsSql.Replace(gated, "count(*) AS recent_rows", StringComparison.Ordinal);
    }

    /// <summary>One shape: how the server's pending rows and one cycle's sightings are laid out.</summary>
    private sealed record Shape(
        string Name,
        int PendingOlder,
        int PendingAtOrAfter,
        int NewRows,
        int Resightings,
        int HashPool,
        bool DuplicateKeys,
        bool ExpectLeftOut);

    private static readonly Shape[] ShapeList =
    [
        /* The steady state: over the cap, every pending row seen before this cycle. The gate skips the ranking and nothing is left out. */
        new("over-cap-all-older", 320, 0, 40, 30, 0, false, false),
        new("over-cap-all-older-with-supersede", 320, 0, 40, 30, 25, true, false),
        /* More rows at or after the sighting time than the cap, so new rows ARE left out. */
        new("over-cap-recent-rows-leave-some-out", 100, 250, 150, 20, 0, false, true),
        new("over-cap-recent-rows-with-supersede", 100, 250, 150, 20, 25, true, true),
        /* Some pending rows at or after the sighting time but not enough to matter: the gate may rank, the answer is the same. */
        new("over-cap-few-recent-rows", 300, 30, 40, 30, 25, true, false),
        /* The incoming rows alone are past the cap. */
        new("incoming-alone-over-cap", 50, 0, 400, 10, 30, true, true),
    ];

    public static TheoryData<string, int> Shapes()
    {
        var data = new TheoryData<string, int>();
        foreach (var shape in ShapeList)
        {
            foreach (var seed in new[] { 11, 12, 13 })
            {
                data.Add(shape.Name, seed);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public async Task TheGatedStatement_WritesWhatTheRankingStatementWrites(string shapeName, int seed)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8789 gate test (it mints its own scratch database).");

        var shape = ShapeList.Single(s => s.Name == shapeName);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await MigrateAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(
            new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString);

        var now = new DateTime(2026, 11, 14, 12, 0, 0, DateTimeKind.Unspecified);
        var seenAt = now.AddMinutes(-1);
        var random = new Random(seed);

        var pending = BuildPending(shape, random, seenAt);
        var sightings = BuildSightings(shape, pending, random);
        foreach (var server in new[] { GatedServer, UngatedServer })
        {
            await SeedAsync(postgres, server, pending, ct);
        }

        var gatedWritten = await OversizedPlanBacklog.RecordSightingsAsync(
            postgres, GatedServer, OversizedPlanBacklog.QueryStatsCollectorName, sightings, seenAt, Cap, Limit, null, ct);
        var ungatedNotInserted = await RunAsync(postgres, UngatedSql, UngatedServer, sightings, seenAt, ct);

        Assert.Equal(sightings.Count - ungatedNotInserted, gatedWritten);
        Assert.Equal(await LeftOutAsync(postgres, UngatedServer, ct), await LeftOutAsync(postgres, GatedServer, ct));
        Assert.Equal(await TableRowsAsync(postgres, UngatedServer, ct), await TableRowsAsync(postgres, GatedServer, ct));

        if (shape.ExpectLeftOut)
        {
            Assert.True(ungatedNotInserted > 0, "the shape is meant to leave rows out, so the comparison covers the full path");
        }
        else
        {
            Assert.Equal(0, ungatedNotInserted);
        }
    }

    private sealed record PendingRow(
        string Collector, string PlanHandle, string SqlHandle, string? Hash, long Bytes, DateTime LastSeen, DateTime FirstSeen, bool Tried);

    /// <summary>
    /// The server's pending rows: this collector's, some of another, and a few tried rows. Rows "at or after" carry the sighting time
    /// exactly or a later one (clock skew between collectors). One row in five carries a hash, from a small pool when the shape wants supersede in play.
    /// </summary>
    private static List<PendingRow> BuildPending(Shape shape, Random random, DateTime seenAt)
    {
        var rows = new List<PendingRow>();

        for (var n = 0; n < shape.PendingOlder + shape.PendingAtOrAfter; n++)
        {
            var older = n < shape.PendingOlder;
            var collector = n % 5 == 0 ? "procedure_stats" : OversizedPlanBacklog.QueryStatsCollectorName;
            var lastSeen = older ? seenAt.AddMinutes(-1 - random.Next(600)) : seenAt.AddMinutes(random.Next(4));
            var firstSeen = lastSeen.AddMinutes(-random.Next(2000));
            rows.Add(new PendingRow(
                collector, "0xP" + n.ToString(CultureInfo.InvariantCulture), "0xPS" + n.ToString(CultureInfo.InvariantCulture),
                PickHash(shape, random), 1000 + random.Next(100_000), lastSeen, firstSeen, n % 97 == 3));
        }

        return rows;
    }

    private static string? PickHash(Shape shape, Random random) =>
        shape.HashPool == 0 || random.Next(5) != 0 ? null : "H_" + random.Next(shape.HashPool).ToString(CultureInfo.InvariantCulture);

    /// <summary>The cycle's sightings: re-sightings of query_stats rows the table holds, new rows, and keys given twice in one batch.</summary>
    private static List<OversizedPlanObservation> BuildSightings(Shape shape, List<PendingRow> pending, Random random)
    {
        var sightings = new List<OversizedPlanObservation>();

        var mine = pending.Where(p => p.Collector == OversizedPlanBacklog.QueryStatsCollectorName).ToList();
        foreach (var row in mine.OrderBy(_ => random.Next()).Take(shape.Resightings))
        {
            sightings.Add(new OversizedPlanObservation(
                row.PlanHandle, row.SqlHandle, 0, 400, Database, PickHash(shape, random) ?? row.Hash, 500 + random.Next(100_000)));
        }

        for (var n = 0; n < shape.NewRows; n++)
        {
            sightings.Add(new OversizedPlanObservation(
                "0xN" + n.ToString(CultureInfo.InvariantCulture), "0xNS" + n.ToString(CultureInfo.InvariantCulture), 0, 400, Database,
                PickHash(shape, random), 1000 + random.Next(1_000_000)));
        }

        if (shape.DuplicateKeys)
        {
            sightings.Add(new OversizedPlanObservation("0xN3", "0xNS3", 0, 400, null, null, 77_777));
            sightings.Add(new OversizedPlanObservation(mine[0].PlanHandle, mine[0].SqlHandle, 0, 400, null, null, 66_666));
        }

        return sightings;
    }

    private static async Task SeedAsync(NpgsqlDataSource postgres, int server, List<PendingRow> pending, CancellationToken ct)
    {
        await using var seed = postgres.CreateCommand(@"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, last_attempt_at)
SELECT $1, u.collector, u.plan_handle, u.sql_handle, 0, 400, 'gate_db', u.hash, u.bytes, u.first_seen, u.last_seen,
       CASE WHEN u.tried THEN u.last_seen END
FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[], $7::timestamp[], $8::timestamp[], $9::boolean[])
     AS u (collector, plan_handle, sql_handle, hash, bytes, last_seen, first_seen, tried);");
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = pending.Select(p => p.Collector).ToArray() });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = pending.Select(p => p.PlanHandle).ToArray() });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = pending.Select(p => p.SqlHandle).ToArray() });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = pending.Select(p => p.Hash).ToArray() });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = pending.Select(p => p.Bytes).ToArray() });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = pending.Select(p => p.LastSeen).ToArray() });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = pending.Select(p => p.FirstSeen).ToArray() });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Boolean, Value = pending.Select(p => p.Tried).ToArray() });
        await seed.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs a statement with the parameters <see cref="OversizedPlanBacklog.RecordSightingsAsync"/> binds and returns its scalar.</summary>
    private static async Task<int> RunAsync(
        NpgsqlDataSource postgres, string sql, int server, IReadOnlyList<OversizedPlanObservation> rows, DateTime seenAt, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(sql);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = OversizedPlanBacklog.QueryStatsCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.PlanHandle).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.SqlHandle).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = rows.Select(r => r.StatementStartOffset).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = rows.Select(r => r.StatementEndOffset).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.DatabaseName).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.QueryHash).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = rows.Select(r => r.ObservedBytes).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = seenAt });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Cap });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Limit });
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<long?> LeftOutAsync(NpgsqlDataSource postgres, int server, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(@"
SELECT state_value::bigint
FROM collect.collector_state
WHERE server_id = $1
AND   collector_name = 'oversized_plan_backlog'
AND   state_key = 'query_stats';");
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static async Task<List<string>> TableRowsAsync(NpgsqlDataSource postgres, int server, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(@"
SELECT (to_jsonb(b) - 'server_id')::text
FROM collect.oversized_plan_backlog AS b
WHERE server_id = $1
ORDER BY 1;");
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    private static async Task MigrateAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var migrate = new NpgsqlConnection(scratch.ConnectionString);
        await migrate.OpenAsync(ct);
        await PgMigrations.MigrateAsync(migrate, null, ct);

        await using var registry = new NpgsqlCommand(@"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (4701, 'gate-gated-server', TRUE), (4702, 'gate-ungated-server', TRUE);", migrate);
        await registry.ExecuteNonQueryAsync(ct);
    }
}
