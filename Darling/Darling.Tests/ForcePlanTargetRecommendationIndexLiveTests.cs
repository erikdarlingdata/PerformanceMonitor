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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #8570 (V280): the recommendation half of the force-plan target read took the server's whole lookback of
/// <c>collect.plan_correction</c> through <c>idx_plan_correction_time</c> and dropped the other queries' rows afterwards. V280's
/// <c>idx_plan_correction_server_db_query_time</c> leads on (server, database, query), so the read seeks the target queries. This test
/// seeds many captures of many databases and queries on two servers, migrates the store, and asserts three things: the migration
/// builds the index as a partial index on four key columns, the shipped statement's plan reads it, and the statement returns exactly
/// the rows it returns once the index is dropped.
/// </summary>
public sealed class ForcePlanTargetRecommendationIndexLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live force-plan recommendation index test (it mints its own scratch database).";

    private const string IndexName = "idx_plan_correction_server_db_query_time";

    private const int ServerId = 3;

    /* Targets span three databases, one query of each with many captures, and one query that was never captured. */
    private static readonly ForcePlanTarget[] Targets =
    [
        new("db0", 5, 1), new("db0", 12, 1), new("db1", 1005, 1), new("db2", 2010, 1), new("db3", 3999, 1),
    ];

    [Fact]
    public async Task RecommendationHalf_ReadsTheNewIndex_AndReturnsTheSameRowsWithoutIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
            var now = LiveClock.AnchoredEnd();

            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
            if (scratch.TimescaleEnabled)
            {
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
                await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", null, ct);
            }

            await PgTableTuning.ApplyAsync(connection, null, ct);

            /* The rung built it: present, partial, and four key columns wide. */
            await using (var probe = new NpgsqlCommand(
                $"SELECT indpred IS NOT NULL AND indnkeyatts = 4 FROM pg_index WHERE indexrelid = to_regclass('collect.{IndexName}')", connection))
            {
                Assert.True(await probe.ExecuteScalarAsync(ct) is true, $"V280 did not leave {IndexName} as a partial four-column index");
            }

            await SeedAsync(connection, now, ct);

            var lookbackStart = DateTime.SpecifyKind(now - ForcePlanTargetState.Lookback, DateTimeKind.Unspecified);
            var sql = DarlingForcePlanTargetStateReader.BuildSql(Targets.Length);

            var withIndex = await ReadAsync(connection, sql, lookbackStart, explain: false, ct);
            var withIndexPlan = await ReadAsync(connection, sql, lookbackStart, explain: true, ct);
            Assert.Contains(IndexName, withIndexPlan.PlanText, StringComparison.Ordinal);

            /* The answer is the newest recommendation row per target query, not the newest row of the query (a later row with no
               recommendation is ignored), and the query that was never captured has none. */
            Assert.Equal(Targets.Length, withIndex.Rows.Count);
            var db0Query5 = Assert.Single(withIndex.Rows, static r => r.StartsWith("db0|5|", StringComparison.Ordinal));
            Assert.Contains("State_95", db0Query5, StringComparison.Ordinal);
            Assert.DoesNotContain("State_96", db0Query5, StringComparison.Ordinal);
            var never = Assert.Single(withIndex.Rows, static r => r.StartsWith("db3|3999|", StringComparison.Ordinal));
            Assert.DoesNotContain("State_", never, StringComparison.Ordinal);

            await ExecuteAsync(connection, $"DROP INDEX collect.{IndexName}", null, ct);
            await ExecuteAsync(connection, "ANALYZE plan_correction", null, ct);
            var withoutIndex = await ReadAsync(connection, sql, lookbackStart, explain: false, ct);
            var withoutIndexPlan = await ReadAsync(connection, sql, lookbackStart, explain: true, ct);
            Assert.DoesNotContain(IndexName, withoutIndexPlan.PlanText, StringComparison.Ordinal);
            Assert.Equal(withoutIndex.Rows, withIndex.Rows);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Ten servers, eight databases of thirty queries each, a capture every fifteen minutes for the lookback (96 captures,
    /// the capture with row number k at <paramref name="now"/> less 1 hour less 15 minutes times k, so the newest is an hour old and carries state number 95), each capture's state named after its number so a
    /// wrong pick shows in the row. Plus, for one target query, a newer capture that carries no recommendation (the partial index
    /// leaves it out and the statement's own filter does too).</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        const string insertRecommendations = @"
INSERT INTO plan_correction
    (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_actual_state,
     recommendation_name, recommendation_type, recommendation_state, recommendation_state_reason, score,
     query_id, regressed_plan_id, last_good_plan_id, last_good_plan_forcing_type, last_good_plan_is_forced, execute_action_initiated_by)
SELECT 1, $1::timestamp - interval '1 hour' - (k.cc * 15 || ' minutes')::interval, s.sid, 'srv' || s.sid, 'db' || d.d, 'On',
       'UQ_' || d.d || '_' || q.q, 'FORCE_LAST_GOOD_PLAN', 'State_' || (95 - k.cc), 'Regression', 50 + q.q,
       d.d * 1000 + q.q, q.q * 7 + 1000, q.q * 7, 'NONE', false, 'User'
FROM generate_series(0, 95) AS k(cc)
CROSS JOIN generate_series(1, 10) AS s(sid)
CROSS JOIN generate_series(0, 7) AS d(d)
CROSS JOIN generate_series(1, 30) AS q(q)";

        const string insertNewerWithoutRecommendation = @"
INSERT INTO plan_correction
    (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_actual_state,
     recommendation_name, recommendation_state, score, query_id)
VALUES (9000001, $1::timestamp - interval '10 minutes', 3, 'srv3', 'db0', 'On', NULL, 'State_96', 99, 5)";

        foreach (var statement in new[] { insertRecommendations, insertNewerWithoutRecommendation })
        {
            await ExecuteAsync(connection, statement, DateTime.SpecifyKind(now, DateTimeKind.Unspecified), ct);
        }

        await ExecuteAsync(connection, "VACUUM (ANALYZE) plan_correction", null, ct);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, DateTime? anchor, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (anchor is not null)
        {
            command.Parameters.AddWithValue(anchor.Value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private sealed record Read(List<string> Rows, string PlanText);

    private static async Task<Read> ReadAsync(NpgsqlConnection connection, string sql, DateTime lookbackStart, bool explain, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(explain ? "EXPLAIN (ANALYZE, BUFFERS) " + sql : sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(lookbackStart);
        foreach (var target in Targets)
        {
            command.Parameters.AddWithValue(target.Database);
            command.Parameters.AddWithValue(target.QueryId);
            command.Parameters.AddWithValue(target.PlanId);
        }

        var rows = new List<string>();
        var plan = new StringBuilder();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (explain)
            {
                plan.AppendLine(reader.GetString(0));
                continue;
            }

            var cells = new string[reader.FieldCount];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "<null>";
            }

            rows.Add(string.Join("|", cells));
        }

        return new Read(rows, plan.ToString());
    }
}
