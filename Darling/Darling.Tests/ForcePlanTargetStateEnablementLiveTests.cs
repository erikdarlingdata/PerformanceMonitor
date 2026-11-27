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
using System.Text.RegularExpressions;
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
/// #8570: the force-plan target read took the newest <c>plan_correction</c> row per database by numbering every row of the
/// server's whole lookback and keeping the first, then joined only the target databases. It now probes the newest row of each
/// target database and stops. This test seeds a store with many captures per database, a database that last appeared before the
/// window, a database that never appeared, a NULL state row and a second server, and asserts two things: the shipped statement
/// returns exactly the rows the old statement (kept below as the reference) returns, and it touches fewer buffers.
/// </summary>
public sealed class ForcePlanTargetStateEnablementLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live force-plan target test (it mints its own scratch database).";

    private const int ServerId = 1;

    /* The statement as it was before #8570, kept as the reference. Only the enablement CTE differs from the shipped one. */
    private const string ReferenceTemplate = @"
WITH targets(database_name, query_id, plan_id) AS (
    VALUES {0}
),
target_queries AS (
    SELECT DISTINCT database_name, query_id FROM targets
),
per_collection AS (
    SELECT
        qs.database_name,
        qs.query_id,
        qs.plan_id,
        qs.collection_time,
        MAX(CASE WHEN qs.is_forced_plan THEN 1 ELSE 0 END) AS forced,
        MAX(COALESCE(qs.plan_forcing_type, '')) AS forcing_type,
        MAX(COALESCE(qs.force_failure_count, 0)) AS failures,
        MAX(COALESCE(qs.last_force_failure_reason, '')) AS reason
    FROM target_queries AS tq
    JOIN query_store_stats AS qs
      ON  qs.server_id = $1
      AND qs.database_name = tq.database_name
      AND qs.query_id = tq.query_id
      AND qs.collection_time > $2
    GROUP BY qs.database_name, qs.query_id, qs.plan_id, qs.collection_time
),
plan_latest AS (
    SELECT
        pc.*,
        ROW_NUMBER() OVER (PARTITION BY pc.database_name, pc.query_id, pc.plan_id ORDER BY pc.collection_time DESC) AS rn
    FROM per_collection AS pc
),
other_forced AS (
    SELECT
        pl.database_name,
        pl.query_id,
        pl.plan_id,
        pl.forcing_type,
        pl.collection_time,
        ROW_NUMBER() OVER (PARTITION BY pl.database_name, pl.query_id ORDER BY pl.collection_time DESC) AS rn
    FROM plan_latest AS pl
    WHERE pl.rn = 1
    AND   pl.forced = 1
),
apc_latest AS (
    SELECT
        p.database_name,
        p.query_id,
        p.recommendation_state,
        p.recommendation_state_reason,
        p.regressed_plan_id,
        p.last_good_plan_id,
        p.last_good_plan_forcing_type,
        p.last_good_plan_is_forced,
        p.last_good_plan_force_failure_reason,
        p.execute_action_initiated_by,
        p.collection_time,
        ROW_NUMBER() OVER (PARTITION BY p.database_name, p.query_id ORDER BY p.collection_time DESC, p.score DESC) AS rn
    FROM target_queries AS tq
    JOIN plan_correction AS p
      ON  p.server_id = $1
      AND p.database_name = tq.database_name
      AND p.query_id = tq.query_id
      AND p.collection_time > $2
    WHERE p.recommendation_name IS NOT NULL
),
enablement AS (
    SELECT
        el.database_name,
        el.force_last_good_plan_actual_state,
        el.collection_time
    FROM (
        SELECT
            e.database_name,
            e.force_last_good_plan_actual_state,
            e.collection_time,
            ROW_NUMBER() OVER (PARTITION BY e.database_name ORDER BY e.collection_time DESC) AS rn
        FROM plan_correction AS e
        WHERE e.server_id = $1
        AND   e.collection_time > $2
    ) AS el
    WHERE el.rn = 1
)
SELECT
    t.database_name,
    t.query_id,
    t.plan_id,
    tp.forced,
    tp.forcing_type,
    tp.failures,
    tp.reason,
    tp.collection_time AS plan_observed_at,
    op.plan_id AS other_forced_plan_id,
    op.forcing_type AS other_forced_plan_forcing_type,
    op.collection_time AS other_forced_plan_observed_at,
    a.recommendation_state,
    a.recommendation_state_reason,
    a.regressed_plan_id,
    a.last_good_plan_id,
    a.last_good_plan_forcing_type,
    a.last_good_plan_is_forced,
    a.last_good_plan_force_failure_reason,
    a.execute_action_initiated_by,
    a.collection_time AS apc_observed_at,
    en.force_last_good_plan_actual_state,
    en.collection_time AS enablement_observed_at
FROM targets AS t
LEFT JOIN plan_latest AS tp
  ON  tp.database_name = t.database_name
  AND tp.query_id = t.query_id
  AND tp.plan_id = t.plan_id
  AND tp.rn = 1
LEFT JOIN other_forced AS op
  ON  op.database_name = t.database_name
  AND op.query_id = t.query_id
  AND op.plan_id <> t.plan_id
  AND op.rn = 1
LEFT JOIN apc_latest AS a
  ON  a.database_name = t.database_name
  AND a.query_id = t.query_id
  AND a.rn = 1
LEFT JOIN enablement AS en
  ON  en.database_name = t.database_name
ORDER BY t.database_name, t.query_id, t.plan_id";

    private static readonly ForcePlanTarget[] Targets =
    [
        new("alpha", 1, 10), new("beta", 2, 20), new("gamma", 3, 30), new("delta", 4, 40), new("eps", 5, 50),
    ];

    /* Every database here has a row in the newest hour, so the shipped statement never needs its fallback pass. */
    private static readonly ForcePlanTarget[] Present =
    [
        new("alpha", 1, 10), new("f1", 2, 20), new("f2", 3, 30),
    ];

    [Fact]
    public async Task Enablement_IsTheNewestRowPerTargetDatabase_MatchesTheOldStatement_AndTouchesFewerBuffers()
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
                await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
                await stop.ExecuteNonQueryAsync(ct);
            }

            await PgTableTuning.ApplyAsync(connection, null, ct);
            await SeedAsync(connection, now, ct);

            var lookbackStart = DateTime.SpecifyKind(now - ForcePlanTargetState.Lookback, DateTimeKind.Unspecified);
            var reference = await ReadRowsAsync(connection, ReferenceSql(Targets.Length), lookbackStart, Targets, explain: false, ct);
            var shipped = await ReadRowsAsync(connection, DarlingForcePlanTargetStateReader.BuildSql(Targets.Length), lookbackStart, Targets, explain: false, ct);
            Assert.Equal(Targets.Length, reference.Rows.Count);
            Assert.Equal(reference.Rows, shipped.Rows);

            /* The answer the store itself gives, through the shipped reader: the newest capture's state per database. */
            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            var states = await DarlingForcePlanTargetStateReader.ReadAsync(dataSource, ServerId, Targets, now, 60, ct);
            var alpha = states[ForcePlanTargetKey.Of(Targets[0])];
            Assert.Equal("On", alpha.ForceLastGoodPlanActualState);
            Assert.Equal(DateTime.SpecifyKind(now, DateTimeKind.Utc), alpha.EnablementObservedAtUtc);
            var beta = states[ForcePlanTargetKey.Of(Targets[1])];
            Assert.Equal("Off", beta.ForceLastGoodPlanActualState);
            Assert.Equal(DateTime.SpecifyKind(now.AddHours(-3), DateTimeKind.Utc), beta.EnablementObservedAtUtc);
            foreach (var none in new[] { Targets[2], Targets[3] })
            {
                var state = states[ForcePlanTargetKey.Of(none)];
                Assert.Null(state.ForceLastGoodPlanActualState);
                Assert.Null(state.EnablementObservedAtUtc);
            }

            var eps = states[ForcePlanTargetKey.Of(Targets[4])];
            Assert.Null(eps.ForceLastGoodPlanActualState);
            Assert.NotNull(eps.EnablementObservedAtUtc);

            /* The old statement numbered every row of the day. With every database found in the newest hour the shipped one
               stops at the first row of each. The databases above with no recent row take its fallback pass, which the row
               comparison covers: it is the old pass for those databases, and buffer counts do not compare it fairly (an index
               probe counts a buffer per row, the old sequential read counts a page). */
            var referenceFast = await ReadRowsAsync(connection, ReferenceSql(Present.Length), lookbackStart, Present, explain: true, ct);
            var shippedFast = await ReadRowsAsync(connection, DarlingForcePlanTargetStateReader.BuildSql(Present.Length), lookbackStart, Present, explain: true, ct);
            Assert.True(shippedFast.Buffers * 4 < referenceFast.Buffers * 3,
                $"the shipped statement touched {shippedFast.Buffers} buffers against the old statement's {referenceFast.Buffers}\n{shippedFast.PlanText}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static string ReferenceSql(int targetCount)
    {
        var rows = new StringBuilder();
        for (var i = 0; i < targetCount; i++)
        {
            if (i > 0) rows.Append(", ");
            rows.Append(DarlingForcePlanTargetStateReader.ValuesRow(i));
        }

        return ReferenceTemplate.Replace("{0}", rows.ToString(), StringComparison.Ordinal);
    }

    /// <summary>The window's captures, five minutes apart, newest at <paramref name="now"/>: ten databases with 31 rows a capture (a
    /// recommendation row per query plus the enablement-only row), so the whole day is about 90,000 rows.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO plan_correction
    (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_actual_state,
     recommendation_name, recommendation_type, recommendation_state, recommendation_state_reason, score,
     query_id, regressed_plan_id, last_good_plan_id, last_good_plan_forcing_type, last_good_plan_is_forced, execute_action_initiated_by)
SELECT 1000000 + row_number() OVER (), x.t, 1, 'srv1', x.db,
       CASE
           WHEN x.db = 'alpha' THEN CASE WHEN x.c = 0 THEN 'On' ELSE 'Off' END
           WHEN x.db = 'beta'  THEN CASE WHEN x.c = 36 THEN 'Off' ELSE 'On' END
           ELSE 'On'
       END,
       CASE WHEN x.r = 0 THEN NULL ELSE 'UQ_' || x.r END, 'FORCE_LAST_GOOD_PLAN', 'Active', 'Regression', 50 + x.r,
       CASE WHEN x.r = 0 THEN NULL ELSE x.r END, x.r * 7 + 1000, x.r * 7, 'NONE', false, 'User'
FROM (
    SELECT c.c, ($1::timestamp - (c.c * 5 || ' minutes')::interval) AS t, d.db, r.r
    FROM generate_series(0, 287) AS c(c)
    CROSS JOIN unnest(ARRAY['alpha','beta','f1','f2','f3','f4','f5','f6','f7','f8']) AS d(db)
    CROSS JOIN generate_series(0, 30) AS r(r)
    WHERE d.db <> 'beta' OR c.c >= 36
) AS x;

-- a database whose only row is older than the window, and one whose newest row carries no state
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_actual_state)
VALUES (2000001, $1::timestamp - interval '2 days', 1, 'srv1', 'gamma', 'On'),
       (2000002, $1::timestamp - interval '2 hours', 1, 'srv1', 'eps', NULL),
       (2000003, $1::timestamp - interval '3 hours', 1, 'srv1', 'eps', 'On');

-- another server, newer than every row above, that the server filter has to keep out
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_actual_state)
VALUES (3000001, $1::timestamp, 2, 'srv2', 'alpha', 'Error'),
       (3000002, $1::timestamp, 2, 'srv2', 'delta', 'Error');";

        /* Npgsql sends a parameterised command as one statement, so each of the three goes on its own. */
        foreach (var statement in sql.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            await using var seed = new NpgsqlCommand(statement, connection);
            seed.Parameters.AddWithValue(DateTime.SpecifyKind(now, DateTimeKind.Unspecified));
            await seed.ExecuteNonQueryAsync(ct);
        }

        await using var analyze = new NpgsqlCommand("VACUUM (ANALYZE) plan_correction", connection);
        await analyze.ExecuteNonQueryAsync(ct);
    }

    private sealed record Read(List<string> Rows, long Buffers, string PlanText);

    private static async Task<Read> ReadRowsAsync(
        NpgsqlConnection connection, string sql, DateTime lookbackStart, ForcePlanTarget[] targets, bool explain, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(explain ? "EXPLAIN (ANALYZE, BUFFERS) " + sql : sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(lookbackStart);
        foreach (var target in targets)
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

        /* The root node's own Buffers line is the first one in the text. */
        var buffers = 0L;
        var match = Regex.Match(plan.ToString(), @"Buffers: shared([^\r\n]*)");
        if (match.Success)
        {
            foreach (Match part in Regex.Matches(match.Groups[1].Value, @"(?:hit|read)=(\d+)"))
            {
                buffers += long.Parse(part.Groups[1].Value, CultureInfo.InvariantCulture);
            }
        }

        return new Read(rows, buffers, plan.ToString());
    }
}
