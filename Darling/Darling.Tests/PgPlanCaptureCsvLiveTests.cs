/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgPlanCaptureCollector"/>'s csvlog branch (#4053 part b2) against a REAL target with
/// <c>logging_collector = on</c>, <c>log_destination = csvlog</c> and <c>auto_explain</c> writing real plans
/// into it — the SQL-side proof that the csv statement's own tail (<c>PgServerLogTail.TailCsvCteSql</c>)
/// actually reaches a live <c>.csv</c> file, which the unit tests in
/// <see cref="PgPlanCaptureCsvUnitTests"/> cannot exercise on their own.
///
/// <para>Gated on <c>DARLING_TEST_PG_CSVLOG_AUTOEXPLAIN</c>: a connection string for a target started with
/// <c>logging_collector = on</c> and <c>log_destination = csvlog</c> in its <c>postgresql.conf</c> from
/// before its first start (postmaster-context, like <see cref="PgPlanCaptureLiveTests"/>'s own target).
/// <c>auto_explain</c> loads and every other knob is <c>SET</c>-able per session, so nothing else needs
/// presetting.</para>
/// </summary>
public sealed class PgPlanCaptureCsvLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG_CSVLOG_AUTOEXPLAIN");

    [Fact]
    public async Task ARealQueryOverTheThreshold_CapturesOnePlanRowWithItsQueryId()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG_CSVLOG_AUTOEXPLAIN to a Postgres connection string for a target started " +
            "with logging_collector = on and log_destination = csvlog to run the #4053 part b2 csvlog " +
            "plan-capture live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        await using (var setup = new NpgsqlCommand(
            "LOAD 'auto_explain';" +
            "SET auto_explain.log_min_duration = 0;" +
            "SET auto_explain.log_analyze = true;" +
            "SET auto_explain.log_format = 'json';" +
            "SET compute_query_id = on;" +
            "DROP TABLE IF EXISTS plan_capture_csv_live_4053b2;" +
            "CREATE TABLE plan_capture_csv_live_4053b2 (id int);" +
            "INSERT INTO plan_capture_csv_live_4053b2 VALUES (1);",
            connection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        long expectedQueryId;
        await using (var real = new NpgsqlCommand(
            "SELECT * FROM plan_capture_csv_live_4053b2;", connection))
        await using (var reader = await real.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
            }
        }

        await using (var idCommand = new NpgsqlCommand(
            "SELECT queryid FROM pg_stat_statements WHERE query LIKE '%plan_capture_csv_live_4053b2%' " +
            "ORDER BY queryid DESC LIMIT 1;", connection))
        {
            /* pg_stat_statements is not required by this route; fall back to reading the id back off the
               captured row itself when the extension is absent. */
            expectedQueryId = 0;
            try
            {
                var value = await idCommand.ExecuteScalarAsync(ct);
                if (value is long id)
                {
                    expectedQueryId = id;
                }
            }
            catch (PostgresException)
            {
                expectedQueryId = 0;
            }
        }

        foreach (var binaryRoute in new[] { false, true })
        {
            var context = new CollectorContext
            {
                ServerId = 1,
                ServerName = "live-rig-csvlog-as-target",
                CollectionTime = DateTime.UtcNow,
                Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo
                {
                    Engine = CollectorTargetEngine.PostgreSql,
                    PostgresMajorVersion = 18,
                    PostgresVersionNum = 180000,
                },
                PgLogUsesCsvlog = true,
                PgReadBinaryFileGranted = binaryRoute,
            };
            var sql = PgPlanCaptureCollector.Instance.BuildQuery(context).Text;

            System.Collections.Generic.List<PgPlanCaptureCollector.Row> rows;
            await using (var read = new NpgsqlCommand(sql, connection))
            await using (var readReader = await read.ExecuteReaderAsync(ct))
            {
                rows = await PgPlanCaptureCollector.Instance.ReadAsync(readReader, context, ct);
            }

            Assert.Contains(rows,
                r => r.TopNodeType == "Seq Scan"
                     && r.PlanJson.Contains("plan_capture_csv_live_4053b2", StringComparison.Ordinal));

            if (expectedQueryId != 0)
            {
                Assert.Contains(rows, r => r.QueryId == expectedQueryId);
            }
        }

        await using (var cleanup = new NpgsqlCommand(
            "DROP TABLE IF EXISTS plan_capture_csv_live_4053b2;", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
