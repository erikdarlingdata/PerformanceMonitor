/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6807: the wide-table read guard (#5582) sizes <c>query_store_interval_wide</c>, so it must run only for a panel whose compiled text
/// reads that table. A window older than <c>RetentionTierRouter.RawMaxAge</c> (3 days) routes to the corrected hourly rollup, which never
/// touches the wide table, so a fleet-sized wide-table estimate must not refuse it. The same store, with the same fleet-sized estimate,
/// still refuses a one-day panel that does read the wide table.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it. */
public sealed class ComposeQueryStoreCaggRouteGuardLiveTests
{
    private const string PanelJson =
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\","
        + "\"filters\":[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"mod1\"}]}";

    [Fact]
    public async Task AModuleFilteredSevenDayPanel_ReadsTheRollupAndIsNotRefused_WhileAOneDayWideTableReadOverTheLimitStillIs()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await ComposeStampLiveSupport.ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            /* A fleet-sized wide-table estimate: three servers at 3.2 M rows a day, 9.6 M a day, over the 6.8 M limit for a single day. */
            foreach (var server in new[] { 1, 2, 3 })
            {
                await ComposeStampLiveSupport.ExecAsync(connection,
                    "INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) "
                    + $"VALUES ({server}, DATE '{hourNow.AddDays(-1):yyyy-MM-dd}', 2, now() AT TIME ZONE 'UTC', 3200000)", ct);
                /* One old wide row per server puts the table's floor a purge-edge margin before any window below. */
                await ComposeStampLiveSupport.InsertRowAsync(connection, server, hourNow.AddDays(-20), 8_000_000 + server, ct);
            }

            /* The wide table's coverage claim reaches back past the seven-day window, so the interval table could serve it if it were asked. */
            await ComposeStampLiveSupport.ExecAsync(connection,
                $"UPDATE collect.query_store_interval_wide_coverage SET filled_since = TIMESTAMP '{ComposeStampLiveSupport.At(hourNow.AddDays(-10))}'", ct);

            /* Raw Query Store rows for eight days, and the corrected hourly rollup built over them, so the router has a rollup that
               covers the whole window. */
            await ComposeStampLiveSupport.ExecAsync(connection, $@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT row_number() OVER (), t.ct, 1, 'srv1', 'db0', 100 + q, 1000 + q, 'Regular', t.ct - interval '30 minutes', 'mod' || q, md5('h' || q), 10, 500, 300, 900, 700,
       'PRIMARY', (extract(epoch FROM t.ct)::bigint / 3600) + q, date_trunc('hour', t.ct)
FROM generate_series(TIMESTAMP '{ComposeStampLiveSupport.At(hourNow.AddDays(-8))}', TIMESTAMP '{ComposeStampLiveSupport.At(hourNow.AddHours(-1))}', interval '1 hour') AS t(ct)
CROSS JOIN generate_series(0, 2) AS q", ct);
            foreach (var view in new[] { TimescaleSupport.QueryStoreStatsIntervalHourlyView, TimescaleSupport.QueryStoreStatsCorrectedHourlyView })
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(hourNow.AddDays(-9));
                refresh.Parameters.AddWithValue(hourNow.AddDays(1));
                await refresh.ExecuteNonQueryAsync(ct);
            }


            /* The control: the one-day panel reads the wide table, and the same estimate refuses it. */
            var oneDay = new JsonObject { ["panel"] = JsonNode.Parse(PanelJson), ["hours"] = 24 };
            var refused = await DarlingWebEndpoints.RunComposedPanelAsync(source, oneDay, ct);
            Assert.True(refused.Payload is null, "a one-day panel that reads the wide table must be refused at this estimate; it ran");
            Assert.StartsWith("This panel needs about ", refused.Error, StringComparison.Ordinal);

            /* The panel: seven days, module-filtered, all servers. The router sends it to the rollup, so the wide-table guard does not apply. */
            var sevenDays = new JsonObject { ["panel"] = JsonNode.Parse(PanelJson), ["hours"] = 168 };
            var ran = await DarlingWebEndpoints.RunComposedPanelAsync(source, sevenDays, ct);
            Assert.True(ran.Payload is not null, "a seven-day panel that reads the rollup must not be refused by the wide-table guard: " + ran.Error);
            var sql = ran.Payload!["sql"]!.GetValue<string>();
            Assert.Contains("query_store_stats_corrected_hourly", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("query_store_interval_wide", sql, StringComparison.Ordinal);
            Assert.True(ran.Payload["rows"]!.AsArray().Count > 0, "the module-filtered rollup read must return the seeded module's hours");
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
