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
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5812 bound 2, a manual rig (skipped unless <c>DARLING_NOSPILL_RIG_PLANS</c> is set): does the PLAN_REGRESSION daily read
/// spill to disk at field scale once its live half is bounded by the watermarks? It plants one server with fifteen built
/// days of daily totals (<c>DARLING_NOSPILL_RIG_PLANS</c> plans a day, two per query; today built through 10:00) and one open
/// hour of interval rows after today's watermark, and runs the read under <c>EXPLAIN (ANALYZE, FORMAT JSON)</c> at the
/// server's own <c>work_mem</c>, at each weighed bound, and inside a transaction that runs the collector's
/// <see cref="PgFactCollector.PlanRegressionDailyWorkMemSql"/>. It fails if that last run has a Sort that reports
/// <c>Sort Space Type: Disk</c>, a Hash of more than one batch, or a HashAggregate of more than one batch or any disk use. It prints the rows, the execution time and every sort and hash node.
/// Plant it once on a dev rig; it is not a CI test.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It CREATEs and DROPs its own database through
   ScratchPostgres and works entirely inside it. */
public sealed class PlanRegressionDailyNoSpillRig
{
    private const int ServerA = -5812501;

    /// <summary>The bounds weighed against <see cref="PgFactCollector.PlanRegressionDailyWorkMemSql"/> (64MB): the smaller one,
    /// and the 128MB it replaced; measured and printed, never asserted.</summary>
    private static readonly string[] WeighedWorkMem = ["32MB", "128MB"];

    [Fact]
    public async Task Measure()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var plansText = Environment.GetEnvironmentVariable("DARLING_NOSPILL_RIG_PLANS");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs) || string.IsNullOrEmpty(plansText),
            "A manual rig: set DARLING_TEST_PG and DARLING_NOSPILL_RIG_PLANS (plans per day, e.g. 24000) to plant and measure it.");
        var plans = int.Parse(plansText!, CultureInfo.InvariantCulture);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var today = LiveClock.Now().Date; // #5608
        var window = today.AddDays(-14);
        var watermark = today.AddHours(10);

        /* Fifteen days of totals (T-14 through today), each valid; today built through 10:00. */
        await ExecAsync(connection, @"
INSERT INTO collect.plan_regression_daily
    (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash, execs, cpu_us_sum, dur_us_sum, last_exec, is_forced_plan, force_failure_count)
SELECT @s, d.d::date, 'spilldb', (n + 1) / 2, n, NULL, 'hash' || n, 100, 100 * 10000 * (1 + (n % 2) * 3), 100 * 30000, d.d + interval '12 hours', false, 0
FROM generate_series(@f::timestamp, @t::timestamp, interval '1 day') AS d (d)
CROSS JOIN generate_series(1, @p) AS n",
            ("s", ServerA), ("f", window), ("t", today), ("p", plans));
        await ExecAsync(connection, @"
INSERT INTO collect.plan_regression_daily_built (server_id, day, late_seq, built_seq, built_at, source_rows, built_through)
SELECT @s, d.d::date, 0, 0, d.d, 0, CASE WHEN d.d = @t THEN @w ELSE d.d + interval '1 day' END
FROM generate_series(@f::timestamp, @t::timestamp, interval '1 day') AS d (d)",
            ("s", ServerA), ("f", window), ("t", today), ("w", watermark));
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES (@s, @f, @f)",
            ("s", ServerA), ("f", window.AddDays(-5)));

        /* The open hour: every plan ran again after today's watermark. */
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late");
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
SELECT @s, 'spilldb', (n + 1) / 2, n, NULL, 1, @w + (n % 50) * interval '1 minute', @w + interval '55 minutes',
       'hash' || n, 'qh' || ((n + 1) / 2), 10, 10000 * (1 + (n % 2) * 3), 30000, @w + (n % 50) * interval '1 minute' + interval '5 minutes', false, 0
FROM generate_series(1, @p) AS n", ("s", ServerA), ("w", watermark), ("p", plans));
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late");
        await ExecAsync(connection, "ANALYZE collect.plan_regression_daily");
        await ExecAsync(connection, "ANALYZE collect.plan_regression_daily_built");
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_latest");

        var built = new List<PlanRegressionDaily.BuiltDay>();
        await using (var days = new NpgsqlCommand(PlanRegressionDaily.BuiltDaysSql, connection))
        {
            days.Parameters.AddWithValue(ServerA);
            days.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionWindowFloor(window));
            await using var reader = await days.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                built.Add(new PlanRegressionDaily.BuiltDay(reader.GetFieldValue<DateOnly>(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1)));
            }
        }

        Assert.Equal(15, built.Count);

        /* Before: the server's own work_mem, outside any transaction. Then each smaller bound that was weighed against the
           collector's, printed only, so the choice can be measured again on one plant. After: inside a transaction that runs
           the read's own SET LOCAL (PlanRegressionDailyWorkMemSql), as the collector does. One plant, every run. */
        var before = await ExplainAsync(connection, null, window, built, plans, ct);
        foreach (var weighed in WeighedWorkMem)
        {
            await using var trial = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
            await ExecAsync(connection, $"SET LOCAL work_mem = '{weighed}'");
            await ExplainAsync(connection, trial, window, built, plans, ct);
            await trial.RollbackAsync(ct);
        }

        await using (var snapshot = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct))
        {
            await ExecAsync(connection, PgFactCollector.PlanRegressionDailyWorkMemSql);
            var after = await ExplainAsync(connection, snapshot, window, built, plans, ct);
            await snapshot.RollbackAsync(ct);
            Assert.True(after.Spills.Count == 0, "the read spilled with the collector's work_mem: " + string.Join("; ", after.Spills) + " (before: " + before.Line + ")");
        }
    }

    private sealed record Run(string Line, List<string> Spills);

    private static async Task<Run> ExplainAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, DateTime window, List<PlanRegressionDaily.BuiltDay> built, int plans, System.Threading.CancellationToken ct)
    {
        var workMem = await TextAsync(connection, "SHOW work_mem");
        var floor = PgFactCollector.PlanRegressionWindowFloor(window);
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + PgFactCollector.PlanRegressionDailySql, connection, transaction) { CommandTimeout = 600 };
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = built.Select(b => b.Day).ToArray() });
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionLiveFloorWithWatermarks(floor, built));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor.AddDays(-1));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = built.Select(b => b.ThroughOrDayEnd).ToArray() });
        var root = JsonDocument.Parse((string)(await command.ExecuteScalarAsync(ct))!).RootElement[0];

        var nodes = new List<string>();
        var spills = new List<string>();
        Walk(root.GetProperty("Plan"), nodes, spills);
        var line = string.Create(CultureInfo.InvariantCulture,
            $"no-spill rig: daily_rows={15L * plans} open_hour_rows={plans} work_mem={workMem} execution_ms={root.GetProperty("Execution Time").GetDouble():F0} spill={(spills.Count > 0 ? "yes" : "no")} nodes=[{string.Join("; ", nodes)}]");
        TestContext.Current.SendDiagnosticMessage(line);
        Console.WriteLine(line);
        return new Run(line, spills);
    }

    /// <summary>Every Sort, Hash and Aggregate node, and the ones that went to disk.</summary>
    private static void Walk(JsonElement node, List<string> nodes, List<string> spills)
    {
        var type = node.GetProperty("Node Type").GetString()!;
        var strategy = node.TryGetProperty("Strategy", out var s) ? s.GetString() : null;
        var rows = node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble();
        if (type == "Sort")
        {
            var space = node.TryGetProperty("Sort Space Type", out var t) ? t.GetString() : "?";
            var method = node.TryGetProperty("Sort Method", out var m) ? m.GetString() : "?";
            nodes.Add(string.Create(CultureInfo.InvariantCulture, $"Sort {method} {space} {Number(node, "Sort Space Used")}kB rows={rows}"));
            if (space == "Disk") spills.Add("Sort " + method);
        }
        else if (type == "Hash")
        {
            var batches = Number(node, "Hash Batches");
            nodes.Add(string.Create(CultureInfo.InvariantCulture, $"Hash batches={batches} peak={Number(node, "Peak Memory Usage")}kB rows={rows}"));
            if (batches > 1) spills.Add("Hash batches=" + batches.ToString(CultureInfo.InvariantCulture));
        }
        else if (type == "Aggregate" && strategy == "Hashed")
        {
            var batches = Number(node, "HashAgg Batches");
            var disk = Number(node, "Disk Usage");
            nodes.Add(string.Create(CultureInfo.InvariantCulture, $"HashAggregate batches={batches} disk={disk}kB peak={Number(node, "Peak Memory Usage")}kB rows={rows}"));
            if (batches > 1 || disk > 0) spills.Add("HashAggregate batches=" + batches.ToString(CultureInfo.InvariantCulture));
        }
        else if (type == "WindowAgg" && node.TryGetProperty("Storage", out var storage))
        {
            nodes.Add($"WindowAgg storage={storage.GetString()}");
            if (storage.GetString() == "Disk") spills.Add("WindowAgg");
        }

        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray()) Walk(child, nodes, spills);
        }
    }

    private static double Number(JsonElement node, string name) => node.TryGetProperty(name, out var v) ? v.GetDouble() : 0;

    private static async Task<string> TextAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        foreach (var (name, value) in parameters)
        {
            if (value is DateTime t) command.Parameters.AddWithValue(name, NpgsqlDbType.Timestamp, t);
            else command.Parameters.AddWithValue(name, value);
        }

        await command.ExecuteNonQueryAsync();
    }
}
