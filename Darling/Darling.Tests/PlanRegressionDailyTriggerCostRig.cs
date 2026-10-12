/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5448 cost rig: what the late-row trigger costs the interval table's apply, measured, not
/// asserted. It is a MEASUREMENT, so it skips unless <c>DARLING_COST_RIG_RUNS</c> is set (runs per mode; the plan says 50),
/// and it writes its numbers to <c>DARLING_COST_RIG_OUT</c> (a file path) rather than passing or failing on them.
///
/// <para>The scratch store is seeded with 5,000,000 rows (V153's recipe: <c>generate_series</c>, trigger disabled during
/// the seed so it does not mark 93 percent of them), one hour of intervals per 13,889 rows, newest hour first in the
/// heap's tail as a real store has them. Each run writes <c>MaxRowsPerDatabase</c> (50,000) raw rows, then times the real
/// <see cref="QueryStoreIntervalLatest.UpsertSql"/> in its own transaction (commit included) and reads the WAL
/// the statement wrote; the trigger is switched with <c>ALTER TABLE ... DISABLE/ENABLE TRIGGER</c>, and the two modes
/// alternate run by run so drift hits both.</para>
///
/// <para>The cases: S1 steady (intervals from the last four hours, never late); S2 steady with one-day intervals (rows
/// 24-48 h old refreshed each pass; the share that is late depends on the clock and is reported); S3 backfill (50,000
/// new rows, every one late); S3u (extra) every row an UPDATE of a late row. Pass lines (plan): S1 under 3 percent,
/// S2 under +15 percent, S3 under +60 percent and 2 s.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It CREATEs and DROPs its own database through
   ScratchPostgres and works entirely inside it, disabling a trigger on its own table, which must never happen to a
   shared store. */
public sealed class PlanRegressionDailyTriggerCostRig
{
    private const int ServerId = -5448100;
    private const int BatchRows = 50_000;
    private const int SeedRows = 5_000_000;
    private const int RowsPerHour = 13_889;
    private const string TriggerName = "trg_plan_regression_daily_late";

    private static readonly DateTime CollectionBase = new(2026, 2, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /* The hour every case measures from, taken once. The seed takes minutes, and a case that re-read now() would see a new
       hour when the seed straddled an hour boundary and find fewer rows than a batch (the S1 failure at 14:59 UTC, 41,627
       rows). The trigger's own late test still reads the real clock, which is what is being measured. */
    private static readonly string AnchorHour = "'" + DateTime.UtcNow.ToString("yyyy-MM-dd HH:00:00", CultureInfo.InvariantCulture) + "'::timestamp";

    [Fact]
    public async Task Measure()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var runsText = Environment.GetEnvironmentVariable("DARLING_COST_RIG_RUNS");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs) || string.IsNullOrEmpty(runsText),
            "A measurement: set DARLING_TEST_PG and DARLING_COST_RIG_RUNS (and DARLING_COST_RIG_OUT) to run the #5448 trigger cost rig.");
        var runs = int.Parse(runsText!, CultureInfo.InvariantCulture);
        var outPath = Environment.GetEnvironmentVariable("DARLING_COST_RIG_OUT") ?? Path.Combine(Path.GetTempPath(), "plan-regression-trigger-cost.txt");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var report = new StringBuilder();
        report.AppendLine(CultureInfo.InvariantCulture, $"runs per mode: {runs}; seed rows: {SeedRows}; batch rows: {BatchRows}; {DateTime.UtcNow:o}");

        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER " + TriggerName, ct);
        var seedWatch = Stopwatch.StartNew();
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_latest
    (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
     collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     last_execution_time, is_forced_plan, force_failure_count, query_text)
SELECT {ServerId}, 'db', g % {RowsPerHour}, g % {RowsPerHour}, NULL, g / {RowsPerHour},
       {AnchorHour} - ((359 - g / {RowsPerHour}) * interval '1 hour'),
       '2026-01-01'::timestamp, '0xAB', '0xCD', 10, 500, 1000,
       {AnchorHour} - ((359 - g / {RowsPerHour}) * interval '1 hour') + interval '30 minutes',
       false, 0, NULL
FROM generate_series(0, {SeedRows - 1}) AS g", ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_latest", ct);
        report.AppendLine(CultureInfo.InvariantCulture, $"seed: {seedWatch.Elapsed.TotalSeconds:F1} s, table {await ScalarLongAsync(connection, "SELECT pg_total_relation_size('collect.query_store_interval_latest')", ct) / 1048576} MB");

        /* h = hours old. The seed's interval id is 359 - hours-old, so the newest hour has the largest id. */
        var cases = new (string Name, string Note, Func<int, string> Source)[]
        {
            ("S1", "steady: intervals from the last 4 hours",
                run => RawFromTable($"first_execution_time >= {AnchorHour} - interval '3 hours'", run)),
            ("S2", "steady, 1-day intervals: rows 24-48 h old refreshed each pass",
                run => RawFromTable($"first_execution_time >= {AnchorHour} - interval '47 hours' AND first_execution_time < {AnchorHour} - interval '23 hours' AND query_id % 6 = 0", run)),
            ("S3", "backfill: 50,000 NEW rows, all late",
                run => RawNewLate(run)),
            ("S3u", "extra: every row an UPDATE of a late row",
                run => RawFromTable($"first_execution_time >= {AnchorHour} - interval '59 hours' AND first_execution_time < {AnchorHour} - interval '47 hours'", run)),
        };

        var only = Environment.GetEnvironmentVariable("DARLING_COST_RIG_CASES")?.Split(',');
        foreach (var (name, note, source) in cases.Where(c => only is null || only.Contains(c.Name)))
        {
            var off = new List<(double Ms, long Wal)>();
            var on = new List<(double Ms, long Wal)>();
            long lateRows = 0;
            for (var run = 1; run <= runs * 2; run++)
            {
                var triggerOn = run % 2 == 0;
                await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest "
                    + (triggerOn ? "ENABLE" : "DISABLE") + " TRIGGER " + TriggerName, ct);

                var seq = run + (name == "S3u" ? 1000 : name == "S2" ? 2000 : name == "S1" ? 3000 : 0);
                await ExecAsync(connection, "DELETE FROM collect.query_store_stats WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture), ct);
                await ExecAsync(connection, source(seq), ct);
                if (run == 1)
                {
                    lateRows = await ScalarLongAsync(connection,
                        "SELECT COUNT(*) FROM collect.query_store_stats WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture)
                        + " AND first_execution_time < date_trunc('day', now() AT TIME ZONE 'UTC') - interval '1 day'", ct);
                }

                var sample = await TimeUpsertAsync(connection, seq, ct);
                (triggerOn ? on : off).Add(sample);

                if (name == "S3")
                {
                    /* The rows were new: remove them (DELETE fires no trigger) so the next run inserts again. */
                    await ExecAsync(connection, "DELETE FROM collect.query_store_interval_latest WHERE server_id = "
                        + ServerId.ToString(CultureInfo.InvariantCulture) + " AND runtime_stats_interval_id >= 100000", ct);
                }
            }

            var built = await ScalarLongAsync(connection, "SELECT COALESCE(SUM(late_seq), 0) FROM collect.plan_regression_daily_built", ct);
            report.AppendLine(CultureInfo.InvariantCulture, $"{name} ({note}); late rows in the batch: {lateRows} of {BatchRows}");
            report.AppendLine(CultureInfo.InvariantCulture, $"  trigger off: median {Median(off.Select(s => s.Ms)):F1} ms, median WAL {Median(off.Select(s => (double)s.Wal)):F0} bytes, p90 {Percentile(off.Select(s => s.Ms), 0.9):F1} ms");
            report.AppendLine(CultureInfo.InvariantCulture, $"  trigger on : median {Median(on.Select(s => s.Ms)):F1} ms, median WAL {Median(on.Select(s => (double)s.Wal)):F0} bytes, p90 {Percentile(on.Select(s => s.Ms), 0.9):F1} ms");
            var msPct = (Median(on.Select(s => s.Ms)) / Median(off.Select(s => s.Ms)) - 1) * 100;
            var walPct = (Median(on.Select(s => (double)s.Wal)) / Median(off.Select(s => (double)s.Wal)) - 1) * 100;
            report.AppendLine(CultureInfo.InvariantCulture, $"  delta: {msPct:+0.0;-0.0} % time, {walPct:+0.0;-0.0} % WAL; built.late_seq total so far {built}");
            /* The machine this runs on is rarely quiet, and a median of wall times moves with it. The off and on runs alternate,
               so each adjacent pair saw nearly the same load: the median of the pairs' ratios and the fastest run of each mode
               are steadier than the two medians, and are reported beside them. */
            var pairs = off.Zip(on, (o, n) => (n.Ms / o.Ms - 1) * 100).ToList();
            report.AppendLine(CultureInfo.InvariantCulture, $"  steadier: fastest run off {off.Min(s => s.Ms):F1} ms, on {on.Min(s => s.Ms):F1} ms ({(on.Min(s => s.Ms) / off.Min(s => s.Ms) - 1) * 100:+0.0;-0.0} %); median of {pairs.Count} adjacent-pair deltas {Median(pairs):+0.0;-0.0} %");

            /* After every case, so a long run that is stopped early still leaves the cases it finished. */
            await File.WriteAllTextAsync(outPath, report.ToString(), ct);
        }
    }

    /// <summary>Raw rows copied from the table (identity kept), with a newer collection_time so the upsert's guard passes.</summary>
    private static string RawFromTable(string predicate, int run) => $@"
INSERT INTO collect.query_store_stats
    (collection_id, server_id, server_name, collection_time, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id,
     first_execution_time, last_execution_time, execution_type_desc, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, query_hash, is_forced_plan, force_failure_count)
SELECT 1, server_id, 'rig', '{CollectionBase:yyyy-MM-dd HH:mm:ss}'::timestamp + make_interval(secs => {run}), database_name, query_id, plan_id, replica_role,
       runtime_stats_interval_id, first_execution_time, last_execution_time + interval '1 minute', 'Regular', execution_count + 1,
       avg_cpu_time_us, avg_duration_us, query_plan_hash, query_hash, is_forced_plan, force_failure_count
FROM collect.query_store_interval_latest
WHERE server_id = {ServerId} AND {predicate}
LIMIT {BatchRows}";

    /// <summary>Raw rows with identities the table does not hold yet (a backfill slice), all older than two days.</summary>
    private static string RawNewLate(int run) => $@"
INSERT INTO collect.query_store_stats
    (collection_id, server_id, server_name, collection_time, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id,
     first_execution_time, last_execution_time, execution_type_desc, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, query_hash, is_forced_plan, force_failure_count)
SELECT g, {ServerId}, 'rig', '{CollectionBase:yyyy-MM-dd HH:mm:ss}'::timestamp + make_interval(secs => {run}), 'db', g, g, NULL, 100000 + {run},
       {AnchorHour} - ((48 + g % 300) * interval '1 hour'),
       {AnchorHour} - ((48 + g % 300) * interval '1 hour') + interval '30 minutes',
       'Regular', 5, 500, 1000, '0xAB', '0xCD', false, 0
FROM generate_series(1, {BatchRows}) AS g";

    private static async Task<(double Ms, long Wal)> TimeUpsertAsync(NpgsqlConnection connection, int run, CancellationToken ct)
    {
        var collectionTime = CollectionBase.AddSeconds(run);
        var lsnBefore = await ScalarStringAsync(connection, "SELECT pg_current_wal_lsn()::text", ct);

        var watch = Stopwatch.StartNew();
        await using (var transaction = await connection.BeginTransactionAsync(ct))
        {
            await using var command = new NpgsqlCommand(QueryStoreIntervalLatest.UpsertSql, connection, transaction) { CommandTimeout = 600 };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = new[] { "db" } });
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                await reader.ReadAsync(ct);
                Assert.Equal(BatchRows, reader.GetInt64(1));
                Assert.True(reader.GetInt64(0) > 0, "the upsert applied nothing, so the run measured nothing");
            }

            await transaction.CommitAsync(ct);
        }

        watch.Stop();
        var wal = await ScalarLongAsync(connection,
            "SELECT (pg_current_wal_lsn() - '" + lsnBefore + "'::pg_lsn)::bigint", ct);
        return (watch.Elapsed.TotalMilliseconds, wal);
    }

    private static double Median(IEnumerable<double> values)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted.Count == 0 ? 0 : sorted.Count % 2 == 1 ? sorted[sorted.Count / 2] : (sorted[(sorted.Count / 2) - 1] + sorted[sorted.Count / 2]) / 2;
    }

    private static double Percentile(IEnumerable<double> values, double p)
    {
        var sorted = values.OrderBy(v => v).ToList();
        return sorted.Count == 0 ? 0 : sorted[Math.Min(sorted.Count - 1, (int)(sorted.Count * p))];
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 1800 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<string> ScalarStringAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }
}
