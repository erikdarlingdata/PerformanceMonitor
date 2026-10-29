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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6798: the Job History read's per-job statistics (<c>job_stats</c> in <see cref="DarlingJobHistoryReader.BuildJobHistorySql"/>)
/// read each listed server's whole run-time range through <c>idx_job_history_server_run</c> and dropped the step rows, the
/// failures and the other jobs after fetching them from the heap. V220 adds <c>idx_job_history_job_success</c>, a partial index on
/// the successful job outcomes keyed by (server_id, job_id, run_datetime), and <c>job_stats</c> now hands each server's listed
/// jobs in as an array (<c>job_id = ANY (...)</c>) so the job is a seek key. This file pins three things: the rung builds the index on a migrated store, the
/// shipped read seeks that index on the job, and the rows are the same as the pre-#6798 text's rows.
/// </summary>
[Collection("live-postgres")]
public sealed class JobHistoryJobStatsSeekLiveTests
{
    private const int RungVersion = 220;
    private const string IndexName = "idx_job_history_job_success";

    /* Three servers: two with a server_properties offset and one with none (offset 0). */
    private const int ServerMinus240 = -679_801;
    private const int ServerPlus60 = -679_802;
    private const int ServerNoOffset = -679_803;

    private static readonly int[] AllServerIds = [ServerMinus240, ServerPlus60, ServerNoOffset];

    /// <summary>
    /// The pre-#6798 <c>job_stats</c> CTE, copied verbatim from origin/dev (fleet-wide form, floor $2): the row oracle for the
    /// equivalence fact. The rest of the statement is the shipped text, so the comparison isolates the CTE this fix changed.
    /// </summary>
    private const string OldJobStatsCte = """
job_stats AS (
    SELECT
        so.server_id,
        js.job_id,
        js.avg_success_duration,
        js.last_success_run_local
    FROM (SELECT DISTINCT server_id FROM base) AS b
    JOIN server_offsets AS so ON so.server_id = b.server_id
    CROSS JOIN LATERAL (
        SELECT
            jh.job_id,
            AVG(jh.run_duration_seconds) AS avg_success_duration,
            MAX(jh.run_datetime) AS last_success_run_local
        FROM job_history AS jh
        WHERE jh.server_id = so.server_id
        AND   jh.step_id = 0
        AND   jh.run_status = 1
        AND   jh.collection_time >= $2
        AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes)
        AND   jh.job_id IN (SELECT job_id FROM base WHERE base.server_id = so.server_id)
        GROUP BY jh.job_id
    ) AS js
)
""";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheRung_IsRegisteredOnce_AtItsOwnVersion_AndTheLadderTopAgreesWithTheStorageVersion()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Name == "job-history-success-index");

        Assert.Equal(RungVersion, rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
    }

    /// <summary>
    /// After migrate the index exists with the partial predicate and key the read seeks on, and a second migrate is a no-op. RED on
    /// origin/dev: no rung builds the index.
    /// </summary>
    [Fact]
    public async Task AfterMigrate_TheIndexExists_AndARerunIsIdempotent()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V220 schema pin.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var command = new NpgsqlCommand(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'job_history' AND indexname = $1", connection);
        command.Parameters.AddWithValue(IndexName);
        var definition = (string?)await command.ExecuteScalarAsync(ct);
        Assert.NotNull(definition);
        Assert.Contains("(server_id, job_id, run_datetime DESC)", definition, StringComparison.Ordinal);
        Assert.Contains("INCLUDE (run_duration_seconds, collection_time)", definition, StringComparison.Ordinal);
        Assert.Contains("WHERE ((step_id = 0) AND (run_status = 1))", definition, StringComparison.Ordinal);
    }

    /// <summary>
    /// The shipped fleet read's statistics seek <c>idx_job_history_job_success</c> with the job in the index condition, so each
    /// aggregate reads one job's successful outcomes and nothing else. RED on origin/dev: no rung builds the index there, so the plan
    /// cannot name it. A plan-shape pin, not a timing: the slow cold read is the heap pages this shape never visits. dev's
    /// <c>job_stats</c> is not checked against the same assertion, because on a store this small the planner can turn its
    /// <c>job_id IN (...)</c> into an index condition too; on a store at field scale it did not (the PR's measured plans).
    /// </summary>
    [Fact]
    public async Task TheFleetRead_SeeksTheSuccessIndexOnTheJob()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #6798 job-history live tests (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PrepareAsync(connection, scratch.ConnectionString, ct);

        var now = AnchoredNow();
        var sinceUtc = now.AddHours(-24);
        await SeedAsync(connection, now, ct);

        var shipped = DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false);
        var newPlan = await ExplainFleetAsync(connection, shipped, sinceUtc, ct);
        Assert.True(SeeksTheIndexOnTheJob(newPlan), $"expected job_stats to seek {IndexName} on job_id:\n{newPlan}");
    }

    /// <summary>
    /// The rows the shipped read returns equal the rows the pre-#6798 <c>job_stats</c> gives on the same store: every column, the
    /// order, the last success and the long-running flag. The store holds what the CTE change could get wrong: a job with no
    /// successful run in the window (NULL stats, and no stats row), a job whose only successes lack a
    /// duration (a last success with a NULL average), a long run, a success before the window, step rows and failures, and three
    /// server offsets. Checked at the 100-row cap and at a cap larger than the store, with and without an end instant.
    /// </summary>
    [Fact]
    public async Task TheRows_EqualThePreFixStatement()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #6798 job-history live tests (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PrepareAsync(connection, scratch.ConnectionString, ct);

        var now = AnchoredNow();
        var sinceUtc = now.AddHours(-24);
        await SeedAsync(connection, now, ct);

        foreach (var limit in new[] { 100, 5000 })
        {
            var shipped = DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false);
            var newRows = await ReadRawAsync(connection, shipped, sinceUtc, limit, null, ct);
            var oldRows = await ReadRawAsync(connection, WithOldJobStats(shipped), sinceUtc, limit, null, ct);
            Assert.True(newRows.Count > 0, "the seeded store returned no rows");
            Assert.Equal(oldRows, newRows);
            Assert.Contains(newRows, r => r.Contains("|long=True", StringComparison.Ordinal));
            Assert.Contains(newRows, r => r.Contains("|last=|", StringComparison.Ordinal));
        }

        /* An end instant adds the stats bound to job_stats ($4 in the fleet form); the old CTE gets the same clause in the same place. */
        var until = now.AddHours(-6);
        var filter = new JobHistoryFilter { UntilUtc = until };
        var shippedBounded = DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false, filter);
        var oldBounded = WithOldJobStats(shippedBounded, filter.StatsClause(4));
        var newBoundedRows = await ReadRawAsync(connection, shippedBounded, sinceUtc, 100, filter, ct);
        var oldBoundedRows = await ReadRawAsync(connection, oldBounded, sinceUtc, 100, filter, ct);
        Assert.True(newBoundedRows.Count > 0, "the bounded read returned no rows");
        Assert.Equal(oldBoundedRows, newBoundedRows);
    }

    private static bool SeeksTheIndexOnTheJob(string plan)
    {
        var lines = plan.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (!lines[i].Contains(IndexName, StringComparison.Ordinal) || !lines[i].Contains("Scan using", StringComparison.Ordinal))
            {
                continue;
            }

            for (var j = i + 1; j < lines.Length && !lines[j].Contains("->", StringComparison.Ordinal); j++)
            {
                if (lines[j].Contains("Index Cond:", StringComparison.Ordinal) && lines[j].Contains("job_id = ", StringComparison.Ordinal))
                {
                    return true;
                }
            }
        }
        return false;
    }

    /// <summary>
    /// The shipped statement with its <c>job_stats</c> swapped for dev's (plus the end-instant clause when one is set). Both sides
    /// are cut to LF first: <c>.gitattributes</c> checks the reader out CRLF on Windows, so the shipped literal carries CRLF there
    /// and LF here, and a search for an LF anchor in CRLF text finds nothing (the round 1 CI failure).
    /// </summary>
    private static string WithOldJobStats(string shipped, string statsUntilSql = "")
    {
        shipped = ToLf(shipped);
        var start = shipped.IndexOf("job_stats AS (", StringComparison.Ordinal);
        var end = start < 0 ? -1 : shipped.IndexOf("\nSELECT\n    base.server_id", start, StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the shipped statement no longer has the job_stats CTE this test swaps");
        var old = ToLf(OldJobStatsCte).TrimEnd();
        const string WindowLine = "AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes)\n";
        Assert.Contains(WindowLine, old, StringComparison.Ordinal);
        old = old.Replace(WindowLine, WindowLine[..^1] + statsUntilSql + "\n", StringComparison.Ordinal);
        var swapped = shipped[..start] + old + shipped[end..];
        Assert.Contains("GROUP BY jh.job_id", swapped, StringComparison.Ordinal);
        Assert.NotEqual(shipped, swapped);
        return swapped;
    }

    private static string ToLf(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>
    /// The seed's "now" (#5608): <see cref="LiveClock.AnchoredEnd"/>, so the 30 hours of rows cross a chunk boundary at the same place
    /// whatever hour the test runs. The read takes its window from the bound <c>$1</c>, never from the database clock.
    /// </summary>
    private static DateTime AnchoredNow() => DateTime.SpecifyKind(LiveClock.AnchoredEnd(), DateTimeKind.Utc);

    private static async Task PrepareAsync(NpgsqlConnection connection, string connectionString, CancellationToken ct)
    {
        await CleanupAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(connectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerMinus240, "darling-6798-a", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerPlus60, "darling-6798-b", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerNoOffset, "darling-6798-c", ct);
        await InsertServerPropertiesAsync(connection, ServerMinus240, -240, ct);
        await InsertServerPropertiesAsync(connection, ServerPlus60, 60, ct);

        var id = 0L;
        var instance = 0L;
        foreach (var (serverId, offset) in new[] { (ServerMinus240, -240), (ServerPlus60, 60), (ServerNoOffset, 0) })
        {
            /* Six jobs per server. job_0 runs every 10 minutes for 30 hours with four steps, so it crosses the window's start and
               fills the cap. job_1 runs every two hours, and its newest run is a long one. job_2 only fails. job_3's successes have no duration. job_4 last
               succeeded before the window. job_5 runs every 20 minutes with retries. */
            for (var minutes = 0; minutes < 30 * 60; minutes += 10)
            {
                var job = minutes % 60 == 0 ? 1 : minutes % 20 == 0 ? 5 : 0;
                if (job == 1 && minutes / 60 % 2 == 1)
                {
                    continue;
                }

                var utc = now.AddMinutes(-minutes - 3);
                var status = job == 5 && minutes % 100 == 0 ? 0 : 1;
                var duration = job == 1 && minutes == 0 ? 900L : 30L + minutes % 7;
                for (var step = 0; step < 4; step++)
                {
                    await InsertJobHistoryAsync(connection, ct, ++id, serverId, $"job_{job}", step, step == 0 ? status : 1,
                        step == 0 ? duration : 5, ++instance, utc, offset);
                }
            }

            for (var hours = 0; hours < 20; hours += 3)
            {
                var utc = now.AddHours(-hours).AddMinutes(-1);
                await InsertJobHistoryAsync(connection, ct, ++id, serverId, "job_2", 0, 0, 12, ++instance, utc, offset);
                await InsertJobHistoryAsync(connection, ct, ++id, serverId, "job_3", 0, 1, null, ++instance, utc.AddMinutes(1), offset);
                await InsertJobHistoryAsync(connection, ct, ++id, serverId, "job_4", 0, 0, 40, ++instance, utc.AddMinutes(2), offset);
            }

            await InsertJobHistoryAsync(connection, ct, ++id, serverId, "job_4", 0, 1, 40, ++instance, now.AddHours(-30), offset);
        }

        await using var analyze = new NpgsqlCommand("ANALYZE job_history", connection);
        await analyze.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ExplainFleetAsync(NpgsqlConnection connection, string sql, DateTime sinceUtc, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        /* A seeded store is small enough that a sequential or bitmap scan can cost less than any index; with both off the planner
           picks among the indexes, which is the choice this pin is about. */
        await using (var force = new NpgsqlCommand("SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off;", connection, transaction))
        {
            await force.ExecuteNonQueryAsync(ct);
        }

        await using var command = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + sql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingMcpTestData.Naive(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 100 });
        var plan = new StringBuilder();
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                plan.AppendLine(reader.GetString(0));
            }
        }
        await transaction.RollbackAsync(ct);
        return plan.ToString().Replace("\r\n", "\n", StringComparison.Ordinal);
    }

    /// <summary>Every column of every row, in the statement's order, as one string per row.</summary>
    private static async Task<List<string>> ReadRawAsync(
        NpgsqlConnection connection, string sql, DateTime sinceUtc, int limit, JobHistoryFilter? filter, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingMcpTestData.Naive(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        if (filter is not null)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingMcpTestData.Naive(filter.UntilUtc!.Value) });
        }

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var row = new StringBuilder();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var value = reader.IsDBNull(i) ? "" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture);
                row.Append(i == 15 ? "|last=" : i == 16 ? "|long=" : "|").Append(value);
            }
            row.Append('|');
            rows.Add(row.ToString());
        }
        return rows;
    }

    private static async Task InsertServerPropertiesAsync(NpgsqlConnection connection, int serverId, int offsetMinutes, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes)
              VALUES ($1, $2, $3, $4, $5)",
            6_798_000_000L + serverId, DarlingMcpTestData.Naive(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc)), serverId,
            "srv" + serverId.ToString(CultureInfo.InvariantCulture), offsetMinutes);

    /// <summary>One run row. <paramref name="runUtc"/> is the run's UTC instant: the row stores it on the server's clock, and the
    /// collector picks it up five minutes later, as it does in the field.</summary>
    private static async Task InsertJobHistoryAsync(
        NpgsqlConnection connection, CancellationToken ct, long id, int serverId, string jobId, int stepId, int runStatus,
        long? durationSeconds, long instanceId, DateTime runUtc, int offsetMinutes) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id,
                                       job_id, job_name, job_enabled, step_id, step_name, run_status,
                                       run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
              VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,$9,$10,$11,$12,$13,0,'')",
            6_798_000_000L + id, DarlingMcpTestData.Naive(runUtc.AddMinutes(5)), serverId,
            "srv" + serverId.ToString(CultureInfo.InvariantCulture), instanceId, jobId, jobId, stepId,
            stepId == 0 ? "(Job outcome)" : $"Step {stepId}", runStatus, runStatus == 1 ? "Succeeded" : "Failed",
            DarlingMcpTestData.Naive(runUtc.AddMinutes(offsetMinutes)), durationSeconds is null ? DBNull.Value : durationSeconds.Value);

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", AllServerIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));
        foreach (var table in new[] { "job_history", "server_properties", "servers" })
        {
            await using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids})", connection);
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
