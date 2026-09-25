/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4229 (Lite parity, the gap PR #4256 left open): <see cref="LocalDataService.GetJobHistoryAsync"/> replaced a
/// window function (<c>AVG</c>/<c>MAX</c> <c>OVER (PARTITION BY server_id, job_id)</c>, evaluated over every step
/// row the window matched) with a <c>job_stats</c> <c>GROUP BY</c> over just the step_id-0 success rows, joined to
/// the newest <c>limit</c> rows AFTER they are selected instead of before. That rewrite shipped Darling-side with
/// a live equality test (<c>JobHistoryGroupedStatsMatchWindowFunctionLiveTests</c>) but the Lite mirror was only
/// ever compiled, never run against DuckDB. <see cref="OldScopedJobHistorySql"/> is copied VERBATIM from
/// origin/dev (<c>git show</c>, pre-#4229) — the only thing that would still catch a future edit that silently
/// changes which rows or values come back.
///
/// <para><b>Why scoped-to-one-server for the tie/NULL-average test.</b> A server filter makes the LIMIT boundary
/// unambiguous without needing to also exclude unrelated rows; <see cref="SharedDuckDbFixture"/> resets all data
/// tables before every test anyway, so isolation is not the reason (unlike Darling's shared <c>darlingtest</c>
/// store) — a scoped read is just the simplest shape that still exercises the boundary.</para>
///
/// <para><b>The tie.</b> Eight jobs on the one server share the identical <c>run_datetime</c> <c>tied</c> — a
/// realistic shape (many jobs on one nightly schedule). <c>instance_id</c> is still unique per row, so
/// <c>(run_datetime, instance_id)</c> is a strict total order and the top-N is unambiguous; what this proves is
/// that the OLD (aggregate-then-limit) and NEW (limit-then-join) statements pick the exact same N rows out of the
/// tied group.</para>
///
/// <para><b>The NULL average.</b> <c>null_avg_job</c>'s only step_id-0 row in the window FAILED (run_status 0), so
/// it has no row in <c>job_stats</c> at all — <c>LEFT JOIN</c> must still surface its other row(s) with a NULL
/// avg/last-success, exactly like <c>AVG</c>/<c>MAX</c> of an all-NULL partition gave before. Its rows are timed
/// to rank #1/#2 (highest instance_id, and one a second later than <c>tied</c>), so they are inside the LIMIT
/// boundary this test exercises rather than incidentally excluded by it.</para>
/// </summary>
public sealed class JobHistoryGroupedStatsMatchWindowFunctionTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 501;
    private const int ServerId2 = 502;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;

    public JobHistoryGroupedStatsMatchWindowFunctionTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /// <summary>DuckDB TIMESTAMP keeps microseconds; a .NET tick does not divide evenly into one, and the
    /// LastSuccessfulRun equality assertions below need an exact round trip through the stored value.</summary>
    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    /// <summary>Mirrors <c>LocalDataService.ToInt64</c> (private to that partial class, so not reachable here):
    /// DuckDB returns <see cref="BigInteger"/> for some aggregate/positional reads.</summary>
    private static long ToInt64(object value) => value is BigInteger bi ? (long)bi : Convert.ToInt64(value);

    /// <summary>The pre-#4229 <c>GetJobHistoryAsync</c> SQL text (server-scoped rendering), copied verbatim from
    /// origin/dev (the string no longer exists in source after that fix).</summary>
    private const string OldScopedJobHistorySql = @"
WITH base AS (
    SELECT
        collection_time,
        server_id,
        server_name,
        instance_id,
        job_id,
        job_name,
        job_enabled,
        category_name,
        step_id,
        step_name,
        run_status,
        run_status_desc,
        run_datetime,
        run_duration_seconds,
        retries_attempted,
        message,
        AVG(CASE WHEN step_id = 0 AND run_status = 1 THEN run_duration_seconds END)
            OVER (PARTITION BY server_id, job_id) AS avg_success_duration,
        MAX(CASE WHEN step_id = 0 AND run_status = 1 THEN run_datetime END)
            OVER (PARTITION BY server_id, job_id) AS last_success_run
    FROM v_job_history
    WHERE run_datetime >= $1
    AND   server_id = $2
)
SELECT
    collection_time,
    server_id,
    server_name,
    instance_id,
    job_id,
    job_name,
    job_enabled,
    category_name,
    step_id,
    step_name,
    run_status,
    run_status_desc,
    run_datetime,
    run_duration_seconds,
    retries_attempted,
    message,
    last_success_run,
    CASE
        WHEN step_id = 0
        AND  avg_success_duration IS NOT NULL
        AND  avg_success_duration > 0
        AND  run_duration_seconds > avg_success_duration * 2
        AND  run_duration_seconds > 60
        THEN TRUE
        ELSE FALSE
    END AS is_long_running
FROM base
ORDER BY run_datetime DESC, instance_id DESC
LIMIT $3";

    [Fact]
    public async Task GroupByJoin_MatchesTheOldWindowFunction_ThroughATieAndANullAverage_AtTheLimitBoundary()
    {
        var service = new LocalDataService(_duckDb);
        var connection = await SeedConnectionAsync();

        var now = Truncate(DateTime.Now);
        var tied = now.AddMinutes(-5);
        var cutoff = now.AddHours(-1);

        /* Eight jobs tied on the exact same run_datetime, all successful step-0 outcomes — the group job_stats
           must fold with a plain GROUP BY, and the group the LIMIT boundary cuts through. */
        for (var i = 0; i < 8; i++)
        {
            await InsertJobHistoryAsync(connection, id: 100 + i, at: tied, serverId: ServerId,
                jobId: $"tie_job_{i}", stepId: 0, runStatus: 1, durationSeconds: 100 + i, instanceId: 1000 + i);
        }

        /* null_avg_job: its only step-0 row in the window FAILED, so it has no job_stats row at all — the LEFT
           JOIN must still return its rows with a NULL avg/last-success. Timed to rank #1 (a second after
           `tied`) and #2 (AT `tied`, but the highest instance_id there), so both are inside the limit-5
           boundary rather than incidentally excluded by it. */
        await InsertJobHistoryAsync(connection, id: 200, at: tied, serverId: ServerId,
            jobId: "null_avg_job", stepId: 0, runStatus: 0, durationSeconds: 999, instanceId: 2000,
            message: "The job failed.");
        await InsertJobHistoryAsync(connection, id: 201, at: tied.AddSeconds(1), serverId: ServerId,
            jobId: "null_avg_job", stepId: 1, runStatus: 1, durationSeconds: 50, instanceId: 2000);

        const int limit = 5;

        var oldRows = await ReadOldAsync(connection, cutoff, ServerId, limit);
        var newRows = await service.GetJobHistoryAsync(hoursBack: 1, limit: limit, serverId: ServerId);

        /* Same count, same rows, same order, same values — the join replaces the window function without
           changing what the tab shows. */
        Assert.Equal(limit, newRows.Count);
        Assert.Equal(oldRows.Count, newRows.Count);
        for (var i = 0; i < limit; i++)
        {
            AssertSameRow(oldRows[i], newRows[i], $"row {i}");
        }

        /* The boundary itself: ranks 1-2 are null_avg_job (instance_id 2000, both timestamps), ranks 3-5 are
           tie_job_7/6/5 (instance_id 1007/1006/1005) — tie_job_4..0 (instance_id 1004..1000) must NOT appear,
           proving the cut lands in the same place old and new. */
        Assert.Equal("null_avg_job", newRows[0].JobId);
        Assert.Equal(1, newRows[0].StepId);
        Assert.Equal("null_avg_job", newRows[1].JobId);
        Assert.Equal(0, newRows[1].StepId);
        Assert.Equal(["tie_job_7", "tie_job_6", "tie_job_5"], newRows.Skip(2).Select(r => r.JobId).ToArray());
        Assert.DoesNotContain(newRows, r => r.JobId is "tie_job_4" or "tie_job_3" or "tie_job_2" or "tie_job_1" or "tie_job_0");

        /* null_avg_job: no successful step-0 row in the window, so both its returned rows carry a NULL average
           and NULL last-success — never true for IsLongRunning, since the CASE gates on avg_success_duration
           IS NOT NULL first. */
        Assert.All(newRows.Where(r => r.JobId == "null_avg_job"), r =>
        {
            Assert.Null(r.LastSuccessfulRun);
            Assert.False(r.IsLongRunning);
        });

        /* A tied job's LastSuccessfulRun is its OWN step-0 success time (tied), not some other job's — proving
           job_stats grouped by (server_id, job_id) and not just server_id. */
        var tieJob7 = newRows.Single(r => r.JobId == "tie_job_7");
        Assert.Equal(tied, tieJob7.LastSuccessfulRun);
    }

    /// <summary>
    /// #4229: <c>job_stats</c>' join key is <c>(server_id, job_id)</c>, not <c>job_id</c> alone — two servers can
    /// (and commonly do) run same-named jobs (a shared maintenance-plan job_name is the everyday case). Only the
    /// fleet-wide (unscoped) shape can catch a join that forgets the <c>server_id</c> half: a server-scoped query
    /// filters <c>job_stats</c> down to one server before the join ever runs, so the same mistake is invisible
    /// there. Two servers, one shared job_id, deliberately different durations — a dropped <c>server_id</c> join
    /// predicate fans one <c>base</c> row out to both <c>job_stats</c> rows (wrong row count) or attaches the
    /// wrong server's average (wrong value); this checks both.
    /// </summary>
    [Fact]
    public async Task GroupByJoin_KeysOnServerIdAndJobId_NotJobIdAlone()
    {
        var service = new LocalDataService(_duckDb);
        var connection = await SeedConnectionAsync();

        var now = Truncate(DateTime.Now);
        var at = now.AddMinutes(-5);

        /* Same job_id on two servers, wildly different durations — a cross-server mix-up is unmissable. */
        await InsertJobHistoryAsync(connection, id: 300, at: at, serverId: ServerId,
            jobId: "shared_job", stepId: 0, runStatus: 1, durationSeconds: 100, instanceId: 3001);
        await InsertJobHistoryAsync(connection, id: 301, at: at, serverId: ServerId2,
            jobId: "shared_job", stepId: 0, runStatus: 1, durationSeconds: 9000, instanceId: 3002);

        var rows = await service.GetJobHistoryAsync(hoursBack: 1, limit: 2000, serverId: null);
        var mine = rows.Where(r => r.JobId == "shared_job" && (r.ServerId == ServerId || r.ServerId == ServerId2)).ToList();

        Assert.Equal(2, mine.Count);

        var server1Row = mine.Single(r => r.ServerId == ServerId);
        var server2Row = mine.Single(r => r.ServerId == ServerId2);
        Assert.Equal(at, server1Row.LastSuccessfulRun);
        Assert.Equal(at, server2Row.LastSuccessfulRun);
        /* Each server's own 100s / 9000s run is exactly its own average — not > 2x itself, so neither row is
           long-running under its OWN stats; a cross-server mix would still leave both false here (100 vs
           9000*2, 9000 vs 100*2 are both non-triggers), which is why the count/value assertions above are the
           ones actually carrying this test, not IsLongRunning. */
        Assert.False(server1Row.IsLongRunning);
        Assert.False(server2Row.IsLongRunning);
    }

    /// <summary>Field-by-field row equality (not <see cref="object.Equals(object?)"/> — <see cref="JobHistoryRow"/>
    /// has none), so a mismatch names which field and which row rather than just "not equal".</summary>
    private static void AssertSameRow(JobHistoryRow old, JobHistoryRow @new, string label)
    {
        Assert.True(old.ServerId == @new.ServerId
            && old.InstanceId == @new.InstanceId
            && old.JobId == @new.JobId
            && old.StepId == @new.StepId
            && old.RunStatus == @new.RunStatus
            && old.RunDateTime == @new.RunDateTime
            && old.RunDurationSeconds == @new.RunDurationSeconds
            && old.LastSuccessfulRun == @new.LastSuccessfulRun
            && old.IsLongRunning == @new.IsLongRunning,
            $"{label}: old={old.JobId}/{old.StepId}/{old.LastSuccessfulRun:O}/{old.IsLongRunning}, "
            + $"new={@new.JobId}/{@new.StepId}/{@new.LastSuccessfulRun:O}/{@new.IsLongRunning}");
    }

    private async Task<List<JobHistoryRow>> ReadOldAsync(DuckDBConnection connection, DateTime cutoff, int serverId, int limit)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = OldScopedJobHistorySql;
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = limit });

        var items = new List<JobHistoryRow>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new JobHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                ServerId = (int)ToInt64(reader.GetValue(1)),
                ServerName = reader.GetString(2),
                InstanceId = ToInt64(reader.GetValue(3)),
                JobId = reader.GetString(4),
                JobName = reader.GetString(5),
                JobEnabled = reader.GetBoolean(6),
                CategoryName = reader.IsDBNull(7) ? null : reader.GetString(7),
                StepId = (int)ToInt64(reader.GetValue(8)),
                StepName = reader.IsDBNull(9) ? null : reader.GetString(9),
                RunStatus = (int)ToInt64(reader.GetValue(10)),
                RunStatusDesc = reader.IsDBNull(11) ? null : reader.GetString(11),
                RunDateTime = reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                RunDurationSeconds = ToInt64(reader.GetValue(13)),
                RetriesAttempted = (int)ToInt64(reader.GetValue(14)),
                Message = reader.IsDBNull(15) ? null : reader.GetString(15),
                LastSuccessfulRun = reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                IsLongRunning = !reader.IsDBNull(17) && reader.GetBoolean(17),
            });
        }

        return items;
    }

    private async Task InsertJobHistoryAsync(
        DuckDBConnection connection, long id, DateTime at, int serverId, string jobId,
        int stepId, int runStatus, long durationSeconds, long instanceId, string? message = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, category_name, step_id, step_name, run_status, run_status_desc, run_datetime,
     run_duration_seconds, retries_attempted, message)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)";
        var runDateTime = Truncate(at);
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = runDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"S{serverId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = instanceId });
        cmd.Parameters.Add(new DuckDBParameter { Value = jobId });
        cmd.Parameters.Add(new DuckDBParameter { Value = jobId });
        cmd.Parameters.Add(new DuckDBParameter { Value = true });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Uncategorized (Local)" });
        cmd.Parameters.Add(new DuckDBParameter { Value = stepId });
        cmd.Parameters.Add(new DuckDBParameter { Value = stepId == 0 ? "(Job outcome)" : $"Step {stepId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = runStatus });
        cmd.Parameters.Add(new DuckDBParameter { Value = runStatus == 1 ? "The job succeeded." : "The job failed." });
        cmd.Parameters.Add(new DuckDBParameter { Value = runDateTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationSeconds });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)message ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
