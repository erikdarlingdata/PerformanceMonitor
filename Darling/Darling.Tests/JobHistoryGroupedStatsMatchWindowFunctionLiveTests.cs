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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4229 (Job History part): <see cref="ViewerDataService.BuildJobHistorySql"/> replaced a window function
/// (<c>AVG</c>/<c>MAX</c> <c>OVER (PARTITION BY server_id, job_id)</c>, evaluated over every step row the
/// window matched) with a <c>job_stats</c> <c>GROUP BY</c> over just the step_id-0 success rows, joined to the
/// newest <c>limit</c> rows AFTER they are selected instead of before. <see cref="OldScopedJobHistorySql"/> is
/// copied VERBATIM from origin/dev (<c>git show</c>, pre-this-fix) — the only thing that would still catch a
/// future edit that silently changes which rows or values come back.
///
/// <para><b>Why scoped-to-one-server, not fleet-wide.</b> <c>darlingtest</c> is a store every live class in
/// the suite shares, and <see cref="ViewerDataService.GetJobHistoryAsync"/>'s fleet-wide shape has no SQL-level
/// server filter, so a small <c>limit</c> would compete with whatever rows other classes seeded near "now".
/// Scoping to one synthetic server_id (<see cref="ServerId"/>, never used elsewhere) makes the boundary
/// deterministic regardless of what else the shared store holds.</para>
///
/// <para><b>The tie.</b> Nine jobs on the one server share the identical run_datetime_utc <c>tied</c> —
/// a realistic shape (many jobs on one nightly schedule) rather than a contrived same-instance_id collision.
/// instance_id is still unique per row, so <c>(run_datetime_utc, instance_id)</c> is a strict total order and
/// the top-N is unambiguous; what this proves is that the OLD (aggregate-then-limit) and NEW
/// (limit-then-join) statements pick the exact same N rows out of the tied group, not that either is
/// non-deterministic.</para>
///
/// <para><b>The NULL average.</b> <c>NullAvgJob</c>'s only step_id-0 row in the window FAILED (run_status 0),
/// so it has no row in <c>job_stats</c> at all — <c>LEFT JOIN</c> must still surface its other row(s) with a
/// NULL avg/last-success, exactly like <c>AVG</c>/<c>MAX</c> of an all-NULL partition gave before. Its rows are
/// timed to rank #1/#2 (highest instance_id, and one a second later than <c>tied</c>), so they are
/// inside the LIMIT boundary this test exercises rather than incidentally excluded by it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class JobHistoryGroupedStatsMatchWindowFunctionLiveTests
{
    private const int ServerId = -4229250;
    private const string ServerName = "darling-4229-job-history-tie";
    private const int ServerId2 = -4229251;
    private const string ServerName2 = "darling-4229-job-history-tie-2";

    /// <summary>The pre-fix <c>BuildJobHistorySql(scopedToServer: true)</c> text, copied verbatim from
    /// origin/dev (the constant no longer exists in source after this fix).</summary>
    private const string OldScopedJobHistorySql = """
        WITH svr AS (
            SELECT DISTINCT ON (server_id)
                server_id,
                utc_offset_minutes
            FROM server_properties
            WHERE utc_offset_minutes IS NOT NULL
            ORDER BY server_id, collection_time DESC
        ),
        base AS (
            SELECT
                jh.server_id,
                COALESCE(reg.display_name, jh.server_name) AS server_name,
                jh.instance_id,
                jh.job_id,
                jh.job_name,
                jh.job_enabled,
                jh.category_name,
                jh.step_id,
                jh.step_name,
                jh.run_status,
                jh.run_status_desc,
                jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) AS run_datetime_utc,
                jh.run_duration_seconds,
                jh.retries_attempted,
                jh.message,
                AVG(CASE WHEN jh.step_id = 0 AND jh.run_status = 1 THEN jh.run_duration_seconds END)
                    OVER (PARTITION BY jh.server_id, jh.job_id) AS avg_success_duration,
                MAX(CASE WHEN jh.step_id = 0 AND jh.run_status = 1
                         THEN jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) END)
                    OVER (PARTITION BY jh.server_id, jh.job_id) AS last_success_run_utc
            FROM job_history AS jh
            LEFT JOIN svr ON svr.server_id = jh.server_id
            LEFT JOIN servers AS reg ON reg.server_id = jh.server_id
            WHERE jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) >= $1
            AND   jh.collection_time >= $3
            AND   jh.server_id = $2
        )
        SELECT
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
            run_datetime_utc,
            run_duration_seconds,
            retries_attempted,
            message,
            last_success_run_utc,
            CASE
                WHEN step_id = 0
                AND  avg_success_duration IS NOT NULL
                AND  avg_success_duration > 0
                AND  run_duration_seconds > avg_success_duration * 2
                AND  run_duration_seconds > 60
                THEN true
                ELSE false
            END AS is_long_running
        FROM base
        ORDER BY run_datetime_utc DESC, instance_id DESC
        LIMIT $4
        """;

    [Fact]
    public async Task GroupByJoin_MatchesTheOldWindowFunction_ThroughATieAndANullAverage_AtTheLimitBoundary()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4229 job-history equality test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* Truncated to whole seconds: Postgres keeps microseconds, a .NET tick does not divide evenly
               into one, and the LastSuccessfulRunUtc equality assertion below needs an exact round trip. */
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var sinceUtc = now.AddHours(-1);
            var tied = now.AddMinutes(-5);

            /* Eight jobs tied on the exact same run_datetime_utc, all successful step-0 outcomes — the
               group job_stats must fold with a plain GROUP BY, and the group the LIMIT boundary cuts through. */
            for (var i = 0; i < 8; i++)
            {
                await InsertJobHistoryAsync(connection, ct, id: 100 + i, at: tied, jobId: $"tie_job_{i}",
                    stepId: 0, runStatus: 1, durationSeconds: 100 + i, instanceId: 1000 + i);
            }

            /* NullAvgJob: its only step-0 row in the window FAILED, so it has no job_stats row at all — the
               LEFT JOIN must still return its rows with a NULL avg/last-success. Timed to rank #1 (a second
               after `tied`) and #2 (AT `tied`, but the highest instance_id there), so both are inside the
               limit-5 boundary rather than incidentally excluded by it. */
            await InsertJobHistoryAsync(connection, ct, id: 200, at: tied, jobId: "null_avg_job",
                stepId: 0, runStatus: 0, durationSeconds: 999, instanceId: 2000, message: "The job failed.");
            await InsertJobHistoryAsync(connection, ct, id: 201, at: tied.AddSeconds(1), jobId: "null_avg_job",
                stepId: 1, runStatus: 1, durationSeconds: 50, instanceId: 2000);

            const int limit = 5;

            var oldRows = await ReadOldAsync(connection, sinceUtc, ct, limit);
            var newRows = await viewer.GetJobHistoryAsync(sinceUtc, ServerId, limit, ct);

            /* Same count, same rows, same order, same values — the join replaces the window function without
               changing what the tab shows. */
            Assert.Equal(limit, newRows.Count);
            Assert.Equal(oldRows.Count, newRows.Count);
            for (var i = 0; i < limit; i++)
            {
                AssertSameRow(oldRows[i], newRows[i], $"row {i}");
            }

            /* The boundary itself: ranks 1-2 are null_avg_job (instance_id 2000, both timestamps), ranks 3-5
               are tie_job_7/6/5 (instance_id 1007/1006/1005) — tie_job_4..0 (instance_id 1004..1000) must NOT
               appear, proving the cut lands in the same place old and new. */
            Assert.Equal("null_avg_job", newRows[0].JobId);
            Assert.Equal(1, newRows[0].StepId);
            Assert.Equal("null_avg_job", newRows[1].JobId);
            Assert.Equal(0, newRows[1].StepId);
            Assert.Equal(["tie_job_7", "tie_job_6", "tie_job_5"], newRows.Skip(2).Select(r => r.JobId).ToArray());
            Assert.DoesNotContain(newRows, r => r.JobId is "tie_job_4" or "tie_job_3" or "tie_job_2" or "tie_job_1" or "tie_job_0");

            /* NullAvgJob: no successful step-0 row in the window, so both its returned rows carry a NULL
               average and NULL last-success — never true for is_long_running, since the CASE gates on
               avg_success_duration IS NOT NULL first. */
            Assert.All(newRows.Where(r => r.JobId == "null_avg_job"), r =>
            {
                Assert.Null(r.LastSuccessfulRunUtc);
                Assert.False(r.IsLongRunning);
            });

            /* A tied job's last_success_run_utc is its OWN step-0 success time (tied), not some other job's —
               proving job_stats grouped by (server_id, job_id) and not just server_id. */
            var tieJob7 = newRows.Single(r => r.JobId == "tie_job_7");
            Assert.Equal(tied, tieJob7.LastSuccessfulRunUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }

    /// <summary>Field-by-field row equality (not <see cref="object.Equals(object?)"/> — <see cref="ViewerJobHistoryRow"/>
    /// has none), so a mismatch names which field and which row rather than just "not equal".</summary>
    private static void AssertSameRow(ViewerJobHistoryRow old, ViewerJobHistoryRow @new, string label)
    {
        Assert.True(old.ServerId == @new.ServerId
            && old.InstanceId == @new.InstanceId
            && old.JobId == @new.JobId
            && old.StepId == @new.StepId
            && old.RunStatus == @new.RunStatus
            && old.RunDateTimeUtc == @new.RunDateTimeUtc
            && old.RunDurationSeconds == @new.RunDurationSeconds
            && old.LastSuccessfulRunUtc == @new.LastSuccessfulRunUtc
            && old.IsLongRunning == @new.IsLongRunning,
            $"{label}: old={old.JobId}/{old.StepId}/{old.LastSuccessfulRunUtc:O}/{old.IsLongRunning}, "
            + $"new={@new.JobId}/{@new.StepId}/{@new.LastSuccessfulRunUtc:O}/{@new.IsLongRunning}");
    }

    private static async Task<List<ViewerJobHistoryRow>> ReadOldAsync(
        NpgsqlConnection connection, DateTime sinceUtc, CancellationToken ct, int limit)
    {
        var rows = new List<ViewerJobHistoryRow>();
        await using var command = new NpgsqlCommand(OldScopedJobHistorySql, connection);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = sinceUtc });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new ViewerJobHistoryRow
            {
                ServerId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                InstanceId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                JobId = reader.IsDBNull(3) ? "" : reader.GetString(3),
                JobName = reader.IsDBNull(4) ? "" : reader.GetString(4),
                JobEnabled = !reader.IsDBNull(5) && reader.GetBoolean(5),
                CategoryName = reader.IsDBNull(6) ? null : reader.GetString(6),
                StepId = reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                StepName = reader.IsDBNull(8) ? null : reader.GetString(8),
                RunStatus = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                RunStatusDesc = reader.IsDBNull(10) ? null : reader.GetString(10),
                RunDateTimeUtc = reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                RunDurationSeconds = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                RetriesAttempted = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                Message = reader.IsDBNull(14) ? null : reader.GetString(14),
                LastSuccessfulRunUtc = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                IsLongRunning = !reader.IsDBNull(16) && reader.GetBoolean(16),
            });
        }

        return rows;
    }

    private static async Task InsertJobHistoryAsync(
        NpgsqlConnection connection, CancellationToken ct, long id, DateTime at, string jobId,
        int stepId, int runStatus, long durationSeconds, long instanceId, string? message = null, int serverId = ServerId) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id,
                                       job_id, job_name, job_enabled, step_id, step_name, run_status,
                                       run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
              VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,$9,$10,$11,$12,$13,0,$14)",
            4_229_250_000L + id, at, serverId, serverId == ServerId ? ServerName : ServerName2, instanceId, jobId, jobId,
            stepId, stepId == 0 ? "(Job outcome)" : $"Step {stepId}", runStatus,
            runStatus == 1 ? "The job succeeded." : "The job failed.", at, durationSeconds, message);

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM job_history WHERE server_id IN ($1, $2)", ServerId, ServerId2);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id IN ($1, $2)", ServerId, ServerId2);
    }

    /// <summary>
    /// #4229: <c>job_stats</c>' join key is <c>(server_id, job_id)</c>, not <c>job_id</c> alone — two servers
    /// can (and commonly do) run same-named jobs (a shared maintenance-plan job_name is the everyday case).
    /// Only the fleet-wide (unscoped) shape can catch a join that forgets the <c>server_id</c> half: a
    /// server-scoped query filters <c>job_stats</c> down to one server before the join ever runs, so the same
    /// mistake is invisible there. Two servers, one shared job_id, deliberately different durations — a
    /// dropped <c>server_id</c> join predicate fans one <c>base</c> row out to both <c>job_stats</c> rows
    /// (wrong row count) or attaches the wrong server's average (wrong value); this checks both.
    /// </summary>
    [Fact]
    public async Task GroupByJoin_KeysOnServerIdAndJobId_NotJobIdAlone()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4229 job-history equality test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId2, ServerName2, ct);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var sinceUtc = now.AddHours(-1);
            var at = now.AddMinutes(-5);

            /* Same job_id on two servers, wildly different durations — a cross-server mix-up is unmissable. */
            await InsertJobHistoryAsync(connection, ct, id: 300, at: at, jobId: "shared_job",
                stepId: 0, runStatus: 1, durationSeconds: 100, instanceId: 3001, serverId: ServerId);
            await InsertJobHistoryAsync(connection, ct, id: 301, at: at, jobId: "shared_job",
                stepId: 0, runStatus: 1, durationSeconds: 9000, instanceId: 3002, serverId: ServerId2);

            /* Fleet-wide (no server filter) is the shape whose job_stats join actually needs the server_id
               half; a large limit plus a client-side filter to just these two rows keeps the shared
               darlingtest store's other rows from being the thing that decides the LIMIT boundary here. */
            var rows = await viewer.GetJobHistoryAsync(sinceUtc, serverId: null, limit: 2000, ct);
            var mine = rows.Where(r => r.JobId == "shared_job" && (r.ServerId == ServerId || r.ServerId == ServerId2)).ToList();

            Assert.Equal(2, mine.Count);

            var server1Row = mine.Single(r => r.ServerId == ServerId);
            var server2Row = mine.Single(r => r.ServerId == ServerId2);
            Assert.Equal(at, server1Row.LastSuccessfulRunUtc);
            Assert.Equal(at, server2Row.LastSuccessfulRunUtc);
            /* Each server's own 100s / 9000s run is exactly its own average — not > 2x itself, so neither
               row is long-running under its OWN stats; a cross-server mix would still leave both false here
               (100 vs 9000*2, 9000 vs 100*2 are both non-triggers), which is why the count/value assertions
               above are the ones actually carrying this test, not IsLongRunning. */
            Assert.False(server1Row.IsLongRunning);
            Assert.False(server2Row.IsLongRunning);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }
}
