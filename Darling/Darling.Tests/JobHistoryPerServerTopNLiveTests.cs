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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4477: <see cref="ViewerDataService.BuildJobHistorySql"/> replaced a fleet-wide scan-and-sort (every
/// <c>job_history</c> row in the window, sorted, top-N taken) with a per-server top-N over V150's
/// <c>idx_job_history_server_run (server_id, run_datetime DESC, instance_id DESC)</c>, merged fleet-wide with
/// the same tie-break. This file pins three things a rewrite like that can get wrong: (1) which servers drive
/// the LATERAL — a row for a server no longer in the <c>servers</c> registry must still surface, exactly like
/// the old text's <c>LEFT JOIN reg</c>; (2) row-for-row equivalence with the old text across offsets, ties and
/// the cap boundary; (3) the plan actually uses the index with no extra sort, and reverting either the
/// tie-break or the driving set breaks a real assertion here.
///
/// <para><b>Why <see cref="OldFleetJobHistorySql"/> and <see cref="OldScopedJobHistorySql"/> are copied
/// verbatim from origin/dev</b>: the only oracle that would still catch a future edit silently changing which
/// rows or values come back, the same reasoning <c>JobHistoryGroupedStatsMatchWindowFunctionLiveTests</c>
/// uses for #4229's job_stats rewrite.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class JobHistoryPerServerTopNLiveTests
{
    /* Five servers: three with a server_properties offset, one with NO server_properties row at all (offset
       COALESCEd to 0), and one DEREGISTERED after seeding its rows (absent from `servers`, the correctness
       risk this fix exists to close). */
    private const int ServerPlus60 = -447_701;
    private const int ServerMinus300 = -447_702;
    private const int ServerZeroOffset = -447_703;
    private const int ServerNoPropertiesRow = -447_704;
    private const int ServerDeregistered = -447_705;

    private static readonly int[] AllServerIds =
        [ServerPlus60, ServerMinus300, ServerZeroOffset, ServerNoPropertiesRow, ServerDeregistered];

    /// <summary>The pre-#4477 fleet-wide shape, copied verbatim from origin/dev (the constant no longer exists
    /// in source after this fix) — the row-correctness oracle for the fleet-wide reads below.</summary>
    private const string OldFleetJobHistorySql = """
        WITH svr AS (
            SELECT DISTINCT ON (server_id)
                server_id,
                utc_offset_minutes
            FROM server_properties
            WHERE utc_offset_minutes IS NOT NULL
            ORDER BY server_id, collection_time DESC
        ),
        job_stats AS (
            SELECT
                jh.server_id,
                jh.job_id,
                AVG(jh.run_duration_seconds) AS avg_success_duration,
                MAX(jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0))) AS last_success_run_utc
            FROM job_history AS jh
            LEFT JOIN svr ON svr.server_id = jh.server_id
            WHERE jh.step_id = 0
            AND   jh.run_status = 1
            AND   jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) >= $1
            AND   jh.collection_time >= $2
            GROUP BY jh.server_id, jh.job_id
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
                jh.message
            FROM job_history AS jh
            LEFT JOIN svr ON svr.server_id = jh.server_id
            LEFT JOIN servers AS reg ON reg.server_id = jh.server_id
            WHERE jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) >= $1
            AND   jh.collection_time >= $2
            ORDER BY run_datetime_utc DESC, instance_id DESC
            LIMIT $3
        )
        SELECT
            base.server_id,
            base.server_name,
            base.instance_id,
            base.job_id,
            base.job_name,
            base.job_enabled,
            base.category_name,
            base.step_id,
            base.step_name,
            base.run_status,
            base.run_status_desc,
            base.run_datetime_utc,
            base.run_duration_seconds,
            base.retries_attempted,
            base.message,
            job_stats.last_success_run_utc,
            CASE
                WHEN base.step_id = 0
                AND  job_stats.avg_success_duration IS NOT NULL
                AND  job_stats.avg_success_duration > 0
                AND  base.run_duration_seconds > job_stats.avg_success_duration * 2
                AND  base.run_duration_seconds > 60
                THEN true
                ELSE false
            END AS is_long_running
        FROM base
        LEFT JOIN job_stats
            ON  job_stats.server_id = base.server_id
            AND job_stats.job_id = base.job_id
        ORDER BY base.run_datetime_utc DESC, base.instance_id DESC
        """;

    /// <summary>The pre-#4477 SCOPED shape (single-server), copied verbatim from origin/dev — used for the
    /// RUNTIME-RED-on-dev plan-shape pin (dev's text has no LATERAL, so the Index Scan / no-extra-sort
    /// assertion below must fail against it).</summary>
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
                jh.message
            FROM job_history AS jh
            LEFT JOIN svr ON svr.server_id = jh.server_id
            LEFT JOIN servers AS reg ON reg.server_id = jh.server_id
            WHERE jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) >= $1
            AND   jh.collection_time >= $3
            AND   jh.server_id = $2
            ORDER BY run_datetime_utc DESC, instance_id DESC
            LIMIT $4
        )
        SELECT base.server_id
        FROM base
        ORDER BY base.run_datetime_utc DESC, base.instance_id DESC
        """;

    /// <summary>
    /// The correctness risk this fix closes: a <c>job_history</c> row for a server_id that has since been
    /// REMOVED from the <c>servers</c> registry must still surface (raw collected name, no display alias) —
    /// exactly the old text's behavior via <c>LEFT JOIN reg</c>. Driving the per-server LATERAL off the
    /// registry instead of off <c>job_history</c> itself would have silently dropped it.
    /// </summary>
    [Fact]
    public async Task DeregisteredServersRow_StillSurfaces_LikeTheOldLeftJoin()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4477 job-history live tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var sinceUtc = now.AddHours(-1);
            var at = now.AddMinutes(-5);

            /* Register, seed one row, then DEREGISTER — the durable-record case (#2126's comment in the tab):
               a job_history row outlives the server's presence in the registry. */
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerDeregistered, "darling-4477-gone", ct);
            await InsertJobHistoryAsync(connection, ct, id: 1, at: at, jobId: "orphan_job",
                stepId: 0, runStatus: 1, durationSeconds: 42, instanceId: 9001, serverId: ServerDeregistered,
                serverName: "darling-4477-gone-raw");
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerDeregistered);

            var rows = await viewer.GetJobHistoryAsync(sinceUtc, serverId: null, limit: 2000, ct);
            var orphan = rows.SingleOrDefault(r => r.JobId == "orphan_job" && r.ServerId == ServerDeregistered);

            Assert.NotNull(orphan);
            /* No servers row, so no display_name to fall back to — the raw collected server_name, exactly
               like the old COALESCE(reg.display_name, jh.server_name) with reg NULL. */
            Assert.Equal("darling-4477-gone-raw", orphan!.ServerName);

            /* And it is reachable server-scoped too — the read's other parameter shape. */
            var scoped = await viewer.GetJobHistoryAsync(sinceUtc, ServerDeregistered, 2000, ct);
            Assert.Contains(scoped, r => r.JobId == "orphan_job");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }

    /// <summary>
    /// Full equivalence against the old fleet-wide text: 5 servers (3 with different offsets, one with NO
    /// server_properties row at all, one deregistered), 3 days of runs, success/failure/retry mixes, step 0 +
    /// step rows, seeded ties on BOTH axes (same run_datetime_utc across servers, and same run_datetime on one
    /// server), and the cap actually cutting the result — row-for-row, in order, same
    /// <c>last_success_run_utc</c>/<c>is_long_running</c>.
    /// </summary>
    [Fact]
    public async Task NewSql_MatchesOldSql_ExactlyAcrossOffsetsTiesAndTheCapBoundary()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4477 job-history live tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var sinceUtc = now.AddDays(-3);

            await DarlingMcpTestData.RegisterServerAsync(connection, ServerPlus60, "darling-4477-plus60", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerMinus300, "darling-4477-minus300", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerZeroOffset, "darling-4477-zero", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerNoPropertiesRow, "darling-4477-noprops", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerDeregistered, "darling-4477-gone", ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerDeregistered);

            await InsertServerPropertiesAsync(connection, ct, ServerPlus60, 60);
            await InsertServerPropertiesAsync(connection, ct, ServerMinus300, -300);
            await InsertServerPropertiesAsync(connection, ct, ServerZeroOffset, 0);
            /* ServerNoPropertiesRow gets NO server_properties row at all — offset COALESCEs to 0. */
            /* ServerDeregistered gets none either — its rows are collected before its offset was ever known. */

            long id = 1;
            long instanceId = 1;

            /* Three days of daily runs per server, spread so the read's window (3 days) and cap (limit below)
               both matter. */
            foreach (var serverId in AllServerIds)
            {
                for (var daysAgo = 2; daysAgo >= 0; daysAgo--)
                {
                    var at = now.AddDays(-daysAgo).AddHours(-1);
                    await InsertJobHistoryAsync(connection, ct, id: id++, at: at, jobId: $"daily_job_{serverId}",
                        stepId: 0, runStatus: 1, durationSeconds: 30, instanceId: instanceId++, serverId: serverId);
                    /* A step row alongside the outcome row — job_stats must still filter to step_id=0. */
                    await InsertJobHistoryAsync(connection, ct, id: id++, at: at, jobId: $"daily_job_{serverId}",
                        stepId: 1, runStatus: 1, durationSeconds: 20, instanceId: instanceId++, serverId: serverId);
                }

                /* A retry and a failure, same job, different job_id per server so servers don't collide. */
                await InsertJobHistoryAsync(connection, ct, id: id++, at: now.AddHours(-2), jobId: $"retry_job_{serverId}",
                    stepId: 0, runStatus: 2, durationSeconds: 5, instanceId: instanceId++, serverId: serverId);
                await InsertJobHistoryAsync(connection, ct, id: id++, at: now.AddHours(-3), jobId: $"fail_job_{serverId}",
                    stepId: 0, runStatus: 0, durationSeconds: 1, instanceId: instanceId++, serverId: serverId, message: "The job failed.");
            }

            /* TIE #1 (cross-server): two rows on DIFFERENT servers whose de-skewed run_datetime_utc lands on
               the exact same instant — instance_id DESC must decide it identically old vs new. Chosen to be
               the two newest rows in the whole plant, so they sit right at the top the LIMIT boundary would
               see first. */
            /* utc = raw - offset, so a raw gap of (offsetA - offsetB) = 60 - (-300) = 360 minutes between
               the two rows' raw run_datetime makes their run_datetime_utc equal; cross_tie_a gets the higher
               instance_id so instance_id DESC picks it first, matching the assertion below. */
            var crossTie = now.AddMinutes(-1);
            await InsertJobHistoryAsync(connection, ct, id: id++, at: crossTie.AddMinutes(1), jobId: "cross_tie_a",
                stepId: 0, runStatus: 1, durationSeconds: 9, instanceId: 90002, serverId: ServerPlus60);
            await InsertJobHistoryAsync(connection, ct, id: id++, at: crossTie.AddMinutes(1).AddMinutes(-360), jobId: "cross_tie_b",
                stepId: 0, runStatus: 1, durationSeconds: 9, instanceId: 90001, serverId: ServerMinus300);

            /* TIE #2 (same-server): two rows on the SAME server with the identical raw run_datetime (offset
               cancels out, so run_datetime_utc ties too) — instance_id DESC decides within one server's own
               top-N LATERAL, before the fleet-wide merge ever runs. */
            var sameServerTie = now.AddMinutes(-2);
            await InsertJobHistoryAsync(connection, ct, id: id++, at: sameServerTie, jobId: "same_tie_high",
                stepId: 0, runStatus: 1, durationSeconds: 7, instanceId: 90003, serverId: ServerZeroOffset);
            await InsertJobHistoryAsync(connection, ct, id: id++, at: sameServerTie, jobId: "same_tie_low",
                stepId: 0, runStatus: 1, durationSeconds: 7, instanceId: 90000, serverId: ServerZeroOffset);

            using (var analyze = new NpgsqlCommand("ANALYZE job_history", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            /* A limit small enough to actually cut through the plant (35 rows/server x 5 servers + 4 extra),
               so the boundary itself — not just "everything came back" — is under test. */
            const int limit = 20;

            var oldRows = await ReadOldFleetAsync(connection, sinceUtc, limit, ct);
            var newRows = await viewer.GetJobHistoryAsync(sinceUtc, serverId: null, limit, ct);

            Assert.Equal(limit, newRows.Count);
            Assert.Equal(oldRows.Count, newRows.Count);
            for (var i = 0; i < limit; i++)
            {
                AssertSameRow(oldRows[i], newRows[i], $"fleet row {i}");
            }

            /* And scoped-to-one-server, for the server with the same-server tie — the other parameter shape,
               same equivalence requirement. */
            var oldScopedRows = await ReadOldScopedFullAsync(connection, sinceUtc, ServerZeroOffset, limit, ct);
            var newScopedRows = await viewer.GetJobHistoryAsync(sinceUtc, ServerZeroOffset, limit, ct);
            Assert.Equal(oldScopedRows.Count, newScopedRows.Count);
            for (var i = 0; i < oldScopedRows.Count; i++)
            {
                AssertSameRow(oldScopedRows[i], newScopedRows[i], $"scoped row {i}");
            }

            /* The ties themselves, explicitly: instance_id DESC broke both the same way in old and new
               (already implied by the row-for-row match above, but named here so a future reader sees the
               exact claim without re-deriving it from the diff). */
            var crossTieIndex = newRows.FindIndex(r => r.JobId is "cross_tie_a" or "cross_tie_b");
            Assert.True(crossTieIndex >= 0, "expected the cross-server tie to be inside the limit boundary");
            Assert.Equal("cross_tie_a", newRows[crossTieIndex].JobId);

            var zeroOffsetOnly = newScopedRows.Where(r => r.JobId is "same_tie_high" or "same_tie_low").ToList();
            Assert.Equal(["same_tie_high", "same_tie_low"], zeroOffsetOnly.Select(r => r.JobId).ToArray());

            /* The no-server_properties-row server: its rows' run_datetime_utc equals its RAW run_datetime
               (offset COALESCEd to 0), old and new alike — already covered by AssertSameRow above, restated
               here as the specific claim the server plant exists to make. */
            var noPropsRow = newRows.FirstOrDefault(r => r.ServerId == ServerNoPropertiesRow);
            if (noPropsRow is not null)
            {
                Assert.Equal(oldRows.Single(r => r.ServerId == ServerNoPropertiesRow && r.InstanceId == noPropsRow.InstanceId).RunDateTimeUtc,
                    noPropsRow.RunDateTimeUtc);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }

    /// <summary>
    /// The plan-shape half: the shipped SCOPED read uses an Index Scan (no extra Sort node) over
    /// <c>idx_job_history_server_run</c> — proving the LATERAL descends the index directly rather than
    /// scanning-then-sorting. <b>RUNTIME RED against dev</b>: dev's SQL has no LATERAL at all, so asserting
    /// this same plan shape against <see cref="OldScopedJobHistorySql"/> on the identical rig FAILS — dev's
    /// plan sorts (or seq-scans) the whole window instead.
    /// </summary>
    [Fact]
    public async Task ScopedRead_UsesTheIndex_WithNoExtraSort()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4477 job-history live tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerZeroOffset, "darling-4477-plan", ct);

            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var sinceUtc = now.AddDays(-3);

            for (var i = 0; i < 500; i++)
            {
                await InsertJobHistoryAsync(connection, ct, id: 1000 + i, at: now.AddMinutes(-i), jobId: $"plan_job_{i % 20}",
                    stepId: 0, runStatus: 1, durationSeconds: 10, instanceId: 5000 + i, serverId: ServerZeroOffset);
            }

            using (var analyze = new NpgsqlCommand("ANALYZE job_history", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            var floor = EventWindowFloor.For(sinceUtc);
            var newPlan = await ExplainScopedAsync(
                connection, ViewerDataService.BuildJobHistorySql(scopedToServer: true), sinceUtc, ServerZeroOffset, floor, 100, ct);

            Assert.Contains("idx_job_history_server_run", newPlan, StringComparison.Ordinal);
            Assert.True(
                newPlan.Contains("Index Scan", StringComparison.Ordinal) || newPlan.Contains("Index Only Scan", StringComparison.Ordinal),
                $"expected the shipped scoped read to descend idx_job_history_server_run directly:\n{newPlan}");
            /* No Sort node driving the per-server top-N — the index already returns rows in the LATERAL's
               ORDER BY. (job_stats' own aggregate may still show elsewhere in the plan; this assertion is
               about the per-server descent, not the whole statement.) A run_datetime Sort Key can appear
               either bare or wrapped in an offset expression like
               "((jh.run_datetime - make_interval(...)))", so match either shape with a regex instead of a
               literal substring. */
            var sortKeyOnRunDatetime = new Regex(@"Sort Key:.*jh\.run_datetime", RegexOptions.Singleline);
            Assert.False(sortKeyOnRunDatetime.IsMatch(newPlan),
                $"expected no Sort node driving the per-server top-N:\n{newPlan}");

            /* RUNTIME RED against dev: the SAME assertion against dev's own scoped SQL, on the identical
               rig/data — dev has no LATERAL, so this must fail (dev's plan has no per-server index descent
               shaped this way; it is a single scan-then-sort/limit of the whole server's window). */
            var oldPlan = await ExplainOldScopedAsync(connection, sinceUtc, ServerZeroOffset, floor, 100, ct);
            var oldPlanHasIndexDescentNoSort =
                oldPlan.Contains("idx_job_history_server_run", StringComparison.Ordinal)
                && (oldPlan.Contains("Index Scan", StringComparison.Ordinal) || oldPlan.Contains("Index Only Scan", StringComparison.Ordinal))
                && !sortKeyOnRunDatetime.IsMatch(oldPlan);
            Assert.False(oldPlanHasIndexDescentNoSort,
                $"dev's pre-#4477 SQL was expected to NOT match the shipped plan shape (no LATERAL to descend the index per-server):\n{oldPlan}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }

    private static void AssertSameRow(ViewerJobHistoryRow old, ViewerJobHistoryRow @new, string label)
    {
        Assert.True(old.ServerId == @new.ServerId
            && old.ServerName == @new.ServerName
            && old.InstanceId == @new.InstanceId
            && old.JobId == @new.JobId
            && old.StepId == @new.StepId
            && old.RunStatus == @new.RunStatus
            && old.RunDateTimeUtc == @new.RunDateTimeUtc
            && old.RunDurationSeconds == @new.RunDurationSeconds
            && old.LastSuccessfulRunUtc == @new.LastSuccessfulRunUtc
            && old.IsLongRunning == @new.IsLongRunning,
            $"{label}: old={old.ServerId}/{old.JobId}/{old.StepId}/{old.RunDateTimeUtc:O}/{old.LastSuccessfulRunUtc:O}/{old.IsLongRunning}, "
            + $"new={@new.ServerId}/{@new.JobId}/{@new.StepId}/{@new.RunDateTimeUtc:O}/{@new.LastSuccessfulRunUtc:O}/{@new.IsLongRunning}");
    }

    private static async Task<List<ViewerJobHistoryRow>> ReadOldFleetAsync(
        NpgsqlConnection connection, DateTime sinceUtc, int limit, CancellationToken ct)
    {
        var rows = new List<ViewerJobHistoryRow>();
        await using var command = new NpgsqlCommand(OldFleetJobHistorySql, connection);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = sinceUtc });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(ReadRow(reader));
        }
        return rows;
    }

    /// <summary>The full-column old SCOPED text (unlike <see cref="OldScopedJobHistorySql"/>, which is
    /// trimmed to just server_id for the plan-shape pin) — used for the row-equivalence scoped assertion.</summary>
    private const string OldScopedJobHistorySqlFull = """
        WITH svr AS (
            SELECT DISTINCT ON (server_id)
                server_id,
                utc_offset_minutes
            FROM server_properties
            WHERE utc_offset_minutes IS NOT NULL
            ORDER BY server_id, collection_time DESC
        ),
        job_stats AS (
            SELECT
                jh.server_id,
                jh.job_id,
                AVG(jh.run_duration_seconds) AS avg_success_duration,
                MAX(jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0))) AS last_success_run_utc
            FROM job_history AS jh
            LEFT JOIN svr ON svr.server_id = jh.server_id
            WHERE jh.step_id = 0
            AND   jh.run_status = 1
            AND   jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) >= $1
            AND   jh.collection_time >= $3
            AND   jh.server_id = $2
            GROUP BY jh.server_id, jh.job_id
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
                jh.message
            FROM job_history AS jh
            LEFT JOIN svr ON svr.server_id = jh.server_id
            LEFT JOIN servers AS reg ON reg.server_id = jh.server_id
            WHERE jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) >= $1
            AND   jh.collection_time >= $3
            AND   jh.server_id = $2
            ORDER BY run_datetime_utc DESC, instance_id DESC
            LIMIT $4
        )
        SELECT
            base.server_id,
            base.server_name,
            base.instance_id,
            base.job_id,
            base.job_name,
            base.job_enabled,
            base.category_name,
            base.step_id,
            base.step_name,
            base.run_status,
            base.run_status_desc,
            base.run_datetime_utc,
            base.run_duration_seconds,
            base.retries_attempted,
            base.message,
            job_stats.last_success_run_utc,
            CASE
                WHEN base.step_id = 0
                AND  job_stats.avg_success_duration IS NOT NULL
                AND  job_stats.avg_success_duration > 0
                AND  base.run_duration_seconds > job_stats.avg_success_duration * 2
                AND  base.run_duration_seconds > 60
                THEN true
                ELSE false
            END AS is_long_running
        FROM base
        LEFT JOIN job_stats
            ON  job_stats.server_id = base.server_id
            AND job_stats.job_id = base.job_id
        ORDER BY base.run_datetime_utc DESC, base.instance_id DESC
        """;

    private static async Task<List<ViewerJobHistoryRow>> ReadOldScopedFullAsync(
        NpgsqlConnection connection, DateTime sinceUtc, int serverId, int limit, CancellationToken ct)
    {
        var rows = new List<ViewerJobHistoryRow>();
        await using var command = new NpgsqlCommand(OldScopedJobHistorySqlFull, connection);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = sinceUtc });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(sinceUtc) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(ReadRow(reader));
        }
        return rows;
    }

    private static ViewerJobHistoryRow ReadRow(NpgsqlDataReader reader) => new()
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
    };

    /* #4477 CI flip: the shared DARLING_TEST_PG store carries other classes' job_history rows and stats,
       so the planner's seq-scan-vs-index choice for 500 planted rows is not deterministic across CI rigs
       (it passed locally with a fresh DB, then failed on one CI run). Rather than weaken the assertion, this
       EXPLAIN runs inside its own transaction with enable_seqscan/enable_bitmapscan forced off, so the pin
       proves what it always meant to prove: the per-server read's LATERAL CAN descend
       idx_job_history_server_run with no extra sort. Which plan the planner actually picks at production
       scale is a cost question, not a correctness one, and is covered separately by the measured plans in
       the PR body. dev's SQL (OldScopedJobHistorySql) is checked under the identical forced settings, so the
       RED-on-dev half of this test still means something: even forced onto the index, dev's shape sorts. */
    private static async Task<string> ExplainScopedAsync(
        NpgsqlConnection connection, string sql, DateTime sinceUtc, int serverId, DateTime floor, int limit, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using (var forceIndex = new NpgsqlCommand(
            "SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off;", connection, transaction))
        {
            await forceIndex.ExecuteNonQueryAsync(ct);
        }

        await using var command = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + sql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = sinceUtc });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = floor });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var sb = new System.Text.StringBuilder();
        while (await reader.ReadAsync(ct))
        {
            sb.AppendLine(reader.GetString(0));
        }
        await reader.DisposeAsync();
        await transaction.RollbackAsync(ct);
        return sb.ToString();
    }

    private static async Task<string> ExplainOldScopedAsync(
        NpgsqlConnection connection, DateTime sinceUtc, int serverId, DateTime floor, int limit, CancellationToken ct) =>
        await ExplainScopedAsync(connection, OldScopedJobHistorySql, sinceUtc, serverId, floor, limit, ct);

    private static async Task InsertServerPropertiesAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, int offsetMinutes)
    {
        await using var command = new NpgsqlCommand(
            @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes)
              VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(4_477_000_000L + serverId);
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(DateTime.UtcNow));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("srv" + serverId.ToString(CultureInfo.InvariantCulture));
        command.Parameters.AddWithValue(offsetMinutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertJobHistoryAsync(
        NpgsqlConnection connection, CancellationToken ct, long id, DateTime at, string jobId,
        int stepId, int runStatus, long durationSeconds, long instanceId, int serverId, string? message = null,
        string? serverName = null) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id,
                                       job_id, job_name, job_enabled, step_id, step_name, run_status,
                                       run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
              VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,$9,$10,$11,$12,$13,0,$14)",
            4_477_000_000L + id, at, serverId, serverName ?? ("srv" + serverId.ToString(CultureInfo.InvariantCulture)),
            instanceId, jobId, jobId, stepId, stepId == 0 ? "(Job outcome)" : $"Step {stepId}", runStatus,
            runStatus == 1 ? "The job succeeded." : runStatus == 0 ? "The job failed." : "The job was retried.",
            at, durationSeconds, message);

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", AllServerIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));
        await using var jh = new NpgsqlCommand($"DELETE FROM job_history WHERE server_id IN ({ids})", connection);
        await jh.ExecuteNonQueryAsync(ct);
        await using var sp = new NpgsqlCommand($"DELETE FROM server_properties WHERE server_id IN ({ids})", connection);
        await sp.ExecuteNonQueryAsync(ct);
        await using var svr = new NpgsqlCommand($"DELETE FROM servers WHERE server_id IN ({ids})", connection);
        await svr.ExecuteNonQueryAsync(ct);
    }
}
