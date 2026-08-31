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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One write's outcome — success, or the error text the journal records.</summary>
public sealed record PlanForceExecutionResult(bool Succeeded, string? Error)
{
    public static PlanForceExecutionResult Success { get; } = new(true, null);

    public static PlanForceExecutionResult Failed(string error) => new(false, error);
}

/// <summary>The verify read's answer — the inputs <c>ForcePlanSelfReview.Evaluate</c> judges.</summary>
public sealed record PlanForceVerifyResult(
    bool PlanIsStillForced,
    long ForceFailureCount,
    string? LastForceFailureReason,
    long ExecutionsSinceForce,
    double? ObservedCpuPerExecUs);

/// <summary>
/// The bot's ONLY route to a monitored SQL Server (#2138). A seam rather than inline SQL so the
/// orchestration is testable with a fake, and so there is exactly one reviewable place where a write
/// statement can originate. Every implementation must treat the write methods as privileged: nothing
/// reaches them unless <see cref="PerformanceMonitor.Analysis.ForcePlanBotPolicy"/> returned a live
/// Force/Unforce verdict, and phase 1 ships with both global gates in the state where that cannot
/// happen.
/// </summary>
public interface IPlanForceExecutor
{
    Task<PlanForceExecutionResult> ForcePlanAsync(string database, long queryId, long planId, CancellationToken ct);

    Task<PlanForceExecutionResult> UnforcePlanAsync(string database, long queryId, long planId, CancellationToken ct);

    /// <summary>
    /// The evict-first lever: targeted <c>DBCC FREEPROCCACHE(plan_handle)</c> for every cached plan
    /// whose <c>query_plan_hash</c> matches, giving the optimizer one free shot at recovering on its
    /// own before any force. Present and tested in phase 1; the bot's orchestration of it (evict,
    /// observe, then force only if the bad plan returns) is phase 2.
    /// </summary>
    Task<PlanForceExecutionResult> EvictPlanAsync(string database, string queryPlanHashHex, CancellationToken ct);

    /// <summary>The self-review's read: did the force stick, and what has the query cost since.
    /// Read-only — safe on any server regardless of gates.</summary>
    Task<PlanForceVerifyResult?> VerifyAsync(string database, long queryId, long planId, DateTime forcedAtUtc, CancellationToken ct);
}

/// <summary>
/// The live implementation over the server's existing monitoring connection string. SQL text lives
/// in constants so tests pin the exact statements a server would receive; the database is selected
/// via <see cref="SqlConnection.ChangeDatabase"/> and every value travels as a bound parameter, so
/// no collected string is ever concatenated into a statement.
/// </summary>
public sealed class SqlServerPlanForceExecutor : IPlanForceExecutor
{
    private readonly string _connectionString;

    public SqlServerPlanForceExecutor(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    /* The three-argument-free named form on purpose: #1914 measured that the documented four-argument
       call fails with error 12463 on both 2022 and 2025 unless @disable_optimized_plan_forcing = 1,
       while the plain named two-argument call succeeds on both. @replica_group_id is deliberately
       omitted — it defaults to the primary, and a secondary-evidence target never reaches this code
       (ForcePlanBlockers names it secondary_replica_evidence and the bot treats every blocker as a
       hard stop). */
    public const string ForceSql =
        "EXEC sys.sp_query_store_force_plan @query_id = @query_id, @plan_id = @plan_id;";

    /* Unforce defaults @replica_group_id to the LOCAL replica, not the primary (the asymmetry
       FactRemediation's unforce-scope note warns operators about) — which is why the bot only ever
       runs this over the same connection that forced, so local IS the replica the force landed on. */
    public const string UnforceSql =
        "EXEC sys.sp_query_store_unforce_plan @query_id = @query_id, @plan_id = @plan_id;";

    /* Two result sets, one round trip: the forced plan's state, then the query's post-force cost.
       The cost aggregates over ALL plans for the query — what the query costs now, whatever runs —
       because that is the honest comparison against the pre-force baseline; scoping to the forced
       plan would grade only the executions the force won. Interval overlap is accepted: an interval
       straddling the force time smears in some pre-force executions, which biases AGAINST the force
       (never in its favor), so the review can only be too strict, not too kind. */
    public const string VerifySql = @"
SELECT
    is_forced = MAX(CASE WHEN qsp.is_forced_plan = 1 THEN 1 ELSE 0 END),
    force_failure_count = MAX(qsp.force_failure_count),
    last_failure_reason = MAX(qsp.last_force_failure_reason_desc)
FROM sys.query_store_plan AS qsp
WHERE qsp.query_id = @query_id
AND   qsp.plan_id = @plan_id
OPTION(RECOMPILE);

SELECT
    executions_since = SUM(qsrs.count_executions),
    cpu_per_exec_us = SUM(qsrs.avg_cpu_time * qsrs.count_executions) / NULLIF(SUM(qsrs.count_executions), 0)
FROM sys.query_store_runtime_stats AS qsrs
JOIN sys.query_store_plan AS qsp
  ON qsp.plan_id = qsrs.plan_id
JOIN sys.query_store_runtime_stats_interval AS qsrsi
  ON qsrsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
WHERE qsp.query_id = @query_id
AND   qsrsi.end_time >= @since_utc
OPTION(RECOMPILE);";

    /* Bounded lookup, then one DBCC per handle client-side: DBCC cannot be driven set-based, and a
       T-SQL cursor here would put loop machinery on the monitored server for what is at most a
       handful of handles. The hash arrives as a bound binary(8) parameter — parsed from the stored
       hex string in C#, so no string-to-binary CONVERT ever runs on the target. */
    public const string EvictHandleLookupSql = @"
SELECT TOP (16)
    plan_handle = deqs.plan_handle
FROM sys.dm_exec_query_stats AS deqs
WHERE deqs.query_plan_hash = @query_plan_hash
GROUP BY deqs.plan_handle
OPTION(RECOMPILE);";

    public const string EvictSql = @"
DECLARE @handle varbinary(64) = @plan_handle;
DBCC FREEPROCCACHE(@handle) WITH NO_INFOMSGS;";

    public async Task<PlanForceExecutionResult> ForcePlanAsync(string database, long queryId, long planId, CancellationToken ct) =>
        await ExecuteWriteAsync(database, ForceSql, queryId, planId, ct);

    public async Task<PlanForceExecutionResult> UnforcePlanAsync(string database, long queryId, long planId, CancellationToken ct) =>
        await ExecuteWriteAsync(database, UnforceSql, queryId, planId, ct);

    public async Task<PlanForceExecutionResult> EvictPlanAsync(string database, string queryPlanHashHex, CancellationToken ct)
    {
        if (!TryParsePlanHash(queryPlanHashHex, out var hashBytes))
        {
            return PlanForceExecutionResult.Failed($"query_plan_hash '{queryPlanHashHex}' is not a parseable 0x hex string");
        }

        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);
            await connection.ChangeDatabaseAsync(database, ct);

            var handles = new List<byte[]>();
            await using (var lookup = new SqlCommand(EvictHandleLookupSql, connection))
            {
                lookup.Parameters.Add(new SqlParameter("@query_plan_hash", System.Data.SqlDbType.Binary, 8) { Value = hashBytes });
                await using var reader = await lookup.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    handles.Add((byte[])reader[0]);
                }
            }

            if (handles.Count == 0)
            {
                /* Nothing cached under that hash is a legitimate outcome (the plan aged out on its
                   own), not a failure — the journal records it and the caller decides what it means. */
                return PlanForceExecutionResult.Failed("no cached plans matched the hash — nothing to evict");
            }

            foreach (var handle in handles)
            {
                await using var evict = new SqlCommand(EvictSql, connection);
                evict.Parameters.Add(new SqlParameter("@plan_handle", System.Data.SqlDbType.VarBinary, 64) { Value = handle });
                await evict.ExecuteNonQueryAsync(ct);
            }

            return PlanForceExecutionResult.Success;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return PlanForceExecutionResult.Failed(ex.Message);
        }
    }

    public async Task<PlanForceVerifyResult?> VerifyAsync(string database, long queryId, long planId, DateTime forcedAtUtc, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);
            await connection.ChangeDatabaseAsync(database, ct);

            await using var command = new SqlCommand(VerifySql, connection);
            command.Parameters.AddWithValue("@query_id", queryId);
            command.Parameters.AddWithValue("@plan_id", planId);
            /* end_time is datetimeoffset; the journal stamp is naive UTC, so it is re-labeled UTC
               here rather than converted — the review compares instants, and the store's convention
               is that every naive timestamp IS UTC. */
            command.Parameters.AddWithValue("@since_utc",
                new DateTimeOffset(DateTime.SpecifyKind(forcedAtUtc, DateTimeKind.Utc)));

            await using var reader = await command.ExecuteReaderAsync(ct);

            if (!await reader.ReadAsync(ct))
            {
                return null;
            }

            var isForced = !reader.IsDBNull(0) && Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture) == 1;
            var failureCount = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
            var failureReason = reader.IsDBNull(2) ? null : reader.GetString(2);

            long executions = 0;
            double? cpuPerExec = null;
            if (await reader.NextResultAsync(ct) && await reader.ReadAsync(ct))
            {
                executions = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture);
                cpuPerExec = reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture);
            }

            return new PlanForceVerifyResult(isForced, failureCount, failureReason, executions, cpuPerExec);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A verify that cannot read is "no evidence", never a verdict: the review's state machine
               keeps watching on null, which is the safe direction for a read failure. */
            _ = ex;
            return null;
        }
    }

    /// <summary>Parses the collector's <c>0x</c>-prefixed hex plan-hash string into the binary(8)
    /// value <c>sys.dm_exec_query_stats.query_plan_hash</c> actually is.</summary>
    internal static bool TryParsePlanHash(string? hex, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        if (string.IsNullOrWhiteSpace(hex))
        {
            return false;
        }

        var s = hex.Trim();
        if (s.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            s = s[2..];
        }

        if (s.Length == 0 || s.Length % 2 != 0 || s.Length > 16)
        {
            return false;
        }

        try
        {
            bytes = Convert.FromHexString(s);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private async Task<PlanForceExecutionResult> ExecuteWriteAsync(
        string database, string sql, long queryId, long planId, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(ct);
            await connection.ChangeDatabaseAsync(database, ct);

            await using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@query_id", queryId);
            command.Parameters.AddWithValue("@plan_id", planId);
            await command.ExecuteNonQueryAsync(ct);
            return PlanForceExecutionResult.Success;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return PlanForceExecutionResult.Failed(ex.Message);
        }
    }
}
