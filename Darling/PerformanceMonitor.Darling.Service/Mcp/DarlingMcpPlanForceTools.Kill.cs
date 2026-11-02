/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The kill tool as a prepare step and a write step (#7108, child of #6980). The prepare step runs every check that follows the gate, and the
/// dry run, and takes stored facts (the server id and the parsed target fields), never <c>agent_action</c>. The write step takes the binding the
/// prepare step returned and sends the kill. <c>kill_session</c> calls the two in order, so its answers are what they were.
/// <para>The permit is the one proof a caller brings in: the executor refuses a call without it, and the gate that issues it needs the agent's
/// request. The write checks the binding (#7109): on SQL Server and Azure the kill batch compares the session id, the login time and
/// <c>last_request_end_time</c>, and on PostgreSQL the statement compares the pid, the start second, <c>xact_start</c> and <c>state_change</c>. A
/// session that ran something new since the dry run is refused as <c>session_changed</c> and nothing is killed. On an idle session the prepare and the
/// write in one call see the same values, so the agent's path answers as it did.</para>
/// </summary>
public sealed partial class DarlingMcpPlanForceTools
{
    /// <summary>What the prepare step stops on, shared by both engines. At most one of <see cref="FixedAnswer"/> and <see cref="NotDone"/> is set.</summary>
    internal abstract record KillPrepareStop(string? FixedAnswer, string? RecordReason, RemediationResult? NotDone)
    {
        /// <summary>True when every check passed and the dry run read the session.</summary>
        public bool Proceed => FixedAnswer is null && NotDone is null;
    }

    /// <summary>
    /// The facts that decide a SQL Server or Azure kill. Times are UTC, with the server-local value the kill batch compares kept beside it.
    /// <see cref="LoginTimeServerLocal"/> is the value the dry run matched, and the write sends it unchanged.
    /// </summary>
    internal sealed record SqlKillBinding(int SessionId, DateTime LoginTimeUtc, DateTime LoginTimeServerLocal, DateTime? LastRequestEndTimeUtc,
        DateTime? LastRequestEndTimeServerLocal, string? Status, int? OpenTransactionCount);

    /// <summary>The facts that decide a PostgreSQL kill. <see cref="BackendStartEpoch"/> is the start second the statement compares.</summary>
    internal sealed record PgKillBinding(int Pid, string? BackendId, long BackendStartEpoch, DateTime? XactStartUtc, DateTime? StateChangeUtc, string? State);

    /// <summary>A PostgreSQL kill's binding as a proposal stores it (#7109), and as a press compares it: one shape for both.</summary>
    internal static object PgKillBindingImage(PgKillBinding binding) => new
    {
        pid = binding.Pid,
        backend_id = binding.BackendId,
        backend_start_epoch = binding.BackendStartEpoch,
        xact_start = binding.XactStartUtc,
        state_change = binding.StateChangeUtc,
        state = binding.State,
    };

    internal sealed record SqlKillPrepared(SqlKillBinding? Binding, SqlServerRemediationExecutor? Executor, ServerClock? Clock, RemediationResult? Dry,
        object? Before, string[] Warnings, string? FixedAnswer, string? RecordReason, RemediationResult? NotDone)
        : KillPrepareStop(FixedAnswer, RecordReason, NotDone);

    internal sealed record PgKillPrepared(PgKillBinding? Binding, PostgresRemediationExecutor? Executor, RemediationResult? Dry,
        object? Before, string[] Warnings, string? FixedAnswer, string? RecordReason, RemediationResult? NotDone)
        : KillPrepareStop(FixedAnswer, RecordReason, NotDone);

    /// <summary>The kill batch of either SQL Server engine. The Azure kill reads its database from the permit, the one the gate was asked about.</summary>
    private static Task<RemediationResult> KillSqlBatchAsync(SqlServerRemediationExecutor executor, ServerActionPermit permit, bool azure,
        int sessionId, DateTime loginServerLocal, bool dryRun, CancellationToken ct, KillExpectedActivity? expected = null) =>
        !azure
            ? executor.KillSessionAsync(permit, sessionId, loginServerLocal, dryRun, ct, expected)
            : executor.KillAzureSessionAsync(permit, sessionId, loginServerLocal, dryRun, ct, expected);

    /// <summary>
    /// Prepare step, SQL Server and Azure SQL Database: the server's clock, its settings row, the Azure database checks and the dry-run batch.
    /// <paramref name="postgres"/> is the store's read data source and <paramref name="owner"/> the store owner's. The caller records
    /// <see cref="KillPrepareStop.RecordReason"/> as a refusal on its request, then answers <see cref="KillPrepareStop.FixedAnswer"/>, or renders
    /// <see cref="KillPrepareStop.NotDone"/> with its own dry-run flag.
    /// </summary>
    internal static async Task<SqlKillPrepared> PrepareKillSessionAsync(NpgsqlDataSource postgres, NpgsqlDataSource owner, ILogger logger, ServerActionPermit permit,
        int serverId, string displayName, bool azure, int sessionId, DateTime loginUtc, string? targetDatabase, CancellationToken ct)
    {
        /* login_time is the engine's own local clock. Without the server's clock Darling cannot say what the UTC value means there, and it does not guess. */
        var clock = await DarlingServerClockReader.ReadOrNullAsync(postgres, serverId, ct);
        if (clock is null)
        {
            return new SqlKillPrepared(null, null, null, null, null, [],
                Answer("refused", ServerClockUnknown,
                    $"Darling has not collected {displayName}'s clock yet, so it cannot turn login_time into that server's own time. Nothing was run."),
                ServerClockUnknown, null);
        }

        var loginLocal = clock.ToServerLocal(loginUtc);

        var row = await ReadServerRowAsync(owner, serverId, ct);
        if (row is null)
        {
            return new SqlKillPrepared(null, null, clock, null, null, [],
                Answer("failed", "server_row_missing", $"{displayName} has no row in the monitored-server settings."), null, null);
        }

        var executor = new SqlServerRemediationExecutor(serverId, row.Server, row.UserName, row.SecretSlot, logger, row.MonitoringUserName);

        /* Azure SQL Database: the database is never free-form, it is checked against the registry entry and the collected data. */
        if (azure)
        {
            var refusal = await AzureKillDatabaseAsync(postgres, serverId, row, targetDatabase!, ct);
            if (refusal is not null)
                return new SqlKillPrepared(null, null, clock, null, null, [], DarlingAgentActionGate.RefusalAnswer(refusal), refusal.Reason, null);
        }

        /* One batch decides, for a dry run and before a write: the engine, own session, gone, reused, not a user session, the grant. */
        var dry = await KillSqlBatchAsync(executor, permit, azure, sessionId, loginLocal, dryRun: true, ct);
        if (dry.Outcome != RemediationOutcome.Done)
        {
            var reason = dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null ? dry.Reason : null;
            return new SqlKillPrepared(null, executor, clock, dry, null, [], null, reason, dry);
        }

        var facts = dry.Session;
        var binding = new SqlKillBinding(
            sessionId,
            facts?.LoginTime is { } login ? clock.ToUtc(login) : loginUtc,
            loginLocal,
            facts?.LastRequestEndTime is { } ended ? clock.ToUtc(ended) : null,
            facts?.LastRequestEndTime,
            facts?.Status,
            facts?.OpenTransactionCount);
        return new SqlKillPrepared(binding, executor, clock, dry, KillSessionImage(facts, clock), KillWarnings(facts), null, null, null);
    }

    /// <summary>
    /// Write step, SQL Server and Azure SQL Database: sends the kill batch for the session the binding names. The batch checks the session id, the
    /// login time and <c>last_request_end_time</c> (a NULL equals only a NULL), and kills nothing when the last end time changed. The caller wraps
    /// this in its journal.
    /// </summary>
    internal static Task<RemediationResult> WriteKillSessionAsync(SqlServerRemediationExecutor executor, ServerActionPermit permit, bool azure,
        SqlKillBinding binding, CancellationToken ct) =>
        KillSqlBatchAsync(executor, permit, azure, binding.SessionId, binding.LoginTimeServerLocal, dryRun: false, ct,
            new KillExpectedActivity(binding.LastRequestEndTimeServerLocal));

    /// <summary>
    /// Prepare step, PostgreSQL: the settings row and the dry-run read of the backend (gone, own session, start time, reused, client session, and
    /// what the login may do).
    /// </summary>
    internal static async Task<PgKillPrepared> PrepareKillBackendAsync(NpgsqlDataSource owner, ILogger logger, ServerActionPermit permit,
        int serverId, string displayName, int pid, long startEpoch, CancellationToken ct)
    {
        var row = await ReadServerRowAsync(owner, serverId, ct);
        if (row is null)
        {
            return new PgKillPrepared(null, null, null, null, [],
                Answer("failed", "server_row_missing", $"{displayName} has no row in the monitored-server settings."), null, null);
        }

        var executor = new PostgresRemediationExecutor(serverId, row.Server, row.UserName, row.SecretSlot, logger, row.MonitoringUserName);
        var dry = await executor.KillBackendAsync(permit, pid, startEpoch, dryRun: true, ct);
        if (dry.Outcome != RemediationOutcome.Done)
        {
            var reason = dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null ? dry.Reason : null;
            return new PgKillPrepared(null, executor, dry, null, [], null, reason, dry);
        }

        var facts = dry.PgSession;
        var binding = new PgKillBinding(pid, facts?.BackendId, startEpoch, facts?.XactStartUtc, facts?.StateChangeUtc, facts?.State);
        return new PgKillPrepared(binding, executor, dry, PgKillSessionImage(facts), PgKillWarnings(facts), null, null, null);
    }

    /// <summary>
    /// Write step, PostgreSQL: ends the backend the binding names. The statement checks the pid, the start second, <c>xact_start</c> and
    /// <c>state_change</c> (a NULL equals only a NULL), and ends nothing when either changed. The caller wraps this in its journal.
    /// </summary>
    internal static Task<RemediationResult> WriteKillBackendAsync(PostgresRemediationExecutor executor, ServerActionPermit permit, PgKillBinding binding,
        CancellationToken ct) =>
        executor.KillBackendAsync(permit, binding.Pid, binding.BackendStartEpoch, dryRun: false, ct,
            new PgKillExpectedActivity(binding.XactStartUtc, binding.StateChangeUtc));
}
