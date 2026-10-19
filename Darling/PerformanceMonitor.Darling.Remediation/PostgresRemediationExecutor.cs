/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Remediation;

/// <summary>
/// The server write code for ONE monitored PostgreSQL server (#5632): kill one backend with <c>pg_terminate_backend</c>, with a dry run. Like
/// <see cref="SqlServerRemediationExecutor"/>, it takes a <see cref="ServerActionPermit"/> first and refuses a permit issued for another
/// action, server or object before it reads the secret or opens a connection, and the secret reaches the connection string and nothing else.
/// <para><b>The identity of the backend.</b> The agent names a backend by the <c>backend_id</c> the collector stores and
/// <c>get_pg_blocking</c> and <c>get_pg_session_states</c> return: the backend's start time in whole epoch seconds, then its pid
/// zero-padded to seven digits. The caller unpacks it into <c>pid</c> and <c>startEpoch</c>. <see cref="KillSql"/> checks both in the SAME
/// statement that terminates, so a pid the operating system gave to a new backend is never signalled. Seconds are enough because for a
/// different backend to carry the same pid and the same start second, the operating system would have to wrap its whole pid space inside
/// that one second.</para>
/// <para><b>What is read first.</b> <see cref="FactsSql"/> reads the target's row and what the remediation login may do to it, for a dry run
/// and before a write, so a refusal can say which grant is missing. It decides nothing about identity on its own: the kill statement does.
/// When the kill statement matches nothing, <see cref="AfterSql"/> reads the backend AFTER it and says which refusal it was (gone or reused).</para>
/// <para>Azure SQL Database kill is not built. This class is PostgreSQL only.</para>
/// </summary>
public sealed class PostgresRemediationExecutor
{
    private readonly int _serverId;
    private readonly RemediationServer _server;
    private readonly string? _userName;
    private readonly string? _secretSlot;
    private readonly ILogger _logger;

    public PostgresRemediationExecutor(int serverId, RemediationServer server, string? remediationUserName, string? remediationSecretSlot, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(server);
        _serverId = serverId;
        _server = server;
        _userName = remediationUserName;
        _secretSlot = remediationSecretSlot;
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// What the kill is decided from: the target's row and what the signed-in login may do to it. A login that cannot see a backend's details
    /// (no <c>pg_read_all_stats</c>, not the backend's own role) gets NULL for everything but a few columns, so <c>start_epoch</c> is NULL there.
    /// <c>pg_has_role(.., 'USAGE')</c> is the test the server itself uses (the privileges of a role, not only membership in it).
    /// </summary>
    internal const string FactsSql = @"
SELECT a.pid,
       a.backend_start,
       extract(epoch FROM a.backend_start)::bigint AS start_epoch,
       a.usename::text,
       a.application_name,
       a.client_addr::text,
       a.state,
       a.backend_type,
       a.datname::text,
       a.xact_start,
       a.state_change,
       pg_backend_pid() AS own_pid,
       coalesce(r.rolsuper, false) AS target_is_superuser,
       (a.usesysid IS NOT NULL AND pg_has_role(current_user, a.usesysid, 'USAGE')) AS caller_has_target_role,
       pg_has_role(current_user, 'pg_signal_backend', 'USAGE') AS caller_has_signal_backend,
       coalesce((SELECT c.rolsuper FROM pg_roles AS c WHERE c.rolname = current_user), false) AS caller_is_superuser
FROM pg_stat_activity AS a
LEFT JOIN pg_roles AS r ON r.oid = a.usesysid
WHERE a.pid = @pid;";

    /// <summary>
    /// The kill: the identity check and the termination are ONE statement, so nothing separates them. The start second is derived exactly the way the
    /// collector derives the one inside <c>backend_id</c> (<c>extract(epoch FROM backend_start)::bigint</c>, the cast's own rounding), and a backend
    /// with no visible start time matches nothing. No row means nothing was signalled. A row is <c>pg_terminate_backend</c>'s answer: true when the
    /// server accepted the signal. The tool's own backend is never matched.
    /// </summary>
    internal const string KillSql = @"
SELECT pg_terminate_backend(a.pid)
FROM pg_stat_activity AS a
WHERE a.pid = @pid
AND   extract(epoch FROM a.backend_start)::bigint = @start_epoch
AND   a.pid <> pg_backend_pid();";

    /// <summary>Read AFTER a kill statement that signalled nothing: is the pid still there, and is it the same start second.</summary>
    internal const string AfterSql = @"
SELECT extract(epoch FROM a.backend_start)::bigint AS start_epoch, (a.pid = pg_backend_pid()) AS is_own
FROM pg_stat_activity AS a
WHERE a.pid = @pid;";

    /// <summary>The backend's row as <see cref="FactsSql"/> reads it, with what the login may do to it.</summary>
    internal sealed record PgBackendRow(
        PgKillSessionFacts Facts, int OwnPid, bool TargetIsSuperuser, bool CallerHasTargetRole, bool CallerHasSignalBackend, bool CallerIsSuperuser);

    /// <summary>
    /// Kills one backend, only when it is still the backend the caller named: the same pid and the same start second. A dry run reads and decides and
    /// kills nothing. Every refusal comes before the kill statement, and a login that may not signal the target is refused, not tried.
    /// </summary>
    public async Task<RemediationResult> KillBackendAsync(ServerActionPermit permit, int pid, long startEpoch, bool dryRun, CancellationToken ct)
    {
        const string name = "kill_session";
        if (pid <= 0 || startEpoch <= 0)
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadSession, "A backend id names a positive pid and a start time. Nothing was run."));
        if (permit is null
            || !string.Equals(permit.ServerAction, ServerActionNames.KillSession, StringComparison.Ordinal)
            || permit.ServerId != _serverId
            || !string.Equals(permit.Id, pid.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal))
        {
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.PermitMismatch,
                "The gate's permit is not for this action, this server and this object. Nothing was run."));
        }

        var resolution = RemediationLoginResolver.Resolve(_userName, _secretSlot);
        if (resolution.Login is not { } login)
            return Log(RemediationResult.Refused(name, dryRun, ReasonFor(resolution.Error), resolution.Message ?? "No remediation login."));

        try
        {
            await using var connection = new NpgsqlConnection(RemediationConnection.BuildPostgresConnectionString(_server, login));
            await connection.OpenAsync(ct);

            var row = await ReadRowAsync(connection, pid, ct);
            var refusal = KillRefusal(name, dryRun, pid, startEpoch, row);
            if (refusal is not null)
                return Log(refusal);
            var facts = row!.Facts;
            if (dryRun)
            {
                return Log(RemediationResult.Done(name, dryRun, wrote: false,
                    $"Dry run: backend {pid} is still the backend that was named and would be ended. Nothing was run.") with { PgSession = facts });
            }

            /* The kill, with the identity check in the same statement. */
            bool? terminated = null;
            try
            {
                await using var kill = new NpgsqlCommand(KillSql, connection) { CommandTimeout = 30 };
                kill.Parameters.Add(new NpgsqlParameter("pid", NpgsqlDbType.Integer) { Value = pid });
                kill.Parameters.Add(new NpgsqlParameter("start_epoch", NpgsqlDbType.Bigint) { Value = startEpoch });
                var answer = await kill.ExecuteScalarAsync(ct);
                terminated = answer is bool flag ? flag : null;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
            {
                /* A grant changed after the read: the server refused, and nothing was signalled. */
                return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotPermitted,
                    $"The server says the remediation login may not end backend {pid}. It needs the pg_signal_backend role, or membership in the role that owns the session, and only a superuser can end a superuser's session. Nothing was run.") with { PgSession = facts });
            }

            if (terminated == true)
                return Log(RemediationResult.Done(name, dryRun, wrote: true, $"Backend {pid} was ended.") with { PgSession = facts });

            /* Nothing matched, or the server did not take the signal: read AFTER the statement and say which. */
            return Log(await AfterAsync(connection, name, pid, startEpoch, facts, ct));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (PostgresException ex)
        {
            /* 28P01 and 28000 are a failed sign-in; 3D000 is a database the login cannot open. */
            var reason = ex.SqlState is "28P01" or "28000" or "3D000" ? RemediationResult.Reasons.SignInFailed : null;
            return Log(RemediationResult.Failed(name, dryRun, SqlServerRemediationExecutor.Scrub(ex.MessageText, login), reason));
        }
        catch (Exception ex)
        {
            return Log(RemediationResult.Failed(name, dryRun, SqlServerRemediationExecutor.Scrub(ex.GetType().Name + ": " + ex.Message, login)));
        }
    }

    private static async Task<PgBackendRow?> ReadRowAsync(NpgsqlConnection connection, int pid, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(FactsSql, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new NpgsqlParameter("pid", NpgsqlDbType.Integer) { Value = pid });
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;
        var facts = new PgKillSessionFacts(
            reader.GetInt32(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTime>(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetString(8),
            reader.IsDBNull(9) ? null : reader.GetFieldValue<DateTime>(9),
            reader.IsDBNull(10) ? null : reader.GetFieldValue<DateTime>(10));
        return new PgBackendRow(facts, reader.GetInt32(11), reader.GetBoolean(12), reader.GetBoolean(13), reader.GetBoolean(14), reader.GetBoolean(15));
    }

    private static async Task<RemediationResult> AfterAsync(NpgsqlConnection connection, string name, int pid, long startEpoch, PgKillSessionFacts facts, CancellationToken ct)
    {
        long? actual = null;
        var own = false;
        var found = false;
        await using (var command = new NpgsqlCommand(AfterSql, connection) { CommandTimeout = 30 })
        {
            command.Parameters.Add(new NpgsqlParameter("pid", NpgsqlDbType.Integer) { Value = pid });
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                found = true;
                actual = reader.IsDBNull(0) ? null : reader.GetInt64(0);
                own = !reader.IsDBNull(1) && reader.GetBoolean(1);
            }
        }

        return AfterAnswer(name, pid, startEpoch, facts, found, actual, own);
    }

    /// <summary>
    /// Turns the read after a kill statement that signalled nothing into the answer. Pure, so a test needs no server. Gone: no row. Reused: a row with
    /// another start second. Anything else is a failure, never a kill: the statement matched and the server did not take the signal.
    /// </summary>
    internal static RemediationResult AfterAnswer(string name, int pid, long startEpoch, PgKillSessionFacts facts, bool found, long? actualEpoch, bool own)
    {
        if (!found)
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.SessionGone, $"Backend {pid} is gone. Nothing was run.");
        if (own)
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.OwnSession, $"Backend {pid} is the remediation connection's own session. Nothing was run.");
        if (actualEpoch != startEpoch)
        {
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.SessionReused,
                $"Backend {pid} is now a different session: it has another start time, so the server reused the pid. Nothing was run.")
                with { PgSession = facts with { BackendStartEpoch = actualEpoch, BackendStartUtc = null } };
        }

        return RemediationResult.Failed(name, false, $"The server did not take the signal for backend {pid}. Nothing is known to have ended.");
    }

    /// <summary>
    /// The decision before the kill statement, for a dry run and a write alike. Null means every check passed. Pure, so a test needs no server.
    /// The order: gone, own session, a start time the login cannot see, reused, not a client backend, then what the login may do to the backend.
    /// </summary>
    internal static RemediationResult? KillRefusal(string name, bool dryRun, int pid, long startEpoch, PgBackendRow? row)
    {
        if (row is null)
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionGone, $"Backend {pid} is gone. Nothing was run.");
        var facts = row.Facts;
        if (facts.Pid == row.OwnPid)
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.OwnSession, $"Backend {pid} is the remediation connection's own session. Nothing was run.");
        if (facts.BackendStartEpoch is not { } actual)
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotPermitted,
                $"The remediation login cannot see the details of backend {pid} (its start time), so Darling cannot check that it is still the backend you named. Grant it the pg_read_all_stats role (or pg_monitor), or sign in as the role that owns the session. Nothing was run.");
        }

        if (actual != startEpoch)
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionReused,
                $"Backend {pid} is now a different session: it has another start time, so the server reused the pid. Nothing was run.") with { PgSession = facts };
        }

        if (!string.Equals(facts.BackendType, "client backend", StringComparison.Ordinal))
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotUserSession,
                $"Backend {pid} is not a client session (it is {facts.BackendType ?? "unknown"}). Nothing was run.") with { PgSession = facts };
        }

        if (row.TargetIsSuperuser && !row.CallerIsSuperuser)
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotPermitted,
                $"Backend {pid} belongs to a superuser, and PostgreSQL lets only a superuser end a superuser's session (the pg_signal_backend role does not reach it). The remediation login is not a superuser. Nothing was run.") with { PgSession = facts };
        }

        if (!row.CallerIsSuperuser && !row.CallerHasTargetRole && !row.CallerHasSignalBackend)
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotPermitted,
                $"The remediation login may not end backend {pid}: it lacks the pg_signal_backend role, and it is not a member of the role that owns the session. A person can run GRANT pg_signal_backend TO the remediation login. Nothing was run.") with { PgSession = facts };
        }

        return null;
    }

    private static string ReasonFor(RemediationLoginError error) => error switch
    {
        RemediationLoginError.LoginMissing => RemediationResult.Reasons.LoginMissing,
        RemediationLoginError.SecretSlotMissing => RemediationResult.Reasons.SecretSlotMissing,
        RemediationLoginError.StoredPasswordRefused => RemediationResult.Reasons.StoredPassword,
        RemediationLoginError.SecretUnreadable => RemediationResult.Reasons.SecretUnreadable,
        _ => RemediationResult.Reasons.SecretEmpty,
    };

    private RemediationResult Log(RemediationResult result)
    {
        /* The message was built without the secret and scrubbed of it where the server's text is in it, so it is safe to log. */
        _logger.LogInformation("Remediation {Action} on server {ServerId}: {Outcome}{DryRun} {Reason} {Message}",
            result.Action, _serverId, result.Outcome, result.DryRun ? " (dry run)" : "", result.Reason, result.Message);
        return result;
    }
}

/// <summary>
/// What the server's session view held for the backend a PostgreSQL kill named, read in the sign-in that decided (#5632). No statement text is
/// ever read. <c>BackendStartEpoch</c> is the start time in whole epoch seconds, derived the way the collector derives it; <c>BackendId</c> is the
/// value <c>get_pg_blocking</c> and <c>get_pg_session_states</c> return for the same backend (epoch seconds, then the pid in seven digits).
/// </summary>
public sealed record PgKillSessionFacts(
    int Pid, DateTime? BackendStartUtc, long? BackendStartEpoch, string? UserName, string? ApplicationName, string? ClientAddr, string? State,
    string? BackendType, string? DatabaseName, DateTime? XactStartUtc, DateTime? StateChangeUtc)
{
    /// <summary>The collector's synthetic id for this backend: its start epoch in seconds, then the pid zero-padded to seven digits. Null when the start time is not visible or the pid has more than seven digits.</summary>
    public string? BackendId => BackendStartEpoch is { } epoch && Pid is > 0 and <= PostgresBackendId.MaxPid
        ? PostgresBackendId.Pack(epoch, Pid).ToString(CultureInfo.InvariantCulture)
        : null;
}

/// <summary>
/// The collector's <c>backend_id</c> (<c>(extract(epoch FROM backend_start)::bigint::text || to_char(pid, 'FM0000000'))::bigint</c>) packed and
/// unpacked in C#. A pid is at most seven digits on every platform PostgreSQL runs on that the monitor supports; a larger one cannot be named.
/// </summary>
public static class PostgresBackendId
{
    private const long PidSpan = 10_000_000;

    public const int MaxPid = (int)(PidSpan - 1);

    public static long Pack(long startEpoch, int pid) => startEpoch * PidSpan + pid;

    /// <summary>Splits the id the reads return. False for text that is not a whole number, or that holds no start time or no pid.</summary>
    public static bool TryUnpack(string? text, out int pid, out long startEpoch)
    {
        pid = 0;
        startEpoch = 0;
        if (string.IsNullOrWhiteSpace(text)
            || !long.TryParse(text.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var id)
            || id < PidSpan)
        {
            return false;
        }

        pid = (int)(id % PidSpan);
        startEpoch = id / PidSpan;
        return pid > 0 && startEpoch > 0;
    }
}
