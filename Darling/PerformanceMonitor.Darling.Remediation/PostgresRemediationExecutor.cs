/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
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
/// zero-padded to seven digits. The caller unpacks it into <c>pid</c> and <c>startEpoch</c>, and the agent must also pass the pid on its own,
/// which has to agree. <see cref="KillSql"/> checks both in the SAME statement that terminates, so a pid given to a backend that started in
/// another second is never signalled. The start second is not a full guarantee: on Linux a pid comes back only after the system has handed
/// out much of its pid range, but on Windows a pid can come back within about a second, so two backends can share a pid and a start
/// second there.</para>
/// <para><b>What is read first.</b> <see cref="FactsSql"/> reads the target's row and what the remediation login may do to it, for a dry run
/// and before a write, so a refusal can say which grant is missing. It decides nothing about identity on its own: the kill statement does.
/// When the kill statement matches nothing, <see cref="AfterSql"/> reads the backend AFTER it and says which refusal it was (gone or reused).
/// When it signalled, the same read runs once, after a short wait, to say whether the backend is still there.</para>
/// <para>Azure SQL Database kill is not built. This class is PostgreSQL only.</para>
/// </summary>
public sealed class PostgresRemediationExecutor
{
    private readonly int _serverId;
    private readonly RemediationServer _server;
    private readonly string? _userName;
    private readonly string? _secretSlot;
    private readonly string? _monitoringUserName;
    private readonly ILogger _logger;

    /// <param name="monitoringUserName">
    /// The server's monitoring role (the <c>username</c> of its entry). A kill counts a backend as Darling's own only when its
    /// <c>application_name</c> starts with <c>PerformanceMonitorDarling</c> AND its role is this one or the remediation role (any client can
    /// set its own application name). <b>When this is null the application name alone decides.</b> That fallback is in <see cref="KillSql"/>
    /// (an unset <c>@monitoring_role</c>), <see cref="KillRefusal"/> and <see cref="AfterAsync"/>.
    /// </param>
    public PostgresRemediationExecutor(int serverId, RemediationServer server, string? remediationUserName, string? remediationSecretSlot, ILogger? logger = null,
        string? monitoringUserName = null)
    {
        ArgumentNullException.ThrowIfNull(server);
        _serverId = serverId;
        _server = server;
        _userName = remediationUserName;
        _secretSlot = remediationSecretSlot;
        _monitoringUserName = string.IsNullOrWhiteSpace(monitoringUserName) ? null : monitoringUserName.Trim();
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// What the kill is decided from: the target's row and what the signed-in login may do to it. A login that cannot see a backend's details
    /// (no <c>pg_read_all_stats</c>, not the backend's own role) gets NULL for everything but a few columns, so <c>start_epoch</c> is NULL there.
    /// <c>pg_has_role(.., 'USAGE')</c> is the test the server itself uses (the privileges of a role, not only membership in it).
    /// <para>Every statement here names each relation, function, type and operator with its schema, <c>pg_catalog</c>, as well as signing in
    /// with search_path set to <c>pg_catalog</c> (<see cref="RemediationConnection.PostgresSearchPathOption"/>): an object of the same name
    /// in another schema is never used. <c>extract(.. FROM ..)</c>, <c>coalesce</c>, <c>current_user</c> and <c>::bigint</c> are SQL syntax
    /// that the parser itself resolves to pg_catalog.</para>
    /// </summary>
    internal const string FactsSql = @"
SELECT a.pid,
       a.backend_start,
       extract(epoch FROM a.backend_start)::bigint AS start_epoch,
       a.usename::pg_catalog.text,
       a.application_name,
       a.client_addr::pg_catalog.text,
       a.state,
       a.backend_type,
       a.datname::pg_catalog.text,
       a.xact_start,
       a.state_change,
       pg_catalog.pg_backend_pid() AS own_pid,
       coalesce(r.rolsuper, false) AS target_is_superuser,
       (a.usesysid IS NOT NULL AND pg_catalog.pg_has_role(current_user, a.usesysid, 'USAGE')) AS caller_has_target_role,
       pg_catalog.pg_has_role(current_user, 'pg_signal_backend', 'USAGE') AS caller_has_signal_backend,
       coalesce((SELECT c.rolsuper FROM pg_catalog.pg_roles AS c WHERE c.rolname OPERATOR(pg_catalog.=) current_user), false) AS caller_is_superuser,
       a.wait_event,
       extract(epoch FROM pg_catalog.pg_postmaster_start_time())::bigint AS postmaster_epoch,
       coalesce(a.usename OPERATOR(pg_catalog.=) current_user, false) AS target_is_caller
FROM pg_catalog.pg_stat_activity AS a
LEFT JOIN pg_catalog.pg_roles AS r ON r.oid OPERATOR(pg_catalog.=) a.usesysid
WHERE a.pid OPERATOR(pg_catalog.=) @pid;";

    /// <summary>
    /// The kill: the identity check and the termination are ONE statement, so nothing separates them. The start second is derived exactly the way the
    /// collector derives the one inside <c>backend_id</c> (<c>extract(epoch FROM backend_start)::bigint</c>, the cast's own rounding), and a backend
    /// with no visible start time matches nothing. No row means nothing was signalled. A row is <c>pg_terminate_backend</c>'s answer: true when the
    /// server accepted the signal. The tool's own backend is never matched, nor is a session of Darling (an <c>application_name</c> that starts with
    /// <c>PerformanceMonitorDarling</c> AND a role that is the monitoring role (<c>@monitoring_role</c>; unset means the application name alone) or the
    /// remediation role, or the remediation role with any application name), and neither is anything but a client session: the check
    /// <see cref="KillRefusal"/> makes from the read before is repeated here, in the statement that ends the session. A write that holds a binding
    /// (<c>@check_activity</c>) also matches nothing unless <c>xact_start</c> and <c>state_change</c> are still the values the dry run read, and a
    /// NULL equals only a NULL (#7109).
    /// </summary>
    internal const string KillSql = @"
SELECT pg_catalog.pg_terminate_backend(a.pid)
FROM pg_catalog.pg_stat_activity AS a
WHERE a.pid OPERATOR(pg_catalog.=) @pid
AND   extract(epoch FROM a.backend_start)::bigint OPERATOR(pg_catalog.=) @start_epoch
AND   a.backend_type OPERATOR(pg_catalog.=) 'client backend'
AND   (NOT @check_activity::pg_catalog.bool
       OR (a.xact_start IS NOT DISTINCT FROM @expected_xact_start::pg_catalog.timestamptz
           AND a.state_change IS NOT DISTINCT FROM @expected_state_change::pg_catalog.timestamptz))
AND   a.pid OPERATOR(pg_catalog.<>) pg_catalog.pg_backend_pid()
AND   NOT coalesce(a.usename OPERATOR(pg_catalog.=) current_user, false)
AND   NOT (pg_catalog.lower(pg_catalog.left(coalesce(a.application_name, ''), 25)) OPERATOR(pg_catalog.=) 'performancemonitordarling'
           AND (@monitoring_role::pg_catalog.text IS NULL
                OR coalesce(a.usename::pg_catalog.text OPERATOR(pg_catalog.=) @monitoring_role::pg_catalog.text, false)));";

    /// <summary>
    /// Read AFTER the kill statement: is the pid still there, is it the same start second, is it a client session, and is it a session of Darling
    /// now (its <c>application_name</c>, whether its role is the remediation role, and whether it is the monitoring role). A backend can set its own
    /// application name between the read before and the kill statement, which then matches nothing: this read tells that apart from a failure.
    /// </summary>
    internal const string AfterSql = @"
SELECT extract(epoch FROM a.backend_start)::bigint AS start_epoch,
       (a.pid OPERATOR(pg_catalog.=) pg_catalog.pg_backend_pid()) AS is_own,
       a.backend_type,
       a.application_name,
       coalesce(a.usename OPERATOR(pg_catalog.=) current_user, false) AS is_caller_role,
       coalesce(a.usename::pg_catalog.text OPERATOR(pg_catalog.=) @monitoring_role::pg_catalog.text, false) AS is_monitoring_role,
       a.xact_start,
       a.state_change
FROM pg_catalog.pg_stat_activity AS a
WHERE a.pid OPERATOR(pg_catalog.=) @pid;";

    /// <summary>The backend's row as <see cref="FactsSql"/> reads it, with what the login may do to it.</summary>
    internal sealed record PgBackendRow(
        PgKillSessionFacts Facts, int OwnPid, bool TargetIsSuperuser, bool CallerHasTargetRole, bool CallerHasSignalBackend, bool CallerIsSuperuser,
        long? PostmasterEpoch = null, bool TargetIsCallerRole = false);

    /// <summary>
    /// How long the read after a kill that signalled waits first. pg_terminate_backend only sends the signal; an idle backend is gone a few
    /// milliseconds later, so a read with no wait would nearly always still find it.
    /// </summary>
    internal static readonly TimeSpan StillThereWait = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// The test seam for the kill statement (#5632): null runs it with <c>ExecuteScalarAsync</c>. A test sets it, for its own async flow only,
    /// to run the real statement and then lose the reply, the way a dropped connection would.
    /// </summary>
    internal static readonly AsyncLocal<Func<NpgsqlCommand, CancellationToken, Task<object?>>?> KillStatementSeam = new();

    /// <summary>
    /// The test seam for the kill connection's string (#6143): null uses it as built. A test sets it, for its own async flow only, to change the
    /// string, for example to drop the search_path option the way a connection pooler does, and so meet a server-side search_path that is not pinned.
    /// </summary>
    internal static readonly AsyncLocal<Func<string, string>?> ConnectionStringSeam = new();

    /// <summary>
    /// Kills one backend, only when it is still the backend the caller named: the same pid and the same start second. A dry run reads and decides and
    /// kills nothing. Every refusal comes before the kill statement, and a login that may not signal the target is refused, not tried.
    /// </summary>
    public async Task<RemediationResult> KillBackendAsync(ServerActionPermit permit, int pid, long startEpoch, bool dryRun, CancellationToken ct,
        PgKillExpectedActivity? expected = null)
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
            var connectionString = RemediationConnection.BuildPostgresConnectionString(_server, login);
            if (ConnectionStringSeam.Value is { } seamString)
                connectionString = seamString(connectionString);
            await using var connection = new NpgsqlConnection(connectionString);
            var notOpened = await OpenAsync(connection, name, dryRun, login, ct);
            if (notOpened is not null)
                return Log(notOpened);

            var row = await ReadRowAsync(connection, pid, ct);
            var refusal = KillRefusal(name, dryRun, pid, startEpoch, row, _monitoringUserName);
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
                kill.Parameters.Add(MonitoringRoleParameter(_monitoringUserName));
                AddKillActivityParameters(kill, expected);
                var answer = KillStatementSeam.Value is { } seam ? await seam(kill, ct) : await kill.ExecuteScalarAsync(ct);
                terminated = answer is bool flag ? flag : null;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
            {
                /* A grant changed after the read: the server refused, and nothing was signalled. */
                return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotPermitted,
                    $"The server says the remediation login may not end backend {pid}. It needs the pg_signal_backend role, or membership in the role that owns the session, and only a superuser can end a superuser's session. Nothing was run.") with { PgSession = facts });
            }
            catch (Exception ex) when (IsOutcomeUnknown(ex))
            {
                /* The statement was sent and its reply was lost: the server may have ended the backend. Never reported as failed (#5632). */
                return Log(RemediationResult.Unknown(name,
                    $"The kill statement for backend {pid} was sent, but its reply was lost ({SqlServerRemediationExecutor.ErrorText(ex, login)}). Backend {pid} may have ended. Read get_pg_session_states to see whether it is still there before you try again.")
                    with { PgSession = facts });
            }

            if (terminated == true)
            {
                var stillThere = await StillThereAsync(connection, pid, startEpoch, _monitoringUserName, ct);
                var message = stillThere switch
                {
                    false => $"Backend {pid} was signalled to end, and it was gone when read right after.",
                    true => $"Backend {pid} was signalled to end, and it was still there when read right after. It ends when it next checks for the signal; read get_pg_session_states to see it go.",
                    null => $"Backend {pid} was signalled to end. Whether it is gone was not read; read get_pg_session_states to see it go.",
                };
                return Log(RemediationResult.Done(name, dryRun, wrote: true, message) with { PgSession = facts, PgStillThere = stillThere });
            }

            /* Nothing matched, or the server did not take the signal: read AFTER the statement and say which. */
            return Log(await AfterAsync(connection, name, pid, startEpoch, facts, _monitoringUserName, expected, ct));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (PostgresException ex)
        {
            /* 28P01 and 28000 are a failed sign-in; 3D000 is a database the login cannot open. */
            var reason = ex.SqlState is "28P01" or "28000" or "3D000" ? RemediationResult.Reasons.SignInFailed : null;
            return Log(RemediationResult.Failed(name, dryRun, SqlServerRemediationExecutor.ErrorText([], ex.MessageText, login.Secret), reason));
        }
        catch (Exception ex)
        {
            return Log(RemediationResult.Failed(name, dryRun, SqlServerRemediationExecutor.ErrorText(ex, login)));
        }
    }

    /// <summary>
    /// Opens the remediation connection. Null means it is open. A sign-in the connection's own rules refuse
    /// (<see cref="RemediationConnection.BuildPostgresConnectionString"/>: SCRAM-SHA-256 only, with channel binding unless the certificate is
    /// verified) and a <c>postgres</c> database the login may not open are refusals that say what to change; nothing was sent but the sign-in.
    /// </summary>
    private static async Task<RemediationResult?> OpenAsync(NpgsqlConnection connection, string name, bool dryRun, RemediationLogin login, CancellationToken ct)
    {
        try
        {
            await connection.OpenAsync(ct);

            /* #6143: type loading is off, so this is the first statement on the connection. The sign-in sends search_path=pg_catalog,pg_temp; a
               connection pooler can drop that option without an error, and then the server's own setting for the role or database applies. The
               statements below rely on the pin, so a connection without it is not used. */
            await using var searchPath = new NpgsqlCommand(PostgresSearchPathPin.ReadSql, connection) { CommandTimeout = 30 };
            var observed = await searchPath.ExecuteScalarAsync(ct) as string;
            if (!PostgresSearchPathPin.IsPinned(observed))
            {
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SearchPathNotPinned,
                    $"The remediation connection did not get the search_path this service sends when it signs in ('pg_catalog, pg_temp'): it has '{CleanSearchPath(observed)}'. "
                    + "The usual cause is a connection pooler that drops the startup 'options' parameter, as PgBouncer does when 'options' is listed in ignore_startup_parameters. "
                    + "The kill needs a direct connection to the PostgreSQL server: point the server entry at the server's own host and port. Nothing was run.");
            }

            return null;
        }
        catch (PostgresException ex) when (ex.SqlState is "3D000" or "42501")
        {
            /* 3D000: no such database; 42501: no CONNECT privilege on it. */
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SignInFailed,
                $"The remediation login could not open the database {RemediationConnection.PostgresDatabase} on this server: {SqlServerRemediationExecutor.ErrorText([], ex.MessageText, login.Secret)}. "
                + $"The kill always connects to {RemediationConnection.PostgresDatabase}; a person can run GRANT CONNECT ON DATABASE {RemediationConnection.PostgresDatabase} TO the remediation login. Nothing was run.");
        }
        catch (NpgsqlException ex) when (ex is not PostgresException && IsSignInRuleRefusal(ex))
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SignInNotStrongEnough,
                $"The remediation connection did not sign in: {SqlServerRemediationExecutor.ErrorText([], ex.Message, login.Secret).TrimEnd('.')}. {SignInRule} Nothing was run.");
        }
    }

    /// <summary>What the server reported for search_path, without control characters and capped, since it reaches a message.</summary>
    private static string CleanSearchPath(string? observed)
    {
        var text = new string((observed ?? string.Empty).Select(c => char.IsControl(c) ? ' ' : c).ToArray());
        return text.Length > 200 ? text[..200] + "..." : text;
    }

    /// <summary>
    /// True for an error that came after the kill statement was sent and is not the server refusing the statement: a lost or closed connection,
    /// a timeout, the statement cancelled (57014), its connection ended by the server (57P01 to 57P03, 57P04 database dropped), a serialization
    /// failure (40001), or any error the server sends at FATAL or PANIC severity, which ends the session whatever its code. Each can end our
    /// statement after pg_terminate_backend ran, so the server may have run it. Any other server error response at ERROR severity, such as 42501,
    /// means the statement did not run.
    /// </summary>
    internal static bool IsOutcomeUnknown(Exception ex) => ex switch
    {
        OperationCanceledException => false,
        PostgresException pg => pg.InvariantSeverity is "FATAL" or "PANIC"
            || pg.SqlState is PostgresErrorCodes.QueryCanceled or PostgresErrorCodes.AdminShutdown or PostgresErrorCodes.CrashShutdown
                or PostgresErrorCodes.CannotConnectNow or PostgresErrorCodes.DatabaseDropped or PostgresErrorCodes.SerializationFailure,
        _ => true,
    };

    /// <summary>
    /// The one read after a kill that signalled: true when the same backend is still there. Null when the read failed or the caller cancelled
    /// during it; it never turns a kill into an error. A cancel here must not escape: the kill is confirmed, and only a kill the executor returns
    /// as done is journaled as done (#5632). The caller's cancel still ends the call when the store next uses the token.
    /// </summary>
    private static async Task<bool?> StillThereAsync(NpgsqlConnection connection, int pid, long startEpoch, string? monitoringRole, CancellationToken ct)
    {
        try
        {
            await Task.Delay(StillThereWait, ct);
            await using var command = new NpgsqlCommand(AfterSql, connection) { CommandTimeout = 30 };
            command.Parameters.Add(new NpgsqlParameter("pid", NpgsqlDbType.Integer) { Value = pid });
            command.Parameters.Add(MonitoringRoleParameter(monitoringRole));
            await using var reader = await command.ExecuteReaderAsync(ct);
            return await reader.ReadAsync(ct) && !reader.IsDBNull(0) && reader.GetInt64(0) == startEpoch;
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>What the remediation connection needs from the server's sign-in, and what to change when the server does not offer it.</summary>
    internal const string SignInRule =
        "The remediation login signs in with SCRAM-SHA-256 only (set password_encryption = 'scram-sha-256', set the login's password again, "
        + "and give it a scram-sha-256 line in pg_hba.conf). Unless the server entry verifies the server's certificate in full (encryption not "
        + "Optional and Trust server certificate off), it also needs SCRAM channel binding, which needs TLS turned on at the server (ssl = on).";

    /// <summary>
    /// Npgsql's own refusals of a sign-in the connection string rules out: the server asked for a method other than SCRAM-SHA-256
    /// (<c>RequireAuth</c>), or offered SCRAM without channel binding when it is required (<c>ChannelBinding=Require</c>, which is what a
    /// server without TLS offers). Npgsql raises both as a plain <see cref="NpgsqlException"/>, so the text is what tells them apart.
    /// </summary>
    internal static bool IsSignInRuleRefusal(NpgsqlException ex) =>
        ex.InnerException is null
        && (ex.Message.Contains("authentication method is not allowed", StringComparison.Ordinal)
            || ex.Message.Contains(nameof(ChannelBinding), StringComparison.Ordinal));

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
            reader.IsDBNull(10) ? null : reader.GetFieldValue<DateTime>(10),
            reader.IsDBNull(16) ? null : reader.GetString(16));
        return new PgBackendRow(facts, reader.GetInt32(11), reader.GetBoolean(12), reader.GetBoolean(13), reader.GetBoolean(14), reader.GetBoolean(15),
            reader.IsDBNull(17) ? null : reader.GetInt64(17), !reader.IsDBNull(18) && reader.GetBoolean(18));
    }

    /// <summary>
    /// The three parameters the kill statement takes for the activity check (#7109). With no <paramref name="expected"/> the check is off. With one,
    /// the statement matches only a backend whose <c>xact_start</c> and <c>state_change</c> equal the expected values, a NULL equal only to a NULL.
    /// </summary>
    internal static void AddKillActivityParameters(NpgsqlCommand command, PgKillExpectedActivity? expected)
    {
        command.Parameters.Add(new NpgsqlParameter("check_activity", NpgsqlDbType.Boolean) { Value = expected is not null });
        command.Parameters.Add(new NpgsqlParameter("expected_xact_start", NpgsqlDbType.TimestampTz)
        {
            Value = expected?.XactStartUtc is { } xact ? DateTime.SpecifyKind(xact, DateTimeKind.Utc) : DBNull.Value,
        });
        command.Parameters.Add(new NpgsqlParameter("expected_state_change", NpgsqlDbType.TimestampTz)
        {
            Value = expected?.StateChangeUtc is { } change ? DateTime.SpecifyKind(change, DateTimeKind.Utc) : DBNull.Value,
        });
    }

    /// <summary>The <c>@monitoring_role</c> parameter of <see cref="KillSql"/> and <see cref="AfterSql"/>: NULL when the server's monitoring role is not known.</summary>
    private static NpgsqlParameter MonitoringRoleParameter(string? monitoringRole) =>
        new("monitoring_role", NpgsqlDbType.Text) { Value = (object?)monitoringRole ?? DBNull.Value };

    private static async Task<RemediationResult> AfterAsync(NpgsqlConnection connection, string name, int pid, long startEpoch, PgKillSessionFacts facts,
        string? monitoringRole, PgKillExpectedActivity? expected, CancellationToken ct)
    {
        long? actual = null;
        var activityChanged = false;
        var own = false;
        var found = false;
        string? backendType = null;
        string? applicationName = null;
        var callerRole = false;
        var monitoringMatch = false;
        await using (var command = new NpgsqlCommand(AfterSql, connection) { CommandTimeout = 30 })
        {
            command.Parameters.Add(new NpgsqlParameter("pid", NpgsqlDbType.Integer) { Value = pid });
            command.Parameters.Add(MonitoringRoleParameter(monitoringRole));
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                found = true;
                actual = reader.IsDBNull(0) ? null : reader.GetInt64(0);
                own = !reader.IsDBNull(1) && reader.GetBoolean(1);
                backendType = reader.IsDBNull(2) ? null : reader.GetString(2);
                applicationName = reader.IsDBNull(3) ? null : reader.GetString(3);
                callerRole = !reader.IsDBNull(4) && reader.GetBoolean(4);
                monitoringMatch = !reader.IsDBNull(5) && reader.GetBoolean(5);
                if (expected is not null)
                {
                    DateTime? xactStart = reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTime>(6);
                    DateTime? stateChange = reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTime>(7);
                    activityChanged = xactStart != expected.XactStartUtc || stateChange != expected.StateChangeUtc;
                }
            }
        }

        /* With no monitoring role known, the application name alone decides, as KillSql's statement does. */
        return AfterAnswer(name, pid, startEpoch, facts, found, actual, own, backendType, applicationName, callerRole, monitoringRole is null || monitoringMatch, activityChanged);
    }

    /// <summary>
    /// Turns the read after a kill statement that signalled nothing into the answer. Pure, so a test needs no server. Gone: no row. Reused: a row with
    /// another start second. Not a client session: a row whose backend type is another. Anything else is a failure, never a kill: the statement
    /// matched and the server did not take the signal. A backend that is Darling's now (it set its own application name after the read before, so
    /// <see cref="KillSql"/> matched nothing) is refused as <c>tool_login_session</c>, with no facts, as <see cref="KillRefusal"/> refuses it.
    /// </summary>
    internal static RemediationResult AfterAnswer(string name, int pid, long startEpoch, PgKillSessionFacts facts, bool found, long? actualEpoch, bool own, string? backendType = null,
        string? applicationName = null, bool isCallerRole = false, bool isMonitoringRole = false, bool activityChanged = false)
    {
        if (!found)
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.SessionGone, $"Backend {pid} is gone. Nothing was run.");
        if (own)
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.OwnSession, $"Backend {pid} is the remediation connection's own session. Nothing was run.");
        if (actualEpoch != startEpoch)
        {
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.SessionReused,
                $"Backend {pid} is now a different session: it has another start time, so the server reused the pid. Nothing was run.");
        }

        if (backendType is not null && !string.Equals(backendType, "client backend", StringComparison.Ordinal))
        {
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.NotUserSession,
                $"Backend {pid} is not a client session (it is {backendType}). Nothing was run.") with { PgSession = facts };
        }

        if (IsDarlingSession(applicationName, isCallerRole, isMonitoringRole))
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.ToolLoginSession, DarlingSessionMessage(pid));

        if (activityChanged)
        {
            return RemediationResult.Refused(name, false, RemediationResult.Reasons.SessionChanged,
                $"Backend {pid} has run something new since the dry run: its transaction or its state changed, so it may hold different work than the work that was checked. Nothing was run. Read it again with a dry run to see what it holds now.");
        }

        return RemediationResult.Failed(name, false, $"The server did not take the signal for backend {pid}. Nothing is known to have ended.");
    }

    /// <summary>
    /// The decision before the kill statement, for a dry run and a write alike. Null means every check passed. Pure, so a test needs no server.
    /// The order: gone, own session, a start time the login cannot see, a backend_id that holds the server's start time, reused, not a client
    /// backend, then what the login may do to the backend. A reused pid answers nothing about the session that has it now.
    /// </summary>
    internal static RemediationResult? KillRefusal(string name, bool dryRun, int pid, long startEpoch, PgBackendRow? row, string? monitoringRole = null)
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

        if (actual != startEpoch && row.PostmasterEpoch == startEpoch)
        {
            /* The collector falls back to the server's start time when backend_start is NULL, which is what the monitoring login sees
               without pg_read_all_stats. Such an id names no session, so "reused" would be wrong. */
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BackendIdHoldsServerStart,
                $"This backend_id holds the server's start time, not the start time of backend {pid}: the monitoring login could not read the session's start time when it was collected, so the id cannot name a session. Grant pg_read_all_stats (or pg_monitor) to the monitoring login; the ids collected after that name sessions. Nothing was run.");
        }

        if (actual != startEpoch)
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionReused,
                $"Backend {pid} is now a different session: it has another start time, so the server reused the pid. Nothing was run.");
        }

        if (!string.Equals(facts.BackendType, "client backend", StringComparison.Ordinal))
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotUserSession,
                $"Backend {pid} is not a client session (it is {facts.BackendType ?? "unknown"}). Nothing was run.") with { PgSession = facts };
        }

        /* The tool never ends the remediation role's sessions, nor a session whose application name AND role say it is Darling's: any client can set
           its own application name, so the name alone proves nothing when the monitoring role is known. A session of the monitoring role under another
           application name is not matched: a server monitored as a superuser would make its real blocker unkillable. No facts: they are not the
           caller's to read (#5632). */
        if (IsDarlingSession(facts.ApplicationName, row.TargetIsCallerRole,
                monitoringRole is null || string.Equals(facts.UserName, monitoringRole, StringComparison.Ordinal)))
        {
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.ToolLoginSession, DarlingSessionMessage(pid));
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

    /// <summary>
    /// True for a backend that is Darling's own (#5632): the remediation role with any application name, or an application name that starts with
    /// <c>PerformanceMonitorDarling</c> AND the monitoring role (<paramref name="isMonitoringRole"/>, true when the monitoring role is not known).
    /// </summary>
    internal static bool IsDarlingSession(string? applicationName, bool isCallerRole, bool isMonitoringRole) =>
        isCallerRole || (IsDarlingProgram(applicationName) && isMonitoringRole);

    private static string DarlingSessionMessage(int pid) =>
        $"Backend {pid} belongs to Darling itself (its application name and its role say it is Darling's, or it is the remediation login's), which this tool never ends. Nothing was run.";

    /// <summary>True for an application name that starts with <c>PerformanceMonitorDarling</c> (the monitoring and the remediation connections), whatever its case.</summary>
    internal static bool IsDarlingProgram(string? applicationName) =>
        applicationName is not null && applicationName.StartsWith("PerformanceMonitorDarling", StringComparison.OrdinalIgnoreCase);

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
/// What a PostgreSQL kill write expects of the backend beyond its pid and start second (#7109): the <c>xact_start</c> and <c>state_change</c> the
/// dry run read, in UTC. NULL equals only NULL.
/// </summary>
public sealed record PgKillExpectedActivity(DateTime? XactStartUtc, DateTime? StateChangeUtc);

/// <summary>
/// What the server's session view held for the backend a PostgreSQL kill named, read in the sign-in that decided (#5632). No statement text is
/// ever read. <c>BackendStartEpoch</c> is the start time in whole epoch seconds, derived the way the collector derives it; <c>BackendId</c> is the
/// value <c>get_pg_blocking</c> and <c>get_pg_session_states</c> return for the same backend (epoch seconds, then the pid in seven digits).
/// </summary>
public sealed record PgKillSessionFacts(
    int Pid, DateTime? BackendStartUtc, long? BackendStartEpoch, string? UserName, string? ApplicationName, string? ClientAddr, string? State,
    string? BackendType, string? DatabaseName, DateTime? XactStartUtc, DateTime? StateChangeUtc, string? WaitEvent = null)
{
    /// <summary>True when the backend waits for a synchronous standby to confirm its commit: its transaction has already committed on this server.</summary>
    public bool WaitsForSyncReplication => string.Equals(WaitEvent, "SyncRep", StringComparison.Ordinal);

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
