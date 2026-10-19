/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Remediation;

/// <summary>
/// The server write code (#5632, step 6): force and unforce a Query Store plan, evict one plan handle and kill one session on ONE monitored
/// SQL Server, each with a dry run. <b>Nothing calls this yet, and nothing registers a tool for it:</b> steps 7, 10 and 12 will. The service
/// assembly does not reference this one.
/// <para><b>No write runs without a passed gate check.</b> Every method takes a <see cref="ServerActionPermit"/> first, the value
/// <see cref="InvestigationRequestStore.AuthorizeServerActionAsync"/> hands out only when the gate returned no refusal, and its constructor
/// is internal to the Storage assembly. A permit is for one action, one server, and one object: a method refuses a permit issued for another
/// action, another server, another database or another object, before it reads the secret or opens a connection.</para>
/// <para><b>The sign-in</b> is the server's remediation login: its name, and the secret read from the <c>env:</c> or <c>file:</c> reference when
/// the action runs. The secret reaches the connection string and nothing else: every message that leaves here has it removed first.</para>
/// <para><b>PostgreSQL is not built:</b> this is SQL Server only, and there is no PostgreSQL kill.</para>
/// </summary>
public sealed class SqlServerRemediationExecutor
{
    private readonly int _serverId;
    private readonly RemediationServer _server;
    private readonly string? _userName;
    private readonly string? _secretSlot;
    private readonly ILogger _logger;

    /// <param name="serverId">The monitored server's id; a permit issued for another server is refused.</param>
    /// <param name="server">How to reach it (host, TLS and certificate settings).</param>
    /// <param name="remediationUserName">The server's remediation login name.</param>
    /// <param name="remediationSecretSlot">The server's secret slot: an <c>env:</c> or <c>file:</c> reference. A stored password is refused.</param>
    public SqlServerRemediationExecutor(int serverId, RemediationServer server, string? remediationUserName, string? remediationSecretSlot,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(server);
        _serverId = serverId;
        _server = server;
        _userName = remediationUserName;
        _secretSlot = remediationSecretSlot;
        _logger = logger ?? NullLogger.Instance;
    }

    internal const string ForceSql = "sys.sp_query_store_force_plan";
    internal const string UnforceSql = "sys.sp_query_store_unforce_plan";

    /// <summary>The server's own record of the plan, read in the target database: does this plan exist for this query, and is it forced.</summary>
    internal const string PlanStateSql =
        "SELECT is_forced_plan FROM sys.query_store_plan WHERE plan_id = @plan_id AND query_id = @query_id OPTION(RECOMPILE);";

    internal const string HandleCachedSql =
        "SELECT cached = COUNT_BIG(*) FROM sys.dm_exec_cached_plans WHERE plan_handle = @plan_handle OPTION(RECOMPILE);";

    /// <summary>The handle goes in as a typed parameter: the validated text is converted to bytes and never joins the batch text.</summary>
    internal const string EvictSql = "DBCC FREEPROCCACHE (@plan_handle);";

    /// <summary>
    /// One batch: <c>KILL</c> runs only when <c>sys.dm_exec_sessions</c> still has that session id with that <c>login_time</c>. The session id is an
    /// int parameter and the only thing that joins the <c>KILL</c> text is its decimal form. The answer is 1 when it killed.
    /// </summary>
    internal const string KillSql = @"
DECLARE @killed bit = 0;
IF EXISTS (SELECT 1 FROM sys.dm_exec_sessions AS s WHERE s.session_id = @session_id AND s.login_time = @login_time)
BEGIN
    DECLARE @kill nvarchar(40) = N'KILL ' + CONVERT(nvarchar(11), @session_id) + N';';
    EXEC (@kill);
    SET @killed = 1;
END;
SELECT killed = @killed;";

    internal const string SessionMatchesSql =
        "SELECT matches = COUNT_BIG(*) FROM sys.dm_exec_sessions AS s WHERE s.session_id = @session_id AND s.login_time = @login_time OPTION(RECOMPILE);";

    public Task<RemediationResult> ForcePlanAsync(ServerActionPermit permit, long queryId, long planId, bool dryRun, CancellationToken ct) =>
        ForceOrUnforceAsync(permit, ServerActionNames.ForcePlan, "force_plan", queryId, planId, dryRun, force: true, ct);

    public Task<RemediationResult> UnforcePlanAsync(ServerActionPermit permit, long queryId, long planId, bool dryRun, CancellationToken ct) =>
        ForceOrUnforceAsync(permit, ServerActionNames.ForcePlan, "unforce_plan", queryId, planId, dryRun, force: false, ct);

    private async Task<RemediationResult> ForceOrUnforceAsync(ServerActionPermit permit, string serverAction, string name, long queryId, long planId,
        bool dryRun, bool force, CancellationToken ct)
    {
        if (PermitRefusal(permit, serverAction, planId.ToString(CultureInfo.InvariantCulture), name, dryRun) is { } mismatch)
            return mismatch;
        var database = permit.Database;
        if (string.IsNullOrWhiteSpace(database) || database.Length > 128 || ContainsControl(database))
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadDatabase,
                "A plan is forced in one named database. The permit names none, or names one that is not a database name."));
        if (queryId <= 0 || planId <= 0)
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.PlanNotFound, "A query id and a plan id are positive numbers."));

        return await RunAsync(name, dryRun, database, async (connection, secret) =>
        {
            /* The server's own state, read in that database now: the store's 24-hour window is not consulted. */
            bool? forced;
            await using (var read = Command(connection, PlanStateSql))
            {
                read.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });
                read.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });
                var value = await read.ExecuteScalarAsync(ct);
                forced = value is null or DBNull ? null : Convert.ToBoolean(value, CultureInfo.InvariantCulture);
            }

            if (forced is null)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.PlanNotFound,
                    $"Plan {planId} does not exist for query {queryId} in {database} on the server's Query Store.");
            if (force && forced == true)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.AlreadyForced,
                    $"Plan {planId} is already forced for query {queryId}. Nothing was written.");
            if (!force && forced == false)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotForced,
                    $"Plan {planId} is not forced for query {queryId}. Nothing was written.");

            if (dryRun)
                return RemediationResult.Done(name, dryRun, wrote: false,
                    $"Dry run: plan {planId} exists for query {queryId} and could be {(force ? "forced" : "unforced")}. Nothing was written.");

            await using var write = Command(connection, force ? ForceSql : UnforceSql);
            write.CommandType = CommandType.StoredProcedure;
            write.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });
            write.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });
            await write.ExecuteNonQueryAsync(ct);
            return RemediationResult.Done(name, dryRun, wrote: true, $"Plan {planId} was {(force ? "forced" : "unforced")} for query {queryId} in {database}.");
        }, ct);
    }

    /// <summary>Evicts ONE plan handle from the plan cache. <paramref name="planHandle"/> is <c>0x</c> and hex, at most 64 bytes; anything else is
    /// refused before any connection opens.</summary>
    public async Task<RemediationResult> EvictPlanAsync(ServerActionPermit permit, string planHandle, bool dryRun, CancellationToken ct)
    {
        const string name = "evict_plan";
        if (!PlanHandleText.TryParse(planHandle, out var bytes))
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadHandle,
                "A plan handle is 0x followed by hex digits in pairs, at most 64 bytes. Nothing was run."));
        if (PermitRefusal(permit, ServerActionNames.EvictPlan, planHandle, name, dryRun, ignoreCase: true) is { } mismatch)
            return mismatch;

        return await RunAsync(name, dryRun, null, async (connection, secret) =>
        {
            long cached;
            await using (var read = Command(connection, HandleCachedSql))
            {
                read.Parameters.Add(new SqlParameter("@plan_handle", SqlDbType.VarBinary, 64) { Value = bytes });
                cached = Convert.ToInt64(await read.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
            }

            if (cached == 0)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotCached,
                    "That plan handle is not in the plan cache (it may have aged out). Nothing was run.");
            if (dryRun)
                return RemediationResult.Done(name, dryRun, wrote: false, "Dry run: that plan handle is in the plan cache. Nothing was run.");

            await using var evict = Command(connection, EvictSql);
            evict.Parameters.Add(new SqlParameter("@plan_handle", SqlDbType.VarBinary, 64) { Value = bytes });
            await evict.ExecuteNonQueryAsync(ct);
            return RemediationResult.Done(name, dryRun, wrote: true, "That plan handle was evicted from the plan cache.");
        }, ct);
    }

    /// <summary>
    /// Kills one session, only when it still has that <paramref name="loginTime"/> (read from <c>sys.dm_exec_sessions.login_time</c>, a server-local
    /// time with no zone: pass it back as it was read). A reused session id has a different login time, so it is left alone.
    /// </summary>
    public async Task<RemediationResult> KillSessionAsync(ServerActionPermit permit, int sessionId, DateTime loginTime, bool dryRun, CancellationToken ct)
    {
        const string name = "kill_session";
        if (sessionId <= 0)
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadSession, "A session id is a positive whole number. Nothing was run."));
        if (PermitRefusal(permit, ServerActionNames.KillSession, sessionId.ToString(CultureInfo.InvariantCulture), name, dryRun) is { } mismatch)
            return mismatch;

        return await RunAsync(name, dryRun, null, async (connection, secret) =>
        {
            if (dryRun)
            {
                await using var read = Command(connection, SessionMatchesSql);
                AddSessionParameters(read, sessionId, loginTime);
                var matches = Convert.ToInt64(await read.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0;
                return matches
                    ? RemediationResult.Done(name, dryRun, wrote: false, $"Dry run: session {sessionId} still has that login time and would be killed. Nothing was run.")
                    : RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionChanged,
                        $"Session {sessionId} is gone or no longer has that login time. Nothing was run.");
            }

            await using var command = Command(connection, KillSql);
            AddSessionParameters(command, sessionId, loginTime);
            var killed = await command.ExecuteScalarAsync(ct) is true;
            return killed
                ? RemediationResult.Done(name, dryRun, wrote: true, $"Session {sessionId} was killed.")
                : RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionChanged,
                    $"Session {sessionId} is gone or no longer has that login time. Nothing was killed.");
        }, ct);
    }

    private static void AddSessionParameters(SqlCommand command, int sessionId, DateTime loginTime)
    {
        command.Parameters.Add(new SqlParameter("@session_id", SqlDbType.Int) { Value = sessionId });
        command.Parameters.Add(new SqlParameter("@login_time", SqlDbType.DateTime) { Value = DateTime.SpecifyKind(loginTime, DateTimeKind.Unspecified) });
    }

    /// <summary>The permit rule: for this action, this server, and this object. A mismatch is refused before the secret is read.</summary>
    private RemediationResult? PermitRefusal(ServerActionPermit? permit, string serverAction, string id, string name, bool dryRun, bool ignoreCase = false)
    {
        var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (permit is null
            || !string.Equals(permit.ServerAction, serverAction, StringComparison.Ordinal)
            || permit.ServerId != _serverId
            || !string.Equals(permit.Id, id, comparison))
        {
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.PermitMismatch,
                "The gate's permit is not for this action, this server and this object. Nothing was run."));
        }

        return null;
    }

    private async Task<RemediationResult> RunAsync(string name, bool dryRun, string? database,
        Func<SqlConnection, string, Task<RemediationResult>> body, CancellationToken ct)
    {
        var resolution = RemediationLoginResolver.Resolve(_userName, _secretSlot);
        if (resolution.Login is not { } login)
            return Log(RemediationResult.Refused(name, dryRun, ReasonFor(resolution.Error), resolution.Message ?? "No remediation login."));

        try
        {
            await using var connection = new SqlConnection(RemediationConnection.BuildConnectionString(_server, login, database));
            await connection.OpenAsync(ct);
            return Log(await body(connection, login.Secret));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (SqlException ex)
        {
            /* 18456 is a failed sign-in; 4060 is a database the login cannot open. */
            var reason = ex.Number is 18456 or 4060 ? RemediationResult.Reasons.SignInFailed : null;
            return Log(RemediationResult.Failed(name, dryRun, Scrub(ex.Message, login), reason));
        }
        catch (Exception ex)
        {
            return Log(RemediationResult.Failed(name, dryRun, Scrub(ex.GetType().Name + ": " + ex.Message, login)));
        }
    }

    private static SqlCommand Command(SqlConnection connection, string text) =>
        new(text, connection) { CommandTimeout = 30 };

    internal static string Scrub(string message, RemediationLogin login)
    {
        var secret = login.Secret;
        return secret.Length == 0 ? message : message.Replace(secret, "***", StringComparison.Ordinal);
    }

    private static string ReasonFor(RemediationLoginError error) => error switch
    {
        RemediationLoginError.LoginMissing => RemediationResult.Reasons.LoginMissing,
        RemediationLoginError.SecretSlotMissing => RemediationResult.Reasons.SecretSlotMissing,
        RemediationLoginError.StoredPasswordRefused => RemediationResult.Reasons.StoredPassword,
        RemediationLoginError.SecretUnreadable => RemediationResult.Reasons.SecretUnreadable,
        _ => RemediationResult.Reasons.SecretEmpty,
    };

    private static bool ContainsControl(string text)
    {
        foreach (var c in text)
            if (char.IsControl(c))
                return true;
        return false;
    }

    private RemediationResult Log(RemediationResult result)
    {
        /* The message was built without the secret and scrubbed of it where the server's text is in it, so it is safe to log. */
        _logger.LogInformation("Remediation {Action} on server {ServerId}: {Outcome}{DryRun} {Reason} {Message}",
            result.Action, _serverId, result.Outcome, result.DryRun ? " (dry run)" : "", result.Reason, result.Message);
        return result;
    }
}

/// <summary>The one strict reading of a plan handle (#5632, step 6): <c>0x</c>, then hex digits in whole bytes, 1 to 64 bytes.</summary>
internal static class PlanHandleText
{
    internal static bool TryParse(string? text, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        if (text is null || text.Length < 4 || text.Length > 2 + 128 || (text.Length & 1) != 0)
            return false;
        if (text[0] != '0' || (text[1] != 'x' && text[1] != 'X'))
            return false;
        for (var i = 2; i < text.Length; i++)
        {
            var c = text[i];
            if (!(c is >= '0' and <= '9' || c is >= 'a' and <= 'f' || c is >= 'A' and <= 'F'))
                return false;
        }

        bytes = Convert.FromHexString(text.AsSpan(2));
        return true;
    }
}
