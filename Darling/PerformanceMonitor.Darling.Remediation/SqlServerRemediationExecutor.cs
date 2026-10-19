/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json.Serialization;
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
/// SQL Server, each with a dry run. <b>One class in the service calls the force and unforce methods</b> (#5632, step 7:
/// <c>DarlingMcpPlanForceTools</c>); steps 10 and 12 will add the evict and the kill. <c>PlanForceNoWritePathTests</c> pins that no other
/// service type touches this class, and that the service assembly holds none of the write statements: they live here.
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

    /// <summary>
    /// The exact statement a force or an unforce runs, for a dry run to show: the database named the way <c>QUOTENAME</c> names it (a closing
    /// bracket doubled), the ids as plain numbers. The write itself calls the procedure by its parameters on a connection to that database.
    /// </summary>
    public static string StatementFor(string database, long queryId, long planId, bool force)
    {
        ArgumentNullException.ThrowIfNull(database);
        var procedure = force ? ForceSql : UnforceSql;
        var quoted = "[" + database.Replace("]", "]]", StringComparison.Ordinal) + "]";
        return string.Create(CultureInfo.InvariantCulture,
            $"EXEC {quoted}.{procedure} @query_id = {queryId}, @plan_id = {planId};");
    }

    /// <summary>Query Store plan rows: the plan asked about, and any plan of the query that is forced now.</summary>
    internal const string PlanFactsSql =
        "SELECT p.plan_id, is_forced_plan = CONVERT(bit, p.is_forced_plan), p.force_failure_count, p.last_force_failure_reason_desc " +
        "FROM sys.query_store_plan AS p WHERE p.query_id = @query_id AND (p.plan_id = @plan_id OR p.is_forced_plan = 1) OPTION(RECOMPILE);";

    /// <summary><see cref="PlanFactsSql"/> with the forcing type, for SQL Server 2022 and later.</summary>
    internal const string PlanFactsWithTypeSql =
        "SELECT p.plan_id, is_forced_plan = CONVERT(bit, p.is_forced_plan), p.force_failure_count, p.last_force_failure_reason_desc, p.plan_forcing_type_desc " +
        "FROM sys.query_store_plan AS p WHERE p.query_id = @query_id AND (p.plan_id = @plan_id OR p.is_forced_plan = 1) OPTION(RECOMPILE);";

    /// <summary>SQL Server 2022 and later say how a plan was forced (<c>plan_forcing_type_desc</c>: MANUAL or AUTO); earlier versions have no such column.</summary>
    internal const string ForcingTypeColumnSql =
        "SELECT has_type = CASE WHEN COL_LENGTH(N'sys.query_store_plan', N'plan_forcing_type_desc') IS NULL THEN 0 ELSE 1 END;";

    /// <summary>
    /// Reads, on the remediation connection and in the permit's database, which plan of the query is forced now and how, and the state of the
    /// plan asked about. Read-only, and it needs the same permit a force does, so a sign-in with the remediation login still follows a passed gate.
    /// </summary>
    public async Task<PlanStateRead> ReadPlanStateAsync(ServerActionPermit permit, long queryId, long planId, CancellationToken ct)
    {
        const string name = "read_plan_state";
        if (PermitRefusal(permit, ServerActionNames.ForcePlan, planId.ToString(CultureInfo.InvariantCulture), name, dryRun: true) is { } mismatch)
            return PlanStateRead.From(mismatch, null);
        var database = permit.Database;
        if (string.IsNullOrWhiteSpace(database) || database.Length > 128 || ContainsControl(database))
            return PlanStateRead.From(Log(RemediationResult.Refused(name, true, RemediationResult.Reasons.BadDatabase,
                "A plan is forced in one named database. The permit names none, or names one that is not a database name.")), null);
        if (queryId <= 0 || planId <= 0)
            return PlanStateRead.From(Log(RemediationResult.Refused(name, true, RemediationResult.Reasons.PlanNotFound, "A query id and a plan id are positive numbers.")), null);

        PlanForceState? state = null;
        var result = await RunAsync(name, true, database, async (connection, secret) =>
        {
            bool hasType;
            await using (var probe = Command(connection, ForcingTypeColumnSql))
                hasType = Convert.ToInt32(await probe.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) == 1;

            PlanRow? requested = null;
            PlanRow? forced = null;
            await using (var read = Command(connection, hasType ? PlanFactsWithTypeSql : PlanFactsSql))
            {
                read.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });
                read.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });
                await using var reader = await read.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var row = new PlanRow(reader.GetInt64(0), reader.GetBoolean(1),
                        reader.IsDBNull(2) ? null : reader.GetInt64(2), reader.IsDBNull(3) ? null : reader.GetString(3),
                        hasType ? (reader.IsDBNull(4) ? "unknown" : reader.GetString(4)) : "unknown");
                    if (row.PlanId == planId)
                        requested = row;
                    if (row.IsForced)
                        forced = row;
                }
            }

            state = new PlanForceState(database, queryId, planId,
                requested is null ? null : new RequestedPlanState(requested.PlanId, requested.IsForced, requested.FailureCount, requested.FailureReason),
                forced is null ? null : new ForcedPlanState(forced.PlanId, forced.ForcingType, forced.FailureCount, forced.FailureReason));
            return RemediationResult.Done(name, true, wrote: false, "Read the Query Store state of the query. Nothing was written.");
        }, ct);
        return PlanStateRead.From(result, state);
    }

    private sealed record PlanRow(long PlanId, bool IsForced, long? FailureCount, string? FailureReason, string ForcingType);

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
                    $"Dry run: plan {planId} exists for query {queryId} and could be {(force ? "forced" : "unforced")}. Nothing was written.")
                    with { Statement = StatementFor(database, queryId, planId, force) };

            await using var write = Command(connection, force ? ForceSql : UnforceSql);
            write.CommandType = CommandType.StoredProcedure;
            write.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });
            write.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });
            await write.ExecuteNonQueryAsync(ct);
            return RemediationResult.Done(name, dryRun, wrote: true, $"Plan {planId} was {(force ? "forced" : "unforced")} for query {queryId} in {database}.")
                with { Statement = StatementFor(database, queryId, planId, force) };
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

/// <summary>The plan asked about, as the server's Query Store holds it now.</summary>
public sealed record RequestedPlanState(
    [property: JsonPropertyName("plan_id")] long PlanId,
    [property: JsonPropertyName("is_forced")] bool IsForced,
    [property: JsonPropertyName("force_failure_count")] long? ForceFailureCount,
    [property: JsonPropertyName("last_force_failure_reason")] string? LastForceFailureReason);

/// <summary>The plan of the query that is forced now. <c>forcing_type</c> is MANUAL or AUTO on SQL Server 2022 and later, else "unknown".</summary>
public sealed record ForcedPlanState(
    [property: JsonPropertyName("plan_id")] long PlanId,
    [property: JsonPropertyName("forcing_type")] string ForcingType,
    [property: JsonPropertyName("force_failure_count")] long? ForceFailureCount,
    [property: JsonPropertyName("last_force_failure_reason")] string? LastForceFailureReason);

/// <summary>
/// The server's own Query Store state for one query (#5632, step 7), the before and after image a force or an unforce journals. A null
/// <see cref="ForcedPlan"/> means no plan of the query is forced; a null <see cref="RequestedPlan"/> means the plan is not among the query's rows.
/// </summary>
public sealed record PlanForceState(
    [property: JsonPropertyName("database")] string Database,
    [property: JsonPropertyName("query_id")] long QueryId,
    [property: JsonPropertyName("plan_id")] long PlanId,
    [property: JsonPropertyName("requested_plan")] RequestedPlanState? RequestedPlan,
    [property: JsonPropertyName("forced_plan")] ForcedPlanState? ForcedPlan);

/// <summary>What <see cref="SqlServerRemediationExecutor.ReadPlanStateAsync"/> found: the state, or the refusal or failure that stopped the read.</summary>
public sealed record PlanStateRead(RemediationOutcome Outcome, string? Reason, string Message, PlanForceState? State)
{
    internal static PlanStateRead From(RemediationResult result, PlanForceState? state) =>
        new(result.Outcome, result.Reason, result.Message, result.Outcome == RemediationOutcome.Done ? state : null);
}
