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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Remediation;

/// <summary>
/// The server write code (#5632, step 6): force and unforce a Query Store plan, evict one plan handle and kill one session on ONE monitored
/// SQL Server, each with a dry run. <b>One class in the service calls the force, unforce, evict and kill methods</b> (#5632, steps 7, 10 and 12:
/// <c>DarlingMcpPlanForceTools</c>). <c>PlanForceNoWritePathTests</c> pins that no other
/// service type touches this class, and that the service assembly holds none of the write statements: they live here.
/// <para><b>No write runs without a passed gate check.</b> Every method takes a <see cref="ServerActionPermit"/> first, the value
/// <see cref="InvestigationRequestStore.AuthorizeServerActionAsync"/> hands out only when the gate returned no refusal, and its constructor
/// is internal to the Storage assembly. A permit is for one action, one server, and one object: a method refuses a permit issued for another
/// action, another server, another database or another object, before it reads the secret or opens a connection.</para>
/// <para><b>The sign-in</b> is the server's remediation login: its name, and the secret read from the <c>env:</c> or <c>file:</c> reference when
/// the action runs. The secret reaches the connection string and nothing else: every message that leaves here has it removed first.</para>
/// <para>This class is SQL Server only. The PostgreSQL kill (<c>pg_terminate_backend</c>) is <see cref="PostgresRemediationExecutor"/>. An Azure SQL Database's kill is <see cref="KillAzureSessionAsync"/>: it connects to the session's own database.</para>
/// </summary>
public sealed partial class SqlServerRemediationExecutor
{
    private readonly int _serverId;
    private readonly RemediationServer _server;
    private readonly string? _userName;
    private readonly string? _secretSlot;
    private readonly string? _monitoringUserName;
    private readonly ILogger _logger;

    /// <param name="serverId">The monitored server's id; a permit issued for another server is refused.</param>
    /// <param name="server">How to reach it (host, TLS and certificate settings).</param>
    /// <param name="remediationUserName">The server's remediation login name.</param>
    /// <param name="remediationSecretSlot">The server's secret slot: an <c>env:</c> or <c>file:</c> reference. A stored password is refused.</param>
    /// <param name="monitoringUserName">
    /// The server's monitoring login name (the <c>username</c> of its entry). A kill counts a session as Darling's own only when its program name
    /// starts with <c>PerformanceMonitorDarling</c> AND its login is this one or the remediation login (any client can set its own program name).
    /// <b>When this is null (a Windows-authenticated entry has no SQL login name) the program name alone decides.</b> The fallback is in
    /// <see cref="KillSql"/> and <see cref="AzureKillSql"/>, where an unset <c>@monitoring_login</c> counts every Darling-named session as Darling's.
    /// </param>
    public SqlServerRemediationExecutor(int serverId, RemediationServer server, string? remediationUserName, string? remediationSecretSlot,
        ILogger? logger = null, string? monitoringUserName = null)
    {
        ArgumentNullException.ThrowIfNull(server);
        _serverId = serverId;
        _server = server;
        _userName = remediationUserName;
        _secretSlot = remediationSecretSlot;
        _monitoringUserName = string.IsNullOrWhiteSpace(monitoringUserName) ? null : monitoringUserName.Trim();
        _logger = logger ?? NullLogger.Instance;
    }

    internal const string ForceSql = "sys.sp_query_store_force_plan";
    internal const string UnforceSql = "sys.sp_query_store_unforce_plan";

    /// <summary>The server's own record of the plan, read in the target database: does this plan exist for this query, and is it forced.</summary>
    internal const string PlanStateSql =
        "SELECT is_forced_plan FROM sys.query_store_plan WHERE plan_id = @plan_id AND query_id = @query_id OPTION(RECOMPILE);";

    internal const string HandleCachedSql =
        "SELECT cached = COUNT_BIG(*) FROM sys.dm_exec_cached_plans WHERE plan_handle = @plan_handle OPTION(RECOMPILE);";

    /// <summary><c>SERVERPROPERTY('EngineEdition')</c> of an Azure SQL Database.</summary>
    internal const long AzureSqlDatabaseEdition = 5;

    /// <summary>
    /// The evict read batch (#5632, step 10): the engine edition, then whether the login holds <c>ALTER SERVER STATE</c>, then the cached-plan count
    /// (the same count as <see cref="HandleCachedSql"/>). The count runs only on an engine that has the statement and for a login that holds the
    /// grant, so a refusal for either reason never ends in a permission error from the view.
    /// </summary>
    internal const string EvictFactsSql = @"
DECLARE @edition bigint = CONVERT(bigint, SERVERPROPERTY('EngineEdition'));
DECLARE @granted bigint = CONVERT(bigint, ISNULL(HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER SERVER STATE'), 0));
DECLARE @cached bigint = NULL;
IF @edition <> 5 AND @granted = 1
    SELECT @cached = COUNT_BIG(*) FROM sys.dm_exec_cached_plans WHERE plan_handle = @plan_handle OPTION(RECOMPILE);
SELECT edition = @edition, granted = @granted, cached = @cached;";

    /// <summary>The handle goes in as a typed parameter: the validated text is converted to bytes and never joins the batch text.</summary>
    internal const string EvictSql = "DBCC FREEPROCCACHE (@plan_handle);";

    /// <summary>
    /// The one kill batch (#5632, step 12), for a dry run and a write alike (<c>@dry</c>). It reads the session's facts, then decides in this order
    /// and stops at the first that applies: the engine is an Azure SQL Database (<c>azure_sql_database</c>: its kill is <see cref="AzureKillSql"/>, in the session's own database), the session is this connection's
    /// own (<c>own_session</c>), no such session (<c>session_gone</c>), a different <c>login_time</c> (<c>session_reused</c>: the id was reused),
    /// not a user session (<c>not_user_session</c>), a session of Darling itself (a program name that starts with <c>PerformanceMonitorDarling</c> AND the monitoring
    /// or the remediation login, or the remediation login with any program name: <c>tool_login_session</c>; with no monitoring login known, the program name alone), the login lacks <c>ALTER ANY CONNECTION</c> (<c>kill_permission_denied</c>). A write whose <c>@expected_last_end</c> is not the session's <c>last_request_end_time</c>, when <c>@check_activity</c> is 1, stops at <c>session_changed</c> (#7109). Then it runs the
    /// <c>KILL</c> unless <c>@dry</c>, and answers <c>dry_ok</c> or <c>killed</c>. The check and the <c>KILL</c> are one batch but not one atomic step:
    /// a session can end and its id be reused between them. The <c>KILL</c> text is built from the int parameter's decimal form only. After a real
    /// <c>KILL</c> it reads the session's request once (<c>command</c>, <c>percent_complete</c>), so a rollback shows. That read sits in TRY/CATCH, so
    /// nothing after the <c>KILL</c> can raise an error that would read as a failed kill: a read that fails answers <c>rollback_read = 0</c>, and the
    /// kill is still <c>killed</c> with its rollback not read. No statement text and no input buffer are read.
    /// </summary>
    internal const string KillSql = @"
DECLARE @edition bigint = CONVERT(bigint, SERVERPROPERTY('EngineEdition'));
DECLARE @outcome varchar(40) = NULL;
DECLARE @found bit = 0;
DECLARE @login_time_actual datetime = NULL;
DECLARE @is_user bit = NULL;
DECLARE @login_name nvarchar(128) = NULL;
DECLARE @host_name nvarchar(128) = NULL;
DECLARE @program_name nvarchar(128) = NULL;
DECLARE @status nvarchar(30) = NULL;
DECLARE @open_tran int = NULL;
DECLARE @last_end datetime = NULL;
DECLARE @log_bytes bigint = NULL;
DECLARE @command nvarchar(32) = NULL;
DECLARE @percent real = NULL;
DECLARE @rollback_read bit = 1;
IF @edition = 5
    SET @outcome = 'azure_sql_database';
ELSE
BEGIN
    SELECT
        @found = 1,
        @login_time_actual = s.login_time,
        @is_user = s.is_user_process,
        @login_name = s.login_name,
        @host_name = s.host_name,
        @program_name = s.program_name,
        @status = s.status,
        @open_tran = s.open_transaction_count,
        @last_end = s.last_request_end_time,
        @log_bytes =
        (
            SELECT SUM(CONVERT(bigint, ddt.database_transaction_log_bytes_used))
            FROM sys.dm_tran_session_transactions AS stx
            JOIN sys.dm_tran_database_transactions AS ddt
              ON ddt.transaction_id = stx.transaction_id
            WHERE stx.session_id = s.session_id
        )
    FROM sys.dm_exec_sessions AS s
    WHERE s.session_id = @session_id
    OPTION(RECOMPILE);

    IF @session_id = @@SPID
        SET @outcome = 'own_session';
    ELSE IF @found = 0
        SET @outcome = 'session_gone';
    ELSE IF @login_time_actual <> @login_time
        SET @outcome = 'session_reused';
    ELSE IF @is_user = 0
        SET @outcome = 'not_user_session';
    ELSE IF UPPER(@login_name) = UPPER(SUSER_SNAME())
         OR
         (
             @program_name COLLATE Latin1_General_100_CI_AS LIKE N'PerformanceMonitorDarling%'
             AND (@monitoring_login IS NULL OR UPPER(@login_name) = UPPER(@monitoring_login))
         )
        SET @outcome = 'tool_login_session';
    ELSE IF ISNULL(HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY CONNECTION'), 0) <> 1
        SET @outcome = 'kill_permission_denied';
    ELSE IF @check_activity = 1
         AND NOT (@last_end = @expected_last_end OR (@last_end IS NULL AND @expected_last_end IS NULL))
        SET @outcome = 'session_changed';
    ELSE IF @dry = 1
        SET @outcome = 'dry_ok';
    ELSE
    BEGIN
        DECLARE @kill nvarchar(40) = N'KILL ' + CONVERT(nvarchar(11), @session_id) + N';';
        EXEC (@kill);
        SET @outcome = 'killed';
        BEGIN TRY
            SELECT @command = r.command, @percent = r.percent_complete
            FROM sys.dm_exec_requests AS r
            WHERE r.session_id = @session_id
            OPTION(RECOMPILE);
        END TRY
        BEGIN CATCH
            SET @command = NULL;
            SET @percent = NULL;
            SET @rollback_read = 0;
        END CATCH;
    END;
END;
SELECT outcome = @outcome, found = @found, login_time_actual = @login_time_actual, login_name = @login_name, host_name = @host_name,
       program_name = @program_name, status = @status, open_transaction_count = @open_tran, last_request_end_time = @last_end,
       log_bytes_used = @log_bytes, rollback_command = @command, percent_complete = @percent, rollback_read = @rollback_read;";

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
                    with { Statement = PlanForceStatement.For(database, queryId, planId, force) };

            await using var write = Command(connection, force ? ForceSql : UnforceSql);
            write.CommandType = CommandType.StoredProcedure;
            write.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });
            write.Parameters.Add(new SqlParameter("@plan_id", SqlDbType.BigInt) { Value = planId });
            try
            {
                if (PlanWriteSeam.Value is { } seam)
                    await seam(write, ct);
                else
                    await write.ExecuteNonQueryAsync(ct);
            }
            catch (Exception ex) when (ex is OperationCanceledException || IsOutcomeUnknown(ex, connection.State == ConnectionState.Open, ct.IsCancellationRequested))
            {
                /* The procedure was sent and its reply was lost, or the call was cancelled while it ran: the server may have changed the plan.
                   Never reported as failed, which would say nothing changed (#5632). */
                var detail = ex is OperationCanceledException ? "the call was cancelled while the server ran it" : ErrorText(ex, secret);
                return RemediationResult.Unknown(name,
                    $"The statement to {(force ? "force" : "unforce")} plan {planId} for query {queryId} was sent, but its reply was lost ({detail}). The plan may have been {(force ? "forced" : "unforced")}. Read the plan's current state before you try again.");
            }

            return RemediationResult.Done(name, dryRun, wrote: true, $"Plan {planId} was {(force ? "forced" : "unforced")} for query {queryId} in {database}.")
                with { Statement = PlanForceStatement.For(database, queryId, planId, force) };
        }, ct);
    }

    /// <summary>
    /// Evicts ONE plan handle from the plan cache. <paramref name="planHandle"/> is <c>0x</c> and hex, at most 64 bytes; anything else is
    /// refused before any connection opens. One read batch gathers three facts, in this order, before any write: the engine edition (Azure SQL
    /// Database has no plan-handle eviction, <c>evict_unsupported_on_platform</c>), whether the login holds <c>ALTER SERVER STATE</c>
    /// (<c>evict_permission_denied</c>), and how many cached plans have the handle (none, <c>plan_not_cached</c>). Those are the only refusals.
    /// The result's <see cref="RemediationResult.Statement"/> is the text a person would run, with the handle in its normal form.
    /// </summary>
    public async Task<RemediationResult> EvictPlanAsync(ServerActionPermit permit, string planHandle, bool dryRun, CancellationToken ct)
    {
        const string name = "evict_plan";
        if (!PlanHandleText.TryParse(planHandle, out var bytes))
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadHandle,
                "A plan handle is 0x followed by hex digits in pairs, at most 64 bytes. Nothing was run."));
        if (PermitRefusal(permit, ServerActionNames.EvictPlan, planHandle, name, dryRun, ignoreCase: true) is { } mismatch)
            return mismatch;
        var statement = PlanEvictStatement.For(PlanHandleText.Normal(bytes));

        return await RunAsync(name, dryRun, null, async (connection, secret) =>
        {
            long? edition;
            bool granted;
            long? cached;
            await using (var read = Command(connection, EvictFactsSql))
            {
                read.Parameters.Add(new SqlParameter("@plan_handle", SqlDbType.VarBinary, 64) { Value = bytes });
                await using var reader = await read.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct))
                    return RemediationResult.Failed(name, dryRun, "The server returned no answer to the plan cache check.");
                edition = reader.IsDBNull(0) ? null : Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture);
                granted = !reader.IsDBNull(1) && Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture) == 1;
                cached = reader.IsDBNull(2) ? null : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture);
            }

            if (EvictRefusal(name, dryRun, edition, granted, cached, statement) is { } refusal)
                return refusal;
            if (dryRun)
                return RemediationResult.Done(name, dryRun, wrote: false, "Dry run: that plan handle is in the plan cache. Nothing was run.")
                    with { Statement = statement };

            await using var evict = Command(connection, EvictSql);
            evict.Parameters.Add(new SqlParameter("@plan_handle", SqlDbType.VarBinary, 64) { Value = bytes });
            await evict.ExecuteNonQueryAsync(ct);
            return RemediationResult.Done(name, dryRun, wrote: true, "That plan handle was evicted from the plan cache.") with { Statement = statement };
        }, ct);
    }

    /// <summary>
    /// The only refusals of an eviction, in the order the read batch gathered their facts: the engine has no plan handle eviction (Azure SQL
    /// Database), the login lacks <c>ALTER SERVER STATE</c>, the handle is not cached now. Null when none applies. Pure, so a test needs no server.
    /// </summary>
    internal static RemediationResult? EvictRefusal(string name, bool dryRun, long? edition, bool granted, long? cached, string statement)
    {
        if (edition == AzureSqlDatabaseEdition)
            return RemediationResult.Refused(name, dryRun, EvictDegradeReasons.UnsupportedOnPlatform,
                "This server is an Azure SQL Database, which has no plan handle eviction. Nothing was run.") with { Statement = statement };
        if (!granted)
            return RemediationResult.Refused(name, dryRun, EvictDegradeReasons.PermissionDenied,
                "The remediation login does not hold ALTER SERVER STATE on this server, which evicting a plan needs. A person can grant it. Nothing was run.")
                with { Statement = statement };
        if (cached is null or 0)
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotCached,
                "That plan handle is not in the plan cache (it may have aged out). Nothing was run.") with { Statement = statement };
        return null;
    }

    /// <summary>
    /// Kills one session, only when it is still the session the caller named: the same session id and the same <paramref name="loginTime"/>
    /// (<c>sys.dm_exec_sessions.login_time</c>, a <c>datetime</c> in the server's own local time with no zone: pass it as the server's clock shows it).
    /// A reused session id has a different login time, so it is left alone. <see cref="KillSql"/> decides in one batch, for a dry run and a write
    /// alike; the result carries the session's <see cref="RemediationResult.Session"/> facts, and after a real kill its
    /// <see cref="RemediationResult.Rollback"/>. Not atomic: a session can end and its id be reused between the check and the <c>KILL</c>.
    /// When <paramref name="expected"/> is set the batch also refuses (<c>session_changed</c>) unless the session's <c>last_request_end_time</c> is still the
    /// value it holds, and a NULL equals only a NULL (#7109).
    /// </summary>
    public async Task<RemediationResult> KillSessionAsync(ServerActionPermit permit, int sessionId, DateTime loginTime, bool dryRun, CancellationToken ct,
        KillExpectedActivity? expected = null)
    {
        const string name = "kill_session";
        if (sessionId <= 0)
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadSession, "A session id is a positive whole number. Nothing was run."));
        if (PermitRefusal(permit, ServerActionNames.KillSession, sessionId.ToString(CultureInfo.InvariantCulture), name, dryRun) is { } mismatch)
            return mismatch;

        return await RunAsync(name, dryRun, null, async (connection, secret) =>
        {
            await using var command = Command(connection, KillSql);
            command.Parameters.Add(new SqlParameter("@session_id", SqlDbType.Int) { Value = sessionId });
            command.Parameters.Add(new SqlParameter("@login_time", SqlDbType.DateTime) { Value = DateTime.SpecifyKind(loginTime, DateTimeKind.Unspecified) });
            command.Parameters.Add(new SqlParameter("@dry", SqlDbType.Bit) { Value = dryRun });
            command.Parameters.Add(new SqlParameter("@monitoring_login", SqlDbType.NVarChar, 128) { Value = (object?)_monitoringUserName ?? DBNull.Value });
            AddKillActivityParameters(command, expected);
            try
            {
                return await ReadKillAnswerAsync(command, name, dryRun, sessionId, ct);
            }
            catch (Exception ex) when (!dryRun && IsOutcomeUnknown(ex, connection.State == ConnectionState.Open, ct.IsCancellationRequested))
            {
                /* The batch was sent and its reply was lost: the server may have run the KILL. Never reported as failed (#5632). */
                var detail = ErrorText(ex, secret);
                return RemediationResult.Unknown(name,
                    $"The kill batch for session {sessionId} was sent, but its reply was lost ({detail}). Session {sessionId} may have been killed. Read get_blocking to see whether it is still there before you try again.");
            }
        }, ct);
    }

    /// <summary>
    /// The test seam for the kill batch (#5632): null runs it with <c>ExecuteReaderAsync</c>. A test sets it, for its own async flow only, to run
    /// the real batch and then lose the reply, the way a dropped connection would.
    /// </summary>
    internal static readonly AsyncLocal<Func<SqlCommand, CancellationToken, Task<SqlDataReader>>?> KillBatchSeam = new();

    /// <summary>
    /// The test seam for the plan force and unforce write (#5632): null runs the procedure with <c>ExecuteNonQueryAsync</c>. A test sets it, for its
    /// own async flow only, to run the real procedure and then lose the reply or cancel, the way a dropped connection or a cancelled call would.
    /// </summary>
    internal static readonly AsyncLocal<Func<SqlCommand, CancellationToken, Task>?> PlanWriteSeam = new();

    /// <summary>
    /// True for an error that came after the kill batch was sent and is not the server refusing a statement: a broken connection (the
    /// connection is no longer open, or the error is severity 20 or higher), a timeout (-2), a cancel, or an error that is not a
    /// <see cref="SqlException"/> at all. The server may have run the KILL. An error the server sent on a connection that is still open
    /// means the batch stopped there.
    /// </summary>
    internal static bool IsOutcomeUnknown(Exception ex, bool connectionOpen, bool cancelled) => ex switch
    {
        OperationCanceledException => false,
        SqlException sql => !connectionOpen || cancelled || sql.Number == -2 || sql.Class >= 20,
        _ => true,
    };

    /// <summary>
    /// The two parameters both kill batches take for the activity check (#7109). With no <paramref name="expected"/> the check is off (a dry run,
    /// or a caller that holds no binding). With one, the batch refuses unless <c>last_request_end_time</c> equals the expected value, where a NULL
    /// equals only a NULL.
    /// </summary>
    internal static void AddKillActivityParameters(SqlCommand command, KillExpectedActivity? expected)
    {
        command.Parameters.Add(new SqlParameter("@check_activity", SqlDbType.Bit) { Value = expected is not null });
        command.Parameters.Add(new SqlParameter("@expected_last_end", SqlDbType.DateTime)
        {
            Value = expected?.LastRequestEnd is { } ended ? DateTime.SpecifyKind(ended, DateTimeKind.Unspecified) : DBNull.Value,
        });
    }

    private static async Task<RemediationResult> ReadKillAnswerAsync(SqlCommand command, string name, bool dryRun, int sessionId, CancellationToken ct,
        string? database = null)
    {
        {
            await using var reader = !dryRun && KillBatchSeam.Value is { } seam ? await seam(command, ct) : await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return RemediationResult.Failed(name, dryRun, "The server returned no answer to the session check.");
            var outcome = reader.IsDBNull(0) ? null : reader.GetString(0);
            var found = !reader.IsDBNull(1) && Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture) == 1;
            KillSessionFacts? facts = null;
            if (found && outcome is not (KillOutcomes.OwnSession or KillOutcomes.ToolLoginSession or KillOutcomes.SessionInOtherDatabase))
            {
                facts = new KillSessionFacts(sessionId,
                    reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6),
                    reader.IsDBNull(7) ? null : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture),
                    reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                    reader.IsDBNull(9) ? null : Convert.ToInt64(reader.GetValue(9), CultureInfo.InvariantCulture));
            }

            var rollbackCommand = reader.IsDBNull(10) ? null : reader.GetString(10);
            double? percent = reader.IsDBNull(11) ? null : Convert.ToDouble(reader.GetValue(11), CultureInfo.InvariantCulture);
            var rollbackRead = reader.IsDBNull(12) || Convert.ToInt64(reader.GetValue(12), CultureInfo.InvariantCulture) == 1;
            return KillAnswer(name, dryRun, sessionId, outcome, facts, rollbackCommand, percent, rollbackRead, database);
        }
    }

    /// <summary>
    /// Turns the kill batch's outcome into the executor's answer. Pure, so a test needs no server. An outcome the batch does not name is a failure,
    /// never a kill: only <c>killed</c> says <see cref="RemediationResult.Wrote"/>. A kill whose rollback read failed (<paramref name="rollbackRead"/>
    /// false) is still a kill, with no <see cref="RemediationResult.Rollback"/>, as PostgreSQL's kill answers a still-there read that failed.
    /// <paramref name="database"/> is set for the Azure SQL Database kill (<see cref="AzureKillSql"/>): its outcomes and grants are worded for it.
    /// </summary>
    internal static RemediationResult KillAnswer(string name, bool dryRun, int sessionId, string? outcome, KillSessionFacts? facts, string? rollbackCommand, double? percent,
        bool rollbackRead = true, string? database = null)
    {
        var azure = database is not null;
        var result = outcome switch
        {
            KillOutcomes.AzureSqlDatabase => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.DatabaseRequired,
                "This server answered as an Azure SQL Database, but Darling has not recorded its engine edition yet (Darling records it when its collector connects to the server, which has not happened yet or has not happened since the server became an Azure SQL Database), so this call went the SQL Server way and database_name was not used. Call kill_session again after the next collection; then pass database_name, the database the session is in. Nothing was run."),
            KillOutcomes.NotAzureSqlDatabase => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotAzureSqlDatabase,
                "The server answered as an engine that is not an Azure SQL Database, so the Azure kill was not run. Nothing was run."),
            KillOutcomes.WrongDatabase => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.WrongDatabase,
                $"The connection did not land in database {database}, so nothing there was checked. Nothing was run."),
            KillOutcomes.NotPrimary => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotPrimary,
                $"Database {database} is not read-write on this connection, so this is not its primary. The kill runs only on the primary; a session on a named replica or a read-only secondary cannot be reached from it. Nothing was run."),
            KillOutcomes.ViewStateDenied => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.ViewStatePermissionDenied,
                $"The remediation login's user in database {database} does not hold VIEW DATABASE STATE, so it cannot see other sessions there. A person can run GRANT VIEW DATABASE STATE TO <user> in {database}. Nothing was run."),
            KillOutcomes.SessionInOtherDatabase => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionInOtherDatabase,
                $"Session {sessionId} is connected to another database, not {database}. Nothing was run."),
            KillOutcomes.ToolLoginSession => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.ToolLoginSession,
                $"Session {sessionId} belongs to Darling itself (its program name and its login say it is Darling's, it has no login name, or it is the remediation login's), which this tool never kills. Nothing was run."),
            KillOutcomes.SessionGone when azure => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionGone,
                $"Session {sessionId} was not found on the primary of database {database}. It is gone, or it is on a named replica or a read-only secondary, which this tool does not reach. Nothing was run."),
            KillOutcomes.PermissionDenied when azure => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.KillPermissionDenied,
                $"The remediation login's user in database {database} does not hold KILL DATABASE CONNECTION, which killing a session there needs. A person can run GRANT KILL DATABASE CONNECTION TO <user> in {database}. Nothing was run."),
            KillOutcomes.OwnSession => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.OwnSession,
                $"Session {sessionId} is the remediation connection's own session. Nothing was run."),
            KillOutcomes.SessionGone => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionGone,
                $"Session {sessionId} is gone. Nothing was run."),
            KillOutcomes.SessionReused => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionReused,
                $"Session {sessionId} is now a different session: it has another login time, so the server reused the id. Nothing was run."),
            KillOutcomes.SessionChanged => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SessionChanged,
                $"Session {sessionId} has run something new since the dry run: its last request ended at another time, so it may hold a different transaction than the one that was checked. Nothing was run. Read it again with a dry run to see what it holds now."),
            KillOutcomes.NotUserSession => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotUserSession,
                $"Session {sessionId} is not a user session. Nothing was run."),
            KillOutcomes.PermissionDenied => RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.KillPermissionDenied,
                "The remediation login does not hold ALTER ANY CONNECTION on this server, which killing a session needs. A person can grant it. Nothing was run."),
            KillOutcomes.DryOk => RemediationResult.Done(name, dryRun, wrote: false,
                $"Dry run: session {sessionId} is still the session that was named and would be killed. Nothing was run."),
            KillOutcomes.Killed => RemediationResult.Done(name, dryRun, wrote: true, $"Session {sessionId} was killed."),
            _ => RemediationResult.Failed(name, dryRun, "The server returned an answer to the session check that this code does not know. Nothing is known to have run."),
        };

        /* A reused id answers nothing about the session that has it now, and neither does a refusal about the tool's own session or a session in
           another database: those facts are not the session the caller was allowed to ask about (#5632). */
        result = result with
        {
            Session = outcome is KillOutcomes.SessionReused or KillOutcomes.OwnSession or KillOutcomes.ToolLoginSession or KillOutcomes.SessionInOtherDatabase ? null : facts,
        };
        if (outcome == KillOutcomes.Killed && !rollbackRead)
        {
            result = result with { Message = $"Session {sessionId} was killed. Whether a rollback is running was not read; read get_blocking to see its locks clear." };
        }
        else if (outcome == KillOutcomes.Killed)
        {
            var inProgress = rollbackCommand is not null && rollbackCommand.StartsWith("KILLED/ROLLBACK", StringComparison.OrdinalIgnoreCase);
            result = result with { Rollback = new KillRollback(inProgress, inProgress ? percent : null) };
        }

        return result;
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
            if (!string.IsNullOrWhiteSpace(database))
            {
                /* The server resolves the sign-in database under its own collation, which can ignore character width (and accents), so a name the
                   caller's exclusion list check did not match can still land in a database it excludes. Read the name the connection really landed
                   in and compare it with the requested one the way the exclusion list does, before any statement of the action runs. */
                string? landed;
                await using (var probe = Command(connection, "SELECT DB_NAME();"))
                    landed = (await probe.ExecuteScalarAsync(ct)) as string;
                if (LandedElsewhere(name, dryRun, database, landed) is { } wrong)
                    return Log(wrong);
            }

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
            return Log(RemediationResult.Failed(name, dryRun, ErrorText(ex, login), reason));
        }
        catch (Exception ex)
        {
            return Log(RemediationResult.Failed(name, dryRun, ErrorText(ex, login)));
        }
    }

    /// <summary>
    /// The refusal when the connection landed in a database whose real name is not the requested one, or null when it is the requested one (compared
    /// trimmed and without regard to case, as the exclusion list is). Names the database asked for and the one reached, and says nothing was run.
    /// </summary>
    internal static RemediationResult? LandedElsewhere(string name, bool dryRun, string requested, string? landed)
    {
        if (landed is not null && string.Equals(landed, requested.Trim(), StringComparison.OrdinalIgnoreCase))
            return null;
        return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.WrongDatabase,
            $"The connection was asked to open database {requested.Trim()}, but it reached {(landed is null ? "no database" : "database " + landed)}, which is not the same name. Nothing was run.");
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

/// <summary>What the kill batch answers (#5632, step 12). The batch is <see cref="SqlServerRemediationExecutor.KillSql"/>.</summary>
internal static class KillOutcomes
{
    internal const string AzureSqlDatabase = "azure_sql_database";
    internal const string NotAzureSqlDatabase = "not_azure_sql_database";
    internal const string WrongDatabase = "wrong_database";
    internal const string NotPrimary = "not_primary";
    internal const string ViewStateDenied = "view_state_permission_denied";
    internal const string SessionInOtherDatabase = "session_in_other_database";
    internal const string ToolLoginSession = "tool_login_session";
    internal const string OwnSession = "own_session";
    internal const string SessionGone = "session_gone";
    internal const string SessionReused = "session_reused";
    internal const string SessionChanged = "session_changed";
    internal const string NotUserSession = "not_user_session";
    internal const string PermissionDenied = "kill_permission_denied";
    internal const string DryOk = "dry_ok";
    internal const string Killed = "killed";
}

/// <summary>
/// What the server's session view held for the session the kill named, read in the same batch as the decision (#5632, step 12). <c>LoginTime</c> is
/// the session's actual value, so a reused id shows the new session's time. Times are the server's own clock, with no zone. No statement text and no
/// input buffer: they are never read.
/// </summary>
public sealed record KillSessionFacts(
    int SessionId, DateTime? LoginTime, string? LoginName, string? HostName, string? ProgramName, string? Status,
    int? OpenTransactionCount, DateTime? LastRequestEndTime, long? LogBytesUsed);

/// <summary>
/// What a SQL Server or Azure kill write expects of the session beyond its id and login time (#7109): the <c>last_request_end_time</c> the dry run
/// read, in the server's own clock. NULL means the session had never finished a request, and it equals only a NULL.
/// </summary>
public sealed record KillExpectedActivity(DateTime? LastRequestEnd);

/// <summary>Whether the session's request showed a rollback right after a real kill, read once. <c>PercentComplete</c> is null when none runs.</summary>
public sealed record KillRollback(bool InProgress, double? PercentComplete);

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

    /// <summary>The one normal form of a handle: <c>0x</c> and upper-case hex.</summary>
    internal static string Normal(byte[] bytes) => "0x" + Convert.ToHexString(bytes);
}

/// <summary>The one normal form of a plan handle, and the exact statement an eviction runs (#5632, step 10). The statement text lives in this assembly only.</summary>
public static class PlanEvictStatement
{
    /// <summary>The handle as <c>0x</c> and upper-case hex, or false when it is not a plan handle (<c>0x</c>, whole bytes, 1 to 64 of them).</summary>
    public static bool TryNormalize(string? handle, out string normal)
    {
        normal = "";
        if (!PlanHandleText.TryParse(handle, out var bytes))
            return false;
        normal = PlanHandleText.Normal(bytes);
        return true;
    }

    /// <summary>The statement a person would run. <paramref name="normalHandle"/> is the output of <see cref="TryNormalize"/>; the executor itself sends the handle as a typed parameter.</summary>
    public static string For(string normalHandle)
    {
        if (!TryNormalize(normalHandle, out var normal))
            throw new ArgumentException("Not a plan handle.", nameof(normalHandle));
        return "DBCC FREEPROCCACHE (" + normal + ");";
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

/// <summary>The exact statement a force or an unforce runs, for a dry run to show (#5632, step 7).</summary>
public static class PlanForceStatement
{
    /// <summary>
    /// The database named the way <c>QUOTENAME</c> names it (a closing bracket doubled), the ids as plain numbers. The write itself calls the
    /// procedure by its parameters on a connection to that database.
    /// </summary>
    public static string For(string database, long queryId, long planId, bool force)
    {
        ArgumentNullException.ThrowIfNull(database);
        var procedure = force ? SqlServerRemediationExecutor.ForceSql : SqlServerRemediationExecutor.UnforceSql;
        var quoted = "[" + database.Replace("]", "]]", StringComparison.Ordinal) + "]";
        return string.Create(CultureInfo.InvariantCulture,
            $"EXEC {quoted}.{procedure} @query_id = {queryId}, @plan_id = {planId};");
    }
}
