/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Remediation;

/// <summary>
/// The Azure SQL Database kill (#5632). An Azure SQL Database scopes <c>KILL</c>, <c>sys.dm_exec_sessions</c> and
/// <c>sys.dm_exec_requests</c> to the database the connection is in, so the kill connects to the session's own database (never
/// <c>master</c>), read-write, and the batch checks that it is where it was sent: the engine is an Azure SQL Database, <c>DB_NAME()</c> is the
/// database the caller named, and that database is read-write here (on Hyperscale, the primary compute, not a named replica or an HA
/// secondary). Then the same identity rule as the SQL Server kill (session id plus <c>login_time</c>), and the session's own
/// <c>database_id</c> must be <c>DB_ID()</c>.
/// </summary>
public sealed partial class SqlServerRemediationExecutor
{
    /// <summary>
    /// The Azure SQL Database kill batch (#5632), for a dry run and a write alike (<c>@dry</c>). It decides in this order and stops at the first
    /// that applies: the engine is not an Azure SQL Database (<c>not_azure_sql_database</c>); the connection is not in the database the caller
    /// named (<c>wrong_database</c>); that database is not read-write on this connection (<c>not_primary</c>); the login lacks
    /// <c>VIEW DATABASE STATE</c>, without which the session view shows only its own session (<c>view_state_permission_denied</c>); the session
    /// is this connection's own (<c>own_session</c>); no such session on this compute (<c>session_gone</c>); a different <c>login_time</c>
    /// (<c>session_reused</c>); the session is in another database (<c>session_in_other_database</c>); not a user session
    /// (<c>not_user_session</c>); the session has no login name, is the remediation login's with any program name, or has a Darling program name (<c>PerformanceMonitorDarling%</c>) AND the monitoring login (<c>tool_login_session</c>; with no monitoring login known, the program name alone); the login lacks
    /// <c>KILL DATABASE CONNECTION</c> (<c>kill_permission_denied</c>). Then the a write whose <c>@expected_last_end</c> is not the session's <c>last_request_end_time</c>, when <c>@check_activity</c> is 1 (<c>session_changed</c>, #7109). Then the <c>KILL</c> unless <c>@dry</c>, built from the int parameter's
    /// decimal form only, and the same TRY/CATCH read of the session's request after it as <see cref="KillSql"/>. The columns are
    /// <see cref="KillSql"/>'s, so the one reader serves both.
    /// </summary>
    internal const string AzureKillSql = @"
DECLARE @edition bigint = CONVERT(bigint, SERVERPROPERTY('EngineEdition'));
DECLARE @updateability nvarchar(128) = CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Updateability'));
DECLARE @outcome varchar(40) = NULL;
DECLARE @found bit = 0;
DECLARE @login_time_actual datetime = NULL;
DECLARE @is_user bit = NULL;
DECLARE @session_database_id int = NULL;
DECLARE @login_name nvarchar(128) = NULL;
DECLARE @own_login nvarchar(128) = NULL;
DECLARE @login_base nvarchar(128) = NULL;
DECLARE @own_login_base nvarchar(128) = NULL;
DECLARE @monitoring_base nvarchar(128) = NULL;
DECLARE @suser_base nvarchar(128) = NULL;
DECLARE @host_name nvarchar(128) = NULL;
DECLARE @program_name nvarchar(128) = NULL;
DECLARE @status nvarchar(30) = NULL;
DECLARE @open_tran int = NULL;
DECLARE @last_end datetime = NULL;
DECLARE @log_bytes bigint = NULL;
DECLARE @command nvarchar(32) = NULL;
DECLARE @percent real = NULL;
DECLARE @rollback_read bit = 1;
IF @edition IS NULL OR @edition <> 5
    SET @outcome = 'not_azure_sql_database';
ELSE IF DB_NAME() IS NULL OR DB_NAME() <> @database COLLATE Latin1_General_BIN2
    SET @outcome = 'wrong_database';
ELSE IF @updateability IS NULL OR @updateability <> N'READ_WRITE'
    SET @outcome = 'not_primary';
ELSE IF ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DATABASE STATE'), 0) <> 1
    SET @outcome = 'view_state_permission_denied';
ELSE
BEGIN
    SELECT @own_login = s.login_name
    FROM sys.dm_exec_sessions AS s
    WHERE s.session_id = @@SPID
    OPTION(RECOMPILE);

    SELECT
        @found = 1,
        @login_time_actual = s.login_time,
        @is_user = s.is_user_process,
        @session_database_id = s.database_id,
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

    /* A sign-in can be written name or name@server: compare the name part, so either form of a Darling login is the tool's own. The suffix is
       taken off the END of the name only: a name that holds it in the middle is another login. */
    DECLARE @server_suffix nvarchar(200) = N'@' + CONVERT(nvarchar(128), @@SERVERNAME);
    DECLARE @suser nvarchar(128) = CONVERT(nvarchar(128), SUSER_SNAME());
    SET @login_base = CASE WHEN RIGHT(@login_name, LEN(@server_suffix)) = @server_suffix THEN LEFT(@login_name, LEN(@login_name) - LEN(@server_suffix)) ELSE @login_name END;
    SET @own_login_base = CASE WHEN RIGHT(@own_login, LEN(@server_suffix)) = @server_suffix THEN LEFT(@own_login, LEN(@own_login) - LEN(@server_suffix)) ELSE @own_login END;
    SET @suser_base = CASE WHEN RIGHT(@suser, LEN(@server_suffix)) = @server_suffix THEN LEFT(@suser, LEN(@suser) - LEN(@server_suffix)) ELSE @suser END;
    SET @monitoring_base = CASE WHEN RIGHT(@monitoring_login, LEN(@server_suffix)) = @server_suffix THEN LEFT(@monitoring_login, LEN(@monitoring_login) - LEN(@server_suffix)) ELSE @monitoring_login END;

    IF @session_id = @@SPID
        SET @outcome = 'own_session';
    ELSE IF @found = 0
        SET @outcome = 'session_gone';
    ELSE IF @login_time_actual <> @login_time
        SET @outcome = 'session_reused';
    ELSE IF @session_database_id IS NULL OR @session_database_id <> DB_ID()
        SET @outcome = 'session_in_other_database';
    ELSE IF @is_user = 0
        SET @outcome = 'not_user_session';
    ELSE IF @login_name IS NULL
         OR UPPER(@login_base) IN (UPPER(@own_login_base), UPPER(@suser_base))
         OR
         (
             @program_name COLLATE Latin1_General_100_CI_AS LIKE N'PerformanceMonitorDarling%'
             AND (@monitoring_login IS NULL OR UPPER(@login_base) = UPPER(@monitoring_base))
         )
        SET @outcome = 'tool_login_session';
    ELSE IF ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'KILL DATABASE CONNECTION'), 0) <> 1
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

    /// <summary>The grants the remediation login's user needs in each database it may kill in, as the refusals and the README name them.</summary>
    internal const string AzureGrants = "GRANT VIEW DATABASE STATE TO <user>; GRANT KILL DATABASE CONNECTION TO <user>;";

    /// <summary>
    /// Azure errors that say the service, not the statement, failed (#5632): a database that is not available now (40613, also a serverless
    /// database that is paused or resuming), a service error (40197), a busy service (40501), resource limits (49918 to 49920, 10928, 10929),
    /// and every error the shared transient list holds. Before anything is sent they are retried; after the kill batch was sent they answer
    /// "outcome unknown".
    /// </summary>
    internal static bool IsAzureTransient(SqlException ex) =>
        ex.Errors.Cast<SqlError>().Any(e => IsAzureTransientNumber(e.Number)) || IsAzureTransientNumber(ex.Number);

    /// <summary>
    /// A server error as the answer and the log say it (#5632), for the SQL Server and the Azure SQL Database write paths alike. The service's text
    /// for 40613, 40197, 40501, 40615 and 40532 names the logical server, the client's address or a tracing or incident id, so those five are reported by
    /// number with fixed wording. Any other text has the secret, a quoted server name, any GUID and any IPv4 or IPv6 address taken out of it.
    /// </summary>
    internal static string ErrorText(IEnumerable<int> numbers, string message, string secret)
    {
        var all = numbers.ToList();
        if (all.Contains(40613))
            return "SQL error 40613: the database is not available now.";
        if (all.Contains(40197))
            return "SQL error 40197: the service reported an error processing the request.";
        if (all.Contains(40501))
            return "SQL error 40501: the service is busy.";
        if (all.Contains(40615))
            return "SQL error 40615: the server's firewall does not allow this client.";
        if (all.Contains(40532))
            return "SQL error 40532: the server did not accept the server name in the sign-in.";
        var text = secret.Length == 0 ? message : message.Replace(secret, "***", StringComparison.Ordinal);
        text = Regex.Replace(text, @"server\s+['""][^'""]*['""]", "the server", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        text = Regex.Replace(text, @"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<id>", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        return StripAddresses(text);
    }

    /// <summary>
    /// Takes IPv6 and IPv4 addresses out of an error text (a firewall or sign-in error names the client's or the server's address). An IPv6 candidate is
    /// a run of hex digits and colons with at least two colons that <see cref="System.Net.IPAddress"/> accepts, so a clock time stays; a bracket,
    /// a port or a scope id after it stays too.
    /// </summary>
    internal static string StripAddresses(string text)
    {
        var timeout = TimeSpan.FromSeconds(1);
        text = Regex.Replace(text, @"(?<![0-9A-Za-z:.])[0-9A-Fa-f:]*:[0-9A-Fa-f:]*:[0-9A-Fa-f:.]*(?:%[0-9A-Za-z]+)?", match =>
        {
            var candidate = match.Value;
            foreach (var attempt in new[] { candidate, candidate.TrimEnd('.'), candidate.TrimEnd('.', ':') })
            {
                if (attempt.Length >= 2 && System.Net.IPAddress.TryParse(attempt, out var address)
                    && address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
                {
                    return "<address>" + candidate[attempt.Length..];
                }
            }

            return candidate;
        }, RegexOptions.CultureInvariant, timeout);
        return Regex.Replace(text, @"(?<![0-9A-Za-z.])(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(?![0-9A-Za-z])",
            "<address>", RegexOptions.CultureInvariant, timeout);
    }

    /// <summary><see cref="ErrorText(IEnumerable{int}, string, string)"/> for the exception the driver raised.</summary>
    internal static string ErrorText(Exception ex, string secret) => ex is SqlException sql
        ? ErrorText(sql.Errors.Cast<SqlError>().Select(e => e.Number).Append(sql.Number), sql.Message, secret)
        : ErrorText([], ex.GetType().Name + ": " + ex.Message, secret);

    /// <summary><see cref="ErrorText(Exception, string)"/> with the remediation login's secret.</summary>
    internal static string ErrorText(Exception ex, RemediationLogin login) => ErrorText(ex, login.Secret);

    /// <summary>One error number of <see cref="IsAzureTransient"/>.</summary>
    internal static bool IsAzureTransientNumber(int number) =>
        number is 40613 or 40197 or 40501 or 49918 or 49919 or 49920 or 10928 or 10929 || SqlErrorClassification.IsTransient(number);

    /// <summary>
    /// True for an error that came after the Azure kill batch was sent and is not the server refusing a statement: everything
    /// <see cref="IsOutcomeUnknown"/> counts, plus an Azure transient error on a connection that still reads as open. The server may have run
    /// the KILL.
    /// </summary>
    internal static bool IsAzureOutcomeUnknown(Exception ex, bool connectionOpen, bool cancelled) =>
        IsOutcomeUnknown(ex, connectionOpen, cancelled) || ex is SqlException sql && IsAzureTransient(sql);

    /// <summary>How many times the Azure kill tries to open its connection, and the waits between tries. Nothing is sent before an open succeeds.</summary>
    internal static readonly TimeSpan[] AzureOpenWaits = [TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10)];

    /// <summary>
    /// Kills one session on an Azure SQL Database, only when it is still the session the caller named, in the database the caller named, on the
    /// primary. The database is the permit's (<see cref="ServerActionPermit.Database"/>): the gate was asked about that database, so a permit for
    /// one session id never reaches the same id in another database. The connection opens it (never <c>master</c>), read-write.
    /// A transient error on the open is retried (<see cref="AzureOpenWaits"/>); the kill batch is sent once, and never again.
    /// </summary>
    public async Task<RemediationResult> KillAzureSessionAsync(ServerActionPermit permit, int sessionId, DateTime loginTime, bool dryRun, CancellationToken ct,
        KillExpectedActivity? expected = null)
    {
        const string name = "kill_session";
        if (sessionId <= 0)
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadSession, "A session id is a positive whole number. Nothing was run."));
        if (PermitRefusal(permit, ServerActionNames.KillSession, sessionId.ToString(CultureInfo.InvariantCulture), name, dryRun) is { } mismatch)
            return mismatch;
        var database = permit.Database;
        if (string.IsNullOrWhiteSpace(database) || database.Length > 128 || ContainsControl(database)
            || string.Equals(database, "master", StringComparison.OrdinalIgnoreCase))
        {
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadDatabase,
                "The kill on an Azure SQL Database names the user database the session is in, never master. The permit names none, or names one that cannot be. Nothing was run."));
        }

        var resolution = RemediationLoginResolver.Resolve(_userName, _secretSlot);
        if (resolution.Login is not { } login)
            return Log(RemediationResult.Refused(name, dryRun, ReasonFor(resolution.Error), resolution.Message ?? "No remediation login."));

        try
        {
            await using var connection = new SqlConnection(RemediationConnection.BuildConnectionString(_server, login, database));
            if (await OpenAzureAsync(connection, name, dryRun, database, login, ct) is { } notOpened)
                return Log(notOpened);

            await using var command = Command(connection, AzureKillSql);
            command.Parameters.Add(new SqlParameter("@session_id", SqlDbType.Int) { Value = sessionId });
            command.Parameters.Add(new SqlParameter("@login_time", SqlDbType.DateTime) { Value = DateTime.SpecifyKind(loginTime, DateTimeKind.Unspecified) });
            command.Parameters.Add(new SqlParameter("@dry", SqlDbType.Bit) { Value = dryRun });
            command.Parameters.Add(new SqlParameter("@database", SqlDbType.NVarChar, 128) { Value = database });
            command.Parameters.Add(new SqlParameter("@monitoring_login", SqlDbType.NVarChar, 128) { Value = (object?)_monitoringUserName ?? DBNull.Value });
            AddKillActivityParameters(command, expected);
            try
            {
                return Log(await ReadKillAnswerAsync(command, name, dryRun, sessionId, ct, database));
            }
            catch (Exception ex) when (!dryRun && IsAzureOutcomeUnknown(ex, connection.State == ConnectionState.Open, ct.IsCancellationRequested))
            {
                /* The batch was sent and its reply was lost: the server may have run the KILL. Never reported as failed, never sent again (#5632). */
                return Log(RemediationResult.Unknown(name,
                    $"The kill batch for session {sessionId} in database {database} was sent, but its reply was lost ({ErrorText(ex, login)}). Session {sessionId} may have been killed. Read get_blocking to see whether it is still there before you try again."));
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (SqlException ex)
        {
            return Log(RemediationResult.Failed(name, dryRun, ErrorText(ex, login)));
        }
        catch (Exception ex)
        {
            return Log(RemediationResult.Failed(name, dryRun, ErrorText(ex, login)));
        }
    }

    /// <summary>
    /// Opens the Azure kill connection, retrying a transient error the way a collector retries its connect. Null means it is open. Nothing but
    /// the sign-in has been sent when this answers otherwise: a database still unavailable after the last try (a serverless database that is
    /// paused or resuming answers 40613) is a failure that says so, and a sign-in the server refused names the user the login needs.
    /// </summary>
    private static async Task<RemediationResult?> OpenAzureAsync(SqlConnection connection, string name, bool dryRun, string database, RemediationLogin login,
        CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await connection.OpenAsync(ct);
                return null;
            }
            catch (SqlException ex) when (ex.Number is 18456 or 4060 or 916)
            {
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.SignInFailed,
                    $"The remediation login could not open database {database}: {ErrorText(ex, login)} The kill connects to the session's own database, so the "
                    + $"remediation login needs a user there (CREATE USER <user> FROM LOGIN <login>, or a contained user with a password), with {AzureGrants} Nothing was run.");
            }
            catch (SqlException ex) when (IsAzureTransient(ex))
            {
                if (attempt < AzureOpenWaits.Length)
                {
                    connection.Close();
                    await Task.Delay(AzureOpenWaits[attempt], ct);
                    continue;
                }

                var paused = ex.Errors.Cast<SqlError>().Any(e => e.Number == 40613);
                return RemediationResult.Failed(name, dryRun,
                    (paused
                        ? $"Database {database} is not available now. A serverless database that is paused or resuming answers this way; it resumes on this sign-in, so try again in a minute. "
                        : $"Database {database} did not accept the connection after {attempt + 1} tries. ")
                    + $"The server said: {ErrorText(ex, login)} Nothing was sent.",
                    RemediationResult.Reasons.DatabaseUnavailable);
            }
        }
    }
}
