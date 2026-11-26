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
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// What the store login lacks, said once at start (#9698). A store whose login is not a superuser starts, logs "Postgres store ready" and
/// then runs without three things: the continuous aggregates that need CREATE on <c>_timescaledb_internal</c>, the <c>pg_wal</c> size
/// (<c>pg_ls_waldir()</c>) and the capture of the store's own log (<c>pg_ls_logdir()</c> and <c>pg_read_binary_file</c>). Each of those already
/// logged its own warning in its own pass, one at a time and some of them every hour, so an operator saw a stream of unrelated failures and no
/// single answer. This reads the login's privileges from the catalog, names every one that is missing together with the exact grant that fixes
/// it for the real role name, and the caller logs that as one Warning per start. A login that holds every privilege, a superuser included, logs nothing.
///
/// <para><b>Only what is measured to matter.</b> The log capture is named only when the store writes log files (<c>logging_collector</c> is on):
/// a store that keeps none has nothing for a grant to open. It asks for <c>pg_monitor</c> (which carries <c>pg_ls_logdir()</c>, <c>pg_ls_waldir()</c>
/// and, through <c>pg_read_all_settings</c>, the <c>log_directory</c> setting the capture builds its path from) and an explicit EXECUTE on the
/// three-argument <c>pg_read_binary_file</c>, the call the capture makes. A login granted EXECUTE on <c>pg_ls_logdir()</c> alone can list the folder
/// and still fails at the setting, so listing counts only with both. No <c>pg_read_server_files</c>: PostgreSQL lets EXECUTE read any file under
/// the data directory or an absolute <c>log_directory</c>. A relative <c>log_directory</c> that leads out of the data directory
/// is refused without that role, which no privilege probe here can grant, and the capture's own warning names it. The aggregates are named only when a declared aggregate is absent from the
/// catalog and the login cannot create in <c>_timescaledb_internal</c>: with the schema right missing but every aggregate built, nothing is degraded.
/// Function grants are per database, so the line names the database the statements run in.</para>
/// </summary>
public static class StoreLoginReadiness
{
    /// <summary>What one probe read about the connected login.</summary>
    internal sealed record LoginPrivileges(
        string Login, string Database, bool CanListWal, bool CanListLog, bool CanReadLogFile, bool StoreKeepsLogFiles, bool CanCreateInInternalSchema);

    /// <summary>The verdict: the one line to log and the statements that fix everything it names, in the order to run them.</summary>
    public sealed record MissingPrivilegeReport(string Message, IReadOnlyList<string> Statements);

    /// <summary>
    /// One row. A function or role that does not exist on this server answers as granted, so an old or stripped server never reports a
    /// missing privilege for something it does not have.
    /// </summary>
    internal const string LoginPrivilegesSql = @"
SELECT current_user::text,
       current_database()::text,
       COALESCE(has_function_privilege(current_user, to_regprocedure('pg_catalog.pg_ls_waldir()')::oid, 'EXECUTE'), true),
       COALESCE(has_function_privilege(current_user, to_regprocedure('pg_catalog.pg_ls_logdir()')::oid, 'EXECUTE'), true)
           AND pg_has_role(current_user, 'pg_read_all_settings', 'USAGE'),
       COALESCE(has_function_privilege(current_user, to_regprocedure('pg_catalog.pg_read_binary_file(text, bigint, bigint)')::oid, 'EXECUTE'), true),
       COALESCE(current_setting('logging_collector', true), 'off') = 'on',
       COALESCE(has_schema_privilege(current_user, to_regnamespace('_timescaledb_internal')::oid, 'CREATE'), true)";

    /// <summary>
    /// The deadline of both reads. Each is one catalog round trip with no table scan, the same kind as TimescaleDB's job catalog reads, so it takes
    /// their deadline. The start awaits these reads, and one that runs past it costs only the privilege line.
    /// </summary>
    internal const int ReadTimeoutSeconds = TimescaleSupport.JobCatalogReadTimeoutSeconds;

    /// <summary>The declared aggregates present in the catalog. $1 their view names.</summary>
    internal const string PresentAggregateCountSql = @"
SELECT count(*)::int
FROM timescaledb_information.continuous_aggregates
WHERE view_schema = 'collect' AND view_name = ANY($1)";

    /// <summary>
    /// Reads the login's privileges on <paramref name="connection"/> and returns what is lacking, or null when nothing is.
    /// <paramref name="timescaleAvailable"/> is whether the extension is in use: without it there are no aggregates to count.
    /// </summary>
    public static async Task<MissingPrivilegeReport?> ReadLoginPrivilegesAsync(NpgsqlConnection connection, bool timescaleAvailable, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        LoginPrivileges probe;
        using (var command = new NpgsqlCommand(LoginPrivilegesSql, connection) { CommandTimeout = ReadTimeoutSeconds })
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            probe = new LoginPrivileges(
                reader.GetString(0), reader.GetString(1), reader.GetBoolean(2), reader.GetBoolean(3), reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6));
        }

        var declared = TimescaleSupport.DeclaredContinuousAggregateViews;
        var missing = 0;
        if (timescaleAvailable && !probe.CanCreateInInternalSchema)
        {
            using var count = new NpgsqlCommand(PresentAggregateCountSql, connection) { CommandTimeout = ReadTimeoutSeconds };
            count.Parameters.AddWithValue(declared.ToArray());
            missing = declared.Count - Convert.ToInt32(await count.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        return DescribeMissingPrivileges(probe, missing, declared.Count);
    }

    /// <summary>The verdict for one probe and the number of declared aggregates the catalog lacks.</summary>
    internal static MissingPrivilegeReport? DescribeMissingPrivileges(LoginPrivileges probe, int aggregatesMissing, int aggregatesDeclared)
    {
        var login = QuoteIdentifier(probe.Login);
        var statements = new List<string>();
        var items = new List<string>();

        if (aggregatesMissing > 0 && !probe.CanCreateInInternalSchema)
        {
            statements.Add($"GRANT CREATE ON SCHEMA _timescaledb_internal TO {login}");
            items.Add($"{aggregatesMissing} of {aggregatesDeclared} continuous aggregates are not built, so composer reads fall back to raw scans (needs CREATE on schema _timescaledb_internal)");
        }

        var logCaptureLacks = probe.StoreKeepsLogFiles && (!probe.CanListLog || !probe.CanReadLogFile);
        if (!probe.CanListWal || (logCaptureLacks && !probe.CanListLog))
        {
            statements.Add($"GRANT pg_monitor TO {login}");
        }

        if (!probe.CanListWal)
        {
            items.Add("the pg_wal size is recorded without its figures (needs pg_ls_waldir)");
        }

        if (logCaptureLacks)
        {
            if (!probe.CanReadLogFile)
            {
                statements.Add($"GRANT EXECUTE ON FUNCTION pg_read_binary_file(text, bigint, bigint) TO {login}");
            }

            items.Add("the store's own log is not captured (needs pg_ls_logdir, the log_directory setting and pg_read_binary_file)");
        }

        if (items.Count == 0)
        {
            return null;
        }

        var message = $"The store login {login} is not a superuser and lacks privileges the service uses: {string.Join("; ", items)}. "
            + $"As a superuser, connected to database {QuoteIdentifier(probe.Database)}, run {string.Join("; ", statements)}, then restart the service. "
            + "A superuser login needs none of them.";
        return new MissingPrivilegeReport(message, statements);
    }

    /// <summary>
    /// Probes on <paramref name="connection"/> and logs the verdict as one Warning. Never throws: a probe that cannot run costs the line, never the start.
    /// </summary>
    public static async Task LogMissingPrivilegesAsync(NpgsqlConnection connection, bool timescaleAvailable, ILogger logger, CancellationToken cancellationToken = default)
    {
        try
        {
            var report = await ReadLoginPrivilegesAsync(connection, timescaleAvailable, cancellationToken);
            if (report is not null)
            {
                logger.LogWarning("{Message}", report.Message);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "The store login's privileges could not be read, so the start-up privilege line is skipped");
        }
    }

    private static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
