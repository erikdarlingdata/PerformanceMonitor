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
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Darling's <see cref="IPlanFetcher"/> — fetches execution plan XML live from the MONITORED
/// SQL Server (the plan cache, on demand; plan XML is never stored in Postgres), the
/// Dashboard's SqlServerPlanFetcher ported with one seam change: where the Dashboard has a
/// single connection string and Lite looks servers up in its ServerManager, Darling monitors
/// many servers from one service, so the constructor takes a
/// <c>Func&lt;int serverId, string?&gt;</c> resolver.
///
/// <para>
/// THE SEAM: Darling's worker supplies the resolver from its per-server runtimes (each
/// connected <c>ServerRuntime</c> carries the built connection string, keyed by the
/// storage-name-hash ServerId the findings persist) — this project deliberately does NOT
/// reference PerformanceMonitor.Darling.Service, so the dependency points service → analysis
/// and the resolver is the only bridge. Returning null for an unknown/disconnected serverId
/// degrades the fetch to null exactly like Lite's ServerManager miss.
/// </para>
///
/// <para>
/// Same query, timeouts, and degrade semantics as the Dashboard implementation: 10-second
/// connect / 15-second command budgets clamped onto the resolved connection string, null for
/// a plan no longer in cache, and any failure logs and returns null — a plan fetch can never
/// fail an analysis run.
/// </para>
/// </summary>
public sealed class PgPlanFetcher : IPlanFetcher
{
    /// <summary>The Dashboard twin's query verbatim — plan_handle arrives as the '0x...' hex
    /// string the collectors persist and converts server-side (CONVERT style 1).</summary>
    public const string PlanQuery = @"
SET NOCOUNT ON;
SELECT query_plan
FROM sys.dm_exec_query_plan(CONVERT(varbinary(64), @plan_handle, 1));";

    private readonly Func<int, string?> _connectionStringResolver;
    private readonly ILogger? _logger;

    public PgPlanFetcher(Func<int, string?> connectionStringResolver, ILogger? logger = null)
    {
        _connectionStringResolver = connectionStringResolver ?? throw new ArgumentNullException(nameof(connectionStringResolver));
        _logger = logger;
    }

    public async Task<string?> FetchPlanXmlAsync(int serverId, string planHandle, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(planHandle))
        {
            return null;
        }

        try
        {
            var connectionString = _connectionStringResolver(serverId);
            if (string.IsNullOrEmpty(connectionString))
            {
                /* Unknown or not-currently-connected server — degrade like Lite's lookup miss. */
                return null;
            }

            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = 10,
                CommandTimeout = 15
            };

            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(PlanQuery, connection);
            command.CommandTimeout = 15;
            command.Parameters.AddWithValue("@plan_handle", planHandle);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result == null || result is DBNull)
            {
                return null;
            }

            return result.ToString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2443: an abandoned fetch is not a plan that failed to come back. Degrading it to null
               here would log an ERROR for work we called off AND let the pass carry on enriching
               against a token that has already fired — the sibling by-sql_handle fetch has excluded
               cancellation from this arm since it was written, and this one now agrees. */
            _logger?.LogError("[PgPlanFetcher] Failed to fetch plan for handle {PlanHandle}: {Message}", planHandle, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// The database-name → QUOTENAME validation (Lite's <c>GetValidatedDatabaseNameAsync</c>): resolves the
    /// caller-supplied database name to its safely-quoted form via <c>sys.databases</c>, so the by-sql_handle
    /// fetch's three-part <c>[db].sys.sp_executesql</c> can never be a SQL-injection vector. Public const so a
    /// test can pin the shape.
    /// </summary>
    public const string ValidateDatabaseSql = @"
SELECT
    quoted_name = QUOTENAME(d.name)
FROM sys.databases AS d
WHERE d.name = @database_name;";

    /// <summary>
    /// The by-sql_handle plan fetch's inner statement (Lite's <c>FetchPlanBySqlHandleAsync</c> body): the stored
    /// statement's plan by sql_handle + the exact statement offsets, newest execution first. This is the key a
    /// deadlock-graph process / blocked-process-report frame carries (no plan_handle), so it is what closes the
    /// "fetch ANY process's live plan" gap. Public const so a test can pin the DMV shape + read-only-ness.
    /// </summary>
    public const string SqlHandlePlanInnerSql = @"
SELECT TOP (1)
    query_plan_text = tqp.query_plan
FROM sys.dm_exec_query_stats AS qs
OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) AS tqp
WHERE qs.sql_handle = @h
AND   qs.statement_start_offset = @stmt_start
AND   qs.statement_end_offset = @stmt_end
AND   tqp.query_plan IS NOT NULL
ORDER BY
    qs.last_execution_time DESC
OPTION(RECOMPILE);";

    /// <summary>
    /// Fetches a cached plan BY SQL_HANDLE + statement offsets — the second live-cache key
    /// <see cref="FetchPlanXmlAsync"/>'s plan_handle path cannot serve, needed for a deadlock-graph process or a
    /// blocked-process-report frame (those XE payloads carry only a sql_handle). Same seam + degrade semantics
    /// as <see cref="FetchPlanXmlAsync"/>: resolves <paramref name="serverId"/> to the connected runtime's
    /// connection string (null for unknown/disconnected → null), validates the database name to QUOTENAME
    /// (falls back to <c>[master]</c> like Lite), and returns null (never throws) for a plan no longer in cache
    /// or any error — a plan fetch can never fail its caller.
    /// </summary>
    public async Task<string?> FetchPlanBySqlHandleAsync(
        int serverId, string? databaseName, string sqlHandleHex,
        int statementStartOffset, int statementEndOffset, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sqlHandleHex))
        {
            return null;
        }

        var handleBytes = HexStringToBytes(sqlHandleHex);
        if (handleBytes is null || handleBytes.Length == 0)
        {
            return null;
        }

        try
        {
            var connectionString = _connectionStringResolver(serverId);
            if (string.IsNullOrEmpty(connectionString))
            {
                return null;
            }

            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = 10,
                CommandTimeout = 30,
            };

            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            var quotedDbName = await GetValidatedDatabaseNameAsync(connection, databaseName, cancellationToken) ?? "[master]";

            var query = $@"
EXECUTE {quotedDbName}.sys.sp_executesql
    N'{SqlHandlePlanInnerSql}',
    N'@h varbinary(64), @stmt_start int, @stmt_end int',
    @h, @stmt_start, @stmt_end;";

            await using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
            command.Parameters.Add(new SqlParameter("@h", SqlDbType.VarBinary, 64) { Value = handleBytes });
            command.Parameters.Add(new SqlParameter("@stmt_start", SqlDbType.Int) { Value = statementStartOffset });
            command.Parameters.Add(new SqlParameter("@stmt_end", SqlDbType.Int) { Value = statementEndOffset });

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is null or DBNull ? null : result.ToString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogError("[PgPlanFetcher] Failed to fetch plan for sql_handle {SqlHandle}: {Message}", sqlHandleHex, ex.Message);
            return null;
        }
    }

    /// <summary>Resolves a database name to its QUOTENAME'd form (null when unknown) — the injection guard for
    /// the three-part <c>sp_executesql</c>. A null/blank name short-circuits to null (caller uses <c>[master]</c>).</summary>
    private static async Task<string?> GetValidatedDatabaseNameAsync(
        SqlConnection connection, string? databaseName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return null;
        }

        await using var command = new SqlCommand(ValidateDatabaseSql, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new SqlParameter("@database_name", SqlDbType.NVarChar, 128) { Value = databaseName });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }

    /// <summary>Parses a '0x…' (or bare) hex handle to bytes; null for a malformed / odd-length string (Lite's
    /// <c>HexStringToBytes</c> verbatim).</summary>
    internal static byte[]? HexStringToBytes(string hex)
    {
        if (string.IsNullOrEmpty(hex))
        {
            return null;
        }

        var start = hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? 2 : 0;
        var len = hex.Length - start;
        if (len <= 0 || (len % 2) != 0)
        {
            return null;
        }

        var bytes = new byte[len / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            if (!byte.TryParse(hex.AsSpan(start + i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out bytes[i]))
            {
                return null;
            }
        }

        return bytes;
    }
}
