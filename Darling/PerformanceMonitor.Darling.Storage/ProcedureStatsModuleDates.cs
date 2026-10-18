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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store half of the deploy collector (#6009, D16 of #5632 Phase C, option C). Two small pieces and nothing else:
/// <see cref="ReadWatchedAsync"/> says which modules a pending deploy follow-up waits on, for one server, and
/// <see cref="StampAsync"/> writes the <c>modify_date</c> the collector read for those modules onto the rows of the batch just copied,
/// in the COPY's own transaction. Every other row keeps a NULL <c>module_modify_date</c>. The column is the V193 one, so no rung.
///
/// <para>The date goes in by a follow-up UPDATE rather than a payload column on purpose: <c>PayloadColumns</c> is shared with Lite's
/// DuckDB schema, which has no such column, and the rows that carry a date are the few a request watches.</para>
/// </summary>
public static class ProcedureStatsModuleDates
{
    /// <summary>The savepoint the stamp runs behind: a fault there never fails the batch.</summary>
    internal const string SavepointName = "pst_modify_date";

    /// <summary>
    /// The conditions of the pending deploy follow-ups on one server: reported requests with a condition whose follow-up has not
    /// opened. <c>module:</c> is 7 characters, so the names start at 8. $1 server.
    /// </summary>
    internal const string WatchedSql = @"
SELECT DISTINCT substr(r.follow_up_condition, 8)
FROM config.investigation_requests AS r
WHERE r.server_id = $1
  AND r.state = 'reported'
  AND r.practice_of IS NULL
  AND r.follow_up_condition IS NOT NULL
  AND r.follow_up_condition LIKE 'module:%'
  AND NOT EXISTS (SELECT 1 FROM config.investigation_requests AS f WHERE f.follow_up_of = r.request_id)";

    /// <summary>The watch list read's own deadline: a small indexed read that must never hold a collection.</summary>
    internal const int WatchedReadTimeoutSeconds = 10;

    /// <summary>One batch row's date: the batch's collection and the module's names. $1 server, $2 collection time, then the arrays.</summary>
    internal const string StampSql = @"
UPDATE collect.procedure_stats AS p
SET module_modify_date = v.modify_date
FROM unnest($3::text[], $4::text[], $5::text[], $6::timestamp[]) AS v (database_name, schema_name, object_name, modify_date)
WHERE p.server_id = $1
  AND p.collection_time = $2
  AND p.database_name = v.database_name
  AND p.schema_name = v.schema_name
  AND p.object_name = v.object_name";

    /// <summary>
    /// The modules a pending deploy follow-up on <paramref name="serverId"/> waits on. Empty when nothing is watched, and then the
    /// collector sends no module query. A store fault gives an empty list and a debug line: the watch list is a convenience, and
    /// the follow-up still ends at its time.
    /// </summary>
    public static async Task<IReadOnlyList<ProcedureStatsCollector.WatchedModule>> ReadWatchedAsync(
        NpgsqlDataSource dataSource, int serverId, ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        var modules = new List<ProcedureStatsCollector.WatchedModule>();
        try
        {
            await using var command = dataSource.CreateCommand(WatchedSql);
            command.CommandTimeout = WatchedReadTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var parts = reader.GetString(0).Split('.');
                if (parts.Length == 3 && parts.All(static p => p.Length > 0))
                {
                    modules.Add(new ProcedureStatsCollector.WatchedModule(parts[0], parts[1], parts[2]));
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(ex, "Deploy follow-ups (#6009): could not read the watched modules of server {ServerId}; no module date is read this collection.", serverId);
            return Array.Empty<ProcedureStatsCollector.WatchedModule>();
        }

        return modules;
    }

    /// <summary>
    /// Stamps the batch's rows that carry a <c>modify_date</c>, on the COPY's transaction behind <see cref="SavepointName"/>. Runs
    /// nothing when no row carries one, which is every batch but those of a server with a watched module. A fault rolls back to the
    /// savepoint and is logged: the rows are stored without the date and the next collection tries again.
    /// </summary>
    public static async Task<int> StampAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime collectionTime,
        IEnumerable<ProcedureStatsCollector.Row> rows, int commandTimeoutSeconds, ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(rows);

        var dated = rows.Where(static r => r.ModuleModifyDate is not null).ToArray();
        if (dated.Length == 0)
        {
            return 0;
        }

        await transaction.SaveAsync(SavepointName, cancellationToken);
        try
        {
            await using var command = new NpgsqlCommand(StampSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = dated.Select(static r => r.DatabaseName).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = dated.Select(static r => r.SchemaName).ToArray() });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = dated.Select(static r => r.ObjectName).ToArray() });
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp,
                Value = dated.Select(static r => DateTime.SpecifyKind(r.ModuleModifyDate!.Value, DateTimeKind.Unspecified)).ToArray(),
            });
            var written = await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.ReleaseAsync(SavepointName, cancellationToken);
            return written;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await transaction.RollbackAsync(SavepointName, cancellationToken);
            logger?.LogWarning(ex,
                "Deploy follow-ups (#6009): the batch for server {ServerId} at {CollectionTime} stored its rows without the module modify_date; the next collection reads it again.",
                serverId, collectionTime.ToString("o", CultureInfo.InvariantCulture));
            return 0;
        }
    }
}
