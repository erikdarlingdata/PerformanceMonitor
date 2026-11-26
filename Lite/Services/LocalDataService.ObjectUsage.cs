/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.ObjectReferences;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The reads behind Lite's <c>get_object_usage</c> (#8784): one database's index and object stats snapshots over the window (the shared
/// <see cref="ObjectUsageWindowRead"/> statement and mapper), fed to the shared <see cref="ObjectUsageWindow"/>, and its module executions summed from the stored Query Store rows. Lite keeps every collector for its one
/// retention horizon, so it needs no summary table: both reads go through the <c>v_</c> views, which union the hot tables with the parquet
/// archive. The window read answers from the persisted per-snapshot chain where it can (#9596, <see cref="GetObjectUsageWindowAsync"/>) and
/// folds every snapshot where it cannot.
/// </summary>
public partial class LocalDataService
{
    /// <summary>
    /// The window's module executions for one database. Query Store rows are cumulative per interval and the collector re-reads the open
    /// interval every cycle, so only the newest snapshot of each interval is kept before summing (the #1841 dedup, with the #1907
    /// execution_count tie-break every Query Store dedup carries). The window is on the interval start, with the collection_time floor as the
    /// archive-pruning bound the other Query Store reads use. $1 server_id, $2 database_name, $3 the window start (naive UTC).
    /// </summary>
    internal const string ObjectUsageExecutionsSql = @"
WITH deduped AS
(
    SELECT
        module_name,
        execution_count,
        last_execution_time,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   database_name = $2
    AND   COALESCE(interval_start_time_utc, collection_time) >= $3
    AND   collection_time >= $3 - INTERVAL '1 day'
)
SELECT
    module_name,
    CAST(SUM(execution_count) AS BIGINT),
    MAX(last_execution_time)
FROM deduped
WHERE rn = 1
AND   module_name IS NOT NULL
AND   module_name NOT IN ('Adhoc', 'Unknown')
GROUP BY
    module_name";

    /// <summary>
    /// The span of the database's stored Query Store data in the window, whether or not any module ran: a database whose modules never ran has
    /// no module rows, and the reply still has to say which days were looked at. The third column is how many days of the window hold any
    /// Query Store row for the database, which shows a hole the oldest and newest alone would hide. $1 server_id, $2 database_name, $3 the
    /// window start.
    /// </summary>
    internal const string ObjectUsageExecutionSpanSql = @"
SELECT
    MIN(COALESCE(interval_start_time_utc, collection_time)),
    MAX(collection_time),
    COUNT(DISTINCT CAST(COALESCE(interval_start_time_utc, collection_time) AS DATE))
FROM v_query_store_stats
WHERE server_id = $1
AND   database_name = $2
AND   COALESCE(interval_start_time_utc, collection_time) >= $3
AND   collection_time >= $3 - INTERVAL '1 day'";

    /// <summary>
    /// The database's Query Store capture mode in the window, from the stored health snapshots (v64's <c>query_capture_mode</c>): the newest
    /// one that is not <c>ALL</c> where there is one, otherwise the newest, the same rule as Darling's read. No row when none was stored.
    /// $1 server_id, $2 database_name, $3 the window start.
    /// </summary>
    internal const string ObjectUsageCaptureModeSql = @"
SELECT query_capture_mode
FROM v_query_store_health
WHERE server_id = $1
AND   database_name = $2
AND   capture_time >= $3 - INTERVAL '1 day'
AND   query_capture_mode IS NOT NULL
ORDER BY
    upper(trim(query_capture_mode)) = 'ALL',
    capture_time DESC
LIMIT 1";

    /// <summary>
    /// The window's statements for the view usage read (#8784 part 2): each Query Store statement's executions summed over the window (the
    /// same newest-snapshot-per-interval dedup as <see cref="ObjectUsageExecutionsSql"/>) with its text. The database's own statements, and
    /// those of any other database on the server whose text mentions the database's name, since a view can be read from another database by
    /// a three-part name. $1 server_id, $2 database_name, $3 the window start (naive UTC).
    /// </summary>
    internal const string ObjectUsageStatementsSql = @"
WITH deduped AS
(
    SELECT
        database_name,
        query_id,
        query_text,
        execution_count,
        last_execution_time,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   (database_name = $2 OR contains(lower(query_text), lower($2)))
    AND   COALESCE(interval_start_time_utc, collection_time) >= $3
    AND   collection_time >= $3 - INTERVAL '1 day'
)
SELECT
    database_name,
    CAST(SUM(execution_count) AS BIGINT),
    MAX(last_execution_time),
    MAX(query_text)
FROM deduped
WHERE rn = 1
AND   database_name IS NOT NULL
GROUP BY
    database_name,
    query_id";

    /// <summary>The parsed statements, shared across reads for the life of the app.</summary>
    private static readonly QueryTextScanCache s_objectUsageScanCache = new();

    /// <summary>
    /// Reads the names the window's Query Store statements referenced that can be objects of <paramref name="databaseName"/> (#8784 part 2),
    /// parsing the stored text here at read time: Lite keeps its text for its whole retention, so it needs no summary. Lite keeps no object
    /// list, so a statement that does not parse is counted and not searched. The covered days are <paramref name="executions"/>' (the same
    /// stored rows).
    /// </summary>
    public async Task<ObjectReferenceWindow> GetObjectReferencesAsync(
        int serverId, string databaseName, DateTime windowStartUtc, ObjectExecutionWindow executions)
    {
        ArgumentNullException.ThrowIfNull(databaseName);
        ArgumentNullException.ThrowIfNull(executions);

        var aggregator = new ObjectReferenceAggregator(cache: s_objectUsageScanCache);
        using (var connection = await OpenConnectionAsync())
        using (var command = connection.CreateCommand())
        {
            command.CommandText = ObjectUsageStatementsSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified) });
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                aggregator.Add(
                    reader.GetString(0),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture),
                    reader.IsDBNull(2) ? null : reader.GetDateTime(2));
            }
        }

        return ToReferenceWindow(databaseName, aggregator, executions);
    }

    /// <summary>The aggregator's names that can be objects of <paramref name="databaseName"/>: its own statements' names with no other
    /// database part, and any statement's names that give it as the database part. Counts are its own statements'.</summary>
    internal static ObjectReferenceWindow ToReferenceWindow(string databaseName, ObjectReferenceAggregator aggregator, ObjectExecutionWindow executions)
    {
        var references = new List<ObjectReferenceTotals>();
        foreach (var r in aggregator.References)
        {
            var own = string.Equals(r.DatabaseName, databaseName, StringComparison.Ordinal) && r.ReferencedDatabase.Length == 0;
            var named = string.Equals(r.ReferencedDatabase, databaseName, StringComparison.OrdinalIgnoreCase);
            if (own || named)
            {
                references.Add(new ObjectReferenceTotals(r.SchemaName, r.ObjectName, r.Executions, r.LastExecution, r.TextMatched, r.IsFunction));
            }
        }

        long statements = 0, withoutText = 0, unparsed = 0;
        foreach (var d in aggregator.Databases)
        {
            if (string.Equals(d.DatabaseName, databaseName, StringComparison.Ordinal))
            {
                statements += d.Statements;
                withoutText += d.StatementsWithoutText;
                unparsed += d.StatementsUnparsed;
            }
        }

        return new ObjectReferenceWindow(
            executions.CoveredFromUtc,
            executions.CoveredToUtc,
            executions.DaysWithData,
            "Lite's stored Query Store rows, the statement text parsed at read time for the names each statement reads as a table, view or "
            + "table-valued function.",
            references,
            statements,
            withoutText,
            unparsed);
    }

    /// <summary>Reads the module executions for one database over the window, from the stored Query Store rows.</summary>
    public async Task<ObjectExecutionWindow> GetObjectExecutionsAsync(int serverId, string databaseName, DateTime windowStartUtc)
    {
        var start = DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified);
        using var connection = await OpenConnectionAsync();

        DateTime? from = null;
        DateTime? to = null;
        int? daysWithData = null;
        using (var span = connection.CreateCommand())
        {
            span.CommandText = ObjectUsageExecutionSpanSql;
            span.Parameters.Add(new DuckDBParameter { Value = serverId });
            span.Parameters.Add(new DuckDBParameter { Value = databaseName });
            span.Parameters.Add(new DuckDBParameter { Value = start });
            using var reader = await span.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                daysWithData = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture);
                if (!reader.IsDBNull(0))
                {
                    from = reader.GetDateTime(0);
                    to = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
                }
            }
        }

        var modules = new List<ObjectExecutionTotals>();
        using (var command = connection.CreateCommand())
        {
            command.CommandText = ObjectUsageExecutionsSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = start });
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                modules.Add(new ObjectExecutionTotals(
                    reader.GetString(0),
                    reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture),
                    reader.IsDBNull(2) ? null : reader.GetDateTime(2)));
            }
        }

        string? captureMode = null;
        using (var mode = connection.CreateCommand())
        {
            mode.CommandText = ObjectUsageCaptureModeSql;
            mode.Parameters.Add(new DuckDBParameter { Value = serverId });
            mode.Parameters.Add(new DuckDBParameter { Value = databaseName });
            mode.Parameters.Add(new DuckDBParameter { Value = start });
            using var reader = await mode.ExecuteReaderAsync();
            if (await reader.ReadAsync() && !reader.IsDBNull(0))
            {
                captureMode = reader.GetString(0);
            }
        }

        return new ObjectExecutionWindow(
            from,
            to,
            "Lite's stored Query Store rows, the newest snapshot of each interval summed.",
            modules,
            daysWithData,
            captureMode);
    }
}
