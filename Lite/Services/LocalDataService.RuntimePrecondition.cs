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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The most recent run of one collector for one server — the evidence behind the #2546 runtime-precondition
    /// answer. Ordered by <c>log_id</c> rather than <c>collection_time</c> because the id is monotonic per
    /// insert while two runs inside one cycle can share a timestamp, and "the latest run" is the whole claim.
    ///
    /// <para><b>Only the LATEST run</b>, deliberately: a precondition is a current state, and a
    /// SESSION_MISSING from three days ago on a collector that has succeeded since describes something
    /// somebody already fixed. Deliberately takes no <c>asOfUtc</c> — a precondition is a property of the
    /// server now, not of the window the caller asked about, and accepting an anchor here would invite a
    /// caller to believe otherwise. Mirrors Darling's <c>DarlingRuntimePrecondition</c> read.</para>
    /// </summary>
    public async Task<(string? Status, string? ErrorMessage, DateTime? ObservedUtc)> GetLatestCollectorOutcomeAsync(
        int serverId, string collectorName)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT status, error_message, collection_time
FROM collection_log
WHERE server_id = $1
AND   collector_name = $2
ORDER BY log_id DESC
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = collectorName });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return (null, null, null);
        }

        return (
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc));
    }

    /// <summary>
    /// Whether one collector has EVER run for one server, beside when that server last collected anything at
    /// all. Both halves in one round trip, because the inference needs both and they must describe the same
    /// instant: a collector with no rows on a server that is collecting normally is gated off by its
    /// <c>AppliesTo</c>, while a collector with no rows on a server that has collected nothing is just a
    /// collection outage. Mirrors Darling's <c>CollectorEverRanSql</c> — the SAME inference, because the gate
    /// being read is the shared one in <c>PerformanceMonitor.Collectors</c> and a divergence here would mean
    /// the two SKUs disagreed about whether a server was permitted to answer (#2559).
    /// </summary>
    public async Task<(bool EverRan, DateTime? ServerLastCollectedUtc)> GetCollectorEverRanAsync(
        int serverId, string collectorName)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT EXISTS (
           SELECT 1
           FROM collection_log
           WHERE server_id = $1
           AND   collector_name = $2
       ) AS collector_ever_ran,
       (
           SELECT MAX(collection_time)
           FROM collection_log
           WHERE server_id = $1
       ) AS server_last_collected";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = collectorName });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            /* Impossible for this shape (both halves are scalar subqueries), but "we cannot tell" keeps the
               caller on its existing miss rather than asserting a gate that may not be there. */
            return (true, null);
        }

        return (
            !reader.IsDBNull(0) && reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc));
    }

    /// <summary>
    /// The latest per-database Query Store configuration snapshot, optionally narrowed to the databases a
    /// read was scoped to. Reads <c>v_query_store_health</c> — the same relation the Query Store grid and
    /// <c>get_query_store_health</c> answer from, so this can never claim a state those would not confirm.
    ///
    /// <para>The database filter is part of the EVIDENCE rather than a convenience: a caller who scoped a
    /// Query Store read to one database and got nothing needs THAT database's configuration, and a
    /// server-wide answer would hide an off database behind a healthy sibling.</para>
    /// </summary>
    public async Task<(List<CollectorRuntimePrecondition.QueryStoreDatabaseState> States, DateTime? ObservedUtc)>
        GetQueryStoreStatesAsync(int serverId, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var dbClause = BuildDbInClause(databaseNames, "database_name", 2, out var dbValues);

        command.CommandText = @"
SELECT database_name, actual_state, capture_time
FROM v_query_store_health
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM v_query_store_health WHERE server_id = $1)" + dbClause + @"
ORDER BY database_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var states = new List<CollectorRuntimePrecondition.QueryStoreDatabaseState>();
        DateTime? observedUtc = null;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            states.Add(new CollectorRuntimePrecondition.QueryStoreDatabaseState(
                reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1)));

            observedUtc ??= reader.IsDBNull(2)
                ? null
                : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
        }

        return (states, observedUtc);
    }
}
