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

/// <summary>
/// What the store says about which collectors have ever run for one server. A tab reads it once per refresh and its
/// empty-state notes ask it <see cref="NeverRan"/> and <see cref="NotYetRunNote"/>.
/// </summary>
/// <param name="LoggedCollectors">Collectors with at least one collection_log row for the server, in all retained history.</param>
/// <param name="CollectorsWithData">In-scope collectors whose data table holds at least one row for the server.</param>
/// <param name="ServerHasAnyLogRow">Whether the server has a collection_log row from any collector.</param>
/// <param name="ServerLastCollectedUtc">The server's newest collection_log row from any collector, or null when there is none.</param>
/// <param name="ServerFirstCollectedUtc">The server's oldest collection_log row from any collector, or null when there is none.
/// With <paramref name="ServerLastCollectedUtc"/> it decides the first-run grace; null makes no grace claim.</param>
public sealed record CollectorRunHistory(
    IReadOnlySet<string> LoggedCollectors,
    IReadOnlySet<string> CollectorsWithData,
    bool ServerHasAnyLogRow,
    DateTime? ServerLastCollectedUtc,
    DateTime? ServerFirstCollectedUtc)
{
    /// <summary>No claim: nothing is known about this server, so nothing is said to have never run.</summary>
    public static CollectorRunHistory Empty { get; } = new(
        new HashSet<string>(StringComparer.Ordinal),
        new HashSet<string>(StringComparer.Ordinal),
        false,
        null,
        null);

    /// <summary>
    /// True when the server has collected something, this collector has no log row and no data row in all retained
    /// history, and its first-run grace is over (<see cref="CollectorRuntimePrecondition.IsInsideFirstRunGrace"/>). There is
    /// no time window. The server_config and trace_flags collectors run once at load, so a window would call them never-run
    /// once that load-time row ages out.
    /// </summary>
    public bool NeverRan(string collectorName) =>
        HasNoRun(collectorName)
        && !CollectorRuntimePrecondition.IsInsideFirstRunGrace(collectorName, ServerLastCollectedUtc, ServerFirstCollectedUtc);

    /// <summary>
    /// The shared "not run yet" sentence (<see cref="CollectorRuntimePrecondition.NotYetRunMessage"/>) for a collector with
    /// no run that is inside its first-run grace, or null. Inside the grace <see cref="NeverRan"/> is false, so no surface
    /// says the collector never ran.
    /// </summary>
    public string? NotYetRunNote(string serverName, string collectorName)
    {
        return HasNoRun(collectorName)
            ? CollectorRuntimePrecondition.NotYetRunMessage(serverName, collectorName, ServerLastCollectedUtc, ServerFirstCollectedUtc)
            : null;
    }

    /// <summary>The server has collected something, and this collector has no log row and no data row for it.</summary>
    private bool HasNoRun(string collectorName) =>
        ServerHasAnyLogRow && !LoggedCollectors.Contains(collectorName) && !CollectorsWithData.Contains(collectorName);

    /// <summary>
    /// The collectors whose empty surfaces ask <see cref="NeverRan"/>: the ones whose data table the read probes. Once every
    /// one of them has been seen to run, a further read cannot change an answer, so a refresh skips it (see <see cref="ReadAsync"/>).
    /// </summary>
    internal static readonly string[] SurfaceCollectors = ["server_config", "trace_flags", "memory_pressure_events", "database_states"];

    /// <summary>Whether every collector in <see cref="SurfaceCollectors"/> has a log row or a data row in what is known so far.</summary>
    public bool AllSurfaceCollectorsSeen
    {
        get
        {
            foreach (var collector in SurfaceCollectors)
            {
                if (!LoggedCollectors.Contains(collector) && !CollectorsWithData.Contains(collector))
                    return false;
            }

            return true;
        }
    }

    /// <summary>
    /// What is known after one more read: everything either read saw. A collector that has run stays run. A later read
    /// can lack one that an earlier read listed, once its rows age out of the store, and that does not undo the run. The
    /// server's last collection is the later of the two, and its first collection the earlier.
    /// </summary>
    public CollectorRunHistory Merge(CollectorRunHistory newer)
    {
        var logged = new HashSet<string>(LoggedCollectors, StringComparer.Ordinal);
        logged.UnionWith(newer.LoggedCollectors);

        var withData = new HashSet<string>(CollectorsWithData, StringComparer.Ordinal);
        withData.UnionWith(newer.CollectorsWithData);

        return new CollectorRunHistory(
            logged,
            withData,
            ServerHasAnyLogRow || newer.ServerHasAnyLogRow,
            Later(ServerLastCollectedUtc, newer.ServerLastCollectedUtc),
            Earlier(ServerFirstCollectedUtc, newer.ServerFirstCollectedUtc));
    }

    private static DateTime? Later(DateTime? one, DateTime? other)
    {
        if (one is null || other is null)
        {
            return one ?? other;
        }

        return one.Value >= other.Value ? one : other;
    }

    private static DateTime? Earlier(DateTime? one, DateTime? other)
    {
        if (one is null || other is null)
        {
            return one ?? other;
        }

        return one.Value <= other.Value ? one : other;
    }

    /// <summary>
    /// Brings what is known up to date for one tab refresh. It returns <paramref name="seen"/> without calling
    /// <paramref name="read"/> once every collector in <see cref="SurfaceCollectors"/> has been seen to run, because log and
    /// data rows only appear and nothing a read could find would change an answer. It also skips the read on an Azure SQL
    /// Database while the engine rules out every one of those collectors there, because the engine note answers each surface
    /// first. Otherwise it merges the read into <paramref name="seen"/>. A failed read gives the empty history, which makes
    /// no claim and brings the read back on the next refresh.
    /// </summary>
    internal static async Task<CollectorRunHistory> ReadAsync(
        CollectorRunHistory seen, Func<Task<CollectorRunHistory>> read, bool isAzureSqlDatabase = false)
    {
        if (seen.AllSurfaceCollectorsSeen)
            return seen;

        if (isAzureSqlDatabase && Array.TrueForAll(SurfaceCollectors, collector =>
                !CollectorEngineCapability.IsCollectedOnEngineEdition(collector, CollectorEngineCapability.AzureSqlDatabaseEngineEdition)))
            return seen;

        try
        {
            return seen.Merge(await read());
        }
        catch (Exception)
        {
            return Empty;
        }
    }
}

public partial class LocalDataService
{
    /// <summary>
    /// Reads, for one server, the collectors that have a collection_log row in all retained history, whether the
    /// server has any such row, when it first and last collected, and which of the collectors whose surfaces say "not
    /// collected" have a data row. The data tables are the ones those surfaces read: server_config, trace_flags,
    /// memory_pressure_events and database_states. Any data row proves its collector ran, even where the log row has aged
    /// out.
    ///
    /// <para>The log read goes through <c>v_collection_log</c>, the hot table and the archive. Archival moves a log row
    /// out of the hot table after seven days, and a collector that runs only at load, such as trace_flags, logs once.
    /// Lite keeps archived rows for <see cref="RetentionService.ArchiveRetentionMonths"/> months. A false never-ran note
    /// is possible only when such a collector's log rows have aged out of the archive on a Lite that has not restarted
    /// since, and only on a surface that is already empty.</para>
    /// </summary>
    public async Task<CollectorRunHistory> GetCollectorRunHistoryAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();

        var logged = new HashSet<string>(StringComparer.Ordinal);
        var anyLogRow = false;
        DateTime? lastCollected = null;
        DateTime? firstCollected = null;

        using (var command = connection.CreateCommand())
        {
            /* The server's first and last collection come from the same statement as the collectors, so the first-run
               grace compares two instants of one read. */
            command.CommandText = @"
SELECT collector_name,
       MIN(collection_time) AS first_collected,
       MAX(collection_time) AS last_collected
FROM v_collection_log
WHERE server_id = $1
GROUP BY collector_name";

            command.Parameters.Add(new DuckDBParameter { Value = serverId });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                anyLogRow = true;

                if (!reader.IsDBNull(0))
                {
                    logged.Add(reader.GetString(0));
                }

                if (!reader.IsDBNull(1))
                {
                    var first = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
                    firstCollected = firstCollected is { } earliest && earliest <= first ? earliest : first;
                }

                if (!reader.IsDBNull(2))
                {
                    var last = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
                    lastCollected = lastCollected is { } latest && latest >= last ? latest : last;
                }
            }
        }

        var withData = new HashSet<string>(StringComparer.Ordinal);

        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
SELECT 'server_config' AS collector_name
WHERE EXISTS (SELECT 1 FROM v_server_config WHERE server_id = $1)
UNION ALL
SELECT 'trace_flags'
WHERE EXISTS (SELECT 1 FROM v_trace_flags WHERE server_id = $1)
UNION ALL
SELECT 'memory_pressure_events'
WHERE EXISTS (SELECT 1 FROM v_memory_pressure_events WHERE server_id = $1)
UNION ALL
SELECT 'database_states'
WHERE EXISTS (SELECT 1 FROM v_database_states WHERE server_id = $1)";

            command.Parameters.Add(new DuckDBParameter { Value = serverId });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                withData.Add(reader.GetString(0));
            }
        }

        return new CollectorRunHistory(logged, withData, anyLogRow, lastCollected, firstCollected);
    }
}
