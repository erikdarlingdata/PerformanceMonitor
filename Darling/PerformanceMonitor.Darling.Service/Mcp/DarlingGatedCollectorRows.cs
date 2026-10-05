/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #5249: the collectors a PostgreSQL server's engine does not collect, as rows for <c>get_collection_health</c>.
///
/// <para><b>Why they need a source other than the log.</b> Health rows come from <c>collection_log</c>, and a
/// collector that the dispatch gate never sends at a server writes no log row at all. On a PostgreSQL server
/// several collectors are in that state (the wait-statistics reader on stock PostgreSQL, the sampler on
/// Aurora), so the Collectors panel simply lacked them, and an operator could not tell a collector that
/// failed to run from one that cannot run on this engine. The catalog and
/// <see cref="CollectorEngineCapability"/> already know which is which; this lists them.</para>
///
/// <para><b>Output only.</b> The rows are appended to the tool's response and nowhere else: they are not in the
/// memoized health rows, so they feed no sweep-pressure arithmetic, no failing or regressed count, no fleet
/// roll-up and no caveat. They carry three keys in every response shape (full, compact and partial alike):
/// <c>collector</c>, <c>status</c> and <c>message</c>. They carry no counts or times, because a collector that
/// never runs has none.</para>
///
/// <para>SQL Server servers get no extra rows.</para>
/// </summary>
internal static class DarlingGatedCollectorRows
{
    /// <summary>The status word on a gated row: the same word the <c>not_collected</c> envelope uses.</summary>
    internal const string NotCollectedStatus = "not_collected";

    /// <summary>
    /// The rows for <paramref name="serverName"/>, or none when the server is not PostgreSQL, the engine read
    /// fails, or nothing is gated. Reads the server's engine edition and kind from the registry, then asks
    /// <see cref="CollectorEngineCapability.NotCollectedMessage"/> about every PostgreSQL collector.
    /// A failed registry read answers no rows: the log-driven rows this is appended to are already a good answer.
    /// </summary>
    internal static async Task<IReadOnlyList<object>> AppendAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        IEnumerable<CollectorHealth> loggedRows,
        CancellationToken cancellationToken)
    {
        int engineEdition;
        string? engineKind;
        try
        {
            (engineEdition, engineKind) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, serverId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return Array.Empty<object>();
        }

        return Rows(
            CollectorCatalog.All,
            engineKind,
            loggedRows.Select(r => r.CollectorName).ToHashSet(StringComparer.Ordinal),
            name => CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind, name));
    }

    /// <summary>
    /// The pure half: one row per PostgreSQL <paramref name="catalog"/> collector that <paramref name="messageFor"/>
    /// says is not collected (non-null message) and that has no row in <paramref name="logged"/>, ordered by
    /// collector name (ordinal). Empty unless <paramref name="engineKind"/> is a PostgreSQL kind.
    /// </summary>
    internal static IReadOnlyList<object> Rows(
        IEnumerable<ICollectorSchemaInfo> catalog,
        string? engineKind,
        ISet<string> logged,
        Func<string, string?> messageFor)
    {
        if (!MonitoredEngineKind.IsPostgres(engineKind))
        {
            return Array.Empty<object>();
        }

        var rows = new List<(string Name, string Message)>();
        foreach (var definition in catalog)
        {
            if (definition.TargetEngine != CollectorTargetEngine.PostgreSql || logged.Contains(definition.Name))
            {
                continue;
            }

            var message = messageFor(definition.Name);
            if (message is not null)
            {
                rows.Add((definition.Name, message));
            }
        }

        rows.Sort((a, b) => string.CompareOrdinal(a.Name, b.Name));
        return rows.Select(r => (object)new { collector = r.Name, status = NotCollectedStatus, message = r.Message }).ToList();
    }
}
