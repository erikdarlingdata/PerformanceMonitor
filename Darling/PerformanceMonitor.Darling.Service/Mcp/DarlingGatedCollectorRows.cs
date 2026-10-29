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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #5249: the collectors a PostgreSQL server's engine does not collect, as rows for <c>get_collection_health</c>.
///
/// <para><b>Why they need a source other than the log.</b> Health rows come from <c>collection_log</c>, and a
/// collector that the dispatch gate never sends at a server writes no log row at all. On a PostgreSQL server
/// several collectors are in that state because this engine KIND can never run them (the wait-statistics
/// reader on stock PostgreSQL, the sampler on Aurora). Gates that depend on the server's major version or on
/// recovery state are not listed: the kind gate is the only one asked here. The Collectors panel simply lacked
/// these, and an operator could not tell a collector that
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


    /// <summary>The sentence on a not-applicable Always On row.</summary>
    internal const string AlwaysOnOffMessage = "Always On availability groups are not enabled on this server, so there is nothing for this collector to read.";

    /// <summary>The two Always On collectors, named once in the shared classifier (#6828) so the count surfaces and this list cannot disagree.</summary>
    internal static bool IsAlwaysOnCollector(string collectorName) =>
        CollectorHealthClassifier.IsAlwaysOnCollector(collectorName);

    /// <summary>
    /// True for a logged row that a server without Always On shows as "not applicable" (round-1 L5): an Always On
    /// collector whose own band is HEALTHY, the ordinary run-and-find-nothing case. An Always On collector that is
    /// FAILING (any other band) is a real problem on a HADR-off server too, and keeps its logged row, Last Error
    /// and all, instead of vanishing behind the not-applicable sentence.
    /// </summary>
    internal static bool IsNotApplicableWhenAlwaysOnOff(CollectorHealth row) =>
        CollectorHealthClassifier.IsNotCollectedWhenAlwaysOnOff(row.CollectorName, row.HealthStatus, alwaysOnOff: true);

    /// <summary>
    /// The logged rows a server without Always On still lists: everything except the HEALTHY Always On collectors
    /// (<see cref="IsNotApplicableWhenAlwaysOnOff"/>), which <see cref="AlwaysOnOffRows"/> replaces.
    /// </summary>
    internal static List<CollectorHealth> RowsShownWhenAlwaysOnOff(IEnumerable<CollectorHealth> loggedRows) =>
        loggedRows.Where(r => !IsNotApplicableWhenAlwaysOnOff(r)).ToList();

    /// <summary>
    /// True when the server's latest <c>server_properties</c> snapshot says Always On (HADR) is off. No snapshot, or a
    /// failed read, is false: with no evidence the log-driven rows stand as they are.
    /// </summary>
    internal static async Task<bool> AlwaysOnOffAsync(NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        try
        {
            var properties = await DarlingDataReader.GetLatestServerPropertiesAsync(postgres, serverId, cancellationToken);
            return properties is { IsHadrEnabled: false };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// #6912: one server's engine facts and the collectors they gate off. A gated collector writes no new row, so what it logged
    /// before the gate existed (an Amazon RDS Web or Express target's SESSION_MISSING runs for <c>attention</c>, after #6895) keeps its
    /// FAILING band for the whole seven-day window. Such a collector reads <c>not_collected</c> whatever it logged, and is left out of
    /// the sweep arithmetic and the counts. The gate is <see cref="CollectorEngineCapability.NotCollectedMessage"/>, the one the reads use.
    /// </summary>
    internal sealed record EngineGate(string ServerName, int EngineEdition, string? EngineKind, AwsRdsKind AwsRdsKind, IReadOnlySet<string> Names)
    {
        /// <summary>True when <paramref name="collectorName"/> does not run on this server's engine.</summary>
        internal bool IsGated(string collectorName) => Names.Contains(collectorName);

        /// <summary>The logged rows that still count: everything this engine runs.</summary>
        internal List<CollectorHealth> RowsThatRun(IEnumerable<CollectorHealth> loggedRows) =>
            loggedRows.Where(r => !IsGated(r.CollectorName)).ToList();

        /// <summary>One <c>not_collected</c> row per gated collector that <paramref name="loggedRows"/> lists, with the engine's own sentence.</summary>
        internal IReadOnlyList<object> NotCollectedRows(IEnumerable<CollectorHealth> loggedRows) =>
            loggedRows
                .Select(r => r.CollectorName)
                .Where(IsGated)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(n => n, StringComparer.Ordinal)
                .Select(n => (object)new
                {
                    collector = n,
                    status = NotCollectedStatus,
                    message = CollectorEngineCapability.NotCollectedMessage(ServerName, EngineEdition, EngineKind, n, AwsRdsKind),
                })
                .ToList();
    }

    /// <summary>
    /// The server's <see cref="EngineGate"/>, or null when it gates nothing or the registry read fails (the log-driven rows then stand,
    /// as they do for <see cref="AppendAsync"/>).
    /// </summary>
    internal static async Task<EngineGate?> ReadEngineGateAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, CancellationToken cancellationToken)
    {
        try
        {
            var (edition, kind, rds) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, serverId, cancellationToken);
            var names = FleetEngineGatedCollectors.For(edition, kind, rds);
            return names.Count == 0 ? null : new EngineGate(serverName, edition, kind, rds, names);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// One <c>not_collected</c> row per HEALTHY Always On collector that <paramref name="loggedRows"/> lists, in collector-name order,
    /// with the same three keys as the engine-gated rows.
    /// </summary>
    internal static IReadOnlyList<object> AlwaysOnOffRows(IEnumerable<CollectorHealth> loggedRows) =>
        loggedRows
            .Where(IsNotApplicableWhenAlwaysOnOff)
            .Select(r => r.CollectorName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .Select(n => (object)new { collector = n, status = NotCollectedStatus, message = AlwaysOnOffMessage })
            .ToList();

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
        AwsRdsKind awsRdsKind;
        try
        {
            (engineEdition, engineKind, awsRdsKind) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, serverId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return Array.Empty<object>();
        }

        return Rows(
            CollectorCatalog.All,
            engineKind,
            loggedRows.Select(r => r.CollectorName).ToHashSet(StringComparer.Ordinal),
            name => CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind, name, awsRdsKind));
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
