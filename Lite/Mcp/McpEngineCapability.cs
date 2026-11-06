/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The store half of the #2511 engine-capability answer, Lite's side: reads this server's probed engine
/// edition and, when the collector serving a read cannot run on that engine, returns the
/// <c>not_collected</c> envelope. The DECISION and the WORDS both come from
/// <see cref="CollectorEngineCapability"/>, so this file and its Darling twin
/// (<c>DarlingEngineCapability</c>) hold no capability knowledge and no message text of their own — that is
/// what makes the two SKUs byte-identical here by construction rather than by pinning.
///
/// <para><b>Why <c>not_collected</c> rather than <c>unavailable</c>.</b> <c>not_collected</c> means "the
/// input names something this server does not collect", which is exactly true and final for an engine gap.
/// <c>unavailable</c> means "supported here, just not retrievable now", and sends an operator hunting for a
/// collector to restart — the defect #2511 was filed about, not the fix for it.</para>
///
/// <para><b>Called only on the miss path</b>, never before the read: a server whose registry row says one
/// engine while its collected rows say another still gets its DATA rather than a confident explanation of
/// why it cannot have any.</para>
/// </summary>
internal static class McpEngineCapability
{
    /// <summary>
    /// The <c>not_collected</c> envelope when <paramref name="collectorName"/> cannot run on this server's
    /// engine, or <c>null</c> when it can — in which case the caller falls through to its own
    /// <c>empty</c>/<c>unavailable</c> miss, unchanged.
    ///
    /// <para>A store read that FAILS answers null, deliberately: this runs on a path that has already found
    /// no data, and turning a capability probe into a read error would replace one honest miss with a worse
    /// one.</para>
    /// </summary>
    public static async Task<string?> NotCollectedStatusAsync(
        LocalDataService dataService,
        int serverId,
        string serverName,
        string collectorName,
        AwsRdsKind awsRdsKind)
    {
        var engineEdition = await EngineEditionAsync(dataService, serverId);

        /* engineKind: null — "make no claim on the engine-KIND axis" (#2530). Lite has no PostgreSQL target
           seam at all: nothing in this SKU ever sets CollectorTargetEngine.PostgreSql, its DuckDB schema
           generator emits SQL Server definitions only, and its servers table has no engine_kind column to
           read. Passing the constant MonitoredEngineKind.SqlServer would be true today and would be a lie
           the compiler could not find on the day that changes; null is behaviour-identical here (a SQL
           Server collector on a SQL Server target makes no kind claim either way) and cannot become wrong.
           The parameter is REQUIRED rather than defaulted so that decision is visible at the call site
           instead of being made by omission. awsRdsKind is required for the same reason (#6894). */
        var engineEnvelope = Envelope(serverName, engineEdition, collectorName, awsRdsKind);
        if (engineEnvelope is not null || collectorName is not ("query_store" or "plan_correction" or "query_store_health"))
        {
            return engineEnvelope;
        }

        /* #6554: the collectors skip every readable-secondary database, so on a secondary these reads are empty by design. */
        return await McpRuntimePrecondition.QueryStoreReadableSecondaryStatusAsync(dataService, serverId, serverName);
    }

    /// <summary>
    /// #6894, #6893: the same answer for a read of a collector Amazon RDS gates off (running_jobs, and on the Web and Express editions the four
    /// Extended Events captures). The RDS facts are not in the store, so this takes the server manager and reads them from the server's connection
    /// status; the edition axis cannot show them, because RDS reports an ordinary edition and the collected edition is the real one.
    /// </summary>
    public static Task<string?> NotCollectedStatusAsync(
        LocalDataService dataService,
        ServerManager serverManager,
        int serverId,
        string serverName,
        string collectorName)
    {
        var awsRdsKind = AwsRdsKindOf(serverManager, serverId);
        return NotCollectedStatusAsync(dataService, serverId, serverName, collectorName, awsRdsKind: awsRdsKind);
    }

    /// <summary>The <c>not_collected</c> envelope for this engine and collector, or <c>null</c> when the collector can run there.
    /// No engine-kind claim, for the reason given in <see cref="NotCollectedStatusAsync"/>.</summary>
    private static string? Envelope(string serverName, int engineEdition, string collectorName, AwsRdsKind awsRdsKind)
    {
        var message = CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind: null, collectorName, awsRdsKind);
        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.NotCollectedStatusWord, message);
    }

    /// <summary>
    /// #6912: splits a server's collection-health rows into those its engine runs and the ones it does not. A collector the engine does
    /// not run writes no new row, so what it logged before the gate existed (an Amazon RDS Web or Express server's SESSION_MISSING runs
    /// for <c>attention</c>, after #6893) keeps its FAILING band for the whole window. Those read <c>not_collected</c> whatever they
    /// logged, through the same gate the reads use. A failed edition read makes no claim, so the log-driven rows stand.
    /// </summary>
    public static async Task<(List<CollectorHealthRow> Running, List<object> NotCollected)> SplitCollectionHealthByEngineAsync(
        LocalDataService dataService,
        ServerManager serverManager,
        int serverId,
        string serverName,
        List<CollectorHealthRow> rows)
    {
        var engineEdition = await EngineEditionAsync(dataService, serverId);
        var awsRdsKind = AwsRdsKindOf(serverManager, serverId);
        return SplitCollectionHealthByEngine(serverName, engineEdition, awsRdsKind, rows);
    }

    /// <summary>The pure half of <see cref="SplitCollectionHealthByEngineAsync"/>: the engine facts are given.</summary>
    internal static (List<CollectorHealthRow> Running, List<object> NotCollected) SplitCollectionHealthByEngine(
        string serverName, int engineEdition, AwsRdsKind awsRdsKind, List<CollectorHealthRow> rows)
    {
        var running = new List<CollectorHealthRow>();
        var notCollected = new List<object>();
        foreach (var row in rows)
        {
            var message = CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind: null, row.CollectorName, awsRdsKind);
            if (message is null)
            {
                running.Add(row);
            }
            else
            {
                notCollected.Add(new { collector = row.CollectorName, status = CollectorRuntimePrecondition.NotCollectedStatusWord, message });
            }
        }

        return (running, notCollected);
    }

    /// <summary>
    /// #7375: whether this server's newest <c>server_properties</c> row says Always On is off, the fact Darling's <c>get_collection_health</c>
    /// reads the same way. No row, or a failed read, makes no claim (false): the log-driven rows stand. A NULL flag reads as off, as both
    /// stores map it (#6838).
    /// </summary>
    public static async Task<bool> AlwaysOnOffAsync(LocalDataService dataService, int serverId)
    {
        try
        {
            var properties = await dataService.GetLatestServerPropertiesAsync(serverId);
            return CollectorHealthClassifier.IsAlwaysOnOff(properties is not null, properties?.IsHadrEnabled ?? false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// #7375: on a server with Always On off, the two Always On collectors run, find nothing and log HEALTHY with zero rows, and the
    /// zero-output sentence then called that something that "needs a look". Those rows leave the listed rows and come back as
    /// <c>not_collected</c> rows with Darling's sentence (<see cref="CollectorHealthClassifier.AlwaysOnOffMessage"/>), by the rule
    /// Darling uses (<see cref="CollectorHealthClassifier.IsNotCollectedWhenAlwaysOnOff"/>): a FAILING Always On collector keeps its row and
    /// its Last Error. Only the listing and the detail counts change. The sweep arithmetic keeps every row, as Darling's does.
    /// </summary>
    internal static (List<CollectorHealthRow> Shown, List<object> NotCollected) SplitCollectionHealthByAlwaysOn(
        bool alwaysOnOff, List<CollectorHealthRow> rows)
    {
        var shown = rows
            .Where(r => !CollectorHealthClassifier.IsNotCollectedWhenAlwaysOnOff(r.CollectorName, r.HealthStatus, alwaysOnOff))
            .ToList();
        var notCollected = rows
            .Where(r => CollectorHealthClassifier.IsNotCollectedWhenAlwaysOnOff(r.CollectorName, r.HealthStatus, alwaysOnOff))
            .Select(r => r.CollectorName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .Select(n => (object)new { collector = n, status = CollectorRuntimePrecondition.NotCollectedStatusWord, message = CollectorHealthClassifier.AlwaysOnOffMessage })
            .ToList();
        return (shown, notCollected);
    }

    /// <summary>
    /// #6894, #6893: what the connection check found this server to be: an Amazon RDS for SQL Server instance (the <c>rdsadmin</c> database),
    /// and on the Web or Express edition. Lite keeps the facts in memory on the server's connection status, and a tool holds only the storage
    /// id, so the server is found the way the collectors name it. <see cref="AwsRdsKind.None"/> for a server that is not registered, was never
    /// checked, or whose check failed: no RDS claim, which leaves the read on its own precondition answer.
    /// </summary>
    public static AwsRdsKind AwsRdsKindOf(ServerManager serverManager, int storageServerId)
    {
        foreach (var server in serverManager.GetEnabledServers())
        {
            if (RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server)) == storageServerId)
            {
                var status = serverManager.GetConnectionStatus(server.Id);
                return CollectorEngineCapability.AwsRdsKindOf(status.IsAwsRds, status.IsAwsRdsWebOrExpress);
            }
        }

        return AwsRdsKind.None;
    }

    /// <summary>
    /// #7699 item 7: the empty sentence of get_pvs_stats, which says whether this server's version has no persistent version store
    /// (SQL Server before 2019) or has just not completed a cycle. The version is read the way <see cref="AwsRdsKindOf"/> finds the
    /// server; a server that is not found or was never checked has no version (0, assumed newest) and gets the second sentence.
    /// </summary>
    public static CollectorTargetInfo PvsTarget(ServerManager serverManager, int storageServerId)
    {
        var target = new CollectorTargetInfo();
        foreach (var server in serverManager.GetEnabledServers())
        {
            if (RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server)) == storageServerId)
            {
                var status = serverManager.GetConnectionStatus(server.Id);
                target = new CollectorTargetInfo
                {
                    IsAzureSqlDb = status.SqlEngineEdition == 5,
                    IsAzureManagedInstance = status.SqlEngineEdition == 8,
                    SqlMajorVersion = status.SqlMajorVersion,
                };
                break;
            }
        }

        return target;
    }

    /// <summary>The empty sentence of get_pvs_stats for this server: no persistent version store before SQL Server 2019, or no collection cycle yet.</summary>
    public static string PvsNoRowsMessage(ServerManager serverManager, int storageServerId) =>
        PvsStatsCollector.NoRowsMessage(PvsTarget(serverManager, storageServerId));

    /// <summary>#7822: whether this server's SQL Server version has a persistent version store (2019+, or an unknown version, which is assumed newest).</summary>
    public static bool PvsVersionHasStore(ServerManager serverManager, int storageServerId) =>
        PvsStatsCollector.Instance.AppliesTo(PvsTarget(serverManager, storageServerId));

    /// <summary>#7813: the empty sentence of get_plan_corrections for this server: no automatic tuning before SQL Server 2017, or no stored cycle yet. The version comes from the same connection status <see cref="PvsTarget"/> reads.</summary>
    public static string PlanCorrectionNoRowsMessage(ServerManager serverManager, int storageServerId) =>
        PlanCorrectionCollector.NoRowsMessage(PvsTarget(serverManager, storageServerId));

    /// <summary>
    /// #6178: the <c>not_collected</c> envelope for a read that covers every registered server and found nothing, when
    /// <paramref name="collectorName"/> cannot run on ANY of them (a fleet of only Azure SQL Databases). It asks
    /// <see cref="NotCollectedStatusAsync"/> about each server and returns the first envelope, which names the engine and the
    /// collector (for a fleet of several, the first server). It returns <c>null</c> when at least one server could have had the
    /// data, or when the fleet is empty, so the caller keeps its own <c>empty</c> answer.
    /// </summary>
    public static async Task<string?> FleetNotCollectedStatusAsync(
        LocalDataService dataService,
        IReadOnlyList<(int ServerId, string ServerName)> fleet,
        string collectorName)
    {
        string? first = null;
        foreach (var (serverId, serverName) in fleet)
        {
            /* The same answer NotCollectedStatusAsync gives one server; a store read that fails makes no claim. */
            var envelope = Envelope(serverName, await EngineEditionAsync(dataService, serverId), collectorName, awsRdsKind: AwsRdsKind.None);
            if (envelope is null) return null;
            first ??= envelope;
        }

        return first;
    }

    /// <summary>
    /// #6178: whether the newest stored server properties say this server's database is on Azure SQL Database Hyperscale (the
    /// stored edition or an <c>HS_</c> service objective). A missing row or a failed read is false, so the caller falls back to
    /// reading Hyperscale from its own rows.
    /// </summary>
    public static async Task<bool> IsHyperscaleAsync(LocalDataService dataService, int serverId)
    {
        try
        {
            var row = await dataService.GetLatestServerPropertiesAsync(serverId);
            return row is not null && HyperscaleLogSize.IsHyperscaleTier(row.Edition, row.ServiceObjective);
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>
    /// This server's probed engine edition, or <see cref="CollectorEngineCapability.UnknownEngineEdition"/> when the
    /// store has none or the read fails. The unknown edition makes no claim, so a failed read leaves every answer as it
    /// was. A read that HAS data asks this too when one of its fields has no source on some engine
    /// (<c>get_memory_stats</c>' memory state on an Azure SQL Database).
    /// </summary>
    public static async Task<int> EngineEditionAsync(LocalDataService dataService, int serverId)
    {
        try
        {
            return await dataService.GetSqlEngineEditionAsync(serverId);
        }
        catch (Exception)
        {
            return CollectorEngineCapability.UnknownEngineEdition;
        }
    }
}
