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
/// The store half of the #2546 runtime-precondition answer, Lite's side: reads what collection already
/// recorded about a collector's most recent run — or about the target's Query Store configuration — and,
/// when it names a precondition somebody can satisfy, returns the <c>precondition</c> envelope. The DECISION
/// and the WORDS both come from <see cref="CollectorRuntimePrecondition"/>, so this file and its Darling twin
/// (<c>DarlingRuntimePrecondition</c>) hold no vocabulary of their own — which is what makes the two SKUs
/// byte-identical here by construction rather than by pinning.
///
/// <para><b>Called after the capability answer, and only on the miss path.</b> A permanent engine gap wins:
/// a precondition message on an engine that can never have the surface would re-introduce the defect #2511
/// closed. And like its capability sibling this is asked AFTER the read came back empty, never before, so a
/// server whose recorded state disagrees with its collected rows still gets its DATA.</para>
///
/// <para><b>A store read that FAILS answers null</b>, deliberately: this runs on a path that has already
/// found no data, and turning a diagnostic into a read error would replace one honest miss with a worse
/// one.</para>
///
/// <para><b>What Lite can and cannot see here.</b> The <c>PERMISSIONS</c> half works exactly as it does on
/// Darling — <c>RunCollectorAsync</c> classifies a denial through the same shared
/// <see cref="SqlServerPermissionErrors"/> predicate, including the XE-session ensure that fails because the
/// login was refused. The <c>SESSION_MISSING</c> half is wired and correct but currently silent on this SKU:
/// Lite has no such status, so a capture session that is absent for any reason OTHER than a denial records as
/// ERROR instead. That is a gap in what Lite RECORDS, not in what this reads, and it is the reason the branch
/// is wired anyway rather than left out — the day Lite classifies it, every read here starts answering
/// without another edit, and wiring only the statuses a SKU happens to write today is exactly how the two
/// drift apart.</para>
/// </summary>
internal static class McpRuntimePrecondition
{
    /// <summary>
    /// The <c>precondition</c> envelope when the collector serving this read recorded one on its most recent
    /// run, or <c>null</c> when it did not — in which case the caller falls through to its own
    /// <c>empty</c>/<c>unavailable</c> miss, unchanged.
    /// </summary>
    public static async Task<string?> StatusAsync(
        LocalDataService dataService,
        int serverId,
        string serverName,
        string collectorName)
    {
        string? status;
        string? errorMessage;
        DateTime? observedUtc;

        try
        {
            (status, errorMessage, observedUtc) =
                await dataService.GetLatestCollectorOutcomeAsync(serverId, collectorName);
        }
        catch (Exception)
        {
            return null;
        }

        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            serverName, collectorName, status, errorMessage, observedUtc);

        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, message);
    }

    /// <summary>
    /// #6453: the sentence for a read that found no stored rows while the collector serving it is HEALTHY (its newest run is
    /// <c>SUCCESS</c> with no note), or null so the caller keeps its own <c>unavailable</c> text. The caller answers it with
    /// <c>empty</c>. Darling's twin is <c>DarlingRuntimePrecondition.HealthyEmptyMessageAsync</c>; both build the sentence in
    /// <see cref="CollectorRuntimePrecondition.HealthyEmptyMessage"/>. Call it AFTER <see cref="StatusAsync"/> and the engine gate.
    /// </summary>
    public static async Task<string?> HealthyEmptyMessageAsync(
        LocalDataService dataService,
        int serverId,
        string serverName,
        string collectorName,
        string what)
    {
        string? status;
        string? errorMessage;
        DateTime? observedUtc;

        try
        {
            (status, errorMessage, observedUtc) =
                await dataService.GetLatestCollectorOutcomeAsync(serverId, collectorName);
        }
        catch (Exception)
        {
            return null;
        }

        return CollectorRuntimePrecondition.HealthyEmptyMessage(serverName, collectorName, what, status, errorMessage, observedUtc);
    }

    /// <summary>
    /// The <c>precondition</c> envelope when the collector serving this read is not being invoked against
    /// this server while the server is collecting normally — i.e. its <c>AppliesTo</c> gate is off — or
    /// <c>null</c> otherwise. Darling's twin (#2559). A collector that has never run and is not due yet gets
    /// the <c>unavailable</c> envelope instead (see <see cref="CollectorRuntimePrecondition.GatedOffStatusWord"/>).
    ///
    /// <para>Call this AFTER <see cref="StatusAsync"/>, never instead of it: a collector that ran and
    /// recorded a denial has a specific sentence from the monitored server itself, which beats an inference
    /// drawn from an absence.</para>
    /// </summary>
    /// <param name="gateCandidates">Operator-facing text naming what could switch this collector off. The
    /// deciding facts are not persisted, so the caller supplies the candidates rather than this method
    /// guessing which one applies.</param>
    public static async Task<string?> GatedOffStatusAsync(
        LocalDataService dataService,
        int serverId,
        string serverName,
        string collectorName,
        string gateCandidates)
    {
        DateTime? collectorLastRunUtc;
        DateTime? serverLastCollectedUtc;
        DateTime? serverFirstCollectedUtc;

        try
        {
            (collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc) =
                await dataService.GetCollectorLastRunAsync(serverId, collectorName);
        }
        catch (Exception)
        {
            return null;
        }

        var message = CollectorRuntimePrecondition.GatedOffMessage(
            serverName, collectorName, gateCandidates, collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc);

        return message is null ? null : McpHelpers.Status(
            CollectorRuntimePrecondition.GatedOffStatusWord(collectorName, collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc),
            message);
    }

    /// <summary>
    /// #6181: Lite's twin of Darling's <c>QueryStoreFirstIntervalMessageAsync</c>. The message for a Query Store read that
    /// found no sample EVER stored, when the latest health snapshot says Query Store is on and the query_store collector's
    /// latest run succeeded (<see cref="CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage"/>), or null when it
    /// does not (state unknown, off, read-only, in error, collector run failed or missing) and the caller keeps its
    /// "may be OFF" text. Null as well when a read fails: this runs on a path that already found nothing.
    /// </summary>
    public static async Task<string?> QueryStoreFirstIntervalMessageAsync(
        LocalDataService dataService, int serverId, string serverName, string? databaseName)
    {
        try
        {
            var (states, observedUtc) = await dataService.GetQueryStoreStatesAsync(
                serverId, string.IsNullOrEmpty(databaseName) ? null : new[] { databaseName });
            var (status, _, _) = await dataService.GetLatestCollectorOutcomeAsync(serverId, "query_store");
            return CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(serverName, states, status, observedUtc);
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// #6074: the sentence a Query Store read that found nothing appends when a database it covers is not READ_WRITE
    /// (<see cref="CollectorRuntimePrecondition.QueryStoreNotWritingHint"/>), read from the same <c>v_query_store_health</c> row
    /// <c>get_query_store_health</c> answers from. Null when every database in scope is collecting, there is no health row, or
    /// the read fails: this runs on a path that already found nothing.
    /// </summary>
    public static async Task<string?> QueryStoreNotWritingHintAsync(LocalDataService dataService, int serverId, string? databaseName)
    {
        try
        {
            var (states, observedUtc) = await dataService.GetQueryStoreStatesAsync(
                serverId, string.IsNullOrEmpty(databaseName) ? null : new[] { databaseName });
            return CollectorRuntimePrecondition.QueryStoreNotWritingHint(states, observedUtc);
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// <paramref name="answer"/> with <see cref="QueryStoreNotWritingHintAsync"/>'s sentence appended to its message when the
    /// answer is an empty or unavailable status (#6074). Other statuses already say their own cause.
    /// </summary>
    public static async Task<string> WithQueryStoreNotWritingHintAsync(
        string answer, LocalDataService dataService, int serverId, string serverName, string? databaseName)
    {
        if (!McpHelpers.IsEmptyOrUnavailableStatus(answer))
        {
            return answer;
        }

        /* #6554: on a readable secondary READ_ONLY is the normal state, so say it is not collected instead of hinting at a fault. */
        if (await QueryStoreReadableSecondaryStatusAsync(dataService, serverId, serverName) is { } notCollected)
        {
            return notCollected;
        }

        return McpHelpers.AppendToMessage(answer, await QueryStoreNotWritingHintAsync(dataService, serverId, databaseName));
    }

    /// <summary>
    /// The <c>precondition</c> envelope when this server's recorded Query Store configuration says the
    /// databases the read covered are not recording runtime statistics, or <c>null</c> when it says nothing
    /// of the kind (no snapshot yet, or at least one database in scope is READ_WRITE).
    /// </summary>
    public static async Task<string?> QueryStoreStatusAsync(
        LocalDataService dataService,
        int serverId,
        string serverName,
        string? databaseName)
    {
        List<CollectorRuntimePrecondition.QueryStoreDatabaseState> states;
        DateTime? observedUtc;

        try
        {
            (states, observedUtc) = await dataService.GetQueryStoreStatesAsync(
                serverId,
                string.IsNullOrEmpty(databaseName) ? null : new[] { databaseName });
        }
        catch (Exception)
        {
            return null;
        }

        var message = CollectorRuntimePrecondition.QueryStoreDisabledMessage(serverName, states, observedUtc);
        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, message);
    }

    /// <summary>
    /// #6554: the <c>not_collected</c> envelope for a Query Store read on a readable secondary, or <c>null</c> when the stored
    /// evidence does not say the server is one or the read fails (this runs on a path that already found nothing).
    /// </summary>
    public static async Task<string?> QueryStoreReadableSecondaryStatusAsync(
        LocalDataService dataService, int serverId, string serverName)
    {
        try
        {
            var (role, healthDatabases, healthSecondary) = await dataService.GetReadableSecondaryFactsAsync(serverId);
            var message = CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage(
                serverName, role, healthDatabases, healthSecondary, replicaConfigured: false,
                "Lite collects Query Store only from a server that is not a readable secondary. " +
                "Add the primary to Lite to read its Query Store.");
            return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.NotCollectedStatusWord, message);
        }
        catch (Exception)
        {
            return null;
        }
    }
}
