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
    /// The <c>precondition</c> envelope when the collector serving this read has never run against this
    /// server while the server is collecting normally — i.e. its <c>AppliesTo</c> gate is off — or
    /// <c>null</c> otherwise. Darling's twin (#2559).
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
        bool everRan;
        DateTime? serverLastCollectedUtc;

        try
        {
            (everRan, serverLastCollectedUtc) =
                await dataService.GetCollectorEverRanAsync(serverId, collectorName);
        }
        catch (Exception)
        {
            return null;
        }

        var message = CollectorRuntimePrecondition.GatedOffMessage(
            serverName, collectorName, gateCandidates, everRan, serverLastCollectedUtc);

        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, message);
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
}
