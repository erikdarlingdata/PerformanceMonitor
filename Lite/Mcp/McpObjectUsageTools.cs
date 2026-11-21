/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// Lite's <c>get_object_usage</c> (#8784): which tables, indexed views, procedures and functions in one database were used over a window, for a
/// decommissioning decision. The same tool name and reply shape as Darling's (<see cref="ObjectUsageReport"/> builds both). Lite keeps every
/// collector for its one retention horizon (<see cref="RetentionService.ArchiveRetentionMonths"/> months), so the window is capped there and the
/// coverage says so; it keeps no object list, so a module that never ran is not listed, and it has no ReadOnlyIntent pairing.
/// </summary>
[McpServerToolType]
public sealed class McpObjectUsageTools
{
    /// <summary>The default window, the one both products share.</summary>
    internal const int DefaultDaysBack = ObjectUsageReport.DefaultDaysBack;

    /// <summary>The largest window Lite can hold: its one retention horizon, in days, rounded up.</summary>
    internal const int MaxDaysBack = RetentionService.ArchiveRetentionMonths * 31;

    /// <summary>The default page of object rows, the one both products share.</summary>
    internal const int DefaultLimit = ObjectUsageReport.DefaultLimit;

    /// <summary>The evidence of recent requests, so paging through one reply reads the window once.</summary>
    private static readonly ObjectUsageEvidenceCache s_cache = new();

    [McpServerTool(Name = "get_object_usage"), Description("Which tables, indexed views, procedures and functions in ONE database were used over a long window (default 90 days), for deciding what is safe to decommission. Table and indexed view reads and writes are summed from the daily index/object stats snapshots per answering instance, with every counter reset inside the window (restart, database offline/online, restore) detected, use after it counted and last seen carried forward. Use between the last snapshot before a reset and the reset itself is lost: coverage.tables.blind_hours says how much time that is at most, and covered_days leaves it out. Procedure and function executions are summed from the stored Query Store rows; a module is a candidate only where a stored Query Store capture mode of ALL is known, since an unknown mode may be the default AUTO, which leaves rare statements out. Rows are ordered decommission candidates first: 'Not used in window', then 'Write-only', then used objects. 'Not tracked' rows (memory-optimized tables) leave no trace in this evidence and are not candidates; caveat on a row says why. coverage says how many days the evidence ACTUALLY covers (short_of_window is true when it is less than asked), how many resets were crossed and which instances answered. Lite keeps about three months, so a longer window is cut there and the coverage says so. Lite keeps no object list: a procedure that never ran is NOT listed. Views that are not indexed move only their base tables' counters, so their use is read from the stored Query Store statement text, parsed (names in comments and strings do not count): a view a statement names is 'Referenced', with referencing_executions, and coverage.views says how much text was looked at. Lite keeps no object list, so a view no statement named is NOT listed, and a listed view is a name statements read as a table that is not a known table (it could be a synonym). Only what Query Store captured counts.")]
    public static Task<string> GetObjectUsage(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("How many days back the window reaches. Default 90; Lite holds about three months.")] int days_back = DefaultDaysBack,
        [Description("Maximum object rows to return. Default 100.")] int limit = DefaultLimit,
        [Description("Object rows to skip, for the next page (next_offset in the reply).")] int offset = 0,
        [Description("The database to report on. Required: the window read is per database.")] string? database_name = null) =>
        GetObjectUsage(dataService, serverManager, server_name, database_name, days_back, limit, offset, DateTime.UtcNow, s_cache);

    /// <summary>The tool with "now" supplied, so a test can pin the window. With no <paramref name="cache"/> every call reads the store.</summary>
    internal static async Task<string> GetObjectUsage(
        LocalDataService dataService, ServerManager serverManager, string? server_name, string? database_name, int days_back, int limit, int offset,
        DateTime nowUtc, ObjectUsageEvidenceCache? cache = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        if (string.IsNullOrWhiteSpace(database_name))
        {
            return McpHelpers.Refusal("database_name", "database_name is required: the window read is per database. List the databases with get_database_sizes.");
        }

        if (days_back < 1 || days_back > MaxDaysBack)
        {
            return McpHelpers.Refusal("days_back", $"days_back must be between 1 and {MaxDaysBack}: Lite keeps about {RetentionService.ArchiveRetentionMonths} months.");
        }

        var validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;
        if (offset < 0)
        {
            return McpHelpers.Refusal("offset", "offset must be zero or more.");
        }

        try
        {
            var database = database_name.Trim();
            var cacheKey = ObjectUsageEvidenceCache.Key([resolved.ServerId], database, days_back);
            if (cache is null || !cache.TryGet(cacheKey, out var evidence))
            {
                var windowStart = nowUtc.AddDays(-days_back);

                /* The index stats' last_user_* stamps are the server's local clock in the store; the reply converts each at its own instant
                   through this server's clock, as get_index_usage does (#4793). */
                var serverClock = await McpServerLocalWindow.ClockForAsync(dataService, resolved.ServerId);
                var tables = await dataService.GetObjectUsageWindowAsync(resolved.ServerId, database, windowStart);
                var executions = await dataService.GetObjectExecutionsAsync(resolved.ServerId, database, windowStart);
                var references = await dataService.GetObjectReferencesAsync(resolved.ServerId, database, windowStart, executions);

                if (tables.SnapshotCount == 0 && executions.Modules.Count == 0 && references.References.Count == 0)
                {
                    return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "index_object_stats", awsRdsKind: AwsRdsKind.None)
                        ?? McpHelpers.Status(
                            "empty",
                            $"No index/object stats or module executions for database '{database}' on {resolved.DisplayName} since "
                            + $"{windowStart:yyyy-MM-dd}. Check the name against get_database_sizes: it matches exactly, and an excluded or renamed "
                            + "database looks the same as one never collected. Index/object stats are collected daily.");
                }

                evidence = new ObjectUsageEvidence(
                    windowStart, nowUtc,
                    [new ObjectUsageSide(resolved.DisplayName, tables, serverClock.ToUtc, executions, Inventory: null, References: references)]);
                cache?.Set(cacheKey, evidence);
            }

            return ObjectUsageReport.Build(
                database,
                days_back,
                evidence.WindowStartUtc,
                evidence.ReadAtUtc,
                evidence.Sides,
                $"Lite keeps about {RetentionService.ArchiveRetentionMonths} months of every collector, so that is as far back as the window can "
                + "reach here; for a year of evidence, use Darling with index_object_stats retention set to 400 days.",
                limit,
                offset);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_object_usage", ex);
        }
    }
}
