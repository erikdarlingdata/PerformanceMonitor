/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_object_usage</c> over Darling's store (#8784): which tables, views, procedures and functions in one database were used over a
/// long window, for a decommissioning decision. The same tool name and reply shape as Lite's (<see cref="ObjectUsageReport"/> builds both);
/// Darling adds the object list (so a procedure that never ran is listed) and reads the registration's ReadOnlyIntent partner (<c>X:RO</c>) beside
/// it, because index usage counters are per instance. Query Store is one per database, so module executions are not summed across the two.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpObjectUsageTools
{
    /// <summary>The default window, the one both products share.</summary>
    internal const int DefaultDaysBack = ObjectUsageReport.DefaultDaysBack;

    /// <summary>The default page of object rows, the one both products share.</summary>
    internal const int DefaultLimit = ObjectUsageReport.DefaultLimit;

    /// <summary>The evidence of recent requests, so paging through one reply reads the window once.</summary>
    private static readonly ObjectUsageEvidenceCache s_cache = new();

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_object_usage"), Description("Which tables, indexed views, procedures and functions in ONE database were used over a long window (default 90 days), for deciding what is safe to decommission. Table and indexed view reads and writes are summed from the daily index/object stats snapshots per answering instance, with every counter reset inside the window (restart, database offline/online, restore) detected, use after it counted and last seen carried forward. Use between the last snapshot before a reset and the reset itself is lost: coverage.tables.blind_hours says how much time that is at most, and covered_days leaves it out. Procedure and function executions come from Query Store via a per-day summary kept as long as index_object_stats. Rows are ordered decommission candidates first: 'Not used in window', 'Not executed in window', then 'Write-only', then used objects ('Read', 'Executed', 'Referenced'). coverage says, per registration, how many days the evidence ACTUALLY covers (short_of_window is true when it is less than asked), how many resets were crossed and which instances answered. When the server's ReadOnlyIntent registration (name ending ':RO') is also monitored, both are read, with by_server per object: table reads and writes are per instance and are summed, while a database has one Query Store so a module's executions are the larger of the two reads. coverage.executions is per database: a database with Query Store off shows no covered days, and its modules are then 'Not tracked', not 'Not executed'. 'Not tracked' rows (memory-optimized tables, inline table-valued functions, CLR and natively compiled modules, and modules of a database Query Store holds nothing for) leave no trace in this evidence and are not candidates; caveat on a row says why. A procedure whose body only calls other modules shows no executions however often it runs, so check what references a procedure before dropping it. Views that are not indexed move only their base tables' counters, so their use is read from Query Store statement text (parsed, so names in comments and strings do not count) through a per-day summary kept as long as index_object_stats, and from the daily dependency list (object_dependencies): a view a statement names is 'Referenced', with referencing_executions, and so is one the dependency list reaches from a referenced object (a view, a synonym, or a scalar function a statement calls), a module that ran, or another database's view or module that names it by a three-part name, with used_via naming that object. A view no statement named is 'Not used in window' only with statement text for the database, a stored capture mode of ALL and a dependency list; otherwise it is 'Not tracked'. A module that did not run is likewise 'Not tracked' unless a stored capture mode of ALL is known: an unknown mode may be the default AUTO, which leaves rare statements out. Only what Query Store captured counts as an execution or a reference. Raise index_object_stats retention with set_collector_retention (400 days covers a year); lowering it removes module evidence only after the lower value has stood for 7 days.")]
    public static Task<string> GetObjectUsage(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("How many days back the window reaches. Default 90; up to the index_object_stats retention.")] int days_back = DefaultDaysBack,
        [Description("When true (the default), also read the server's ReadOnlyIntent registration (':RO') or the primary it belongs to, and sum both.")] bool include_paired_replica = true,
        [Description("Maximum object rows to return. Default 100.")] int limit = DefaultLimit,
        [Description("Object rows to skip, for the next page (next_offset in the reply).")] int offset = 0,
        [Description("The database to report on. Required: the window read is per database.")] string? database_name = null,
        CancellationToken cancellationToken = default) =>
        GetObjectUsage(postgres, server_name, database_name, days_back, include_paired_replica, limit, offset, DateTime.UtcNow, cancellationToken, s_cache);

    /// <summary>The tool with "now" supplied, so a test can pin the window. With no <paramref name="cache"/> every call reads the store.</summary>
    internal static async Task<string> GetObjectUsage(
        NpgsqlDataSource postgres, string? server_name, string? database_name, int days_back, bool include_paired_replica, int limit, int offset,
        DateTime nowUtc, CancellationToken cancellationToken, ObjectUsageEvidenceCache? cache = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        if (string.IsNullOrWhiteSpace(database_name))
        {
            return McpHelpers.Refusal("database_name", "database_name is required: the window read is per database. List the databases with get_database_sizes.");
        }

        if (days_back < 1 || days_back > CollectorScheduleDefaults.MaxRetentionDays)
        {
            return McpHelpers.Refusal("days_back", $"days_back must be between 1 and {CollectorScheduleDefaults.MaxRetentionDays}.");
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

            var registrations = new List<(int ServerId, string DisplayName)> { (resolved.ServerId, resolved.DisplayName) };
            if (include_paired_replica)
            {
                var partnerName = resolved.ServerName.EndsWith(ServerIdHelper.ReadOnlySuffix, StringComparison.OrdinalIgnoreCase)
                    ? resolved.ServerName[..^ServerIdHelper.ReadOnlySuffix.Length]
                    : resolved.ServerName + ServerIdHelper.ReadOnlySuffix;

                /* Checked against the reader's grant, not the resolver's pin: a scoped web read is pinned to the server it asked for, so the
                   resolver refuses every other name there and the partner would never be read on the web while MCP read it. A web reader
                   gets the partner only when its grant holds it. One that is not monitored, not enabled or not allowed is simply not read. */
                if (await DarlingServerResolver.ResolvePairedServerAsync(postgres, partnerName, cancellationToken) is { } partner
                    && partner.ServerId != resolved.ServerId)
                {
                    registrations.Add((partner.ServerId, partner.DisplayName));
                }
            }

            /* The key is the resolved ids, after the guard above, so cached evidence only ever answers a request allowed the same servers. */
            var cacheKey = ObjectUsageEvidenceCache.Key(registrations.Select(r => r.ServerId), database, days_back);
            if (cache is null || !cache.TryGet(cacheKey, out var evidence))
            {
                var windowStart = nowUtc.AddDays(-days_back);

                /* Every read is its own command on its own pooled connection, so the registrations and the seven reads of each run together
                   rather than one after another: the window read is the long one and there are two of them when the partner is read. */
                var sides = await Task.WhenAll(registrations.Select(async r =>
                {
                    var clock = DarlingServerClockReader.ReadAsync(postgres, r.ServerId, cancellationToken);
                    var tables = DarlingObjectUsageReader.ReadTablesAsync(postgres, r.ServerId, database, windowStart, cancellationToken);
                    var executions = DarlingObjectUsageReader.ReadExecutionsAsync(postgres, r.ServerId, database, windowStart, cancellationToken);
                    var inventory = DarlingObjectUsageReader.ReadInventoryAsync(postgres, r.ServerId, database, cancellationToken);
                    var references = DarlingObjectUsageReader.ReadReferencesAsync(postgres, r.ServerId, database, windowStart, cancellationToken);
                    var dependencies = DarlingObjectUsageReader.ReadDependenciesAsync(postgres, r.ServerId, database, cancellationToken);
                    var inbound = DarlingObjectUsageReader.ReadInboundDependenciesAsync(postgres, r.ServerId, database, cancellationToken);
                    await Task.WhenAll(clock, tables, executions, inventory, references, dependencies, inbound);
                    return new ObjectUsageSide(
                        r.DisplayName, await tables, (await clock).ToUtc, await executions, await inventory, await references, await dependencies,
                        await inbound);
                }));

                if (Array.TrueForAll(sides, s => s.Tables.SnapshotCount == 0 && s.Executions is not { Modules.Count: > 0 } && s.Inventory is null))
                {
                    return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "index_object_stats", cancellationToken)
                        ?? McpHelpers.Status(
                            "empty",
                            $"No index/object stats, module executions or object list for database '{database}' on {resolved.DisplayName} since "
                            + $"{windowStart:yyyy-MM-dd}. Check the name against get_database_sizes: it matches exactly, and an excluded or renamed "
                            + "database looks the same as one never collected. Index/object stats are collected daily.");
                }

                evidence = new ObjectUsageEvidence(windowStart, nowUtc, sides);
                cache?.Set(cacheKey, evidence);
            }

            return ObjectUsageReport.Build(
                database,
                days_back,
                evidence.WindowStartUtc,
                evidence.ReadAtUtc,
                evidence.Sides,
                "On Darling the window reaches as far back as index_object_stats is kept (90 days by default): to cover a year, set its "
                + "retention to 400 days with set_collector_retention (or --set-collector-retention index_object_stats 400). The module summary is kept "
                + "for the same time, and a shorter retention only removes module evidence after it has stood for "
                + $"{PerformanceMonitor.Darling.Storage.ObjectExecutionsDaily.RetentionLoweringGraceDays} days.",
                limit,
                offset);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_object_usage", ex);
        }
    }
}
