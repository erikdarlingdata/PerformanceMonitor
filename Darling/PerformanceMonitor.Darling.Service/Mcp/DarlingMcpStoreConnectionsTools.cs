/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_store_connections</c> (#9437): the monitoring STORE's own connection pressure, a minute at a time. Darling-only and served by the
/// central store host with the rest of the <c>get_store_*</c> family: Lite keeps its data in DuckDB and has no PostgreSQL store to sample.
/// A new tool rather than a block of <c>get_store_metrics</c> because that read is hourly and size-shaped, and this one is per minute and
/// answers a different question: who held the slots when a client could not get one.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreConnectionsTools
{
    /// <summary>This tool's entries on the web read surface and in Custom Views validation, kept here and not in the shared lists (#9477).</summary>
    internal sealed class Registration : IReadToolRegistration
    {
        public ReadToolRegistration Read { get; } = new(
            "get_store_connections",
            (c, pg, an) => GetStoreConnections(
                pg,
                DarlingWebEndpoints.Hours(c, DefaultHoursBack),
                DarlingWebEndpoints.Rows(c, "limit", DefaultLimit),
                DarlingWebEndpoints.Str(c, "role"),
                DarlingWebEndpoints.Str(c, "application"),
                DarlingWebEndpoints.Str(c, "group_by") ?? "none",
                DarlingWebEndpoints.AsOf(c),
                c.RequestAborted),
            new(
                DarlingWebEndpoints.CatOverview,
                "The monitoring store's OWN connections a minute at a time: client backends by role, application, state and wait type, with the slots left before new logins are refused. Not a monitored server's.",
                new[]
                {
                    DarlingWebEndpoints.PHours(DefaultHoursBack),
                    DarlingWebEndpoints.PLimit(DefaultLimit),
                    DarlingWebEndpoints.PText("role"),
                    DarlingWebEndpoints.PText("application"),
                    DarlingWebEndpoints.PTextDefault("group_by", "none"),
                    DarlingWebEndpoints.PAsOf(),
                }));
    }

    /// <summary>Default minutes returned: an hour at the sample's one-minute grain.</summary>
    public const int DefaultLimit = 60;

    /// <summary>Default window, hours.</summary>
    public const int DefaultHoursBack = 1;

    /// <summary>The longest filter text accepted. A PostgreSQL role or application name is at most 63 bytes, so longer cannot match.</summary>
    internal const int MaxFilterLength = 63;

    /// <summary>The <c>host_load</c> sentence when no returned minute carries a host figure.</summary>
    internal const string HostLoadNotAvailableText =
        "No returned minute has a host figure. The service reads available memory only for a managed store on its own Windows host, "
        + "and reads no CPU percent or disk queue for any store, so a bring-your-own or compose store has none of the three.";

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_connections"), Description(
        "Gets the monitoring STORE's OWN connections a minute at a time, not a monitored server's; no server_name. Each minute: client backends by role, "
        + "application, state and wait type, and slots_available (max_connections less the reserved slots less the backends counted). "
        + "Use it when a client could not get a connection. Newest minute first. <<GUIDE>> "
        + "Reads collect.store_connection_samples and collect.store_connection_slots, which the service fills once a minute from pg_stat_activity (client backends only) and keeps for 14 days. "
        + "slots_available is stored raw and goes below zero when superusers use the reserved slots. A role that is not a superuser and not in pg_read_all_stats sees empty state and wait columns for other roles' backends: the counts stay right. "
        + "role and application are exact-match filters. With a filter, only minutes that have a matching group are returned, so limit counts those minutes, and each minute's slots block still describes the whole store. "
        + "group_by folds each minute's groups to role, application or state (sums of backends, the other columns null). none keeps the full grain. "
        + "host_load says available or not_available for the returned minutes: available_memory_bytes is filled only for a managed store on the service's own host, cpu_percent and disk_queue are never read, and a figure not read is null. "
        + "truncated is true when more minutes lie in the window than limit. An out-of-range window, limit or group_by is refused, never clamped.")]
    public static async Task<string> GetStoreConnections(
        NpgsqlDataSource postgres,
        [Description("Hours of history. Default 1; max 168.")] int hours_back = DefaultHoursBack,
        [Description("Maximum minutes to return, newest first. Default 60; max 1000.")] int limit = DefaultLimit,
        [Description("Only minutes with backends of this role, and only that role's groups. Exact match. Omit for all.")] string? role = null,
        [Description("Only minutes with backends of this application name, and only its groups. Exact match. Omit for all.")] string? application = null,
        [Description("Fold each minute's groups: none (default), role, application or state.")] string group_by = "none",
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null)
        {
            return validation;
        }

        validation = McpHelpers.ValidateTop(limit);
        if (validation != null)
        {
            return validation;
        }

        validation = McpHelpers.ValidateChoice(group_by, DarlingStoreConnectionsReader.GroupByChoices, "group_by");
        if (validation != null)
        {
            return validation;
        }

        var roleFilter = Normalise(role);
        var applicationFilter = Normalise(application);
        if (roleFilter is { Length: > MaxFilterLength })
        {
            return McpHelpers.Refusal("role", $"Invalid role value of {roleFilter.Length} characters. A PostgreSQL role name is at most {MaxFilterLength}.");
        }

        if (applicationFilter is { Length: > MaxFilterLength })
        {
            return McpHelpers.Refusal("application", $"Invalid application value of {applicationFilter.Length} characters. A PostgreSQL application name is at most {MaxFilterLength}.");
        }

        var groupBy = string.IsNullOrWhiteSpace(group_by)
            ? "none"
            : DarlingStoreConnectionsReader.GroupByChoices.First(c => string.Equals(c, group_by.Trim(), StringComparison.OrdinalIgnoreCase));

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);

            /* limit + 1 is the fetch and the extra row is the observed truncation signal (McpHelpers.BoundPage). */
            var fetched = await DarlingStoreConnectionsReader.GetMinutesAsync(
                postgres, windowStart, windowEnd, roleFilter, applicationFilter, limit + 1, cancellationToken);
            var (minutes, truncated) = McpHelpers.BoundPage(fetched, limit);

            if (minutes.Count == 0)
            {
                var filtered = roleFilter != null || applicationFilter != null;
                if (filtered && await DarlingStoreConnectionsReader.AnyMinuteAsync(postgres, windowStart, windowEnd, cancellationToken))
                {
                    return McpHelpers.Status(
                        "empty",
                        $"The service sampled the store's connections {McpHelpers.InTheLast(CountText.Plural(hours_back, "hour"))}, and no minute had a backend that matches the role or application filter.");
                }

                return McpHelpers.Status(
                    "unavailable",
                    $"No store connection sample {McpHelpers.InTheLast(CountText.Plural(hours_back, "hour"))}. The service samples once a minute and keeps {StoreConnectionSamples.RetentionDays} days, "
                    + "so an empty window means it was not running, the store was unreachable, or the window is older than the retention.");
            }

            var newest = minutes[0].SampleTime;
            var oldest = minutes[minutes.Count - 1].SampleTime;
            var groups = await DarlingStoreConnectionsReader.GetGroupsAsync(
                postgres, oldest, newest, roleFilter, applicationFilter, groupBy, cancellationToken);
            var byMinute = groups.ToLookup(g => g.SampleTime);

            var hostLoadAvailable = minutes.Any(m => m.HostCpuPercent != null || m.HostAvailableMemoryBytes != null || m.HostDiskQueue != null);

            return JsonSerializer.Serialize(new
            {
                status = "ok",
                hours_back,
                as_of = windowEnd.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
                retention_days = StoreConnectionSamples.RetentionDays,
                group_by = groupBy,
                role = roleFilter,
                application = applicationFilter,
                minutes_returned = minutes.Count,
                truncated,
                host_load = hostLoadAvailable ? "available" : "not_available",
                host_load_note = hostLoadAvailable ? null : HostLoadNotAvailableText,
                minutes = minutes.Select(m => new
                {
                    sample_time = DateTime.SpecifyKind(m.SampleTime, DateTimeKind.Utc).ToString("o", System.Globalization.CultureInfo.InvariantCulture),
                    slots = new
                    {
                        max_connections = m.MaxConnections,
                        superuser_reserved_connections = m.SuperuserReserved,
                        reserved_connections = m.Reserved,
                        backends_total = m.BackendsTotal,
                        slots_available = m.SlotsAvailable,
                    },
                    host = new
                    {
                        cpu_percent = m.HostCpuPercent,
                        available_memory_bytes = m.HostAvailableMemoryBytes,
                        disk_queue = m.HostDiskQueue,
                    },
                    groups = byMinute[m.SampleTime].Select(g => new
                    {
                        role = g.Role,
                        application = g.Application,
                        state = g.State,
                        wait_event_type = g.WaitEventType,
                        backends = g.Backends,
                    }),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            /* Not an error: the tables are newer than this store, so the answer is the unavailable status below. */
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_connections", ex);
        }

        /* A store below the migration that creates the tables: not an error, so it is answered outside the catch. */
        return McpHelpers.Status(
            "unavailable",
            $"The store's schema is below version {StoreConnectionSamples.Version}, which adds the connection tables. Upgrade the service so the store migrates, then wait a minute for the first sample.");
    }

    private static string? Normalise(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
