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
using System.Globalization;
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
/// <c>get_store_setting_changes</c> (#9427): the changes to the monitoring store's own PostgreSQL settings between service starts, newest first.
/// The service compares the store's <c>pg_settings</c> with its last capture once a minute (<see cref="StoreSettingChanges"/>). Darling only, on the
/// central store host like the other <c>get_store_*</c> reads: Lite has no PostgreSQL store.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreSettingChangesTools
{
    /// <summary>This tool's entries on the web read surface and in Custom Views validation, kept here and not in the shared lists (#9477).</summary>
    internal sealed class Registration : IReadToolRegistration
    {
        public ReadToolRegistration Read { get; } = new(
            "get_store_setting_changes",
            (c, pg, an) => GetStoreSettingChanges(pg, DarlingWebEndpoints.QueryInt(c, "days_back", null, DefaultDaysBack), DarlingWebEndpoints.Str(c, "setting_name"), DarlingWebEndpoints.Rows(c, "limit", DefaultLimit), c.RequestAborted),
            new(
                DarlingWebEndpoints.CatOverview,
                "Changes to the monitoring store's OWN PostgreSQL settings between service starts, newest first: the setting, old and new value, where the new value came from (source, file and line), pending_restart, when it was seen and when the server reloaded its configuration. Compared once a minute; the first capture is a baseline with no rows. setting_name filters to one setting.",
                new[]
                {
                    DarlingWebEndpoints.PInt("days_back", DefaultDaysBack),
                    DarlingWebEndpoints.PText("setting_name"),
                    DarlingWebEndpoints.PLimit(DefaultLimit),
                }));
    }

    /// <summary>The window ceiling, in days: the changes' retention.</summary>
    public const int MaxDaysBack = StoreSettingChanges.RetentionDays;

    /// <summary>The default window, in days.</summary>
    public const int DefaultDaysBack = 7;

    /// <summary>The default page size.</summary>
    public const int DefaultLimit = 50;

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_setting_changes"), Description(
        "Lists changes to the monitoring STORE's own PostgreSQL settings between service starts, newest first (not a monitored server's). "
        + "No server_name. Each row: the setting, old and new value, where the new value came from (source, source file and line), pending_restart, "
        + "when it was seen and when the server reloaded its configuration. Status empty: watching, no change. Status unavailable: no baseline yet. "
        + "<<GUIDE>> The service reads pg_settings once a minute on the store host and compares it with the last capture. A row means the value, the source or "
        + "pending_restart of a setting differed from that capture. The first capture on a store, including the first after the upgrade that adds this record, stores a baseline and writes no row. A later upgrade keeps the last capture, so changes made around it are still rows. "
        + "A move of the source file or line alone writes no row. Values that can hold a credential are masked. seen_at is when the capture saw the change, up to a minute "
        + "after the reload, and conf_load_time is when the server last loaded its configuration (pg_conf_load_time). A value is in the setting's own unit (the unit field), "
        + "as pg_settings gives it. setting_name filters to one setting, any case. Window is days_back (default 7, max 400, the retention). limit default 50, max 1000, "
        + "truncated says more rows exist. Darling only: Lite has no PostgreSQL store.")]
    public static async Task<string> GetStoreSettingChanges(
        NpgsqlDataSource postgres,
        [Description("Days of changes. Default 7; max 400 (the retention).")] int days_back = DefaultDaysBack,
        [Description("Only changes to this setting, such as work_mem. Omit for every setting.")] string? setting_name = null,
        [Description("Rows to return. Default 50, max 1000.")] int limit = DefaultLimit,
        CancellationToken cancellationToken = default)
    {
        var daysError = DarlingReadWindow.ValidateDaysBack(days_back, MaxDaysBack);
        if (daysError != null)
        {
            return daysError;
        }

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null)
        {
            return limitError;
        }

        var name = string.IsNullOrWhiteSpace(setting_name) ? null : setting_name.Trim();

        try
        {
            var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-days_back), DateTimeKind.Unspecified);

            await using (var shape = postgres.CreateCommand(StoreSettingChanges.ShapeSql))
            {
                shape.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                if (!(bool)(await shape.ExecuteScalarAsync(cancellationToken))!)
                {
                    return McpHelpers.Status("unavailable",
                        "This store has no setting history yet: it predates the tables that hold it. The service migrates the store at its next start, then captures a baseline within a minute.");
                }
            }

            int trackedSettings;
            DateTime? watchingSince;
            await using (var state = postgres.CreateCommand(StoreSettingChanges.StateSql))
            {
                state.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                await using var stateReader = await state.ExecuteReaderAsync(cancellationToken);
                await stateReader.ReadAsync(cancellationToken);
                trackedSettings = stateReader.GetInt32(0);
                watchingSince = stateReader.IsDBNull(1) ? null : stateReader.GetDateTime(1);
            }

            if (trackedSettings == 0)
            {
                return McpHelpers.Status("unavailable",
                    "No baseline yet: the service captures the store's settings once a minute and writes the first capture as a baseline, with no change rows. Check again in a minute or two.");
            }

            var fetched = new List<StoreSettingChange>();
            await using (var list = postgres.CreateCommand(StoreSettingChanges.ListSql))
            {
                list.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                list.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = windowStart });
                list.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)name ?? DBNull.Value });
                list.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = limit + 1 });
                await using var reader = await list.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    fetched.Add(new StoreSettingChange(
                        reader.GetInt64(0), reader.GetDateTime(1), reader.IsDBNull(2) ? null : reader.GetDateTime(2), reader.GetString(3),
                        Text(reader, 4), Text(reader, 5), Text(reader, 6), Text(reader, 7), Text(reader, 8), Text(reader, 9), Text(reader, 10),
                        reader.IsDBNull(11) ? null : reader.GetInt32(11), reader.IsDBNull(12) ? null : reader.GetInt32(12),
                        reader.IsDBNull(13) ? null : reader.GetBoolean(13), reader.IsDBNull(14) ? null : reader.GetBoolean(14)));
                }
            }

            if (fetched.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "empty",
                    message = name is null
                        ? "Watching the store's settings, and none changed in the window."
                        : "Watching the store's settings, and this setting did not change in the window.",
                    days_back,
                    setting_name = name,
                    watching_since = Iso(watchingSince),
                    tracked_settings = trackedSettings,
                }, McpHelpers.JsonOptions);
            }

            var (page, truncated) = McpHelpers.BoundPage(fetched, limit);
            return JsonSerializer.Serialize(new
            {
                days_back,
                setting_name = name,
                limit,
                changes_returned = page.Count,
                truncated,
                watching_since = Iso(watchingSince),
                tracked_settings = trackedSettings,
                changes = page.Select(c => new
                {
                    seen_at = Iso(c.SeenAt),
                    conf_load_time = Iso(c.ConfLoadTime),
                    setting_name = c.SettingName,
                    unit = c.Unit,
                    old_value = c.OldValue,
                    new_value = c.NewValue,
                    old_source = c.OldSource,
                    new_source = c.NewSource,
                    old_sourcefile = c.OldSourceFile,
                    new_sourcefile = c.NewSourceFile,
                    old_sourceline = c.OldSourceLine,
                    new_sourceline = c.NewSourceLine,
                    old_pending_restart = c.OldPendingRestart,
                    new_pending_restart = c.NewPendingRestart,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_setting_changes", ex);
        }
    }

    private static string? Text(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    /// <summary>The stored times are naive UTC. They go out as ISO-8601 with a Z, so a client does not read them as local.</summary>
    private static string? Iso(DateTime? value) =>
        value is DateTime v ? DateTime.SpecifyKind(v, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture) : null;
}
