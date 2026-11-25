/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
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
/// #9414: <c>get_store_checkpoints</c>, one row per checkpoint of the monitoring store's OWN PostgreSQL server for a window,
/// with a per-hour summary. A sibling of <c>get_store_log</c>: the rows come from the same hourly read of the store's log.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreCheckpointTools
{
    public const int DefaultLimit = 100;

    internal const int MaxLimit = 500;

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_checkpoints"), Description(
        "Gets the monitoring STORE's OWN PostgreSQL checkpoints, one row each, for a window; no server_name. Each row has the start and "
        + "end time, the reason (time, wal, requested, immediate, shutdown or end-of-recovery), buffers written, WAL files added, removed "
        + "and recycled, write, sync and total time, files synced, longest and average sync, and the WAL distance and estimate. Newest first, "
        + "with a per-hour summary (count, max files synced, longest sync, total write and sync time). <<GUIDE>> "
        + "Reads what the store's own server log says about each checkpoint and restartpoint: the service reads that log hourly, so the newest "
        + "hour may not be read yet. Start-up and shutdown checkpoints are kept. started_by_wal in an hour is how many checkpoints WAL volume "
        + "started rather than the clock: a settings trial compares those, max_files_synced and longest_sync_ms across the same hours before and after. "
        + "A row with no reason is a checkpoint whose start line was never read (log_checkpoints was off, or the line was rotated out); "
        + "ended_at null is a checkpoint that had not finished when the log was last read: the newest one is usually still running. It means a crash or restart only when a later checkpoint of the same kind has started, and the hour summary counts only those in not_completed. unparsed = true is a complete line this build could not read, kept with no figures. "
        + "Times are UTC. status precondition means log_checkpoints is off or lc_messages is not English, so the log cannot give rows, and the "
        + "message names the setting; not_collected means the store log has not been read in the window. truncated is true when limit or the response budget left checkpoints off the end of the window: checkpoints_returned is how many this answer holds and checkpoints_in_window how many the window holds. Page with offset when truncated is true.")]
    public static Task<string> GetStoreCheckpoints(
        NpgsqlDataSource postgres,
        [Description("Hours of history. Default 24; max 168.")] int hours_back = 24,
        [Description("Maximum checkpoint rows to return. Default 100; max 500.")] int limit = DefaultLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Checkpoint rows to skip before the page, newest first. Default 0.")] int offset = 0,
        CancellationToken cancellationToken = default) =>
        GetStoreCheckpoints(postgres, hours_back, limit, as_of, offset, McpResponseBudget.DefaultBytes, cancellationToken);

    /// <param name="settingsOverride">Tests only. <c>log_checkpoints</c> can only change in the server's config file and <c>lc_messages</c>
    /// only to a locale the host has installed, so a test that needs the store to answer "off" or "not English" hands the settings in.</param>
    internal static async Task<string> GetStoreCheckpoints(
        NpgsqlDataSource postgres, int hours_back, int limit, string? as_of, int offset, int? responseByteBudget, CancellationToken cancellationToken,
        DarlingStoreCheckpointReader.Settings? settingsOverride = null)
    {
        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null)
        {
            return validation;
        }

        validation = McpHelpers.ValidateTop(limit, MaxLimit);
        if (validation != null)
        {
            return validation;
        }

        if (offset < 0 || offset > DarlingMcpPgServerStateTools.MaxPageOffset)
        {
            return McpHelpers.Refusal("offset", $"Invalid offset value '{offset}'. Must be 0-{DarlingMcpPgServerStateTools.MaxPageOffset}.");
        }

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);
            var settings = settingsOverride ?? await DarlingStoreCheckpointReader.GetSettingsAsync(postgres, cancellationToken);
            if (!settings.TableExists)
            {
                return McpHelpers.Status(
                    "unavailable",
                    "This store has no checkpoint table yet: it is below the schema version that adds one. Upgrade the store with the service build that carries it, and checkpoints read from then on are kept.");
            }

            var (total, unparsed) = await DarlingStoreCheckpointReader.GetTotalAsync(postgres, windowStart, windowEnd, cancellationToken);
            var logCheckpointsOff = string.Equals(settings.LogCheckpoints, "off", StringComparison.OrdinalIgnoreCase);
            if (total == 0)
            {
                if (logCheckpointsOff)
                {
                    return McpHelpers.Status(
                        "precondition",
                        "log_checkpoints is off on the store, so its log carries no checkpoint lines and none are kept. Turn it on with ALTER SYSTEM SET log_checkpoints = on, then SELECT pg_reload_conf(). "
                        + "It is on by default from PostgreSQL 15. Checkpoints that ran while it was off cannot be recovered.");
                }

                if (!StoreLogClassifier.IsEnglishLcMessages(settings.LcMessages))
                {
                    return McpHelpers.Status(
                        "precondition",
                        $"lc_messages is '{settings.LcMessages}' on the store, so its checkpoint lines are not in English and are not read. Set lc_messages to C or an English locale (ALTER SYSTEM SET lc_messages = 'C', then SELECT pg_reload_conf()) to keep checkpoints from then on.");
                }

                var captures = await DarlingStoreLogReader.GetCaptureSummaryAsync(postgres, windowStart, windowEnd, hours_back, cancellationToken);
                if (captures.Captures == 0)
                {
                    return McpHelpers.Status(
                        "not_collected",
                        $"No store-log captures {McpHelpers.InTheLast(CountText.Plural(hours_back, "hour"))}, so this window makes no claim about the store's checkpoints. "
                        + "The service reads the store's log on the hourly self-metrics tick. get_store_log says why a log is not being read.");
                }

                return McpHelpers.Status(
                    "empty",
                    $"The store's log was read {CountText.Plural((int)captures.Captures, "time")} {McpHelpers.InTheLast(CountText.Plural(hours_back, "hour"))} and holds no checkpoint line in the window, with log_checkpoints on.");
            }

            var note = logCheckpointsOff
                ? "log_checkpoints is off on the store now. The checkpoints below ran while it was on, and none that run from now on are kept."
                : null;
            var hours = await DarlingStoreCheckpointReader.GetHoursAsync(postgres, windowStart, windowEnd, cancellationToken);
            var rows = await DarlingStoreCheckpointReader.GetRowsAsync(postgres, windowStart, windowEnd, limit, offset, cancellationToken);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", $"offset {offset} is past the {CountText.Plural((int)total, "checkpoint")} this window holds.");
            }

            string Reply(int kept) => JsonSerializer.Serialize(new DarlingStoreCheckpointReader.Report
            {
                WindowHours = hours_back,
                AsOf = windowEnd.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
                CheckpointsInWindow = total,
                CheckpointsUnparsed = unparsed,
                CheckpointsSkipped = offset,
                Note = note,
                Hours = hours,
                Checkpoints = kept < rows.Count ? rows.GetRange(0, kept) : rows,
                BudgetTruncated = kept < rows.Count,
                BudgetNote = kept < rows.Count
                    ? McpResponseBudget.PageCutNote(rows.Count, kept, "newest first", "checkpoints",
                        $"Read on with offset {offset + kept}, or narrow the window.")
                    : null,
            }, McpHelpers.JsonOptions);

            return responseByteBudget is int budget
                ? McpResponseBudget.FitPrefix(rows.Count, Reply, budget, minimum: 0).Json
                : Reply(rows.Count);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_checkpoints", ex);
        }
    }
}
