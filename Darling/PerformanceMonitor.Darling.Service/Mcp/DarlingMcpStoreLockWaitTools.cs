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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read over the lock waits the monitoring store's OWN sessions suffered (#9439): who waited, who held the lock, on what, for how
/// long. The service records them hourly from the store's server log (<see cref="StoreLockWaits"/>), so this answers about the store
/// itself and takes no <c>server_name</c>. Darling-only and on the central store host: Lite has no PostgreSQL store.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreLockWaitTools
{
    /// <summary>This tool's entries on the web read surface and in Custom Views validation, kept here and not in the shared lists (#9477).</summary>
    internal sealed class Registration : IReadToolRegistration
    {
        public ReadToolRegistration Read { get; } = new(
            "get_store_lock_waits",
            (c, pg, an) => GetStoreLockWaits(pg, DarlingWebEndpoints.Hours(c, 24), DarlingWebEndpoints.Rows(c, "limit", DefaultLimit), DarlingWebEndpoints.Str(c, "role"), DarlingWebEndpoints.Str(c, "relation"), DarlingWebEndpoints.AsOf(c), c.RequestAborted),
            DarlingWebEndpoints.R(
                DarlingWebEndpoints.CatOverview,
                "The lock waits the monitoring store's OWN sessions suffered (from the store's PostgreSQL log): who waited, who held the lock, on what relation, for how long and how it ended. Only waits of 5 seconds or more.",
                DarlingWebEndpoints.PHours(24), DarlingWebEndpoints.PLimit(DefaultLimit), DarlingWebEndpoints.PText("role"), DarlingWebEndpoints.PText("relation"), DarlingWebEndpoints.PAsOf()));
    }

    /// <summary>Default rows returned.</summary>
    public const int DefaultLimit = 50;

    /// <summary>The most rows one call returns. A request above it is refused, never cut.</summary>
    public const int MaxLimit = 200;

    /// <summary>The longest role or relation filter taken. A longer one cannot match a name PostgreSQL keeps.</summary>
    public const int MaxFilterLength = 128;

    /// <summary>The setting that decides whether PostgreSQL writes lock waits to its log at all.</summary>
    internal const string LogLockWaitsSetting = "log_lock_waits";

    /// <summary>One page of waits, newest first. $1 window start, $2 window end (naive UTC), $3 role or null, $4 relation or null, $5 rows.</summary>
    internal const string WaitsSql = @"
SELECT w.wait_started_at,
       w.waiter_pid,
       w.waiter_role,
       w.waiter_application,
       w.holder_pids,
       w.holder_roles,
       w.holder_applications,
       w.lock_mode,
       w.lock_object,
       w.relation_name,
       w.duration_ms,
       w.outcome
FROM collect.store_lock_waits AS w
WHERE w.wait_started_at >= $1
AND   w.wait_started_at < $2
AND   ($3::text IS NULL OR w.waiter_role = $3 OR $3 = ANY(w.holder_roles))
AND   ($4::text IS NULL OR w.relation_name IS NOT DISTINCT FROM $4)
ORDER BY w.wait_started_at DESC, w.wait_id DESC
LIMIT $5";

    /// <summary>The setting's value now, or no row when the setting is unknown.</summary>
    internal const string SettingSql = @"
SELECT s.setting
FROM pg_catalog.pg_settings AS s
WHERE s.name = 'log_lock_waits'";

    /// <summary>The language the store writes its log in. NULL when the setting is unknown.</summary>
    internal const string LcMessagesSql = "SELECT pg_catalog.current_setting('lc_messages', true)";

    /// <summary>What the answer says about how to read a row.</summary>
    internal const string Note =
        "Each row is one lock wait that lasted at least the threshold (threshold_ms). The holders are the processes PostgreSQL named as "
        + "holding or queued ahead for the lock when it logged the wait. A background process such as an autovacuum worker reads its "
        + "backend type as its role and an empty application. A role or application that reads unresolved is a process the service "
        + "had not seen alive at the wait's start, such as a session that began and ended between two one-minute reads. outcome granted "
        + "means the waiter got the lock, timed_out means lock_timeout ended it, cancelled means the statement was cancelled, deadlock "
        + "means a deadlock was detected, and waiting means the log ended while the wait was still open (duration_ms is its age then).";

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_lock_waits"), Description(
        "Gets the lock waits the monitoring STORE's OWN sessions suffered, not a monitored server's; no server_name. Newest first: waiter and "
        + "holder roles and applications, lock mode, relation, duration, outcome. Only waits past threshold_ms (5 s). Needs log_lock_waits = on "
        + "on the store: precondition names it when it is off. <<GUIDE>> "
        + "Reads collect.store_lock_waits, which the hourly store-log sweep fills from the lock waits PostgreSQL logs on the store. A wait "
        + "is logged after deadlock_timeout, so the log (and this read) only holds waits past that, and the service keeps those that lasted at "
        + "least threshold_ms. role matches the waiter's role or any holder's role exactly. relation matches the relation name as schema.name. "
        + "Both filters are applied before limit, so a page holds matching rows only. limit is 1-200; a larger value is refused, not cut, and "
        + "truncated says the window held more rows than the page. Roles and applications of a holder come from a one-minute read of the "
        + "store's processes (background processes included, by backend type), so a holder that began and ended between two reads shows "
        + "unresolved. status empty means the window holds no wait and the store log was captured in it, which is a true negative only while log_lock_waits is on and the log is "
        + "in English: precondition says when it is not, because the lock wait lines are read by their English text. unavailable means the store log "
        + "was not captured in the window (no log files, a role that cannot read them, or a failing capture), so the window was never read: get_store_log says why.")]
    public static Task<string> GetStoreLockWaits(
        NpgsqlDataSource postgres,
        [Description("Hours of history. Default 24; max 168.")] int hours_back = 24,
        [Description("Maximum waits to return. Default 50; max 200.")] int limit = DefaultLimit,
        [Description("Only waits where this role waited or held the lock (exact name, such as the unresolved marker or a database role). Default none.")] string? role = null,
        [Description("Only waits on this relation, as schema.name. Default none.")] string? relation = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default) =>
        GetStoreLockWaits(postgres, hours_back, limit, role, relation, as_of, lcMessagesOverride: null, cancellationToken);

    /// <param name="lcMessagesOverride">Tests only. <c>lc_messages</c> can only be set to a locale the host has installed, so a test that needs
    /// the store to answer "not English" hands the value in. An empty string reads as a setting that could not be read.</param>
    internal static async Task<string> GetStoreLockWaits(
        NpgsqlDataSource postgres, int hours_back, int limit, string? role, string? relation, string? as_of,
        string? lcMessagesOverride, CancellationToken cancellationToken)
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

        if (role is not null && (role.Length == 0 || role.Length > MaxFilterLength))
        {
            return McpHelpers.Refusal("role", $"Invalid role value. Must be 1-{MaxFilterLength} characters, or left out.");
        }

        if (relation is not null && (relation.Length == 0 || relation.Length > MaxFilterLength))
        {
            return McpHelpers.Refusal("relation", $"Invalid relation value. Must be 1-{MaxFilterLength} characters, or left out.");
        }

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);

            /* The caller's limit + 1 as the fetch, so the extra row is the observed signal that the window held more. */
            var fetched = await ReadAsync(postgres, windowStart, windowEnd, role, relation, limit + 1, cancellationToken);
            var (rows, truncated) = McpHelpers.BoundPage(fetched, limit);

            if (rows.Count == 0)
            {
                if (!await IsLockWaitLoggingOnAsync(postgres, cancellationToken))
                {
                    return McpHelpers.Status(
                        "precondition",
                        $"The store has {LogLockWaitsSetting} off, so PostgreSQL does not write lock waits to its log and none can be recorded here. "
                        + $"Set {LogLockWaitsSetting} = on in the store's postgresql.conf (a managed store gets it from the service) and reload. "
                        + "Waits that happened before it was on are not recoverable.");
                }

                /* The parser reads the log's English text only, so a store that logs in another language writes waits this read cannot see:
                   an empty window there is not a true negative. A setting that cannot be read counts as not English, as the sweep does. */
                var lcMessages = lcMessagesOverride ?? await ReadLcMessagesAsync(postgres, cancellationToken);
                if (!StoreLogClassifier.IsEnglishLcMessages(lcMessages))
                {
                    return McpHelpers.Status(
                        "precondition",
                        string.IsNullOrWhiteSpace(lcMessages)
                            ? "The store's lc_messages could not be read, so its log may not be in English. Lock waits are read only from English log text, so this window may hold waits that were not recorded. "
                              + "Set lc_messages to C or an English locale (ALTER SYSTEM SET lc_messages = 'C', then SELECT pg_reload_conf()) to record waits from then on."
                            : $"lc_messages is '{lcMessages}' on the store. Lock waits are read only from English log text, so waits logged in that language are not recorded and an empty window proves nothing. "
                              + "Set lc_messages to C or an English locale (ALTER SYSTEM SET lc_messages = 'C', then SELECT pg_reload_conf()) to record waits from then on.");
                }

                /* An all-clear needs an observation: the sweep that reads the lock waits is the store-log capture, so a window with no capture
                   (no log files to list, a role that cannot read them, a failing sweep) was never looked at. get_store_log says why. */
                var captures = await DarlingStoreLogReader.GetCaptureSummaryAsync(postgres, windowStart, windowEnd, hours_back, cancellationToken);
                if (captures.Captures == 0)
                {
                    return McpHelpers.Status(
                        "unavailable",
                        $"No store-log capture {McpHelpers.InTheLast(CountText.Plural(hours_back, "hour"))}, so this window makes no claim about the store's lock waits: they are read from the store's log by the same hourly capture. "
                        + "get_store_log says why the log is not being read.");
                }

                var filtered = role != null || relation != null;
                return McpHelpers.Status(
                    "empty",
                    $"No store lock wait of {StoreLockWaits.ThresholdMilliseconds / 1000} seconds or more"
                    + (filtered ? " matching the filters" : string.Empty)
                    + $" began {McpHelpers.InTheLast(CountText.Plural(hours_back, "hour"))}. The log is read hourly, so the newest hour may not be read yet.");
            }

            return JsonSerializer.Serialize(new
            {
                hours_back,
                as_of = windowEnd.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
                status = "lock_waits",
                threshold_ms = StoreLockWaits.ThresholdMilliseconds,
                wait_count = rows.Count,
                truncated,
                note = Note,
                waits = rows.Select(r => new
                {
                    wait_started_at = r.StartedUtc,
                    duration_ms = r.DurationMs,
                    outcome = r.Outcome,
                    lock_mode = r.LockMode,
                    lock_object = r.LockObject,
                    relation = r.Relation,
                    waiter = new { pid = r.WaiterPid, role = r.WaiterRole, application = r.WaiterApplication },
                    holders = r.HolderPids.Select((pid, i) => new
                    {
                        pid,
                        role = i < r.HolderRoles.Length ? r.HolderRoles[i] : StoreLockWaits.Unresolved,
                        application = i < r.HolderApplications.Length ? r.HolderApplications[i] : StoreLockWaits.Unresolved,
                    }),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_lock_waits", ex);
        }
    }

    /// <summary>One stored wait.</summary>
    internal sealed record WaitRow(
        DateTime StartedUtc, int WaiterPid, string WaiterRole, string WaiterApplication, int[] HolderPids, string[] HolderRoles,
        string[] HolderApplications, string LockMode, string LockObject, string? Relation, long DurationMs, string Outcome);

    internal static async Task<List<WaitRow>> ReadAsync(
        NpgsqlDataSource postgres, DateTime windowStartUtc, DateTime windowEndUtc, string? role, string? relation, int rows,
        CancellationToken cancellationToken)
    {
        var result = new List<WaitRow>();
        await using var command = postgres.CreateCommand(WaitsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)role ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)relation ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = rows, NpgsqlDbType = NpgsqlDbType.Integer });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new WaitRow(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.GetInt32(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetFieldValue<int[]>(4),
                reader.GetFieldValue<string[]>(5),
                reader.GetFieldValue<string[]>(6),
                reader.GetString(7),
                reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.GetInt64(10),
                reader.GetString(11)));
        }

        return result;
    }

    /// <summary>The store's <c>lc_messages</c>, or null when the setting cannot be seen.</summary>
    private static async Task<string?> ReadLcMessagesAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(LcMessagesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <summary>True unless the store says <c>log_lock_waits</c> is off. A setting the read cannot see counts as on, so an empty answer is not turned into a precondition on a guess.</summary>
    internal static async Task<bool> IsLockWaitLoggingOnAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(SettingSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        var value = await command.ExecuteScalarAsync(cancellationToken) as string;
        return !string.Equals(value, "off", StringComparison.OrdinalIgnoreCase);
    }
}
