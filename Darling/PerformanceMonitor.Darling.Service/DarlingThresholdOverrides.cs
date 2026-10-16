/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The tunable thresholds Darling can hold an override for (#5916): one row per wire name, which is the name
/// <c>get_alert_settings</c> and <c>update_alert_settings</c> already use for the global value (for example
/// <c>cpu.threshold_percent</c>). Every registry key of #5910 (<see cref="AlertThresholdKey"/>) is here, plus the two
/// PostgreSQL count thresholds, which are Darling's own and are not on the engine's settings contract. PostgreSQL
/// predictor constants are not settings and have no entry. The bounds are the ones <c>update_alert_settings</c> and
/// the Viewer's Settings window enforce on the global value, so an override can never hold a value the global setting
/// would refuse.
/// </summary>
internal static class DarlingThresholdKeys
{
    /// <summary>One tunable threshold: its wire name, the engine key (null for a PostgreSQL count threshold), the scope it
    /// can take and its bounds.</summary>
    internal sealed record Entry(string Wire, AlertThresholdKey? Engine, AlertThresholdScope Scope, int Min, int Max);

    internal const string PgBlockingCount = "blocking.pg_count_threshold";
    internal const string PgDeadlockCount = "deadlocks.pg_count_threshold";

    private static readonly Entry[] All =
    [
        new("cpu.threshold_percent", AlertThresholdKey.CpuThresholdPercent, AlertThresholdScope.Server, 1, 100),
        new("blocking.count_threshold", AlertThresholdKey.BlockingCountThreshold, AlertThresholdScope.Server, PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue),
        new("blocking.wait_threshold_seconds", AlertThresholdKey.BlockingWaitSecondsThreshold, AlertThresholdScope.Server, 0, int.MaxValue),
        new("deadlocks.count_threshold", AlertThresholdKey.DeadlockCountThreshold, AlertThresholdScope.Server, PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue),
        new("runnable_queue.warning_per_scheduler", AlertThresholdKey.RunnableQueueWarningPerScheduler, AlertThresholdScope.Server, 1, int.MaxValue),
        new("runnable_queue.critical_per_scheduler", AlertThresholdKey.RunnableQueueCriticalPerScheduler, AlertThresholdScope.Server, 1, int.MaxValue),
        new("client_timeouts.warning_per_minute", AlertThresholdKey.ClientTimeoutsWarningPerMinute, AlertThresholdScope.Server, 1, int.MaxValue),
        new("client_timeouts.critical_per_minute", AlertThresholdKey.ClientTimeoutsCriticalPerMinute, AlertThresholdScope.Server, 1, int.MaxValue),
        new("long_running_query.threshold_minutes", AlertThresholdKey.LongRunningQueryThresholdMinutes, AlertThresholdScope.Server, 1, int.MaxValue),
        new("tempdb_space.threshold_percent", AlertThresholdKey.TempDbSpaceThresholdPercent, AlertThresholdScope.Server, 1, 100),
        new("low_disk.threshold_percent", AlertThresholdKey.LowDiskThresholdPercent, AlertThresholdScope.Server, 0, 100),
        new("low_disk.threshold_gb", AlertThresholdKey.LowDiskThresholdGb, AlertThresholdScope.Server, 0, int.MaxValue),
        new("low_disk.critical_free_percent", AlertThresholdKey.DiskCriticalFreePercent, AlertThresholdScope.Server, 0, 100),
        new("low_disk.critical_free_gb", AlertThresholdKey.DiskCriticalFreeGb, AlertThresholdScope.Server, 0, int.MaxValue),
        new("pvs.threshold_percent", AlertThresholdKey.PvsThresholdPercent, AlertThresholdScope.ServerAndDatabase, 0, 100),
        new("pvs.floor_gb", AlertThresholdKey.PvsFloorGb, AlertThresholdScope.ServerAndDatabase, 0, int.MaxValue),
        new("file_growth.rise_mb", AlertThresholdKey.FileGrowthRiseMb, AlertThresholdScope.ServerAndDatabase, 0, int.MaxValue),
        new("file_growth.volume_percent", AlertThresholdKey.FileGrowthVolumePercent, AlertThresholdScope.ServerAndDatabase, 0, 100),
        new("file_growth.lookback_minutes", AlertThresholdKey.FileGrowthLookbackMinutes, AlertThresholdScope.Server, 5, 1440),
        new("long_running_job.multiplier", AlertThresholdKey.LongRunningJobMultiplier, AlertThresholdScope.Server, 2, 20),
        new("failed_job.lookback_minutes", AlertThresholdKey.FailedJobLookbackMinutes, AlertThresholdScope.Server, 1, 1440),
        new(PgBlockingCount, null, AlertThresholdScope.Server, PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue),
        new(PgDeadlockCount, null, AlertThresholdScope.Server, PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue),
    ];

    private static readonly Dictionary<string, Entry> ByWire = BuildByWire();
    private static readonly Dictionary<AlertThresholdKey, Entry> ByEngine = BuildByEngine();

    private static Dictionary<string, Entry> BuildByWire()
    {
        var map = new Dictionary<string, Entry>(StringComparer.Ordinal);
        foreach (var e in All)
            map[e.Wire] = e;
        return map;
    }

    private static Dictionary<AlertThresholdKey, Entry> BuildByEngine()
    {
        var map = new Dictionary<AlertThresholdKey, Entry>();
        foreach (var e in All)
        {
            if (e.Engine is { } k)
                map[k] = e;
        }

        return map;
    }

    /// <summary>Every tunable threshold, in display order.</summary>
    internal static IReadOnlyList<Entry> Entries => All;

    internal static bool TryGet(string? wire, out Entry entry)
    {
        entry = null!;
        return wire is not null && ByWire.TryGetValue(wire.Trim(), out entry!);
    }

    internal static Entry For(AlertThresholdKey key) => ByEngine[key];

    /// <summary>
    /// Why a key cannot be overridden, or null when it is a tunable threshold. The reasons name the class of key so an
    /// operator or agent sees why: an override moves a threshold, never a switch, a cadence or a destination.
    /// </summary>
    internal static (string Reason, string Message)? RefusalFor(string? key)
    {
        var k = (key ?? string.Empty).Trim();
        if (k.Length == 0)
            return ("key_required", "key is required: a threshold such as cpu.threshold_percent (see get_alert_settings with server_name).");
        if (TryGet(k, out _))
            return null;
        if (k.StartsWith("self_alerts.", StringComparison.OrdinalIgnoreCase))
            return ("not_tunable", $"'{k}' is a self-monitoring setting (self_alerts.*) about the monitor itself, not a monitored server. It has no per-server override.");
        if (k.EndsWith(".enabled", StringComparison.OrdinalIgnoreCase) || string.Equals(k, "enabled", StringComparison.OrdinalIgnoreCase))
            return ("not_tunable", $"'{k}' is an enable flag. An override changes a threshold, never turns an alert on or off; mute the alert to silence it.");
        if (k.Contains("cooldown", StringComparison.OrdinalIgnoreCase))
            return ("not_tunable", $"'{k}' is a cooldown. It spaces notifications and has no per-server override.");
        if (k.Contains("route", StringComparison.OrdinalIgnoreCase) || k.StartsWith("delivery.", StringComparison.OrdinalIgnoreCase))
            return ("not_tunable", $"'{k}' is a route or delivery setting. It has no per-server override.");
        return ("not_in_registry", $"'{k}' is not a tunable threshold. See get_alert_settings with server_name for the keys that can be overridden.");
    }

    /// <summary>Why <paramref name="value"/> or <paramref name="database"/> is refused for <paramref name="entry"/>, or null.</summary>
    internal static (string Reason, string Message)? ValidateValue(Entry entry, int value, string? database)
    {
        if (value < entry.Min || value > entry.Max)
        {
            var max = entry.Max == int.MaxValue ? "2147483647" : entry.Max.ToString(CultureInfo.InvariantCulture);
            return ("out_of_bounds", $"'{entry.Wire}' must be an integer between {entry.Min.ToString(CultureInfo.InvariantCulture)} and {max}, the same bounds the global setting has.");
        }

        if (!string.IsNullOrWhiteSpace(database) && entry.Scope != AlertThresholdScope.ServerAndDatabase)
            return ("database_scope", $"'{entry.Wire}' is compared against a server-level figure, so it takes a server override only, not a database_name.");
        return null;
    }
}

/// <summary>
/// The per-server and per-database threshold overrides the alert engine and the PostgreSQL count checks resolve through
/// (#5916): the Darling loader behind <see cref="IAlertThresholdOverrides"/>. It holds one immutable snapshot of
/// <c>config.config_threshold_overrides</c> and answers from it, so the sweep path does no I/O. The service reloads it at
/// start and on every control-plane reload (the table is on the <c>config_version</c> beacon), and a failed load keeps the
/// snapshot already in force, as the mute rules do. Lite passes no overrides and stays advise-only.
/// </summary>
internal sealed class DarlingThresholdOverrides : IAlertThresholdOverrides
{
    internal const string SelectSql =
        "SELECT server_id, database_name, threshold_key, value FROM config.config_threshold_overrides";

    private readonly ILogger? _logger;
    private volatile Dictionary<(int Server, string Database, string Key), int> _snapshot = new();

    internal DarlingThresholdOverrides(ILogger? logger = null) => _logger = logger;

    /// <summary>How many overrides are in force.</summary>
    internal int Count => _snapshot.Count;

    /// <summary>
    /// Replaces the snapshot with the table's rows. The read is awaited before the swap and nothing here catches, so a
    /// read that throws leaves the overrides already in force. A row whose key is not a tunable threshold, whose value is
    /// not an integer inside the key's bounds, or whose database name sits on a server-only key is skipped and logged,
    /// never applied: the engine uses the value as-is.
    /// </summary>
    internal async Task LoadAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        var next = new Dictionary<(int, string, string), int>();
        await using var command = postgres.CreateCommand(SelectSql);
        command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var database = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            var key = reader.GetString(2);
            var raw = reader.GetDouble(3);
            if (Accept(key, raw, database, out var value) is { } problem)
            {
                _logger?.LogWarning(
                    "Threshold override for server {ServerId} key '{Key}' database '{Database}' was skipped: {Problem}",
                    serverId, key, database, problem);
                continue;
            }

            next[(serverId, database, key)] = value;
        }

        _snapshot = next;
    }

    /// <summary>Replaces the snapshot from rows already in hand (the loader's own tests, and any caller that read the table).</summary>
    internal void Replace(IEnumerable<(int ServerId, string? Database, string Key, double Value)> rows)
    {
        var next = new Dictionary<(int, string, string), int>();
        foreach (var (serverId, database, key, raw) in rows)
        {
            var db = database ?? string.Empty;
            if (Accept(key, raw, db, out var value) is null)
                next[(serverId, db, key)] = value;
        }

        _snapshot = next;
    }

    private static string? Accept(string key, double raw, string database, out int value)
    {
        value = 0;
        if (!DarlingThresholdKeys.TryGet(key, out var entry))
            return "not a tunable threshold";
        if (raw != Math.Floor(raw) || raw < int.MinValue || raw > int.MaxValue)
            return "not an integer";
        value = (int)raw;
        return DarlingThresholdKeys.ValidateValue(entry, value, database.Length == 0 ? null : database)?.Message;
    }

    /// <inheritdoc />
    public int Resolve(AlertThresholdKey key, string serverKey, string? database, int globalValue) =>
        ResolveWire(DarlingThresholdKeys.For(key).Wire, serverKey, database, globalValue);

    /// <summary>
    /// The value of the wire-named threshold for the server (and database): a database override first, then the
    /// server's, else <paramref name="globalValue"/>. <paramref name="serverKey"/> is the store server id as text, the
    /// engine's server key. This is the one resolution the PostgreSQL count thresholds use too.
    /// </summary>
    internal int ResolveWire(string wire, string serverKey, string? database, int globalValue)
    {
        if (!int.TryParse(serverKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var serverId))
            return globalValue;
        var snapshot = _snapshot;
        if (snapshot.Count == 0)
            return globalValue;
        if (!string.IsNullOrEmpty(database) && snapshot.TryGetValue((serverId, database, wire), out var perDatabase))
            return perDatabase;
        return snapshot.TryGetValue((serverId, string.Empty, wire), out var perServer) ? perServer : globalValue;
    }
}
