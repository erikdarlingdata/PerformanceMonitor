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
