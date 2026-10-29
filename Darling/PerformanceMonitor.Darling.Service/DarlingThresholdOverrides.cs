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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The per-server, per-database and per-tag threshold overrides the alert engine and the PostgreSQL count checks resolve through
/// (#5916): the Darling loader behind <see cref="IAlertThresholdOverrides"/>. It holds one immutable snapshot of
/// <c>config.config_threshold_overrides</c> and answers from it, so the sweep path does no I/O. The service reloads it at
/// start and on every control-plane reload (the table is on the <c>config_version</c> beacon), and a failed load keeps the
/// snapshot already in force, as the mute rules do. Lite passes no overrides and stays advise-only.
/// </summary>
internal sealed class DarlingThresholdOverrides : IAlertThresholdOverrides
{
    internal const string SelectSql =
        "SELECT id, server_id, database_name, scope_tag_id, threshold_key, value FROM config.config_threshold_overrides";

    /// <summary>The direct (server and database) overrides, and the tag override that wins per server and key.</summary>
    private sealed record Snapshot(Dictionary<(int Server, string Database, string Key), int> Direct, Dictionary<(int Server, string Key), int> Tag);

    private readonly ILogger? _logger;
    private volatile Snapshot _snapshot = new(new(), new());

    internal DarlingThresholdOverrides(ILogger? logger = null) => _logger = logger;

    /// <summary>How many overrides are in force: the server and database ones, and the tag ones that some server resolves to.</summary>
    internal int Count => _snapshot.Direct.Count + _snapshot.Tag.Count;

    /// <summary>
    /// Replaces the snapshot with the table's rows. The read is awaited before the swap and nothing here catches, so a
    /// read that throws leaves the overrides already in force. A row whose key is not a tunable threshold, whose value is
    /// not an integer inside the key's bounds, or whose database name sits on a server-only key is skipped and logged,
    /// never applied: the engine uses the value as-is. Tag overrides (#6926) are resolved with the tag tree and the server-to-tag
    /// map, read only when the table holds a tag row; a tag-table read that fails leaves them matching nothing, as a tag-scoped route does.
    /// </summary>
    internal async Task LoadAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        var direct = new List<(int ServerId, string? Database, string Key, double Value)>();
        var tag = new List<(long RowId, int TagId, string Key, double Value)>();
        await using (var command = postgres.CreateCommand(SelectSql))
        {
            command.CommandTimeout = ServiceCommandDeadlines.SerialLoopSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var key = reader.GetString(4);
                var raw = reader.GetDouble(5);
                if (!reader.IsDBNull(3))
                    tag.Add((reader.GetInt64(0), reader.GetInt32(3), key, raw));
                else if (!reader.IsDBNull(1))
                    direct.Add((reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2), key, raw));
            }
        }

        StoreConfigProvider.RouteScopeInputs? inputs = null;
        if (tag.Count > 0)
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            inputs = await StoreConfigProvider.ReadRouteScopeInputsAsync(connection, _logger, cancellationToken);
        }

        Replace(direct, tag, inputs);
    }

    private void LogSkipped(string scope, string key, string database, string problem) =>
        _logger?.LogWarning(
            "Threshold override for {Scope} key '{Key}' database '{Database}' was skipped: {Problem}",
            scope, key, database, problem);

    /// <summary>Replaces the snapshot from rows already in hand (the loader's own tests, and any caller that read the table). A
    /// skipped row is logged the way <see cref="LoadAsync"/> logs it.</summary>
    internal void Replace(IEnumerable<(int ServerId, string? Database, string Key, double Value)> rows) =>
        Replace(rows, [], null);

    /// <summary>The same with tag overrides (#6926). <paramref name="tagRows"/> carry the table row id, which breaks a tie between two
    /// tags of the same depth, and <paramref name="inputs"/> the registry, tag tree and server-to-tag map the routes expand against
    /// (null: the tag tables were not read, and a tag override matches nothing).</summary>
    internal void Replace(
        IEnumerable<(int ServerId, string? Database, string Key, double Value)> rows,
        IEnumerable<(long RowId, int TagId, string Key, double Value)> tagRows,
        StoreConfigProvider.RouteScopeInputs? inputs)
    {
        var direct = new Dictionary<(int, string, string), int>();
        foreach (var (serverId, database, key, raw) in rows)
        {
            var db = database ?? string.Empty;
            if (Accept(key, raw, db, out var value) is { } problem)
                LogSkipped("server " + serverId.ToString(CultureInfo.InvariantCulture), key, db, problem);
            else
                direct[(serverId, db, key)] = value;
        }

        var accepted = new List<TagOverrideCandidate>();
        foreach (var (rowId, tagId, key, raw) in tagRows)
        {
            if (Accept(key, raw, string.Empty, out var value) is { } problem)
                LogSkipped("tag " + tagId.ToString(CultureInfo.InvariantCulture), key, string.Empty, problem);
            else
                accepted.Add(new TagOverrideCandidate(rowId, tagId, key, value));
        }

        var tagWinners = new Dictionary<(int, string), int>();
        if (accepted.Count > 0 && inputs is not null)
        {
            foreach (var (slot, winner) in TagThresholdResolver.Resolve(
                         accepted, inputs.KnownServerIds, inputs.TagParents, inputs.ServerTags, inputs.TagsReadable))
                tagWinners[slot] = (int)winner.Value;
        }

        _snapshot = new Snapshot(direct, tagWinners);
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
    /// server's, then the tag's (#6926: the deepest tag the server is under), else <paramref name="globalValue"/>.
    /// <paramref name="serverKey"/> is the store server id as text, the engine's server key. This is the one resolution the
    /// PostgreSQL count thresholds use too.
    /// </summary>
    internal int ResolveWire(string wire, string serverKey, string? database, int globalValue)
    {
        if (!int.TryParse(serverKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var serverId))
            return globalValue;
        var snapshot = _snapshot;
        if (snapshot.Direct.Count == 0 && snapshot.Tag.Count == 0)
            return globalValue;
        if (!string.IsNullOrEmpty(database) && snapshot.Direct.TryGetValue((serverId, database, wire), out var perDatabase))
            return perDatabase;
        if (snapshot.Direct.TryGetValue((serverId, string.Empty, wire), out var perServer))
            return perServer;
        return snapshot.Tag.TryGetValue((serverId, wire), out var perTag) ? perTag : globalValue;
    }
}
