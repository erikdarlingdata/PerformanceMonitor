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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Where the one-time legacy drop keeps its record: the store in production, memory in a test that never opens one. Both
/// calls throw when the store cannot answer. The runner's collector-state helpers swallow their failures and return no
/// state, which is the safe direction for a watermark and the wrong one here: a record that could not be read must never
/// read as "not dropped yet", because that would drop the session a second time.
/// </summary>
internal interface ILegacyLongQueryRecords
{
    /// <summary>
    /// The state keys already recorded for one registration under the legacy session's collector name, the database
    /// names included. Throws when the store cannot be read.
    /// </summary>
    Task<IReadOnlyCollection<string>> ReadAsync(int serverId, CancellationToken cancellationToken);

    /// <summary>Records one drop under <paramref name="stateKey"/>, with the drop's time as the value. Throws when the store cannot be written.</summary>
    Task WriteAsync(int serverId, string stateKey, DateTime droppedUtc, CancellationToken cancellationToken);
}

/// <summary>
/// The record kept in <c>collect.collector_state</c>: collector <see cref="LegacyLongQuerySession.StateCollector"/>, one
/// state key for each database (<see cref="LegacyLongQuerySession.StateKey"/>), the time of the drop as the value. The
/// table needs no migration, and no prune, archive or reset step deletes these keys.
/// </summary>
internal sealed class StoreLegacyLongQueryRecords : ILegacyLongQueryRecords
{
    private readonly NpgsqlDataSource _postgres;

    public StoreLegacyLongQueryRecords(NpgsqlDataSource postgres)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
    }

    public async Task<IReadOnlyCollection<string>> ReadAsync(int serverId, CancellationToken cancellationToken)
    {
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(
            "SELECT state_key FROM collect.collector_state WHERE server_id = $1 AND collector_name = $2 AND starts_with(state_key, $3)", connection);
        command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(LegacyLongQuerySession.StateCollector);
        command.Parameters.AddWithValue(LegacyLongQuerySession.StateKeyPrefix);

        var keys = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            keys.Add(reader.GetString(0));
        }

        return keys;
    }

    public async Task WriteAsync(int serverId, string stateKey, DateTime droppedUtc, CancellationToken cancellationToken)
    {
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at", connection);
        command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(LegacyLongQuerySession.StateCollector);
        command.Parameters.AddWithValue(stateKey);
        command.Parameters.AddWithValue(droppedUtc.ToString("o", CultureInfo.InvariantCulture));

        /* Naive UTC, Kind-Unspecified: updated_at is a timestamp without time zone, and Npgsql refuses a Kind=Utc value for it. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}

/// <summary>
/// The long-query session that versions before #4961 shared between installs, dropped once for each registration and
/// database, on a full pass or the attempt after the cap, whether the trace is on or off. The drop comes first and the
/// record second: a crash between them costs one more guarded drop, where recording first could lose the drop for good. A
/// failed drop is not recorded, and takes the retry cap the per-install drops take. A record that cannot be read skips the
/// step for that pass and fails the pass the same way, so a read failure never turns into a second drop.
/// <para>Once a key is known to be recorded, that stays in memory until the registration reconnects, so the passes after
/// the first one do not read the store again. A reconnect also lets the one Information line about a legacy session that an
/// older install created again be logged once more.</para>
/// </summary>
internal sealed class DarlingLegacyLongQuerySession
{
    /// <summary>The session's name, for the batches that ask whether an older install created it again.</summary>
    internal const string SessionName = LongQueryCompletionsCollector.LegacyXeSessionName;

    private readonly ILegacyLongQueryRecords _records;
    private readonly object _gate = new();

    /* The state keys known to be recorded, for each registration whose records were read since its last connect. */
    private readonly Dictionary<int, HashSet<string>> _recorded = new();
    private readonly HashSet<int> _noticed = new();

    public DarlingLegacyLongQuerySession(ILegacyLongQueryRecords records)
    {
        _records = records ?? throw new ArgumentNullException(nameof(records));
    }

    /// <summary>
    /// Starts one reconcile pass's legacy drops. A <see cref="LongQueryTracePass.CreateOnly"/> pass has none. Any other pass
    /// reads the registration's records, once per connect, and a read that fails leaves the pass with nothing to drop and a
    /// failure to report (<see cref="LegacyLongQueryDropPass.ToException"/>).
    /// </summary>
    internal async Task<LegacyLongQueryDropPass> BeginPassAsync(ServerRuntime server, LongQueryTracePass pass, ILogger? logger, CancellationToken cancellationToken)
    {
        if (pass == LongQueryTracePass.CreateOnly)
        {
            return new LegacyLongQueryDropPass(this, server, runs: false, afterTheCap: false, logger, readFailure: null);
        }

        var afterTheCap = pass == LongQueryTracePass.RetryAfterCap;
        try
        {
            await LoadAsync(server.ServerId, cancellationToken);
            return new LegacyLongQueryDropPass(this, server, runs: true, afterTheCap, logger, readFailure: null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.Log(afterTheCap ? LogLevel.Debug : LogLevel.Warning,
                "[{Server}] Could not read the record of the legacy long-query completion XE session's drop, so the drop waits for the next pass: {Message}",
                server.Config.DisplayName, ex.Message);
            return new LegacyLongQueryDropPass(
                this, server, runs: true, afterTheCap, logger,
                new InvalidOperationException("The record of the legacy long-query session's drop could not be read: " + ex.Message, ex));
        }
    }

    private async Task LoadAsync(int serverId, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_recorded.ContainsKey(serverId))
            {
                return;
            }
        }

        var keys = await _records.ReadAsync(serverId, cancellationToken);
        lock (_gate)
        {
            if (!_recorded.ContainsKey(serverId))
            {
                _recorded[serverId] = new HashSet<string>(keys, StringComparer.Ordinal);
            }
        }
    }

    /// <summary>True when the drop in this database is known to be recorded. Memory only: a connect reads the store.</summary>
    internal bool IsRecorded(int serverId, string database)
    {
        lock (_gate)
        {
            return _recorded.TryGetValue(serverId, out var keys) && keys.Contains(LegacyLongQuerySession.StateKey(database));
        }
    }

    internal async Task RecordAsync(int serverId, string database, CancellationToken cancellationToken)
    {
        var key = LegacyLongQuerySession.StateKey(database);
        await _records.WriteAsync(serverId, key, DateTime.UtcNow, cancellationToken);
        lock (_gate)
        {
            if (_recorded.TryGetValue(serverId, out var keys))
            {
                keys.Add(key);
            }
        }
    }

    /// <summary>
    /// The per-connect reset: the records are read again at the next full pass, and the Information line about a legacy
    /// session an older install created again can be logged once more.
    /// </summary>
    internal void OnServerReconnected(int serverId)
    {
        lock (_gate)
        {
            _recorded.Remove(serverId);
            _noticed.Remove(serverId);
        }
    }

    /// <summary>
    /// A legacy session is in this database, and the drop here is already recorded: an older Lite or Darling created it
    /// again. It is left alone, and one Information line per connect says so. A session found before the drop is recorded
    /// is not this case: the drop is still owed and the retry cap owns it.
    /// </summary>
    internal void NoteFound(ServerRuntime server, string database, ILogger? logger)
    {
        lock (_gate)
        {
            if (!_recorded.TryGetValue(server.ServerId, out var keys)
                || !keys.Contains(LegacyLongQuerySession.StateKey(database))
                || !_noticed.Add(server.ServerId))
            {
                return;
            }
        }

        var where = database.Length == 0 ? "on this server" : $"in database [{database}]";
        logger?.LogInformation(
            "[{Server}] The long-query Extended Events session {Session} exists {Where}. An older Lite or Darling created it. This install dropped it once and does not touch it again. To remove it, see the README or run --drop-xe-sessions.",
            server.Config.DisplayName, SessionName, where);
    }
}

/// <summary>
/// One reconcile pass's legacy drops (<see cref="DarlingLegacyLongQuerySession.BeginPassAsync"/>). It drops the session in
/// each database it is asked about, skips a database whose drop is recorded, records each drop that succeeds, tries every
/// database before it reports, and logs each failure at Warning, or at Debug for the attempt after the cap.
/// </summary>
internal sealed class LegacyLongQueryDropPass
{
    private readonly DarlingLegacyLongQuerySession _owner;
    private readonly ServerRuntime _server;
    private readonly bool _runs;
    private readonly bool _afterTheCap;
    private readonly ILogger? _logger;
    private readonly Exception? _readFailure;
    private readonly List<string> _failed = new();
    private Exception? _firstFailure;

    internal LegacyLongQueryDropPass(
        DarlingLegacyLongQuerySession owner, ServerRuntime server, bool runs, bool afterTheCap, ILogger? logger, Exception? readFailure)
    {
        _owner = owner;
        _server = server;
        _runs = runs;
        _afterTheCap = afterTheCap;
        _logger = logger;
        _readFailure = readFailure;
    }

    /// <summary>
    /// Drops the session in one database (the empty name is the server), then records the drop. <paramref name="drop"/>
    /// takes the session's name. A failure is kept for <see cref="ToException"/>, never thrown, so the per-install work in
    /// the same database goes on.
    /// </summary>
    internal async Task DropAsync(string database, Func<string, CancellationToken, Task> drop, CancellationToken cancellationToken)
    {
        if (!_runs || _readFailure is not null || _owner.IsRecorded(_server.ServerId, database))
        {
            return;
        }

        try
        {
            await drop(DarlingLegacyLongQuerySession.SessionName, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Fail(database, ex, "Failed to drop the legacy long-query completion XE session");
            return;
        }

        try
        {
            await _owner.RecordAsync(_server.ServerId, database, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Fail(database, ex, "Dropped the legacy long-query completion XE session but could not record the drop, so it is dropped again on the next pass");
        }
    }

    private void Fail(string database, Exception ex, string text)
    {
        _failed.Add(database);
        _firstFailure ??= ex;
        _logger?.Log(_afterTheCap ? LogLevel.Debug : LogLevel.Warning, "[{Server}]{Where} {Text}: {Message}",
            _server.Config.DisplayName, Where(database), text, ex.Message);
    }

    private static string Where(string database) => database.Length == 0 ? string.Empty : $" [{database}]";

    /// <summary>
    /// The failure this pass owes the worker, or null. <paramref name="planned"/> is the databases the pass was to drop in:
    /// a record that could not be read leaves the session where it may remain in each of them. On the server's own session
    /// (<paramref name="onServer"/>) there are no databases to name.
    /// </summary>
    internal LongQueryTraceDropException? ToException(IReadOnlyList<string> planned, string? createNote, bool onServer)
    {
        if (_readFailure is not null)
        {
            if (onServer)
            {
                return LongQueryTraceDropException.ForServer(_readFailure);
            }

            return planned.Count == 0 ? null : new LongQueryTraceDropException(planned, _readFailure, createNote);
        }

        if (_firstFailure is null)
        {
            return null;
        }

        return onServer
            ? LongQueryTraceDropException.ForServer(_firstFailure)
            : new LongQueryTraceDropException(_failed, _firstFailure, createNote);
    }

    /// <summary>The two cleanup failures of one pass, per-install and legacy, as one exception that names every database.</summary>
    internal static LongQueryTraceDropException? Merge(LongQueryTraceDropException? first, LongQueryTraceDropException? second)
    {
        if (first is null)
        {
            return second;
        }

        if (second is null || first.OnServer || second.OnServer)
        {
            return first;
        }

        var databases = first.Databases.Concat(second.Databases).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        return new LongQueryTraceDropException(databases, first.InnerException ?? first, first.CreateNote ?? second.CreateNote);
    }
}
