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
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One recorded change a watch names: what it was, when it happened and a key that never repeats for the same change.</summary>
/// <param name="Key">Stable per change (kind, subject, capture time), so detecting the same change on a later pass records it once.</param>
/// <param name="Kind"><c>server_config</c>, <c>database_config</c>, <c>database_scoped_config</c>, <c>trace_flag</c> or <c>plan_force</c>.</param>
/// <param name="ChangeTimeUtc">The capture time the change was seen at.</param>
/// <param name="Description">One line in words, for the investigation brief.</param>
/// <param name="DatabaseName">The database a plan change belongs to, or null.</param>
public sealed record ChangeWatchChange(string Key, string Kind, DateTime ChangeTimeUtc, string Description, string? DatabaseName = null);

/// <summary>A watch window and what it saw (#6234).</summary>
public sealed record ChangeWatchRow(
    Guid WatchId, int ServerId, string ServerName, DateTime FirstChangeUtc, DateTime LastChangeUtc, DateTime WindowEndsUtc,
    string State, string? Result, Guid? RequestId, DateTime? ClosedUtc, string? ResultJson, IReadOnlyList<ChangeWatchChange> Changes);

/// <summary>
/// #6234: the watch windows a recorded change opens. Every change opens a watch of <see cref="Window"/> for its server, or extends the
/// server's open one (one open watch per server, a partial unique index), and the watch is a row, so a restart loses nothing: a watch
/// whose window ended while the service was down is simply due on the next pass. A change is recorded once under its key
/// (<c>change_watch_changes</c> primary key), so a pass that sees the same change again adds nothing.
/// </summary>
public sealed class ChangeWatchStore
{
    /// <summary>How long a watch runs from its latest change.</summary>
    public static readonly TimeSpan Window = TimeSpan.FromMinutes(60);

    /// <summary>How long a closed watch is kept.</summary>
    public static readonly TimeSpan ClosedRetention = TimeSpan.FromDays(90);

    /// <summary>The <c>result</c> of a closed watch: nothing moved, an investigation request was opened, or something moved and no request was opened (investigations off, no agent route, a limit).</summary>
    public const string ResultFlat = "flat", ResultRequestOpened = "request_opened", ResultMovedNoRequest = "moved_no_request", ResultNoData = "no_data";

    /// <summary>
    /// V207 (#6234): <c>config.change_watches</c>, <c>config.change_watch_changes</c> and <c>config.investigation_requests.trigger_kind</c>
    /// (<c>alert</c> by default, <c>change_watch</c> for a request a watch opened, which no alert ever joins). All additive and idempotent.
    /// No GRANT: provisioning re-grants every <c>config</c> table. An older build ignores the new column and tables.
    /// </summary>
    public const string V207Sql = @"
ALTER TABLE config.investigation_requests ADD COLUMN IF NOT EXISTS trigger_kind text NOT NULL DEFAULT 'alert';

CREATE TABLE IF NOT EXISTS config.change_watches
(
    watch_id uuid PRIMARY KEY,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    opened_at timestamptz NOT NULL,
    first_change_at timestamptz NOT NULL,
    last_change_at timestamptz NOT NULL,
    window_ends_at timestamptz NOT NULL,
    state text NOT NULL DEFAULT 'open',
    closed_at timestamptz,
    result text,
    request_id uuid,
    result_json text,
    CONSTRAINT ck_change_watches_state CHECK (state IN ('open', 'closed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_change_watches_open ON config.change_watches (server_id) WHERE state = 'open';
CREATE INDEX IF NOT EXISTS ix_change_watches_server_opened ON config.change_watches (server_id, opened_at);

CREATE TABLE IF NOT EXISTS config.change_watch_changes
(
    server_id integer NOT NULL,
    change_key text NOT NULL,
    watch_id uuid NOT NULL REFERENCES config.change_watches (watch_id) ON DELETE CASCADE,
    change_kind text NOT NULL,
    change_time timestamptz NOT NULL,
    description text NOT NULL,
    database_name text,
    PRIMARY KEY (server_id, change_key)
);

CREATE INDEX IF NOT EXISTS ix_change_watch_changes_watch ON config.change_watch_changes (watch_id);";

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;
    private readonly Func<DateTime> _utcNow;

    public ChangeWatchStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds, Func<DateTime>? utcNow = null)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    private DateTime Now() => DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);

    private NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, params object?[] values)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = _commandTimeoutSeconds };
        foreach (var value in values)
        {
            command.Parameters.Add(value switch
            {
                DateTime time => new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = Utc(time) },
                null => new NpgsqlParameter { Value = DBNull.Value },
                _ => new NpgsqlParameter { Value = value },
            });
        }

        return command;
    }

    /// <summary>
    /// Records <paramref name="changes"/> for a server: the ones not recorded before open its watch or extend the open one, whose
    /// window then ends <see cref="Window"/> after the latest change. Returns the watch id, or null when every change was already known.
    /// One transaction under a per-server advisory lock.
    /// </summary>
    public async Task<Guid?> RecordChangesAsync(
        int serverId, string serverName, IReadOnlyList<ChangeWatchChange> changes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(changes);
        if (changes.Count == 0)
        {
            return null;
        }

        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var lockCommand = Command(connection, transaction,
            "SELECT pg_advisory_xact_lock(hashtextextended('change_watch:' || $1::text, 6234))", serverId))
        {
            await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var known = new HashSet<string>(StringComparer.Ordinal);
        await using (var command = Command(connection, transaction,
            "SELECT change_key FROM config.change_watch_changes WHERE server_id = $1 AND change_key = ANY($2)", serverId, changes.Select(c => c.Key).ToArray()))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                known.Add(reader.GetString(0));
            }
        }

        var fresh = changes.Where(c => !known.Contains(c.Key)).GroupBy(c => c.Key, StringComparer.Ordinal).Select(g => g.First()).ToList();
        if (fresh.Count == 0)
        {
            await transaction.CommitAsync(cancellationToken);
            return null;
        }

        var first = Utc(fresh.Min(c => c.ChangeTimeUtc));
        var last = Utc(fresh.Max(c => c.ChangeTimeUtc));
        Guid watchId;
        await using (var command = Command(connection, transaction,
            "SELECT watch_id FROM config.change_watches WHERE server_id = $1 AND state = 'open' FOR UPDATE", serverId))
        {
            watchId = await command.ExecuteScalarAsync(cancellationToken) is Guid existing ? existing : Guid.Empty;
        }

        if (watchId == Guid.Empty)
        {
            watchId = Guid.NewGuid();
            await using var insert = Command(connection, transaction, @"
INSERT INTO config.change_watches (watch_id, server_id, server_name, opened_at, first_change_at, last_change_at, window_ends_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)", watchId, serverId, serverName, now, first, last, last + Window);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            await using var update = Command(connection, transaction, @"
UPDATE config.change_watches
SET first_change_at = least(first_change_at, $2),
    last_change_at = greatest(last_change_at, $3),
    window_ends_at = greatest(window_ends_at, $4)
WHERE watch_id = $1", watchId, first, last, last + Window);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var change in fresh)
        {
            await using var insert = Command(connection, transaction, @"
INSERT INTO config.change_watch_changes (server_id, change_key, watch_id, change_kind, change_time, description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (server_id, change_key) DO NOTHING",
                serverId, change.Key, watchId, change.Kind, Utc(change.ChangeTimeUtc), change.Description, change.DatabaseName);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return watchId;
    }

    /// <summary>The open watches whose window has ended, oldest first: a watch that ended while the service was down is among them.</summary>
    public Task<IReadOnlyList<ChangeWatchRow>> DueAsync(CancellationToken cancellationToken = default) =>
        ReadAsync("w.state = 'open' AND w.window_ends_at <= $1", new object?[] { Now() }, 500, cancellationToken);

    /// <summary>Watches for <c>get_change_watches</c>: open and closed, newest first, optionally one server and one state.</summary>
    public Task<IReadOnlyList<ChangeWatchRow>> ListAsync(int? serverId, string? state, int limit, CancellationToken cancellationToken = default) =>
        ReadAsync("($1::int IS NULL OR w.server_id = $1) AND ($2::text IS NULL OR w.state = $2)", new object?[] { serverId, state }, Math.Clamp(limit, 1, 500), cancellationToken);

    private async Task<IReadOnlyList<ChangeWatchRow>> ReadAsync(string where, object?[] parameters, int limit, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var rows = new List<(Guid, int, string, DateTime, DateTime, DateTime, string, string?, Guid?, DateTime?, string?)>();
        await using (var command = Command(connection, null, $@"
SELECT w.watch_id, w.server_id, w.server_name, w.first_change_at, w.last_change_at, w.window_ends_at, w.state, w.result, w.request_id, w.closed_at, w.result_json
FROM config.change_watches AS w
WHERE {where}
ORDER BY w.first_change_at DESC
LIMIT {limit.ToString(CultureInfo.InvariantCulture)}", parameters))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add((reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), Utc(reader.GetDateTime(3)), Utc(reader.GetDateTime(4)),
                    Utc(reader.GetDateTime(5)), reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.IsDBNull(8) ? null : reader.GetGuid(8), reader.IsDBNull(9) ? null : Utc(reader.GetDateTime(9)),
                    reader.IsDBNull(10) ? null : reader.GetString(10)));
            }
        }

        var changes = new Dictionary<Guid, List<ChangeWatchChange>>();
        if (rows.Count > 0)
        {
            await using var command = Command(connection, null, @"
SELECT watch_id, change_key, change_kind, change_time, description, database_name
FROM config.change_watch_changes
WHERE watch_id = ANY($1)
ORDER BY change_time, change_key", rows.Select(r => r.Item1).ToArray());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetGuid(0);
                if (!changes.TryGetValue(id, out var list))
                {
                    changes[id] = list = new List<ChangeWatchChange>();
                }

                list.Add(new ChangeWatchChange(reader.GetString(1), reader.GetString(2), Utc(reader.GetDateTime(3)), reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5)));
            }
        }

        return rows.Select(r => new ChangeWatchRow(r.Item1, r.Item2, r.Item3, r.Item4, r.Item5, r.Item6, r.Item7, r.Item8, r.Item9, r.Item10, r.Item11,
            changes.TryGetValue(r.Item1, out var c) ? c : new List<ChangeWatchChange>())).ToList();
    }

    /// <summary>Closes an open watch with its result. False when it was not open (another pass closed it first).</summary>
    public async Task<bool> CloseAsync(Guid watchId, string result, Guid? requestId, string resultJson, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var close = Command(connection, null, @"
UPDATE config.change_watches
SET state = 'closed', closed_at = $2, result = $3, request_id = $4, result_json = $5
WHERE watch_id = $1 AND state = 'open'", watchId, Now(), result, requestId, resultJson);
        var closed = await close.ExecuteNonQueryAsync(cancellationToken) > 0;
        await using var prune = Command(connection, null,
            "DELETE FROM config.change_watches WHERE state = 'closed' AND closed_at < $1", Now() - ClosedRetention);
        await prune.ExecuteNonQueryAsync(cancellationToken);
        return closed;
    }
}
