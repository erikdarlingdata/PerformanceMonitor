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
/// <param name="Kind"><c>server_config</c>, <c>database_config</c>, <c>database_scoped_config</c>, <c>trace_flag</c>, <c>plan_force</c> or <c>query_store_hint</c> (#6581).</param>
/// <param name="ChangeTimeUtc">The capture time the change was seen at.</param>
/// <param name="Description">One line in words, for the investigation brief.</param>
/// <param name="DatabaseName">The database a plan change belongs to, or null.</param>
/// <param name="QueryId">The Query Store query a plan change belongs to, or null; a watch of plan or Query Store hint changes alone measures these queries.</param>
/// <param name="PlanId">The Query Store plan a plan change forced or unforced, or null: the agent-only test (#6239 round 3, M1) matches an agent's action to a change by server, database and this plan id.</param>
public sealed record ChangeWatchChange(string Key, string Kind, DateTime ChangeTimeUtc, string Description, string? DatabaseName = null, long? QueryId = null, long? PlanId = null)
{
    /// <summary>
    /// #6550: how a <c>plan_force</c> change's plan was forced: <c>MANUAL</c>, <c>AUTO</c> (automatic plan correction), <c>NONE</c> or <c>unknown</c> (before
    /// SQL Server 2017 or not captured); null for any other kind. It rides in <see cref="Description"/> (see <see cref="ChangeWatchPlanForcing"/>), so a stored
    /// change carries it with no column of its own, and a change recorded before this existed reads as <c>unknown</c>.
    /// </summary>
    public string? PlanForcingType => string.Equals(Kind, "plan_force", StringComparison.Ordinal) ? ChangeWatchPlanForcing.Parse(Description) : null;
}

/// <summary>#6550: writes and reads the forcing type a <c>plan_force</c> change's description carries, as a trailing <c>[forcing type X]</c>.</summary>
public static class ChangeWatchPlanForcing
{
    /// <summary>A plan forced by automatic plan correction (<c>FORCE_LAST_GOOD_PLAN</c>).</summary>
    public const string Auto = "AUTO";

    /// <summary>A plan forced by a person or tool.</summary>
    public const string Manual = "MANUAL";

    /// <summary>The server did not say (an older SQL Server, or the capture holds no value).</summary>
    public const string Unknown = "unknown";

    /* The one spelling both Suffix (the writer) and Parse (the reader) use, so rewording it here keeps the two in step. Rows already stored carry the old
       text, though, and read as unknown after a reword: keep the spelling, or read the old one too. */
    private const string Prefix = " [forcing type ";

    /// <summary>The collector's <c>plan_forcing_type</c> as one of <c>MANUAL</c>, <c>AUTO</c>, <c>NONE</c> or <c>unknown</c>.</summary>
    public static string Normalize(string? raw)
    {
        var value = raw?.Trim().ToUpperInvariant();
        return value is Auto or Manual or "NONE" ? value : Unknown;
    }

    /// <summary>The suffix a change description ends with.</summary>
    public static string Suffix(string? raw)
    {
        var type = Normalize(raw);
        return type == Auto ? $"{Prefix}{Auto}: automatic plan correction]" : $"{Prefix}{type}]";
    }

    /// <summary>The forcing type a description names; <c>unknown</c> when it names none.</summary>
    public static string Parse(string? description)
    {
        if (description is null)
        {
            return Unknown;
        }

        var start = description.LastIndexOf(Prefix, StringComparison.Ordinal);
        if (start < 0)
        {
            return Unknown;
        }

        start += Prefix.Length;
        var end = start;
        while (end < description.Length && char.IsLetter(description[end]))
        {
            end++;
        }

        return Normalize(description[start..end]);
    }
}

/// <summary>A watch window and what it saw (#6234).</summary>
public sealed record ChangeWatchRow(
    Guid WatchId, int ServerId, string ServerName, DateTime FirstChangeUtc, DateTime LastChangeUtc, DateTime WindowEndsUtc,
    string State, string? Result, Guid? RequestId, DateTime? ClosedUtc, string? ResultJson, IReadOnlyList<ChangeWatchChange> Changes);

/// <summary>What <see cref="ChangeWatchStore.RecordAsync"/> did: the watch the changes joined or opened (null when none was recorded), and whether some
/// change could not be recorded yet (its time is after the open watch's window): the caller keeps its marks, so the change is read again once that
/// watch has been evaluated.</summary>
public sealed record ChangeWatchRecordResult(Guid? WatchId, bool Deferred);

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

    /// <summary>The <c>result</c> of a watch that saw something move but whose every change was an agent's own plan change made while it handled a change-watch request: it is recorded and measured, and opens no further request (no loop, #6234).</summary>
    public const string ResultAgentChange = "moved_agent_change";

    /// <summary>The <c>result</c> of a watch that saw something move but whose every change was a plan forced or unforced by automatic plan correction (#6550): SQL Server doing its job, so it is recorded and measured and opens no request.</summary>
    public const string ResultAutoPlanCorrection = "moved_auto_plan_correction";

    /// <summary>The <c>state</c> and <c>result</c> of a watch whose evaluation, or whose request open, failed <see cref="MaxAttempts"/> times (#6239 round 3, M2): it is closed for good with the last error in its <c>result_json</c>, and pruned like a closed one.</summary>
    public const string StateFailed = "failed";

    /// <summary>How many times a watch is claimed (an evaluation or a request open that throws, or a claimer that dies, each counts one) before it is closed as <see cref="StateFailed"/>.</summary>
    public const int MaxAttempts = 5;

    /// <summary>The most a watch may span from its first change (#6234): extension stops there, and a later change starts a new watch once this one has closed.</summary>
    public static readonly TimeSpan MaxSpan = TimeSpan.FromMinutes(180);

    /// <summary>How long a claim (<c>evaluating</c>) holds before another pass may take the watch over: the instance that claimed it is taken to have died.</summary>
    public static readonly TimeSpan StaleClaim = TimeSpan.FromMinutes(10);

    /// <summary>The dedup key a watch's request carries (#6234): one request per watch id, enforced by a unique index.</summary>
    public static string RequestKey(Guid watchId) => "change_watch:" + watchId.ToString("D");

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
    claimed_at timestamptz,
    attempts smallint NOT NULL DEFAULT 0,
    CONSTRAINT ck_change_watches_state CHECK (state IN ('open', 'evaluating', 'closed', 'failed'))
);

/* A store that took an earlier draft of this rung has the table without the claim column and with the two-state check. */
ALTER TABLE config.change_watches ADD COLUMN IF NOT EXISTS claimed_at timestamptz;
ALTER TABLE config.change_watches ADD COLUMN IF NOT EXISTS attempts smallint NOT NULL DEFAULT 0;
ALTER TABLE config.change_watches DROP CONSTRAINT IF EXISTS ck_change_watches_state;
ALTER TABLE config.change_watches ADD CONSTRAINT ck_change_watches_state CHECK (state IN ('open', 'evaluating', 'closed', 'failed'));

/* One request per watch: the request's first dedup key is the watch id, so a retry, a crash between the open and the close, or a second
   instance cannot open a second one (the loser's insert fails and it reads the winner's). */
CREATE UNIQUE INDEX IF NOT EXISTS uq_investigation_requests_change_watch
    ON config.investigation_requests (first_dedup_key) WHERE trigger_kind = 'change_watch';

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
    query_id bigint,
    plan_id bigint,
    PRIMARY KEY (server_id, change_key)
);

ALTER TABLE config.change_watch_changes ADD COLUMN IF NOT EXISTS plan_id bigint;

CREATE INDEX IF NOT EXISTS ix_change_watch_changes_watch ON config.change_watch_changes (watch_id);

/* The high-water mark of change detection, per server and per source: detection reads only snapshots newer than it, so a tick costs the
   new rows and not the history. A server with no mark yet gets every mark set to now and opens nothing for what is already stored. */
CREATE TABLE IF NOT EXISTS config.change_watch_marks
(
    server_id integer NOT NULL,
    source text NOT NULL,
    mark timestamptz NOT NULL,
    PRIMARY KEY (server_id, source)
);

/* #6239 round 3, L5: plan detection finds a database's previous forced-plan capture by its marker row (plan_id IS NULL), one index seek per new capture,
   and no longer reads a day of forced-plan rows per server per pass. A few rows per database per pass, so the build is a read of a small plain table. */
CREATE INDEX IF NOT EXISTS idx_query_store_forced_plans_markers
    ON collect.query_store_forced_plans (server_id, database_name, collection_time) WHERE plan_id IS NULL;";

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
        int serverId, string serverName, IReadOnlyList<ChangeWatchChange> changes, CancellationToken cancellationToken = default) =>
        (await RecordAsync(serverId, serverName, changes, cancellationToken)).WatchId;

    /// <summary>
    /// <see cref="RecordChangesAsync"/> with the deferral spelled out. A change joins the server's open watch only when its time is at or before that
    /// watch's window end (a watch whose window has ended is evaluated, never extended), and the window never runs past <see cref="MaxSpan"/> from the
    /// watch's first change; a change that cannot join is deferred. A watch that is evaluating or closed no longer blocks a new one.
    /// </summary>
    public async Task<ChangeWatchRecordResult> RecordAsync(
        int serverId, string serverName, IReadOnlyList<ChangeWatchChange> changes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(changes);
        if (changes.Count == 0)
        {
            return new ChangeWatchRecordResult(null, false);
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
            return new ChangeWatchRecordResult(null, false);
        }

        var watchId = Guid.Empty;
        DateTime windowEnds = default;
        DateTime firstChangeAt = default;
        await using (var command = Command(connection, transaction,
            "SELECT watch_id, window_ends_at, first_change_at FROM config.change_watches WHERE server_id = $1 AND state = 'open' FOR UPDATE", serverId))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                watchId = reader.GetGuid(0);
                windowEnds = Utc(reader.GetDateTime(1));
                firstChangeAt = Utc(reader.GetDateTime(2));
            }
        }

        var deferred = false;
        if (watchId == Guid.Empty)
        {
            /* #6234: one read can now hold an hour of changes. Only the ones a watch opened by the first would have taken in, one at a time, open it; the rest
               are deferred and open their own watch once this one is evaluated, as they did when every change was read within a tick. */
            var firstChange = Utc(fresh.Min(c => c.ChangeTimeUtc));
            var opening = Joining(fresh, firstChange + Window, firstChange);
            deferred = opening.Count < fresh.Count;
            fresh = opening;
            var first = Utc(fresh.Min(c => c.ChangeTimeUtc));
            var last = Utc(fresh.Max(c => c.ChangeTimeUtc));
            watchId = Guid.NewGuid();
            await using var insert = Command(connection, transaction, @"
INSERT INTO config.change_watches (watch_id, server_id, server_name, opened_at, first_change_at, last_change_at, window_ends_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)", watchId, serverId, serverName, now, first, last, new[] { last + Window, first + MaxSpan }.Min());
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            var joining = Joining(fresh, windowEnds, firstChangeAt);
            deferred = joining.Count < fresh.Count;
            fresh = joining;
            if (fresh.Count == 0)
            {
                await transaction.CommitAsync(cancellationToken);
                return new ChangeWatchRecordResult(null, true);
            }

            await using var update = Command(connection, transaction, @"
UPDATE config.change_watches
SET first_change_at = least(first_change_at, $2),
    last_change_at = greatest(last_change_at, $3),
    window_ends_at = least(greatest(window_ends_at, $4), least(first_change_at, $2) + make_interval(mins => $5))
WHERE watch_id = $1", watchId, Utc(fresh.Min(c => c.ChangeTimeUtc)), Utc(fresh.Max(c => c.ChangeTimeUtc)),
                Utc(fresh.Max(c => c.ChangeTimeUtc)) + Window, (int)MaxSpan.TotalMinutes);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var change in fresh)
        {
            await using var insert = Command(connection, transaction, @"
INSERT INTO config.change_watch_changes (server_id, change_key, watch_id, change_kind, change_time, description, database_name, query_id, plan_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (server_id, change_key) DO NOTHING",
                serverId, change.Key, watchId, change.Kind, Utc(change.ChangeTimeUtc), change.Description, change.DatabaseName, change.QueryId, change.PlanId);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return new ChangeWatchRecordResult(watchId, deferred);
    }

    /// <summary>
    /// #6234: the changes, oldest first, that a watch ending at <paramref name="windowEnds"/> takes in when they are recorded one at a time: a change joins
    /// when it is at or before the window's end, and each one that joins moves the end to <see cref="Window"/> after it, never past <see cref="MaxSpan"/>
    /// from the watch's first change. The first change that does not join, and every later one, is deferred.
    /// </summary>
    internal static List<ChangeWatchChange> Joining(IEnumerable<ChangeWatchChange> changes, DateTime windowEnds, DateTime firstChange)
    {
        var joined = new List<ChangeWatchChange>();
        var end = Utc(windowEnds);
        var first = Utc(firstChange);
        foreach (var change in changes.OrderBy(c => c.ChangeTimeUtc))
        {
            var at = Utc(change.ChangeTimeUtc);
            if (at > end)
            {
                break;
            }

            joined.Add(change);
            first = at < first ? at : first;
            var grown = at + Window > end ? at + Window : end;
            end = grown < first + MaxSpan ? grown : first + MaxSpan;
        }

        return joined;
    }

    /// <summary>The detection sources a server has a high-water mark for.</summary>
    public static readonly IReadOnlyList<string> MarkSources =
        ["server_config", "database_config", "database_scoped_config", "trace_flags", "plan_actions", "plan_snapshots", "hint_snapshots"];

    /// <summary>The sources a PostgreSQL target has a mark for (#6234): server-wide settings, per-database and per-role overrides, and
    /// extension install state (the daily extension snapshot). The marks table keys on (server, source), so no migration is needed.</summary>
    public static readonly IReadOnlyList<string> PostgresMarkSources = ["pg_server_config", "pg_scoped_config", "pg_extensions"];

    /// <summary>The server's high-water marks (UTC), by source. Empty for a server detection has never run for.</summary>
    public async Task<Dictionary<string, DateTime>> GetMarksAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var marks = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, "SELECT source, mark FROM config.change_watch_marks WHERE server_id = $1", serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            marks[reader.GetString(0)] = Utc(reader.GetDateTime(1));
        }

        return marks;
    }

    /// <summary>Sets the marks of a server; a mark never moves back.</summary>
    public async Task SetMarksAsync(int serverId, IReadOnlyDictionary<string, DateTime> marks, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(marks);
        if (marks.Count == 0)
        {
            return;
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, @"
INSERT INTO config.change_watch_marks (server_id, source, mark)
SELECT $1, s.source, s.mark FROM unnest($2::text[], $3::timestamptz[]) AS s(source, mark)
ON CONFLICT (server_id, source) DO UPDATE SET mark = greatest(config.change_watch_marks.mark, EXCLUDED.mark)",
            serverId, marks.Keys.ToArray(), marks.Values.Select(Utc).ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The open watches whose window has ended, oldest first: a watch that ended while the service was down is among them.</summary>
    public Task<IReadOnlyList<ChangeWatchRow>> DueAsync(CancellationToken cancellationToken = default) =>
        ReadAsync("w.window_ends_at <= $1 AND (w.state = 'open' OR (w.state = 'evaluating' AND w.claimed_at <= $2))",
            new object?[] { Now(), Now() - StaleClaim }, 500, cancellationToken);

    /// <summary>
    /// Takes a due watch for evaluation: one UPDATE from <c>open</c> to <c>evaluating</c> (or from an <c>evaluating</c> whose claim has gone stale), guarded
    /// by the status it reads, with RETURNING. Null when another pass or instance holds it, so each watch is evaluated, and its request opened, by one.
    /// Each claim counts one attempt (#6239 round 3, M2); the count is returned.
    /// </summary>
    public async Task<int?> ClaimAsync(Guid watchId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var claim = Command(connection, null, @"
UPDATE config.change_watches
SET state = 'evaluating', claimed_at = $2, attempts = attempts + 1
WHERE watch_id = $1 AND window_ends_at <= $2
  AND (state = 'open' OR (state = 'evaluating' AND claimed_at <= $3))
RETURNING attempts", watchId, Now(), Now() - StaleClaim);
        return await claim.ExecuteScalarAsync(cancellationToken) is short attempts ? attempts : null;
    }

    /// <summary>
    /// #6239 round 3, M2: a claimed watch's evaluation, or its request open, failed. Below <see cref="MaxAttempts"/> attempts the watch stays claimed and is tried
    /// again once the claim is stale (a transient error); at <see cref="MaxAttempts"/> it is closed as <see cref="StateFailed"/> with the error in its
    /// <c>result_json</c>, so a permanent error (a read that always times out, a row that always throws) neither loops for ever nor stays in the table for ever.
    /// True when this call closed it.
    /// </summary>
    public async Task<bool> FailAttemptAsync(Guid watchId, string error, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, @"
UPDATE config.change_watches
SET state = 'failed', closed_at = $2, result = 'failed', result_json = $3
WHERE watch_id = $1 AND state = 'evaluating' AND attempts >= $4",
            watchId, Now(), System.Text.Json.JsonSerializer.Serialize(new { error, attempts = MaxAttempts }), (short)MaxAttempts);
        var closed = await command.ExecuteNonQueryAsync(cancellationToken) > 0;
        if (closed)
        {
            await PruneAsync(connection, cancellationToken);
        }

        return closed;
    }

    private async Task PruneAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var prune = Command(connection, null,
            "DELETE FROM config.change_watches WHERE state IN ('closed', 'failed') AND closed_at < $1", Now() - ClosedRetention);
        await prune.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The request already opened for this watch (its dedup key is the watch id), or null: a pass that took over a stale claim reads it instead of opening a second.</summary>
    public async Task<Guid?> FindRequestAsync(Guid watchId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null,
            "SELECT request_id FROM config.investigation_requests WHERE trigger_kind = 'change_watch' AND first_dedup_key = $1", RequestKey(watchId));
        return await command.ExecuteScalarAsync(cancellationToken) is Guid id ? id : null;
    }

    /// <summary>
    /// True when the watch has changes and every one is an agent's plan force or unforce made while it handled a change-watch request: an
    /// <c>action_called</c> journal row of a <c>plan_force</c> or <c>plan_unforce</c> kind, by an agent, on a change-watch request of the change's server, whose
    /// target is the change's database and plan (#6239 round 3, M1: matched by plan, not by time alone, so a person's change near an agent's unrelated one still
    /// opens a request), up to <paramref name="reach"/> before the capture that saw the change (#6234: the forced-plans collector's capture gap limit on that
    /// server, <c>the larger of 30 minutes and twice its frequency plus 5 minutes</c>, not a fixed 30 minutes: at a 60-minute frequency the capture that sees an agent's force comes up to an hour
    /// after it, and a person's change is told apart from the agent's only if the window reaches the action). Such a watch is recorded and measured but opens no request, so an agent's fix
    /// cannot start the next.
    /// </summary>
    public async Task<bool> IsAgentOnlyAsync(Guid watchId, TimeSpan reach, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, @"
SELECT EXISTS (SELECT 1 FROM config.change_watch_changes WHERE watch_id = $1)
   AND NOT EXISTS (
        SELECT 1 FROM config.change_watch_changes AS c
        WHERE c.watch_id = $1
          AND (c.change_kind <> 'plan_force'
               OR NOT EXISTS (
                    SELECT 1 FROM config.investigation_journal AS j
                    JOIN config.investigation_requests AS r ON r.request_id = j.request_id
                    WHERE j.event = 'action_called' AND j.actor LIKE 'agent:%'
                      AND j.detail->>'kind' IN ('plan_force', 'plan_unforce')
                      AND r.trigger_kind = 'change_watch' AND r.server_id = c.server_id
                      AND j.detail->'target'->>'database' = c.database_name AND j.detail->'target'->>'id' = c.plan_id::text
                      AND j.event_time >= c.change_time - $2 * interval '1 minute' AND j.event_time <= c.change_time + interval '5 minutes')))", watchId, (int)Math.Ceiling(reach.TotalMinutes));
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    /// <summary>Watches for <c>get_change_watches</c>: open and closed, newest first, optionally one server, one state or result (<paramref name="state"/> matches a watch's <c>state</c> or its <c>result</c>) and one <paramref name="watchId"/> (#6591).</summary>
    public Task<IReadOnlyList<ChangeWatchRow>> ListAsync(int? serverId, string? state, int limit, Guid? watchId = null, CancellationToken cancellationToken = default) =>
        ReadAsync("($1::int IS NULL OR w.server_id = $1) AND ($2::text IS NULL OR w.state = $2 OR w.result = $2) AND ($3::uuid IS NULL OR w.watch_id = $3)", new object?[] { serverId, state, watchId }, Math.Clamp(limit, 1, 1000), cancellationToken);

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
SELECT watch_id, change_key, change_kind, change_time, description, database_name, query_id, plan_id
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
                    reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetInt64(6), reader.IsDBNull(7) ? null : reader.GetInt64(7)));
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
WHERE watch_id = $1 AND state IN ('open', 'evaluating')", watchId, Now(), result, requestId, resultJson);
        var closed = await close.ExecuteNonQueryAsync(cancellationToken) > 0;
        await PruneAsync(connection, cancellationToken);
        return closed;
    }
}
