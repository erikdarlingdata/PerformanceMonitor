/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// One claimed row of <c>config.external_alert_pending</c>, with the name of the source that submitted it (#5654).
/// <see cref="SubmittedAtUtc"/> and <see cref="ExpiresAtUtc"/> are UTC.
/// </summary>
public sealed record ExternalAlertPendingRow(
    long PendingId,
    int SourceId,
    string SourceName,
    string Kind,
    string AlertKey,
    string Severity,
    string? Title,
    string? Detail,
    string? ServerName,
    double? Value,
    DateTime SubmittedAtUtc,
    DateTime ExpiresAtUtc,
    int Attempts);

/// <summary>How many rows of one source ended in one give-up status (<c>expired</c>, <c>interrupted</c> or <c>failed</c>) inside a window (#5654).</summary>
public sealed record ExternalAlertUndeliveredCount(string SourceName, string Status, int Count);

/// <summary>What a claimed row became. Each maps to one terminal <c>status</c> value of the pending table.</summary>
public enum ExternalAlertOutcome
{
    /// <summary>The channels took it.</summary>
    Delivered,

    /// <summary>A mute rule matched, so nothing was sent.</summary>
    Muted,

    /// <summary>No channel and no route wanted it.</summary>
    NoChannel,

    /// <summary>Every send failed and no retry is left.</summary>
    Failed,

    /// <summary>The drain chose to drop it, for example a clear for a key that never fired.</summary>
    Ignored,

    /// <summary>The run that claimed it ended without reporting. It is never sent again.</summary>
    Interrupted,
}

/// <summary>
/// A state write waited longer than <see cref="ExternalAlertStore.StateWriteLockTimeoutMilliseconds"/> for a lock another
/// session holds (#5654). That write was not made. For a fire nothing was sent either, because its state comes before the
/// send. For a clear the PagerDuty resolve may already have posted, because the resolve comes before the clear's state
/// write. The drain gives the row back to the queue for a later attempt rather than closing it; a repeated clear resolves
/// again and clears the state again, and both steps can be repeated safely.
/// </summary>
public sealed class ExternalAlertStateLockTimeoutException : Exception
{
    public ExternalAlertStateLockTimeoutException(string message, Exception inner) : base(message, inner)
    {
    }
}

/// <summary>
/// What a fire recorded: whether the subject was already firing and at what severity, and the identity the episode is sent
/// under. A fire that finds the subject firing keeps the identity the episode began with; a fire on a cleared or new subject
/// takes the one it brought.
/// </summary>
public sealed record ExternalAlertFireResult(
    bool WasFiring, string? PreviousSeverity, int ServerId, string ServerKey, string ServerName);

/// <summary>
/// The remembered firing state of one <c>(source, key, submitted server name)</c> subject, with the identity its fire was
/// sent under: the server id (0 for a name that was not registered), the server key and the display name. A clear closes
/// the incident under these, whatever the server registry says by then.
/// </summary>
public sealed record ExternalAlertFiringState(
    bool Firing, string? FiredSeverity, DateTime? LastFiredAtUtc, int ServerId, string ServerKey, string ServerName,
    string? FiredSeverities = null);

/// <summary>
/// The service's side of the external alert queue (#5654): claims what a source submitted through
/// <c>config.submit_external_alert</c>, records what became of each row, expires what nobody took in time, reaps what a
/// dead run left claimed, and remembers which keys are firing so a clear pairs with its fire.
///
/// <para><b>Delivery is at least once.</b> A row moves <c>pending</c> to <c>claimed</c> to one terminal status, and goes back
/// to <c>pending</c> through two calls only: <see cref="ReleaseForRetryAsync"/>, which the drain calls for a state write that
/// timed out on a lock (nothing was sent), a fire that every channel failed to send, and a PagerDuty resolve that failed
/// (a repeat leaves the incident as it is), and <see cref="ReleaseUnattemptedAsync"/>, for rows a stopping pass never
/// attempted. A send that timed out after its request left can therefore post twice: PagerDuty drops a repeat trigger that
/// carries the same dedup key, but Slack, Teams, a generic webhook and email can show a duplicate. A run that dies between a
/// send and the mark leaves a claimed row; the reap, which every pass runs, marks it <c>delivered</c> or <c>interrupted</c>
/// once its claim is old enough, and it is not claimed again.</para>
///
/// <para><b>Order.</b> A claim takes at most one row per <c>(source, key)</c>, and only when no earlier row of that key is
/// still pending or claimed, so a fire and the clear that follows it reach the drain in order. The claim runs under a
/// transaction-scoped advisory lock, so two services that share the store cannot both pass that check for the same key
/// in the same instant. The picked rows are also locked <c>FOR UPDATE SKIP LOCKED</c>, so a claimer never waits on a row
/// another claimer holds.</para>
///
/// <para>Every command is parameterized, carries the store's command deadline, and takes a cancellation token. The tables
/// are schema-qualified, so the caller's <c>search_path</c> decides nothing.</para>
/// </summary>
public sealed class ExternalAlertStore
{
    /// <summary>The most rows one claim takes, so a flood cannot hold a pass open.</summary>
    public const int DefaultClaimBatch = 50;

    /// <summary>
    /// Serializes claimers across services. The key is the same constant on every service and no other code takes it.
    /// </summary>
    public const long ClaimAdvisoryLockKey = 5_654_000_001L;

    /// <summary>Takes the claim lock for the rest of the transaction. $1 <see cref="ClaimAdvisoryLockKey"/>.</summary>
    public const string ClaimLockSql = "SELECT pg_advisory_xact_lock($1)";

    /// <summary>
    /// The claim. $1 service instance, $2 batch size. Takes the oldest pending rows that are unexpired and due, at most one
    /// per source and key, and none whose key still has an earlier row pending (unexpired) or claimed.
    /// </summary>
    public const string ClaimSql = @"
WITH picked AS (
    SELECT p.pending_id
    FROM config.external_alert_pending p
    WHERE p.status = 'pending'
      AND p.expires_at > now()
      AND (p.next_attempt_at IS NULL OR p.next_attempt_at <= now())
      AND NOT EXISTS (
          SELECT 1
          FROM config.external_alert_pending e
          WHERE e.source_id = p.source_id
            AND e.alert_key = p.alert_key
            AND e.pending_id < p.pending_id
            AND (e.status = 'claimed' OR (e.status = 'pending' AND e.expires_at > now())))
    ORDER BY p.pending_id
    LIMIT $2
    FOR UPDATE OF p SKIP LOCKED
),
claimed AS (
    UPDATE config.external_alert_pending t
    SET status = 'claimed',
        claimed_at = now(),
        service_instance = $1
    FROM picked
    WHERE t.pending_id = picked.pending_id
      AND t.status = 'pending'
    RETURNING t.pending_id, t.source_id, t.kind, t.alert_key, t.severity, t.title, t.detail, t.server_name, t.value,
              t.submitted_at, t.expires_at, t.attempts
)
SELECT c.pending_id, c.source_id, s.source_name, c.kind, c.alert_key, c.severity, c.title, c.detail, c.server_name,
       c.value, c.submitted_at, c.expires_at, c.attempts
FROM claimed c
JOIN config.external_alert_source s ON s.source_id = c.source_id
ORDER BY c.pending_id";

    /// <summary>
    /// Marks a claimed row finished. $1 pending_id, $2 terminal status, $3 reason. The <c>claimed</c> guard means a row the
    /// reaper already took (or one that was never claimed) is left alone.
    /// </summary>
    public const string MarkSql = @"
UPDATE config.external_alert_pending
SET status = $2,
    completed_at = now(),
    reason = $3
WHERE pending_id = $1
  AND status = 'claimed'";

    /// <summary>
    /// Returns a claimed row to the queue after handling that sent nothing. $1 pending_id,
    /// $2 next attempt time, $3 reason. Counts the attempt.
    /// </summary>
    public const string ReleaseForRetrySql = @"
UPDATE config.external_alert_pending
SET status = 'pending',
    claimed_at = NULL,
    service_instance = NULL,
    attempts = attempts + 1,
    next_attempt_at = $2,
    reason = $3
WHERE pending_id = $1
  AND status = 'claimed'";

    /// <summary>Marks every pending row past its deadline expired. No parameters.</summary>
    public const string ExpireSql = @"
UPDATE config.external_alert_pending
SET status = 'expired',
    completed_at = now(),
    reason = 'not delivered before its deadline'
WHERE status = 'pending'
  AND expires_at <= now()";

    /// <summary>
    /// Marks every row a dead run left claimed interrupted, and never sends it. $1 the instant a claim must predate (UTC now
    /// minus the age, bound as <c>timestamptz</c> by <c>UtcAgo</c>, #5654: the store clock-discipline census
    /// takes no bare clock beside a <c>claimed_at</c> name), so a live service's fresh claim is left alone.
    /// </summary>
    public const string ReapSql = @"
UPDATE config.external_alert_pending
SET status = 'interrupted',
    completed_at = now(),
    reason = 'the run that claimed it ended before reporting a result; not sent again'
WHERE status = 'claimed'
  AND claimed_at < $1";

    /// <summary>
    /// The claimed rows old enough to reap (#5654). $1 the instant a claim must predate, as <see cref="ReapSql"/>. The reap
    /// with history builds each row's history pattern in code from this list, so the pattern has one definition.
    /// </summary>
    public const string ReapCandidatesSql = @"
SELECT pending_id
FROM config.external_alert_pending
WHERE status = 'claimed'
  AND claimed_at < $1";

    /// <summary>
    /// The reap with the delivered-if-logged refinement (#5654). $1 the instant a claim must predate, as <see cref="ReapSql"/>;
    /// $2 the candidate pending ids and $3 their history <c>LIKE</c> patterns, one per id and built by the caller from
    /// <c>AlertContextSerializer.BuildPendingIdLikePattern</c> (the one definition of what a drain history row carries).
    /// A claimed row that old becomes <c>delivered</c> when a history row written after the claim matches its pattern and is
    /// not a send that failed on every channel (the drain writes that row after the send, so its existence is the proof the
    /// send happened), else <c>interrupted</c>. Either way it is never claimed again and never sent again. The status and
    /// age guards are repeated on the update, so a row that was released or restamped since the candidates were read is
    /// left alone. The history scan is bounded below by the claim time, so it reads only rows written since.
    /// </summary>
    public const string ReapWithHistorySql = @"
WITH cand AS (
    SELECT p.pending_id, p.claimed_at, c.pat
    FROM config.external_alert_pending p
    JOIN unnest($2::bigint[], $3::text[]) AS c(id, pat) ON c.id = p.pending_id
    WHERE p.status = 'claimed'
      AND p.claimed_at < $1
),
logged AS (
    SELECT cand.pending_id,
           EXISTS (
               SELECT 1
               FROM config.config_alert_log l
               WHERE l.alert_time >= (cand.claimed_at AT TIME ZONE 'UTC') - interval '1 minute'
                 AND (l.alert_sent OR l.send_error IS NULL)
                 AND l.context_json LIKE cand.pat) AS sent
    FROM cand
),
reaped AS (
    UPDATE config.external_alert_pending p
    SET status = CASE WHEN logged.sent THEN 'delivered' ELSE 'interrupted' END,
        completed_at = now(),
        reason = CASE WHEN logged.sent
                      THEN 'the run that claimed it ended before marking it; its history row shows it was sent'
                      ELSE 'the run that claimed it ended before reporting a result; not sent again' END
    FROM logged
    WHERE p.pending_id = logged.pending_id
      AND p.status = 'claimed'
      AND p.claimed_at < $1
    RETURNING p.status
)
SELECT status, count(*)::int FROM reaped GROUP BY status";

    /// <summary>
    /// Keeps this run's claims young while a pass works through its batch (#5654). $1 the pending ids, $2 the service
    /// instance. Returns the ids that are still claimed by this instance, so a row the reap took meanwhile is not sent.
    /// </summary>
    public const string RestampClaimsSql = @"
UPDATE config.external_alert_pending
SET claimed_at = now()
WHERE pending_id = ANY($1)
  AND status = 'claimed'
  AND service_instance = $2
RETURNING pending_id";

    /// <summary>
    /// Gives claimed rows that were never attempted back to the queue, with no attempt counted (#5654). $1 the pending ids,
    /// $2 the service instance.
    /// </summary>
    public const string ReleaseUnattemptedSql = @"
UPDATE config.external_alert_pending
SET status = 'pending',
    claimed_at = NULL,
    service_instance = NULL
WHERE pending_id = ANY($1)
  AND status = 'claimed'
  AND service_instance = $2";

    /// <summary>How long a state write waits for a row lock before it gives up (<c>SET LOCAL lock_timeout</c>).</summary>
    public const int StateWriteLockTimeoutMilliseconds = 2_000;

    /// <summary>Runs first in a state write's transaction. The value is a constant, so nothing is concatenated from input.</summary>
    public const string StateWriteLockTimeoutSql = "SET LOCAL lock_timeout = '2000ms'";

    /// <summary>Reads one subject's firing state. $1 source_id, $2 alert_key, $3 server_label (the normalized submitted name).</summary>
    public const string GetStateSql = @"
SELECT firing, fired_severity, last_fired_at, server_id, server_key, server_name, fired_severities
FROM config.external_alert_state
WHERE source_id = $1 AND alert_key = $2 AND server_label = $3";

    /// <summary>
    /// Records a fire and returns what the subject was before it (#5654). $1 source_id, $2 alert_key, $3 server_label,
    /// $4 server_id, $5 server_key, $6 server_name. The returned row is the state read before the write, so a repeat fire
    /// reports <c>true</c>. The identity the fire is sent under is kept with the state, and a repeat fire of a subject that is
    /// still firing keeps the identity the episode began with (the columns it returns are the kept ones), so a server
    /// registered or renamed mid-episode re-triggers the same incident.
    /// <para><c>fired_severity</c> is the severity the operator was last TOLD about, as a built-in alert's delivered band is: a
    /// fire that opens the subject leaves it null until <see cref="ConfirmFireSql"/> records a send that went out, and a
    /// repeat fire keeps what was confirmed. A firing subject whose severity is still null is an incident that opened and
    /// has not been delivered.</para>
    /// </summary>
    public const string RecordFireSql = @"
WITH prev AS (
    SELECT firing, fired_severity
    FROM config.external_alert_state
    WHERE source_id = $1 AND alert_key = $2 AND server_label = $3
),
up AS (
    INSERT INTO config.external_alert_state
        (source_id, alert_key, server_label, server_id, server_key, server_name, firing, fired_severity, last_fired_at)
    VALUES ($1, $2, $3, $4, $5, $6, true, NULL, now())
    ON CONFLICT (source_id, alert_key, server_label) DO UPDATE SET
        server_id = CASE WHEN config.external_alert_state.firing THEN config.external_alert_state.server_id ELSE EXCLUDED.server_id END,
        server_key = CASE WHEN config.external_alert_state.firing THEN config.external_alert_state.server_key ELSE EXCLUDED.server_key END,
        server_name = CASE WHEN config.external_alert_state.firing THEN config.external_alert_state.server_name ELSE EXCLUDED.server_name END,
        fired_severity = CASE WHEN config.external_alert_state.firing THEN config.external_alert_state.fired_severity ELSE NULL END,
        fired_severities = CASE WHEN config.external_alert_state.firing THEN config.external_alert_state.fired_severities ELSE NULL END,
        firing = true,
        last_fired_at = EXCLUDED.last_fired_at
    RETURNING server_id, server_key, server_name
)
SELECT COALESCE((SELECT firing FROM prev), false), (SELECT fired_severity FROM prev), up.server_id, up.server_key, up.server_name
FROM up";

    /// <summary>
    /// Records that a fire's send went out (#5654), so the severity is the one the operator was told. $1 source_id,
    /// $2 alert_key, $3 server_label, $4 severity. Touches only a subject that is firing.
    /// <para><c>fired_severities</c> (#6928) is every severity sent during the open episode, comma separated, each once, so the
    /// clear can close what a Warning-only route opened after the alert climbed to Critical. <c>fired_severity</c> stays the newest.</para>
    /// </summary>
    public const string ConfirmFireSql = @"
UPDATE config.external_alert_state
SET fired_severity = $4,
    fired_severities = CASE
        WHEN fired_severities IS NULL THEN $4::text
        WHEN position(',' || $4::text || ',' IN ',' || fired_severities || ',') > 0 THEN fired_severities
        ELSE fired_severities || ',' || $4::text END
WHERE source_id = $1 AND alert_key = $2 AND server_label = $3
  AND firing";

    /// <summary>
    /// Ends an incident whose opening fire gave up without a send going out (#5654), the way a built-in alert leaves a fire
    /// no channel took undelivered. $1 source_id, $2 alert_key, $3 server_label. Touches only a firing subject whose
    /// severity was never confirmed, so a repeat fire that gives up leaves the incident an earlier fire opened firing.
    /// Returns a row when it ended one.
    /// </summary>
    public const string AbandonFireSql = @"
UPDATE config.external_alert_state
SET firing = false
WHERE source_id = $1 AND alert_key = $2 AND server_label = $3
  AND firing
  AND fired_severity IS NULL
RETURNING 1";

    /// <summary>
    /// Records a clear. $1 source_id, $2 alert_key, $3 server_label. Returns the severity the subject fired at when it was
    /// firing (empty when its fire was never confirmed sent), and no row when it was not, so a clear for a key that never
    /// fired is told apart from a real one.
    /// </summary>
    public const string RecordClearSql = @"
UPDATE config.external_alert_state
SET firing = false
WHERE source_id = $1 AND alert_key = $2 AND server_label = $3
  AND firing
RETURNING COALESCE(fired_severity, '')";

    /// <summary>
    /// The rows the drain gave up on inside a window (#5654), by source and terminal status: <c>expired</c> (past its deadline
    /// still pending), <c>interrupted</c> (a stopped run left it claimed) and <c>failed</c> (every send failed, no retry
    /// left). $1 the start of the window, UTC now minus the window, bound as <c>timestamptz</c> and compared with <c>completed_at</c>. Read by the self-monitor alert
    /// "External Alerts Undelivered"; <c>delivered</c>, <c>muted</c>, <c>no_channel</c> and <c>ignored</c> are outcomes a
    /// source or an operator chose, so they are not counted.
    /// </summary>
    public const string CountUndeliveredSql = @"
SELECT s.source_name, p.status, count(*)::int
FROM config.external_alert_pending p
JOIN config.external_alert_source s ON s.source_id = p.source_id
WHERE p.status IN ('expired', 'interrupted', 'failed')
  AND p.completed_at >= $1
GROUP BY s.source_name, p.status
ORDER BY s.source_name, p.status";

    private const int MaxReasonLength = 500;

    /// <summary>
    /// UTC now minus <paramref name="age"/>, <c>Kind=Utc</c> so Npgsql binds it as <c>timestamptz</c> (#5654). The queue's
    /// time columns are <c>timestamptz</c>, so a Utc bound is the right frame; the store clock-discipline census takes the
    /// bound from the service clock rather than a bare <c>now()</c> next to a column name that other tables hold as
    /// naive UTC. The DB stamps <c>claimed_at</c> and <c>completed_at</c> itself, so the two clocks differ by their skew,
    /// seconds against windows of a minute or more.
    /// </summary>
    private static DateTime UtcAgo(TimeSpan age) => DateTime.SpecifyKind(DateTime.UtcNow - age, DateTimeKind.Utc);

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;

    /// <summary>
    /// <paramref name="dataSource"/> must connect as the store owner (the service's owner pool): the queue and the state table
    /// take writes from the owner and from the submit function only.
    /// <paramref name="commandTimeoutSeconds"/> is applied to every command.
    /// </summary>
    public ExternalAlertStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
    }

    /// <summary>
    /// Claims up to <paramref name="batch"/> rows for <paramref name="serviceInstance"/>, oldest first, each one at most once
    /// across every claimer. Returns them in submission order. An empty list means nothing is due.
    /// </summary>
    public async Task<IReadOnlyList<ExternalAlertPendingRow>> ClaimAsync(
        string serviceInstance, int batch = DefaultClaimBatch, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(serviceInstance);
        ArgumentOutOfRangeException.ThrowIfLessThan(batch, 1);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var lockCommand = new NpgsqlCommand(ClaimLockSql, connection, transaction))
        {
            lockCommand.CommandTimeout = _commandTimeoutSeconds;
            lockCommand.Parameters.Add(new NpgsqlParameter<long> { TypedValue = ClaimAdvisoryLockKey });
            await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var rows = new List<ExternalAlertPendingRow>();
        await using (var command = new NpgsqlCommand(ClaimSql, connection, transaction))
        {
            command.CommandTimeout = _commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serviceInstance });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = batch });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(new ExternalAlertPendingRow(
                    reader.GetInt64(0),
                    reader.GetInt32(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    reader.GetString(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.IsDBNull(8) ? null : reader.GetString(8),
                    reader.IsDBNull(9) ? null : reader.GetDouble(9),
                    reader.GetFieldValue<DateTime>(10),
                    reader.GetFieldValue<DateTime>(11),
                    reader.GetInt32(12)));
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return rows;
    }

    /// <summary>Marks a claimed row delivered. False when the row is not claimed any more (the reaper took it).</summary>
    public Task<bool> MarkDeliveredAsync(long pendingId, CancellationToken cancellationToken = default) =>
        MarkAsync(pendingId, ExternalAlertOutcome.Delivered, null, cancellationToken);

    /// <summary>Marks a claimed row muted, with the rule that matched as the reason.</summary>
    public Task<bool> MarkMutedAsync(long pendingId, string? reason, CancellationToken cancellationToken = default) =>
        MarkAsync(pendingId, ExternalAlertOutcome.Muted, reason, cancellationToken);

    /// <summary>Marks a claimed row interrupted, for a drain that stops with the row in hand. It is never sent again.</summary>
    public Task<bool> MarkInterruptedAsync(long pendingId, string? reason, CancellationToken cancellationToken = default) =>
        MarkAsync(pendingId, ExternalAlertOutcome.Interrupted, reason, cancellationToken);

    /// <summary>Marks a claimed row with any terminal outcome. False when the row is not claimed any more.</summary>
    public async Task<bool> MarkAsync(
        long pendingId, ExternalAlertOutcome outcome, string? reason, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(MarkSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = pendingId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = StatusFor(outcome) });
        command.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = Truncate(reason), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    /// <summary>
    /// Puts a claimed row back in the queue to be tried again at <paramref name="nextAttemptAtUtc"/>, and counts the attempt.
    /// Delivery is at least once: a send that timed out after the request left can post twice. PagerDuty drops a repeat trigger
    /// with the same dedup key, but Slack, Teams, a webhook and email can show a duplicate. Expiry still ends the row if the
    /// retries run past its deadline.
    /// </summary>
    public async Task<bool> ReleaseForRetryAsync(
        long pendingId, DateTime nextAttemptAtUtc, string? reason, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ReleaseForRetrySql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = pendingId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(nextAttemptAtUtc, DateTimeKind.Utc) });
        command.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = Truncate(reason), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    /// <summary>
    /// Sets <c>claimed_at</c> to now for the given rows this instance still holds, and returns the ids that are still claimed
    /// by it. The drain calls it before each row's send, with that row and the rest of its batch, so a long pass keeps every
    /// claim younger than the reap age. A row missing from the answer was taken by the reap and must not be sent.
    /// </summary>
    public async Task<IReadOnlySet<long>> RestampClaimsAsync(
        string serviceInstance, IReadOnlyCollection<long> pendingIds, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(serviceInstance);
        ArgumentNullException.ThrowIfNull(pendingIds);
        var live = new HashSet<long>();
        if (pendingIds.Count == 0)
        {
            return live;
        }

        await using var command = _dataSource.CreateCommand(RestampClaimsSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<long[]> { TypedValue = pendingIds.ToArray() });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serviceInstance });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            live.Add(reader.GetInt64(0));
        }

        return live;
    }

    /// <summary>
    /// Gives the claimed rows this instance never attempted back to <c>pending</c>, with no attempt counted and the next
    /// attempt time untouched. For a pass that stops (the service is stopping, or the store failed) with rows still in its
    /// batch. Returns how many went back. A row the reap already took, or another instance holds, is left alone.
    /// </summary>
    public async Task<int> ReleaseUnattemptedAsync(
        string serviceInstance, IReadOnlyCollection<long> pendingIds, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(serviceInstance);
        ArgumentNullException.ThrowIfNull(pendingIds);
        if (pendingIds.Count == 0)
        {
            return 0;
        }

        await using var command = _dataSource.CreateCommand(ReleaseUnattemptedSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<long[]> { TypedValue = pendingIds.ToArray() });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serviceInstance });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// How many rows the drain gave up on in the last <paramref name="window"/>, per source and status
    /// (<see cref="CountUndeliveredSql"/>). Empty when there are none. Read only.
    /// </summary>
    public async Task<IReadOnlyList<ExternalAlertUndeliveredCount>> CountUndeliveredAsync(
        TimeSpan window, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        await using var command = _dataSource.CreateCommand(CountUndeliveredSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = UtcAgo(window) });
        var counts = new List<ExternalAlertUndeliveredCount>();
        /* the window start is UTC now minus the window, bound as timestamptz (the columns are timestamptz) */
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            counts.Add(new ExternalAlertUndeliveredCount(reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
        }

        return counts;
    }

    /// <summary>Marks every pending row past its deadline expired. Returns how many.</summary>
    public async Task<int> ExpireAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ExpireSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Marks every row that has been claimed for at least <paramref name="claimedFor"/> interrupted. Returns how many. Those
    /// rows are not sent again. The drain's own reap is <see cref="ReapWithHistoryAsync"/>.
    /// </summary>
    public async Task<int> ReapAsync(TimeSpan claimedFor, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(claimedFor, TimeSpan.Zero);
        await using var command = _dataSource.CreateCommand(ReapSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = UtcAgo(claimedFor) });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// <see cref="ReapAsync"/> with the delivered-if-logged refinement (#5654): a row a dead run left claimed is
    /// <c>delivered</c> if the alert history holds a row carrying its pending id, else <c>interrupted</c>. Returns how
    /// many rows went each way. Never sends anything. <paramref name="pendingIdLikePattern"/> builds the history
    /// <c>LIKE</c> pattern for one pending id; production passes <c>AlertContextSerializer.BuildPendingIdLikePattern</c>.
    /// </summary>
    public async Task<(int Delivered, int Interrupted)> ReapWithHistoryAsync(
        TimeSpan claimedFor, Func<long, string> pendingIdLikePattern, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(claimedFor, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(pendingIdLikePattern);
        var cutoff = UtcAgo(claimedFor);

        var ids = new List<long>();
        await using (var candidates = _dataSource.CreateCommand(ReapCandidatesSql))
        {
            candidates.CommandTimeout = _commandTimeoutSeconds;
            candidates.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });
            await using var candidateReader = await candidates.ExecuteReaderAsync(cancellationToken);
            while (await candidateReader.ReadAsync(cancellationToken))
            {
                ids.Add(candidateReader.GetInt64(0));
            }
        }

        if (ids.Count == 0)
        {
            return (0, 0);
        }

        await using var command = _dataSource.CreateCommand(ReapWithHistorySql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });
        command.Parameters.Add(new NpgsqlParameter<long[]> { TypedValue = ids.ToArray() });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = ids.Select(pendingIdLikePattern).ToArray() });

        var delivered = 0;
        var interrupted = 0;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var count = reader.GetInt32(1);
            if (string.Equals(reader.GetString(0), "delivered", StringComparison.Ordinal))
            {
                delivered += count;
            }
            else
            {
                interrupted += count;
            }
        }

        return (delivered, interrupted);
    }

    /// <summary>Reads one subject's firing state. Null when the subject has never fired.</summary>
    public async Task<ExternalAlertFiringState?> GetStateAsync(
        int sourceId, string alertKey, string serverLabel, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(GetStateSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        BindSubject(command, sourceId, alertKey, serverLabel);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ExternalAlertFiringState(
            reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTime>(2),
            reader.GetInt32(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6));
    }

    /// <summary>
    /// Records that the subject is firing and returns what it was before: whether it was already firing, and the severity the
    /// operator was last told about (null when it never fired, or opened and has had no send confirmed). The subject is the
    /// normalized submitted name <paramref name="serverLabel"/> (empty when none was given); <paramref name="serverId"/>,
    /// <paramref name="serverKey"/> and <paramref name="serverName"/> are the identity this fire is sent under, kept so the
    /// clear closes the same incident. Written before the send, so a clear that follows pairs with the fire even when a stopped
    /// run never marked it; <see cref="ConfirmFireAsync"/> follows a send that went out and <see cref="AbandonFireAsync"/> a
    /// fire that gave up.
    /// </summary>
    public async Task<ExternalAlertFireResult> RecordFireAsync(
        int sourceId, string alertKey, string serverLabel, int serverId, string serverKey, string serverName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serverKey);
        ArgumentNullException.ThrowIfNull(serverName);
        return await InStateWriteAsync(
            async (connection, transaction) =>
            {
                await using var command = new NpgsqlCommand(RecordFireSql, connection, transaction);
                command.CommandTimeout = _commandTimeoutSeconds;
                BindSubject(command, sourceId, alertKey, serverLabel);
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serverKey });
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serverName });
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                return new ExternalAlertFireResult(
                    reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetInt32(2), reader.GetString(3), reader.GetString(4));
            },
            cancellationToken);
    }

    /// <summary>Records that a fire's send went out (or that nothing is owed it), at <paramref name="severity"/>.</summary>
    public async Task ConfirmFireAsync(
        int sourceId, string alertKey, string serverLabel, string severity, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(severity);
        await InStateWriteAsync(
            async (connection, transaction) =>
            {
                await using var command = new NpgsqlCommand(ConfirmFireSql, connection, transaction);
                command.CommandTimeout = _commandTimeoutSeconds;
                BindSubject(command, sourceId, alertKey, serverLabel);
                command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = severity });
                return await command.ExecuteNonQueryAsync(cancellationToken);
            },
            cancellationToken);
    }

    /// <summary>
    /// Ends an incident whose opening fire gave up with nothing sent, so a later clear closes nothing. Returns true when it
    /// ended one, and false when the subject is not firing or an earlier fire's send was confirmed (the incident stays open).
    /// </summary>
    public async Task<bool> AbandonFireAsync(
        int sourceId, string alertKey, string serverLabel, CancellationToken cancellationToken = default)
    {
        return await InStateWriteAsync(
            async (connection, transaction) =>
            {
                await using var command = new NpgsqlCommand(AbandonFireSql, connection, transaction);
                command.CommandTimeout = _commandTimeoutSeconds;
                BindSubject(command, sourceId, alertKey, serverLabel);
                return await command.ExecuteScalarAsync(cancellationToken) is not null;
            },
            cancellationToken);
    }

    /// <summary>
    /// Records a clear. Returns the severity the subject fired at when it was firing (empty when no send of its fire was ever
    /// confirmed), and null when it was not (a clear with no fire to pair with).
    /// </summary>
    public async Task<string?> RecordClearAsync(
        int sourceId, string alertKey, string serverLabel, CancellationToken cancellationToken = default)
    {
        return await InStateWriteAsync(
            async (connection, transaction) =>
            {
                await using var command = new NpgsqlCommand(RecordClearSql, connection, transaction);
                command.CommandTimeout = _commandTimeoutSeconds;
                BindSubject(command, sourceId, alertKey, serverLabel);
                var severity = await command.ExecuteScalarAsync(cancellationToken);
                return severity is string text ? text : null;
            },
            cancellationToken);
    }

    /// <summary>
    /// Runs one state write in a transaction that waits at most <see cref="StateWriteLockTimeoutMilliseconds"/> for a lock.
    /// The state table's foreign key to the source registry takes a share lock on the source's row, and a session that holds
    /// that row (a submit left open in a transaction) would otherwise stall every write until the command deadline. A wait
    /// that runs out surfaces as <see cref="ExternalAlertStateLockTimeoutException"/>: that write was not made.
    /// </summary>
    private async Task<T> InStateWriteAsync<T>(
        Func<NpgsqlConnection, NpgsqlTransaction, Task<T>> write, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await using (var timeout = new NpgsqlCommand(StateWriteLockTimeoutSql, connection, transaction))
            {
                timeout.CommandTimeout = _commandTimeoutSeconds;
                await timeout.ExecuteNonQueryAsync(cancellationToken);
            }

            var result = await write(connection, transaction);
            await transaction.CommitAsync(cancellationToken);
            return result;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            throw new ExternalAlertStateLockTimeoutException(
                "A state write waited more than " + StateWriteLockTimeoutMilliseconds + " ms for a lock and gave up.", ex);
        }
    }

    private static void BindSubject(NpgsqlCommand command, int sourceId, string alertKey, string serverLabel)
    {
        ArgumentException.ThrowIfNullOrEmpty(alertKey);
        ArgumentNullException.ThrowIfNull(serverLabel);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sourceId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = alertKey });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serverLabel });
    }

    private static string StatusFor(ExternalAlertOutcome outcome) => outcome switch
    {
        ExternalAlertOutcome.Delivered => "delivered",
        ExternalAlertOutcome.Muted => "muted",
        ExternalAlertOutcome.NoChannel => "no_channel",
        ExternalAlertOutcome.Failed => "failed",
        ExternalAlertOutcome.Ignored => "ignored",
        ExternalAlertOutcome.Interrupted => "interrupted",
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Not a terminal outcome."),
    };

    /// <summary>The reason column is free text a log line fills; keep a runaway message out of the row.</summary>
    private static string? Truncate(string? reason)
    {
        if (reason is null || reason.Length <= MaxReasonLength)
        {
            return reason;
        }

        return reason.Substring(0, MaxReasonLength);
    }
}
