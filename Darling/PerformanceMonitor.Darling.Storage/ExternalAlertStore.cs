/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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

/// <summary>The remembered firing state of one <c>(source, key, server)</c> subject.</summary>
public sealed record ExternalAlertFiringState(bool Firing, string? FiredSeverity, DateTime? LastFiredAtUtc);

/// <summary>
/// The service's side of the external alert queue (#5654): claims what a source submitted through
/// <c>config.submit_external_alert</c>, records what became of each row, expires what nobody took in time, reaps what a
/// dead run left claimed, and remembers which keys are firing so a clear pairs with its fire.
///
/// <para><b>Delivery is at most once.</b> A row moves <c>pending</c> to <c>claimed</c> to one terminal status, and nothing
/// moves a claimed row back except <see cref="ReleaseForRetryAsync"/>, which the drain calls only after a send that failed
/// before anything left the process. A run that dies between the send and the mark leaves a claimed row;
/// <see cref="ReapAsync"/> marks it <c>interrupted</c> and it is never claimed again, because a second send could page
/// someone twice for one fire.</para>
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
    /// Returns a claimed row to the queue after a send that failed before anything left the process. $1 pending_id,
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
    /// Marks every row a dead run left claimed interrupted, and never sends it. $1 the age a claim must have reached, as an
    /// interval, so a live service's fresh claim is left alone.
    /// </summary>
    public const string ReapSql = @"
UPDATE config.external_alert_pending
SET status = 'interrupted',
    completed_at = now(),
    reason = 'the run that claimed it ended before reporting a result; not sent again'
WHERE status = 'claimed'
  AND claimed_at < now() - $1";

    /// <summary>Reads one subject's firing state. $1 source_id, $2 alert_key, $3 server_id.</summary>
    public const string GetStateSql = @"
SELECT firing, fired_severity, last_fired_at
FROM config.external_alert_state
WHERE source_id = $1 AND alert_key = $2 AND server_id = $3";

    /// <summary>
    /// Records a fire and returns what the subject was before it. $1 source_id, $2 alert_key, $3 server_id, $4 severity.
    /// The returned row is the state read before the write, so a repeat fire reports <c>true</c>.
    /// </summary>
    public const string RecordFireSql = @"
WITH prev AS (
    SELECT firing, fired_severity
    FROM config.external_alert_state
    WHERE source_id = $1 AND alert_key = $2 AND server_id = $3
),
up AS (
    INSERT INTO config.external_alert_state (source_id, alert_key, server_id, firing, fired_severity, last_fired_at)
    VALUES ($1, $2, $3, true, $4, now())
    ON CONFLICT (source_id, alert_key, server_id) DO UPDATE SET
        firing = true,
        fired_severity = EXCLUDED.fired_severity,
        last_fired_at = EXCLUDED.last_fired_at
    RETURNING 1
)
SELECT COALESCE((SELECT firing FROM prev), false), (SELECT fired_severity FROM prev)
FROM up";

    /// <summary>
    /// Records a clear. $1 source_id, $2 alert_key, $3 server_id. Returns the severity the subject fired at when it was
    /// firing, and no row when it was not, so a clear for a key that never fired is told apart from a real one.
    /// </summary>
    public const string RecordClearSql = @"
UPDATE config.external_alert_state
SET firing = false
WHERE source_id = $1 AND alert_key = $2 AND server_id = $3
  AND firing
RETURNING fired_severity";

    private const int MaxReasonLength = 500;

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
    /// Puts a claimed row back in the queue to be tried again at <paramref name="nextAttemptAtUtc"/>. Only for a send that
    /// failed before anything left the process; expiry still ends the row if the retries run past its deadline.
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

    /// <summary>Marks every pending row past its deadline expired. Returns how many.</summary>
    public async Task<int> ExpireAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ExpireSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Marks every row that has been claimed for at least <paramref name="claimedFor"/> interrupted. The drain calls it when
    /// the service starts, before its first claim, with the age after which a claim cannot belong to a live run, and again
    /// on later passes. Returns how many. Those rows are not sent again.
    /// </summary>
    public async Task<int> ReapAsync(TimeSpan claimedFor, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(claimedFor, TimeSpan.Zero);
        await using var command = _dataSource.CreateCommand(ReapSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = claimedFor });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Reads one subject's firing state. Null when the subject has never fired.</summary>
    public async Task<ExternalAlertFiringState?> GetStateAsync(
        int sourceId, string alertKey, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(GetStateSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        BindSubject(command, sourceId, alertKey, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ExternalAlertFiringState(
            reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTime>(2));
    }

    /// <summary>
    /// Records that the subject is firing at <paramref name="severity"/> and returns what it was before: whether it was
    /// already firing, and at what severity. A subject that never fired reports <c>(false, null)</c>.
    /// </summary>
    public async Task<(bool WasFiring, string? PreviousSeverity)> RecordFireAsync(
        int sourceId, string alertKey, int serverId, string severity, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(severity);
        await using var command = _dataSource.CreateCommand(RecordFireSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        BindSubject(command, sourceId, alertKey, serverId);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = severity });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    /// <summary>
    /// Records a clear. Returns the severity the subject fired at when it was firing, and null when it was not (a clear with
    /// no fire to pair with).
    /// </summary>
    public async Task<string?> RecordClearAsync(
        int sourceId, string alertKey, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(RecordClearSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        BindSubject(command, sourceId, alertKey, serverId);
        var severity = await command.ExecuteScalarAsync(cancellationToken);
        return severity is string text ? text : null;
    }

    private static void BindSubject(NpgsqlCommand command, int sourceId, string alertKey, int serverId)
    {
        ArgumentException.ThrowIfNullOrEmpty(alertKey);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sourceId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = alertKey });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
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
