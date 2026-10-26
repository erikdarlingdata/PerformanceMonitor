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
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One agent change in the list (#5917): the journal's <c>action_called</c> row, the result row that names it
/// (<c>action_done</c> or <c>action_failed</c>, none = <see cref="AgentChangeResult.Unknown"/>), and an undo row when a person took it back.</summary>
public sealed record AgentChangeRow(
    long CalledId,
    DateTime CalledAtUtc,
    Guid? RequestId,
    int? ServerId,
    string Actor,
    string? Reason,
    string DetailJson,
    AgentChangeResult Result,
    string? ResultJson,
    DateTime? UndoneAtUtc,
    string? UndoneBy,
    int? MutedAlerts,
    ForceReviewMark? ForceReview = null,
    string? ServerName = null)
{
    /// <summary>What the Server column and the MCP answer show (#6310): the registry's name, or the id with "(removed)" when the server
    /// is no longer registered; empty for a change with no server.</summary>
    public string ServerLabel => AgentChangeServerLabel.For(ServerId, ServerName);

    /// <summary>The kind word the tool journaled (<c>mute</c>, <c>custom_rule</c> or <c>threshold_override</c>; <see cref="AgentChangeKinds"/>).</summary>
    public string? Kind => ReadString("kind");

    /// <summary>The tool that made the change.</summary>
    public string? Tool => ReadString("tool");

    private string? ReadString(string key) =>
        JsonNode.Parse(DetailJson) is JsonObject o && o[key] is JsonValue v && v.TryGetValue(out string? s) ? s : null;

    /// <summary>True for a plan force or unforce (#5632, step 9) or a plan guide create or drop (#6569): the server's state decides its undo, not the journal's result.</summary>
    public bool IsPlanChange => Kind is AgentChangeKinds.PlanForce or AgentChangeKinds.PlanUnforce or AgentChangeKinds.PlanGuideCreate or AgentChangeKinds.PlanGuideDrop;

    /// <summary>True when an undo can be tried: nobody undid it, it is inside the window, and the kind has an undo. A store kind needs a done
    /// change. A plan force or unforce may be done, failed or unknown, because the undo reads the server first and decides from what it finds.</summary>
    public bool CanTryUndo(DateTime nowUtc) =>
        UndoneAtUtc is null
        && nowUtc - CalledAtUtc <= AgentChangeUndo.Window
        && (IsPlanChange || (Result == AgentChangeResult.Done && AgentChangeUndo.UndoableKinds.Contains(Kind ?? "")));
}

/// <summary>
/// Whether an agent reviewed a plan force (#5632, step 8): the checks Darling opened for it, how many were answered (the request reached
/// <c>reported</c>), and whether the schedule is still running. Computed in <see cref="AgentChangeUndo.ReadAsync"/> for each plan force that was done.
/// </summary>
public sealed record ForceReviewMark(int Opened, int Answered, bool Running)
{
    /// <summary>The list's words: "checks answered 2 of 5" once any is answered, "not reviewed yet" while the schedule runs with none answered,
    /// and "not reviewed" once it ended (or stopped) with none.</summary>
    public string Words => Answered > 0
        ? "checks answered " + Answered.ToString(System.Globalization.CultureInfo.InvariantCulture) + " of " + Opened.ToString(System.Globalization.CultureInfo.InvariantCulture)
        : Running ? "not reviewed yet" : "not reviewed";
}

/// <summary>How a journaled call ended (#5917). A call with neither an <c>action_done</c> nor an <c>action_failed</c> row is
/// <see cref="Unknown"/>: the write may or may not have happened, so the list says "result unknown" and offers no undo.</summary>
public enum AgentChangeResult { Done, Failed, Unknown }

/// <summary>A journaled refusal (#5917): the gate or a tool said no.</summary>
public sealed record AgentRefusalRow(long JournalId, DateTime AtUtc, Guid? RequestId, int? ServerId, string Actor, string? Code, string? Tool, string? ServerName = null)
{
    /// <summary>The registry's name for the server, or the id with "(removed)" (#6310); empty for a refusal with no server.</summary>
    public string ServerLabel => AgentChangeServerLabel.For(ServerId, ServerName);
}

/// <summary>The server text of an Agent changes row (#6310): the name <c>collect.servers</c> holds, the id with "(removed)" when the registry no longer has it.</summary>
public static class AgentChangeServerLabel
{
    public static string For(int? serverId, string? serverName) =>
        serverId is not int id ? "" : serverName is { Length: > 0 } name ? name : "Server " + id.ToString(System.Globalization.CultureInfo.InvariantCulture) + " (removed)";
}

/// <summary>The Agent changes list (#5917): the calls, the refusals and the switch.</summary>
public sealed record AgentChangesRead(bool Enabled, IReadOnlyList<AgentChangeRow> Changes, IReadOnlyList<AgentRefusalRow> Refusals);

/// <summary>One alert muted under an agent's mute rule since the change (#5917).</summary>
public sealed record MutedAlertRow(DateTime AlertTimeUtc, int ServerId, string ServerName, string MetricName, double CurrentValue, double ThresholdValue);

/// <summary>What an undo did (#5917). <see cref="Undone"/> is true when the before image was restored and
/// <c>action_undone</c> journaled; otherwise <see cref="Code"/> is the refusal: <c>changed_since</c>, <c>name_taken</c>, <c>not_found</c>,
/// <c>already_undone</c>, <c>not_undoable</c> or <c>expired</c>, and <see cref="Words"/> says it in plain words.</summary>
public sealed record AgentUndoResult(bool Undone, string? Code, string Words)
{
    internal static AgentUndoResult Ok(string kind) => new(true, null, "Undone: the " + kind + " is back to what it was before the agent's change.");
    internal static AgentUndoResult Refused(string code, string message) => new(false, code, message);
}

/// <summary>
/// One-click undo of an agent change, the list over the journal and the switch (#5917). The journal (#5912) holds each change's
/// before and after image; an undo compares the target's current agent-settable fields with the after image and, only when
/// they are equal, restores the before image in the same transaction as its <c>action_undone</c> row (actor
/// <c>person:&lt;login&gt;</c>). Any difference refuses "changed since": a person edited it after the agent, and their edit stays.
/// <para><b>Images by kind.</b> The tool journals a kind word and flat JSON images (column names; <c>null</c> = the object did not
/// exist). <c>mute</c>: <c>config_mute_rules</c>, target id = the rule id, fields <c>enabled, expires_at_utc, reason, server_name, server_id, metric_name,
/// external_source, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, client_app_pattern</c>. <c>custom_rule</c>:
/// <c>custom_alert_rules</c>, target id = the rule id (<c>Custom:&lt;id&gt;</c> or the number), fields <c>name, definition, description, enabled</c>.
/// <c>threshold_override</c>: <c>config_threshold_overrides</c>, target = server id, database and threshold key as the target id, field <c>value</c>.
/// Never <c>updated_at</c>, <c>updated_by</c>, <c>version</c> or the <c>config_version</c> beacon: they change with every write.</para>
/// <para>Created (before null) is undone by deleting; changed by restoring the before image; deleted (after null) by re-inserting it
/// (a rule with <c>OVERRIDING SYSTEM VALUE</c>, so it keeps its <c>Custom:&lt;id&gt;</c>; a rule with that name since is refused).
/// Undo works for <see cref="Window"/> from the call.</para>
/// </summary>
public sealed partial class AgentChangeUndo
{
    /// <summary>How long after the call an undo is offered (365 days).</summary>
    public static readonly TimeSpan Window = TimeSpan.FromDays(365);

    /// <summary>The kinds an undo exists for.</summary>
    public static readonly IReadOnlySet<string> UndoableKinds = new HashSet<string>(StringComparer.Ordinal) { AgentChangeKinds.Mute, AgentChangeKinds.CustomRule, AgentChangeKinds.ThresholdOverride, AgentChangeKinds.SignatureMerge };

    private sealed record Spec(string Table, (string Name, ImageColumnType Type)[] Fields);

    /* The column lists are the image records' own (AgentChangeImages.cs): the tools journal those records and the undo reads them. */
    private static readonly Spec MuteSpec = new(MuteRuleImage.Table, ImageColumns.Of<MuteRuleImage>());
    private static readonly Spec RuleSpec = new(CustomAlertRuleImage.Table, ImageColumns.Of<CustomAlertRuleImage>());
    private static readonly Spec OverrideSpec = new(ThresholdOverrideImage.Table, ImageColumns.Of<ThresholdOverrideImage>());
    private static readonly Spec MergeSpec = new(SignatureMergeImage.Table, ImageColumns.Of<SignatureMergeImage>());

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;
    private readonly Func<DateTime> _utcNow;

    /// <summary><paramref name="dataSource"/> must connect as a role that may write the config tables and the journal (the store admin).
    /// <paramref name="utcNow"/> is the clock; it defaults to the wall clock.</summary>
    public AgentChangeUndo(NpgsqlDataSource dataSource, int commandTimeoutSeconds, Func<DateTime>? utcNow = null)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    private DateTime Now() => DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);

    internal const string ListSql = @"
SELECT c.journal_id, c.event_time, c.request_id, c.server_id, c.actor, c.reason, c.detail::text,
       r.event, r.detail::text, u.event_time, u.actor, COALESCE(sv.display_name, sv.server_name)
FROM config.investigation_journal AS c
LEFT JOIN collect.servers AS sv ON sv.server_id = c.server_id
LEFT JOIN LATERAL (
    SELECT x.event, x.detail FROM config.investigation_journal AS x
    WHERE x.request_id = c.request_id
      AND x.event IN ('action_done', 'action_failed')
      AND x.detail->>'called_id' = c.journal_id::text
    ORDER BY x.journal_id DESC LIMIT 1) AS r ON true
LEFT JOIN LATERAL (
    SELECT x.event_time, x.actor FROM config.investigation_journal AS x
    WHERE x.request_id = c.request_id
      AND x.event = 'action_undone'
      AND x.detail->>'called_id' = c.journal_id::text
    ORDER BY x.journal_id DESC LIMIT 1) AS u ON true
WHERE c.event = 'action_called' AND c.event_time >= $1 AND ($3::bigint IS NULL OR c.journal_id = $3)
ORDER BY c.event_time DESC, c.journal_id DESC
LIMIT $2";

    /* A gate refusal always carries detail.tool (InvestigationRequestStore.AgentAction, DarlingAgentActionGate). The Level 1 request
       refusals (claim and submit, InvestigationRequestStore.RecordRefusalAsync) are agent-actor refusals too but refuse no change. */
    internal const string RefusalsSql = @"
SELECT j.journal_id, j.event_time, j.request_id, j.server_id, j.actor, j.reason, j.detail->>'tool', COALESCE(sv.display_name, sv.server_name)
FROM config.investigation_journal AS j
LEFT JOIN collect.servers AS sv ON sv.server_id = j.server_id
WHERE j.event = 'refused' AND j.actor LIKE 'agent:%' AND j.detail->>'tool' IS NOT NULL AND j.event_time >= $1
ORDER BY j.event_time DESC, j.journal_id DESC
LIMIT $2";

    /// <summary>The alerts muted since the change on the rule's server and metric (a null on the rule matches any). The alert log
    /// carries no rule id, so this is the link: alerts the rule could have muted, flagged <c>muted</c>, after the change.</summary>
    internal const string MutedAlertsSql = @"
SELECT a.alert_time, a.server_id, a.server_name, a.metric_name, a.current_value, a.threshold_value
FROM config.config_alert_log AS a
WHERE a.muted
  AND a.alert_time >= $1
  AND ($2::integer IS NULL OR a.server_id = $2)
  AND ($3::text IS NULL OR a.metric_name = $3)
ORDER BY a.alert_time DESC
LIMIT $4";

    /// <summary>How many alerts the rule muted since the change: the population, a count above any page (#5917). The predicate is
    /// <see cref="MutedAlertsSql"/>'s, with no LIMIT, so one row crosses the wire however many alerts the rule muted.</summary>
    internal const string MutedAlertsCountSql = @"
SELECT count(*)
FROM config.config_alert_log AS a
WHERE a.muted
  AND a.alert_time >= $1
  AND ($2::integer IS NULL OR a.server_id = $2)
  AND ($3::text IS NULL OR a.metric_name = $3)";

    /// <summary>
    /// The review mark of a done plan force (#5632, step 8), one row: when it was done, the checks opened, the checks answered, and whether the
    /// schedule has stopped: undone, a later done force or unforce of the same query on the server, or a check whose numbers showed it removed.
    /// $1 the force's <c>action_called</c> journal id.
    /// </summary>
    internal const string ForceReviewSql = @"
SELECT d.event_time,
       (SELECT count(*) FROM config.investigation_requests AS f WHERE f.force_check_of = c.journal_id),
       (SELECT count(*) FROM config.investigation_requests AS f WHERE f.force_check_of = c.journal_id AND f.reported_at IS NOT NULL),
       (EXISTS (SELECT 1 FROM config.investigation_journal AS u
                WHERE u.request_id = c.request_id AND u.event = 'action_undone' AND u.detail->>'called_id' = c.journal_id::text)
        OR EXISTS (
            SELECT 1 FROM config.investigation_journal AS c2
            JOIN config.investigation_journal AS d2
              ON d2.request_id = c2.request_id AND d2.event = 'action_done' AND d2.detail->>'called_id' = c2.journal_id::text
            WHERE c2.event = 'action_called'
              AND c2.detail->>'kind' IN ('plan_force', 'plan_unforce')
              AND c2.server_id = c.server_id
              AND c2.journal_id <> c.journal_id
              AND c2.event_time >= c.event_time
              AND COALESCE(c2.detail->'before'->>'database', c2.detail->'target'->>'database') = COALESCE(c.detail->'before'->>'database', c.detail->'target'->>'database')
              AND c2.detail->'before'->>'query_id' = c.detail->'before'->>'query_id'
              AND d2.journal_id > d.journal_id)
        OR EXISTS (
            SELECT 1 FROM config.investigation_requests AS f
            JOIN config.investigation_evidence AS e ON e.request_id = f.request_id AND e.section = 'plan_force_check'
            WHERE f.force_check_of = c.journal_id AND e.content->>'state' = 'removed'))
FROM config.investigation_journal AS c
JOIN LATERAL (
    SELECT x.journal_id, x.event_time FROM config.investigation_journal AS x
    WHERE x.request_id = c.request_id AND x.event = 'action_done' AND x.detail->>'called_id' = c.journal_id::text
    ORDER BY x.journal_id DESC LIMIT 1) AS d ON true
WHERE c.journal_id = $1 AND c.event = 'action_called'";

    private async Task<ForceReviewMark?> ForceReviewAsync(NpgsqlConnection connection, AgentChangeRow row, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, null, ForceReviewSql, P(row.CalledId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;
        var forcedAt = Utc(reader.GetDateTime(0));
        var stopped = reader.GetBoolean(3);
        var running = !stopped && Now() <= forcedAt.AddHours(InvestigationRequestStore.ForceCheckStopHours);
        return new ForceReviewMark((int)reader.GetInt64(1), (int)reader.GetInt64(2), running);
    }

    /// <summary>The list: the calls and the refusals newest first, with the switch's state.</summary>
    public async Task<AgentChangesRead> ReadAsync(int limit = 200, CancellationToken cancellationToken = default)
    {
        limit = Math.Clamp(limit, 1, 1000);
        var since = Now() - Window;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var enabled = await ReadEnabledAsync(connection, null, cancellationToken);
        var changes = await ReadChangesAsync(connection, since, limit, null, cancellationToken);

        for (var i = 0; i < changes.Count; i++)
        {
            if (changes[i].Kind == AgentChangeKinds.Mute && changes[i].Result == AgentChangeResult.Done)
                changes[i] = changes[i] with { MutedAlerts = await MutedAlertCountAsync(connection, changes[i], cancellationToken) };
            else if (changes[i].Kind == AgentChangeKinds.PlanForce && changes[i].Result == AgentChangeResult.Done)
                changes[i] = changes[i] with { ForceReview = await ForceReviewAsync(connection, changes[i], cancellationToken) };
        }

        var refusals = new List<AgentRefusalRow>();
        await using (var command = Command(connection, null, RefusalsSql, P(since), P(limit)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                refusals.Add(new AgentRefusalRow(reader.GetInt64(0), Utc(reader.GetDateTime(1)), reader.IsDBNull(2) ? null : reader.GetGuid(2),
                    reader.IsDBNull(3) ? null : reader.GetInt32(3), reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7)));
            }
        }

        return new AgentChangesRead(enabled, changes, refusals);
    }

    private async Task<List<AgentChangeRow>> ReadChangesAsync(NpgsqlConnection connection, DateTime since, int limit, long? calledId, CancellationToken cancellationToken)
    {
        var changes = new List<AgentChangeRow>();
        await using var command = Command(connection, null, ListSql, P(since), P(limit),
            new NpgsqlParameter<long?> { TypedValue = calledId, NpgsqlDbType = NpgsqlDbType.Bigint });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var resultEvent = reader.IsDBNull(7) ? null : reader.GetString(7);
            changes.Add(new AgentChangeRow(
                reader.GetInt64(0), Utc(reader.GetDateTime(1)), reader.IsDBNull(2) ? null : reader.GetGuid(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3), reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? "{}" : reader.GetString(6),
                resultEvent switch { "action_done" => AgentChangeResult.Done, "action_failed" => AgentChangeResult.Failed, _ => AgentChangeResult.Unknown },
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : Utc(reader.GetDateTime(9)),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                null, null, reader.IsDBNull(11) ? null : reader.GetString(11)));
        }

        return changes;
    }

    /// <summary>The alerts muted since a mute change was made, newest first; empty for any other kind.</summary>
    public async Task<IReadOnlyList<MutedAlertRow>> ReadMutedAlertsAsync(long calledId, int limit = 200, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var row = (await ReadAsync(connection, calledId, cancellationToken));
        return row is null || row.Kind != AgentChangeKinds.Mute ? [] : await MutedAlertsAsync(connection, row, Math.Clamp(limit, 1, 1000), cancellationToken);
    }

    /// <summary>The server and metric a mute rule's image names (a null matches any): the link into the alert log.</summary>
    private static (int? ServerId, string? Metric) MuteScope(AgentChangeRow row)
    {
        var done = row.ResultJson is null ? null : JsonNode.Parse(row.ResultJson) as JsonObject;
        var image = AfterImageNode(AgentChangeKinds.Mute, done?["after"]) as JsonObject ?? (JsonNode.Parse(row.DetailJson) as JsonObject)?["before"] as JsonObject;
        var serverId = image?["server_id"] is JsonValue sv && sv.TryGetValue(out int sid) ? (int?)sid : null;
        var metric = image?["metric_name"] is JsonValue mv && mv.TryGetValue(out string? m) ? m : null;
        return (serverId, metric);
    }

    /* The list's count is a statement of its own, not the length of a page (#5917, #3679): one count(*) row per mute change, however
       many alerts the rule muted. */
    private async Task<int> MutedAlertCountAsync(NpgsqlConnection connection, AgentChangeRow row, CancellationToken cancellationToken)
    {
        var (serverId, metric) = MuteScope(row);
        await using var command = Command(connection, null, MutedAlertsCountSql, P(row.CalledAtUtc),
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer }, PText(metric));
        var count = await command.ExecuteScalarAsync(cancellationToken);
        return (int)Math.Min(Convert.ToInt64(count, System.Globalization.CultureInfo.InvariantCulture), int.MaxValue);
    }

    private async Task<List<MutedAlertRow>> MutedAlertsAsync(NpgsqlConnection connection, AgentChangeRow row, int limit, CancellationToken cancellationToken)
    {
        var (serverId, metric) = MuteScope(row);
        var rows = new List<MutedAlertRow>();
        await using var command = Command(connection, null, MutedAlertsSql, P(row.CalledAtUtc),
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer }, PText(metric), P(limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            rows.Add(new MutedAlertRow(Utc(reader.GetDateTime(0)), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), reader.GetDouble(4), reader.GetDouble(5)));
        return rows;
    }

    /// <summary>One change by its journal id (no window), or null when no such <c>action_called</c> row exists. The web route reads the kind from it.</summary>
    public async Task<AgentChangeRow?> GetAsync(long calledId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await ReadAsync(connection, calledId, cancellationToken);
    }

    private async Task<AgentChangeRow?> ReadAsync(NpgsqlConnection connection, long calledId, CancellationToken cancellationToken) =>
        (await ReadChangesAsync(connection, DateTime.UnixEpoch, 1, calledId, cancellationToken)).SingleOrDefault();

    /// <summary>True when this store login may undo and switch (it can write the journal and the service row: the admin role). The viewer
    /// role on a managed or compose store cannot, so the web lists only.</summary>
    public async Task<bool> CanDecideAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null,
            "SELECT has_any_column_privilege(current_user, 'config.investigation_journal', 'INSERT') AND has_table_privilege(current_user, 'config.config_service', 'UPDATE')");
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    /// <summary>The switch. A store with no <c>config_service</c> row reads as on, its default.</summary>
    public async Task<bool> ReadEnabledAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await ReadEnabledAsync(connection, null, cancellationToken);
    }

    private async Task<bool> ReadEnabledAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, InvestigationRequestStore.AgentActionsEnabledSql);
        return await command.ExecuteScalarAsync(cancellationToken) is not bool on || on;
    }

    /// <summary>What a person is told when the switch waited for an agent change in progress and that change did not finish in time.</summary>
    public const string SwitchWaitTimedOutMessage =
        "Waiting for an agent change in progress to finish took longer than 60 seconds, so the switch was not changed. Try again.";

    /// <summary>What a person is told when the switch finds no service row to update (#6401): the service creates the row when it copies
    /// darling.json into the store, and a switch must not create it first, or the service takes the store as set up and skips that copy.</summary>
    public const string SwitchStoreNotSetUpMessage =
        "The store is not set up yet; nothing changed. Try again after the service has started once.";

    /// <summary>Turns every agent change on or off at once and journals <c>agent_actions_switched</c> by <c>person:&lt;login&gt;</c>, in one
    /// transaction. Off makes the gate refuse <c>actions_off</c>; nothing in flight is undone. False when the store has no service row yet:
    /// nothing was written (<see cref="SwitchStoreNotSetUpMessage"/>).</summary>
    public Task<bool> SetEnabledAsync(bool enabled, string login, CancellationToken cancellationToken = default) =>
        SetEnabledAsActorAsync(enabled, PersonActor(login), cancellationToken);

    /// <summary><see cref="SetEnabledAsync"/> for a caller that is not a person (#6401): the MCP switch tool journals <c>mcp:</c> and the
    /// token and client it came in on, so a person is never credited with what a tool did. <paramref name="actor"/> is written as is.
    /// The write updates the service row and never creates it: false, with nothing written or journaled, when there is no row.</summary>
    public async Task<bool> SetEnabledAsActorAsync(bool enabled, string actor, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        int updated;
        await using (var command = Command(connection, transaction, @"
UPDATE config.config_service
   SET agent_actions_enabled = $1, updated_at = now() AT TIME ZONE 'UTC', updated_by = $2
 WHERE id = 1", P(enabled), PText(actor)))
        {
            /* An agent change in progress holds the service row until its remote work ends (at most AgentActionRemoteWorkSeconds), and this
               write waits for it. The wait must outlast that work, or the switch gives up exactly when a slow server makes it matter (#5632). */
            command.CommandTimeout = Math.Max(_commandTimeoutSeconds, InvestigationRequestStore.AgentOffSwitchWaitSeconds);
            try
            {
                updated = await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is NpgsqlException { InnerException: TimeoutException } || ex is PostgresException { SqlState: PostgresErrorCodes.QueryCanceled })
            {
                throw new TimeoutException(SwitchWaitTimedOutMessage, ex);
            }
        }

        if (updated == 0)
        {
            return false;
        }

        await JournalAsync(connection, transaction, "agent_actions_switched", actor, null, null,
            new JsonObject { ["enabled"] = enabled }.ToJsonString(), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Undoes the change the <c>action_called</c> row <paramref name="calledId"/> made, as <paramref name="login"/>. One transaction
    /// that takes an advisory lock on the call (a second click waits, then sees <c>already_undone</c>), reads the target
    /// <c>FOR UPDATE</c>, compares and restores. Never throws for a refusal.
    /// </summary>
    public Task<AgentUndoResult> UndoAsync(long calledId, string login, CancellationToken cancellationToken = default) =>
        UndoAsActorAsync(calledId, PersonActor(login), cancellationToken);

    /// <summary><see cref="UndoAsync"/> for a caller that is not a person (#6401): the MCP undo tool journals <c>mcp:</c> and the token and
    /// client it came in on. <paramref name="actor"/> is written as is. The web route and the tool share this one copy of the undo.</summary>
    public async Task<AgentUndoResult> UndoAsActorAsync(long calledId, string actor, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        string? calledDetail;
        Guid? requestId;
        int? serverId;
        DateTime calledAt;
        /* The journal is append-only (the admin role may INSERT but not UPDATE it), so a second click waits on a transaction-scoped
           advisory lock on the call's id instead of a row lock, then finds the first click's action_undone row. */
        await using (var lockCommand = Command(connection, transaction, "SELECT pg_advisory_xact_lock($1)", P(calledId)))
        {
            await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var command = Command(connection, transaction, @"
SELECT detail::text, request_id, server_id, event_time FROM config.investigation_journal
WHERE journal_id = $1 AND event = 'action_called'", P(calledId)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
                return AgentUndoResult.Refused("not_found", "That change is not in the journal.");
            calledDetail = reader.IsDBNull(0) ? null : reader.GetString(0);
            requestId = reader.IsDBNull(1) ? null : reader.GetGuid(1);
            serverId = reader.IsDBNull(2) ? null : reader.GetInt32(2);
            calledAt = Utc(reader.GetDateTime(3));
        }

        if (Now() - calledAt > Window)
            return AgentUndoResult.Refused("expired", "Undo works for 365 days from the change, and this one is older.");
        if (JsonNode.Parse(calledDetail ?? "{}") is not JsonObject called)
            return AgentUndoResult.Refused("not_undoable", "This change has no recorded before image.");

        if (await FirstDetailAsync(connection, transaction, "action_undone", calledId, requestId, cancellationToken) is not null)
            return AgentUndoResult.Refused("already_undone", "This change was already undone.");
        var done = await FirstDetailAsync(connection, transaction, "action_done", calledId, requestId, cancellationToken);
        if (done is null)
            return AgentUndoResult.Refused("not_undoable", "The result of this call is unknown (no done row), so what it changed is unknown and it cannot be undone from here.");

        var kind = called["kind"] is JsonValue kv && kv.TryGetValue(out string? k) ? k : null;
        var spec = kind switch
        {
            AgentChangeKinds.Mute => MuteSpec,
            AgentChangeKinds.CustomRule => RuleSpec,
            AgentChangeKinds.ThresholdOverride => OverrideSpec,
            AgentChangeKinds.SignatureMerge => MergeSpec,
            _ => null,
        };
        if (kind is null || spec is null)
            return AgentUndoResult.Refused("not_undoable", "This kind of change has no undo.");

        /* What the Level 2 tools journal: the before image on the called row, and on the done row the tool's answer. The mute and
           custom rule tools answer with a status word; the mute tools carry the rule in mute_rule, the custom rule tools answer
           with the rule itself, and a delete answers {status: "deleted"} (no rule left: the after image is null). The override
           tool journals its image ({value}) as is, null for a cleared override. */
        var answer = done["after"] as JsonObject;
        var status = answer?["status"] is JsonValue sv0 && sv0.TryGetValue(out string? st) ? st : null;
        if (status is "unchanged" or "already_exists")
            return AgentUndoResult.Refused("not_undoable", "That call changed nothing (the tool answered '" + status + "'), so there is nothing to undo.");
        var afterNode = AfterImageNode(kind, done["after"]);
        var before = ImageObject(kind, called["before"]);
        var after = ImageObject(kind, afterNode);
        var target = called["target"] as JsonObject;
        var targetId = target?["id"] is JsonValue tv && tv.TryGetValue(out string? t) ? t : target?["id"]?.ToString();
        if (string.IsNullOrWhiteSpace(targetId))
            targetId = (after ?? before)?["id"]?.ToString();
        var targetServer = target?["server_id"] is JsonValue sv && sv.TryGetValue(out int s) ? (int?)s : serverId;
        var targetDb = target?["database"] is JsonValue dv && dv.TryGetValue(out string? d) ? d : null;
        if (before is null && after is null)
            return AgentUndoResult.Refused("not_undoable", "This change has no recorded images.");

        var key = Key(kind, targetId, targetServer, targetDb);
        if (key is null)
            return AgentUndoResult.Refused("not_undoable", "This change's target is not recorded.");

        var current = await ReadCurrentAsync(connection, transaction, spec, key, cancellationToken);
        var label = kind switch { AgentChangeKinds.Mute => "mute rule", AgentChangeKinds.CustomRule => "custom alert rule", AgentChangeKinds.SignatureMerge => "deadlock signature merge", _ => "threshold override" };
        if (!Equal(spec, current, after))
            return AgentUndoResult.Refused("changed_since", $"Not undone: the {label} changed since the agent's change, so its current settings are kept. Review it and change it by hand if you still want it back.");

        if (after is not null && before is null && kind == AgentChangeKinds.SignatureMerge)
        {
            /* #6006: the merge row is the record of the decision, so an undo stamps it and keeps it (the live index counts only
               rows with no undone_at). */
            await StampMergeUndoneAsync(connection, transaction, key, actor, cancellationToken);
        }
        else if (after is not null && before is null)
        {
            await DeleteAsync(connection, transaction, spec, key, cancellationToken);
        }
        else if (before is not null && current is not null)
        {
            await UpdateAsync(connection, transaction, spec, key, before, actor, cancellationToken);
        }
        else
        {
            /* Deleted by the agent: put it back. A rule keeps its Custom:<id>; a name clash refuses. */
            if (kind == AgentChangeKinds.CustomRule)
            {
                var name = before!["name"] is JsonValue nv && nv.TryGetValue(out string? n) ? n : null;
                await using var clash = Command(connection, transaction, "SELECT 1 FROM config.custom_alert_rules WHERE name = $1", PText(name));
                if (await clash.ExecuteScalarAsync(cancellationToken) is not null)
                    return AgentUndoResult.Refused("name_taken", $"Not undone: a custom alert rule named '{name}' exists now, so the deleted one cannot come back with its name.");
            }

            await InsertAsync(connection, transaction, spec, key, before!, actor, requestId, cancellationToken);
        }

        await JournalAsync(connection, transaction, "action_undone", actor, requestId, serverId,
            new JsonObject
            {
                ["called_id"] = calledId,
                ["kind"] = kind,
                ["target"] = target?.DeepClone(),
                ["restored"] = before?.DeepClone(),
                ["replaced"] = after?.DeepClone(),
            }.ToJsonString(), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return AgentUndoResult.Ok(label);
    }

    // ---- the per-kind row access ----

    /// <summary>
    /// The after image inside the tool's answer on the <c>action_done</c> row: a mute tool answers <c>{status, mute_rule}</c>, a custom
    /// rule tool answers the rule itself, the override tool journals its image as is, and a delete answers <c>{status: "deleted"}</c>
    /// (null: nothing is left).
    /// </summary>
    private static JsonNode? AfterImageNode(string kind, JsonNode? answer)
    {
        if (answer is JsonObject o && o["status"] is JsonValue s && s.TryGetValue(out string? word) && word == "deleted")
            return null;
        return kind == AgentChangeKinds.Mute && answer is JsonObject m ? m["mute_rule"] : answer;
    }

    /// <summary>A journaled image as the flat object of column names the undo compares and writes, through the kind's shared image record.</summary>
    private static JsonObject? ImageObject(string kind, JsonNode? node) => kind switch
    {
        AgentChangeKinds.Mute => ImageColumns.ToObject<MuteRuleImage>(node),
        AgentChangeKinds.CustomRule => ImageColumns.ToObject<CustomAlertRuleImage>(node),
        AgentChangeKinds.SignatureMerge => ImageColumns.ToObject<SignatureMergeImage>(node),
        _ => ImageColumns.ToObject<ThresholdOverrideImage>(node),
    };

    private sealed record RowKey(string Kind, string? Id, long? RuleId, int ServerId, string? Database);

    private static RowKey? Key(string kind, string? id, int? serverId, string? database)
    {
        if (string.IsNullOrWhiteSpace(id))
            return null;
        if (kind == AgentChangeKinds.CustomRule)
        {
            var digits = id.StartsWith("Custom:", StringComparison.OrdinalIgnoreCase) ? id[7..] : id;
            return long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var n) ? new RowKey(kind, id, n, 0, null) : null;
        }

        if (kind is AgentChangeKinds.ThresholdOverride or AgentChangeKinds.SignatureMerge)
            return serverId is int sid ? new RowKey(kind, id, null, sid, database) : null;
        return new RowKey(kind, id, null, 0, null);
    }

    private static string Where(RowKey key) => key.Kind switch
    {
        AgentChangeKinds.Mute => "id = $1",
        AgentChangeKinds.CustomRule => "id = $1",
        AgentChangeKinds.SignatureMerge => "server_id = $1 AND signature = $2 AND undone_at IS NULL",
        _ => "server_id = $1 AND coalesce(database_name, '') = coalesce($2::text, '') AND threshold_key = $3",
    };

    private NpgsqlParameter[] KeyParams(RowKey key) => key.Kind switch
    {
        AgentChangeKinds.Mute => [PText(key.Id)],
        AgentChangeKinds.CustomRule => [P(key.RuleId!.Value)],
        AgentChangeKinds.SignatureMerge => [P(key.ServerId), PText(key.Id)],
        _ => [P(key.ServerId), PText(key.Database), PText(key.Id)],
    };

    private static int Next(RowKey key) => key.Kind == AgentChangeKinds.ThresholdOverride ? 4 : 2;

    private async Task<JsonObject?> ReadCurrentAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Spec spec, RowKey key, CancellationToken cancellationToken)
    {
        var columns = string.Join(", ", spec.Fields.Select(f => f.Type == ImageColumnType.Json ? $"{f.Name}::text" : f.Name));
        await using var command = Command(connection, transaction, $"SELECT {columns} FROM {spec.Table} WHERE {Where(key)} FOR UPDATE", KeyParams(key));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;
        var row = new JsonObject();
        for (var i = 0; i < spec.Fields.Length; i++)
        {
            var (name, type) = spec.Fields[i];
            row[name] = reader.IsDBNull(i) ? null : type switch
            {
                ImageColumnType.Bool => JsonValue.Create(reader.GetBoolean(i)),
                ImageColumnType.Whole => JsonValue.Create(reader.GetInt32(i)),
                ImageColumnType.Real => JsonValue.Create(reader.GetDouble(i)),
                ImageColumnType.Time => JsonValue.Create(Utc(reader.GetDateTime(i)).ToString("o", CultureInfo.InvariantCulture)),
                ImageColumnType.Json => JsonNode.Parse(reader.GetString(i)),
                _ => JsonValue.Create(reader.GetString(i)),
            };
        }

        return row;
    }

    /// <summary>Equality on the spec's fields only. Null and absent are equal; numbers by value; times by instant; JSON by structure.</summary>
    private static bool Equal(Spec spec, JsonObject? current, JsonObject? after)
    {
        if (current is null || after is null)
            return current is null && after is null;
        foreach (var (name, type) in spec.Fields)
        {
            if (!after.ContainsKey(name))
            {
                /* A field the image does not name was not the agent's to set: it is not compared. */
                continue;
            }

            if (!SameValue(type, current[name], after[name]))
                return false;
        }

        return true;
    }

    private static bool SameValue(ImageColumnType type, JsonNode? a, JsonNode? b)
    {
        if (a is null || b is null)
            return a is null && b is null;
        switch (type)
        {
            case ImageColumnType.Real:
                return a.GetValue<double>() == b.GetValue<double>();
            case ImageColumnType.Time:
                return DateTime.TryParse(a.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var x)
                    && DateTime.TryParse(b.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var y)
                    /* PostgreSQL keeps microseconds and a tool's .NET time carries ticks, so equal means within one microsecond. */
                    && Math.Abs((x - y).Ticks) < TimeSpan.TicksPerMillisecond / 1000;
            case ImageColumnType.Json:
                return JsonNode.DeepEquals(Structured(a), Structured(b));
            case ImageColumnType.Bool:
                return a.GetValue<bool>() == b.GetValue<bool>();
            case ImageColumnType.Whole:
                return a.GetValue<int>() == b.GetValue<int>();
            default:
                return string.Equals(a.GetValue<string>(), b.GetValue<string>(), StringComparison.Ordinal);
        }
    }

    private static JsonNode? Structured(JsonNode node) =>
        node is JsonValue v && v.TryGetValue(out string? s) && s is not null && (s.TrimStart().StartsWith('{') || s.TrimStart().StartsWith('['))
            ? JsonNode.Parse(s) : node;

    private async Task StampMergeUndoneAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, RowKey key, string actor, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction,
            $"UPDATE {SignatureMergeImage.Table} SET undone_at = now(), undone_by = $3 WHERE {Where(key)}", [.. KeyParams(key), PText(actor)]);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task DeleteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Spec spec, RowKey key, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, $"DELETE FROM {spec.Table} WHERE {Where(key)}", KeyParams(key));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private NpgsqlParameter FieldParam(ImageColumnType type, JsonNode? value) => type switch
    {
        ImageColumnType.Bool => new NpgsqlParameter<bool?> { TypedValue = value?.GetValue<bool>(), NpgsqlDbType = NpgsqlDbType.Boolean },
        ImageColumnType.Whole => new NpgsqlParameter<int?> { TypedValue = value?.GetValue<int>(), NpgsqlDbType = NpgsqlDbType.Integer },
        ImageColumnType.Real => new NpgsqlParameter<double?> { TypedValue = value?.GetValue<double>(), NpgsqlDbType = NpgsqlDbType.Double },
        ImageColumnType.Time => new NpgsqlParameter<DateTime?>
        {
            TypedValue = value is null ? null : DateTime.SpecifyKind(DateTime.Parse(value.ToString(), CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal), DateTimeKind.Unspecified),
            NpgsqlDbType = NpgsqlDbType.Timestamp,
        },
        ImageColumnType.Json => new NpgsqlParameter<string?> { TypedValue = value is null ? null : (Structured(value) ?? value).ToJsonString(), NpgsqlDbType = NpgsqlDbType.Jsonb },
        _ => PText(value?.GetValue<string>()),
    };

    private async Task UpdateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Spec spec, RowKey key, JsonObject before, string actor, CancellationToken cancellationToken)
    {
        var parameters = KeyParams(key).ToList();
        var sets = new List<string>();
        var n = Next(key);
        foreach (var (name, type) in spec.Fields)
        {
            /* A partial image restores only what it names. */
            if (!before.ContainsKey(name))
                continue;
            sets.Add($"{name} = ${n++}");
            parameters.Add(FieldParam(type, before[name]));
        }

        var extra = key.Kind switch
        {
            AgentChangeKinds.CustomRule => $", version = version + 1, updated_at = (now() AT TIME ZONE 'UTC'), updated_by = ${n}",
            AgentChangeKinds.ThresholdOverride => $", updated_at = now(), set_by = ${n}",
            _ => "",
        };
        if (extra.Length > 0)
            parameters.Add(PText(actor));
        await using var command = Command(connection, transaction,
            $"UPDATE {spec.Table} SET {string.Join(", ", sets)}{extra} WHERE {Where(key)}", parameters.ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task InsertAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Spec spec, RowKey key, JsonObject image, string actor, Guid? requestId, CancellationToken cancellationToken)
    {
        var columns = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        void Add(string column, NpgsqlParameter parameter)
        {
            columns.Add(column);
            parameters.Add(parameter);
        }

        string overriding = "";
        switch (key.Kind)
        {
            case AgentChangeKinds.Mute:
                Add("id", PText(key.Id));
                break;
            case AgentChangeKinds.CustomRule:
                Add("id", P(key.RuleId!.Value));
                overriding = " OVERRIDING SYSTEM VALUE";
                Add("updated_by", PText(actor));
                break;
            default:
                Add("server_id", P(key.ServerId));
                Add("database_name", PText(key.Database));
                Add("threshold_key", PText(key.Id));
                Add("set_by", PText(actor));
                Add("request_id", new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid });
                break;
        }

        foreach (var (name, type) in spec.Fields)
        {
            if (image.ContainsKey(name))
                Add(name, FieldParam(type, image[name]));
        }

        if (key.Kind == AgentChangeKinds.Mute && !image.ContainsKey("created_at_utc"))
            Add("created_at_utc", FieldParam(ImageColumnType.Time, JsonValue.Create(Now().ToString("o", CultureInfo.InvariantCulture))));
        var holes = string.Join(", ", Enumerable.Range(1, columns.Count).Select(i => "$" + i.ToString(CultureInfo.InvariantCulture)));
        await using var command = Command(connection, transaction,
            $"INSERT INTO {spec.Table} ({string.Join(", ", columns)}){overriding} VALUES ({holes})", parameters.ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<JsonObject?> FirstDetailAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string journalEvent, long calledId, Guid? requestId, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, @"
SELECT detail::text FROM config.investigation_journal
WHERE event = $1 AND request_id IS NOT DISTINCT FROM $2 AND detail->>'called_id' = $3
ORDER BY journal_id DESC LIMIT 1",
            PText(journalEvent), new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid },
            PText(calledId.ToString(CultureInfo.InvariantCulture)));
        return await command.ExecuteScalarAsync(cancellationToken) is string text ? JsonNode.Parse(text) as JsonObject ?? new JsonObject() : null;
    }

    private async Task JournalAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, string journalEvent, string actor,
        Guid? requestId, int? serverId, string detailJson, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, InvestigationRequestStore.AgentActionJournalSql,
            new NpgsqlParameter<DateTime> { TypedValue = Now(), NpgsqlDbType = NpgsqlDbType.TimestampTz },
            new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid },
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            PText(journalEvent), PText(actor), PText(null), PText(detailJson));
        await command.ExecuteScalarAsync(cancellationToken);
    }

    /// <summary>The longest person name, in UTF-16 units, once trimmed and normalised. The journal's <c>actor</c> and every other column a
    /// <c>person:</c> actor is written to are unbounded <c>text</c> with no length CHECK, so this is the only bound; a web sign-in name
    /// (an email or an Auth0 subject) can run past 200 characters.</summary>
    public const int LoginMaxLength = 256;

    /// <summary>The punctuation a person's name may hold besides letters and digits: an email's local part, a Windows
    /// <c>DOMAIN\user</c>, an Auth0 <c>provider|id</c>. Quotes, brackets, angle brackets, commas and semicolons stay out.</summary>
    private const string LoginPunctuation = ".:_-@\\!#$%&'*+/=?^`{|}~";

    /// <summary>The sentence that says what a person's name may hold. Never carries the name itself.</summary>
    public const string LoginRefusal =
        "The name must be 1 to 256 characters: letters (accents allowed), digits, single spaces between other characters, and . _ : - @ \\ ! # $ % & ' * + / = ? ^ ` { | } ~. Quotes, brackets, commas, semicolons, control and invisible characters are not allowed.";

    /// <summary>A name trimmed and put in Unicode normal form C, so one name has one spelling in the journal. Null reads as empty.</summary>
    public static string NormalizeLogin(string? login)
    {
        var trimmed = (login ?? "").Trim();
        try
        {
            return trimmed.Normalize(NormalizationForm.FormC);
        }
        catch (ArgumentException)
        {
            return trimmed;
        }
    }

    /// <summary>True when <paramref name="normalized"/> (already trimmed and in form C) is 1 to <see cref="LoginMaxLength"/> UTF-16 units, each
    /// an ASCII letter or digit, one of the punctuation marks above, a single space between two other characters, a non-ASCII letter in the
    /// Basic Multilingual Plane (not one of the four that draw nothing), or one of at most two combining marks right after a letter.
    /// Control and format characters (bidi, zero-width), other space characters and surrogates are refused.</summary>
    public static bool IsValidLogin(string normalized)
    {
        if (normalized.Length is 0 or > LoginMaxLength)
            return false;

        var marks = 0;
        for (var i = 0; i < normalized.Length; i++)
        {
            var c = normalized[i];
            if (c == ' ')
            {
                if (i == 0 || i == normalized.Length - 1 || normalized[i - 1] == ' ' || normalized[i + 1] == ' ')
                    return false;
                marks = 0;
                continue;
            }

            if (char.IsAsciiLetterOrDigit(c) || LoginPunctuation.Contains(c))
            {
                marks = 0;
                continue;
            }

            if (c < 0x80)
                return false;

            var category = char.GetUnicodeCategory(c);
            if (category is UnicodeCategory.NonSpacingMark or UnicodeCategory.SpacingCombiningMark)
            {
                var invisible = c is '\u034F' or '\u17B4' or '\u17B5' || c is >= '\u180B' and <= '\u180D' || c is >= '\uFE00' and <= '\uFE0F';
                /* The letter this run of marks hangs on sits `marks` units back (marks counts this one). */
                if (invisible || ++marks > 2 || i - marks < 0 || !char.IsLetter(normalized[i - marks]))
                    return false;
                continue;
            }

            marks = 0;
            if (!char.IsLetter(c) || c is '\u115F' or '\u1160' or '\u3164' or '\uFFA0')
                return false;
        }

        return true;
    }

    /// <summary>The journal actor for a person's undo or switch: <c>person:&lt;login&gt;</c>, the login trimmed, normalised and held to the
    /// identity shape of <see cref="IsValidLogin"/>.</summary>
    public static string PersonActor(string login)
    {
        var normalized = NormalizeLogin(login);
        if (!IsValidLogin(normalized))
            throw new ArgumentException(LoginRefusal, nameof(login));
        return "person:" + normalized;
    }

    private NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.AddRange(parameters);
        return command;
    }

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);

    private static NpgsqlParameter<T> P<T>(T value) => new() { TypedValue = value };

    private static NpgsqlParameter<DateTime> P(DateTime value) =>
        new() { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.TimestampTz };

    private static NpgsqlParameter<string?> PText(string? value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };
}
