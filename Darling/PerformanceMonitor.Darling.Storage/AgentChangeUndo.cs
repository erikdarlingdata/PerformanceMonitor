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
    int? MutedAlerts)
{
    /// <summary>The kind word the tool journaled (<c>mute</c>, <c>rule</c> or <c>override</c>).</summary>
    public string? Kind => ReadString("kind");

    /// <summary>The tool that made the change.</summary>
    public string? Tool => ReadString("tool");

    private string? ReadString(string key) =>
        JsonNode.Parse(DetailJson) is JsonObject o && o[key] is JsonValue v && v.TryGetValue(out string? s) ? s : null;

    /// <summary>True when an undo can be tried: the change was made, nobody undid it, and the kind has an undo.</summary>
    public bool CanTryUndo(DateTime nowUtc) =>
        Result == AgentChangeResult.Done && UndoneAtUtc is null
        && AgentChangeUndo.UndoableKinds.Contains(Kind ?? "")
        && nowUtc - CalledAtUtc <= AgentChangeUndo.Window;
}

/// <summary>How a journaled call ended (#5917). A call with neither an <c>action_done</c> nor an <c>action_failed</c> row is
/// <see cref="Unknown"/>: the write may or may not have happened, so the list says "result unknown" and offers no undo.</summary>
public enum AgentChangeResult { Done, Failed, Unknown }

/// <summary>A journaled refusal (#5917): the gate or a tool said no.</summary>
public sealed record AgentRefusalRow(long JournalId, DateTime AtUtc, Guid? RequestId, int? ServerId, string Actor, string? Code, string? Tool);

/// <summary>The Agent changes list (#5917): the calls, the refusals and the switch.</summary>
public sealed record AgentChangesRead(bool Enabled, IReadOnlyList<AgentChangeRow> Changes, IReadOnlyList<AgentRefusalRow> Refusals);

/// <summary>One alert muted under an agent's mute rule since the change (#5917).</summary>
public sealed record MutedAlertRow(DateTime AlertTimeUtc, int ServerId, string ServerName, string MetricName, double CurrentValue, double ThresholdValue);

/// <summary>What an undo did (#5917). <see cref="Undone"/> is true when the before image was restored and
/// <c>action_undone</c> journaled; otherwise <see cref="Code"/> is the refusal: <c>changed_since</c>, <c>name_taken</c>, <c>not_found</c>,
/// <c>already_undone</c>, <c>not_undoable</c> or <c>expired</c>, and <see cref="Message"/> says it in plain words.</summary>
public sealed record AgentUndoResult(bool Undone, string? Code, string Message)
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
/// database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, client_app_pattern</c>. <c>rule</c>:
/// <c>custom_alert_rules</c>, target id = the rule id (<c>Custom:&lt;id&gt;</c> or the number), fields <c>name, definition, description, enabled</c>.
/// <c>override</c>: <c>config_threshold_overrides</c>, target = server id, database and threshold key as the target id, field <c>value</c>.
/// Never <c>updated_at</c>, <c>updated_by</c>, <c>version</c> or the <c>config_version</c> beacon: they change with every write.</para>
/// <para>Created (before null) is undone by deleting; changed by restoring the before image; deleted (after null) by re-inserting it
/// (a rule with <c>OVERRIDING SYSTEM VALUE</c>, so it keeps its <c>Custom:&lt;id&gt;</c>; a rule with that name since is refused).
/// Undo works for <see cref="Window"/> from the call.</para>
/// </summary>
public sealed class AgentChangeUndo
{
    /// <summary>How long after the call an undo is offered (365 days).</summary>
    public static readonly TimeSpan Window = TimeSpan.FromDays(365);

    /// <summary>The kinds an undo exists for.</summary>
    public static readonly IReadOnlySet<string> UndoableKinds = new HashSet<string>(StringComparer.Ordinal) { "mute", "rule", "override" };

    private enum T { Text, Bool, Int, Double, Time, Json }

    private sealed record Spec(string Table, (string Name, T Type)[] Fields);

    private static readonly Spec MuteSpec = new("config.config_mute_rules",
    [
        ("enabled", T.Bool), ("expires_at_utc", T.Time), ("reason", T.Text), ("server_name", T.Text), ("server_id", T.Int),
        ("metric_name", T.Text), ("database_pattern", T.Text), ("query_text_pattern", T.Text), ("wait_type_pattern", T.Text),
        ("job_name_pattern", T.Text), ("client_app_pattern", T.Text),
    ]);

    private static readonly Spec RuleSpec = new("config.custom_alert_rules",
        [("name", T.Text), ("definition", T.Json), ("description", T.Text), ("enabled", T.Bool)]);

    private static readonly Spec OverrideSpec = new("config.config_threshold_overrides", [("value", T.Double)]);

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
       r.event, r.detail::text, u.event_time, u.actor
FROM config.investigation_journal AS c
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

    internal const string RefusalsSql = @"
SELECT journal_id, event_time, request_id, server_id, actor, reason, detail->>'tool'
FROM config.investigation_journal
WHERE event = 'refused' AND actor LIKE 'agent:%' AND event_time >= $1
ORDER BY event_time DESC, journal_id DESC
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
            if (changes[i].Kind == "mute" && changes[i].Result == AgentChangeResult.Done)
                changes[i] = changes[i] with { MutedAlerts = (await MutedAlertsAsync(connection, changes[i], 1000, cancellationToken)).Count };
        }

        var refusals = new List<AgentRefusalRow>();
        await using (var command = Command(connection, null, RefusalsSql, P(since), P(limit)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                refusals.Add(new AgentRefusalRow(reader.GetInt64(0), Utc(reader.GetDateTime(1)), reader.IsDBNull(2) ? null : reader.GetGuid(2),
                    reader.IsDBNull(3) ? null : reader.GetInt32(3), reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6)));
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
                null));
        }

        return changes;
    }

    /// <summary>The alerts muted since a mute change was made, newest first; empty for any other kind.</summary>
    public async Task<IReadOnlyList<MutedAlertRow>> ReadMutedAlertsAsync(long calledId, int limit = 200, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var row = (await ReadAsync(connection, calledId, cancellationToken));
        return row is null || row.Kind != "mute" ? [] : await MutedAlertsAsync(connection, row, Math.Clamp(limit, 1, 1000), cancellationToken);
    }

    private async Task<List<MutedAlertRow>> MutedAlertsAsync(NpgsqlConnection connection, AgentChangeRow row, int limit, CancellationToken cancellationToken)
    {
        var done = row.ResultJson is null ? null : JsonNode.Parse(row.ResultJson) as JsonObject;
        var image = done?["after"] as JsonObject ?? (JsonNode.Parse(row.DetailJson) as JsonObject)?["before"] as JsonObject;
        var serverId = image?["server_id"] is JsonValue sv && sv.TryGetValue(out int sid) ? (int?)sid : null;
        var metric = image?["metric_name"] is JsonValue mv && mv.TryGetValue(out string? m) ? m : null;
        var rows = new List<MutedAlertRow>();
        await using var command = Command(connection, null, MutedAlertsSql, P(row.CalledAtUtc),
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer }, PText(metric), P(limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            rows.Add(new MutedAlertRow(Utc(reader.GetDateTime(0)), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), reader.GetDouble(4), reader.GetDouble(5)));
        return rows;
    }

    private async Task<AgentChangeRow?> ReadAsync(NpgsqlConnection connection, long calledId, CancellationToken cancellationToken) =>
        (await ReadChangesAsync(connection, DateTime.UnixEpoch, 1, calledId, cancellationToken)).SingleOrDefault();

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

    /// <summary>Turns every agent change on or off at once and journals <c>agent_actions_switched</c> by <c>person:&lt;login&gt;</c>, in one
    /// transaction. Off makes the gate refuse <c>actions_off</c>; nothing in flight is undone.</summary>
    public async Task SetEnabledAsync(bool enabled, string login, CancellationToken cancellationToken = default)
    {
        var actor = PersonActor(login);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var command = Command(connection, transaction, @"
INSERT INTO config.config_service (id, agent_actions_enabled, updated_at, updated_by)
VALUES (1, $1, now() AT TIME ZONE 'UTC', $2)
ON CONFLICT (id) DO UPDATE SET agent_actions_enabled = EXCLUDED.agent_actions_enabled,
    updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by", P(enabled), PText(actor)))
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await JournalAsync(connection, transaction, "agent_actions_switched", actor, null, null,
            new JsonObject { ["enabled"] = enabled }.ToJsonString(), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// Undoes the change the <c>action_called</c> row <paramref name="calledId"/> made, as <paramref name="login"/>. One transaction
    /// that locks the call's journal row (a second click waits, then sees <c>already_undone</c>), reads the target
    /// <c>FOR UPDATE</c>, compares and restores. Never throws for a refusal.
    /// </summary>
    public async Task<AgentUndoResult> UndoAsync(long calledId, string login, CancellationToken cancellationToken = default)
    {
        var actor = PersonActor(login);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        string? calledDetail;
        Guid? requestId;
        int? serverId;
        DateTime calledAt;
        await using (var command = Command(connection, transaction, @"
SELECT detail::text, request_id, server_id, event_time FROM config.investigation_journal
WHERE journal_id = $1 AND event = 'action_called' FOR UPDATE", P(calledId)))
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
        var spec = kind switch { "mute" => MuteSpec, "rule" => RuleSpec, "override" => OverrideSpec, _ => null };
        if (kind is null || spec is null)
            return AgentUndoResult.Refused("not_undoable", "This kind of change has no undo.");

        var before = called["before"] as JsonObject;
        var after = done["after"] as JsonObject;
        var target = called["target"] as JsonObject;
        var targetId = target?["id"] is JsonValue tv && tv.TryGetValue(out string? t) ? t : target?["id"]?.ToString();
        var targetServer = target?["server_id"] is JsonValue sv && sv.TryGetValue(out int s) ? (int?)s : serverId;
        var targetDb = target?["database"] is JsonValue dv && dv.TryGetValue(out string? d) ? d : null;
        if (before is null && after is null)
            return AgentUndoResult.Refused("not_undoable", "This change has no recorded images.");

        var key = Key(kind, targetId, targetServer, targetDb);
        if (key is null)
            return AgentUndoResult.Refused("not_undoable", "This change's target is not recorded.");

        var current = await ReadCurrentAsync(connection, transaction, spec, key, cancellationToken);
        var label = kind switch { "mute" => "mute rule", "rule" => "custom alert rule", _ => "threshold override" };
        if (!Equal(spec, current, after))
            return AgentUndoResult.Refused("changed_since", $"Not undone: the {label} changed since the agent's change, so its current settings are kept. Review it and change it by hand if you still want it back.");

        if (after is not null && before is null)
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
            if (kind == "rule")
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

    private sealed record RowKey(string Kind, string? Id, long? RuleId, int ServerId, string? Database);

    private static RowKey? Key(string kind, string? id, int? serverId, string? database)
    {
        if (string.IsNullOrWhiteSpace(id))
            return null;
        if (kind == "rule")
        {
            var digits = id.StartsWith("Custom:", StringComparison.OrdinalIgnoreCase) ? id[7..] : id;
            return long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var n) ? new RowKey(kind, id, n, 0, null) : null;
        }

        if (kind == "override")
            return serverId is int sid ? new RowKey(kind, id, null, sid, database) : null;
        return new RowKey(kind, id, null, 0, null);
    }

    private static string Where(RowKey key) => key.Kind switch
    {
        "mute" => "id = $1",
        "rule" => "id = $1",
        _ => "server_id = $1 AND coalesce(database_name, '') = coalesce($2::text, '') AND threshold_key = $3",
    };

    private NpgsqlParameter[] KeyParams(RowKey key) => key.Kind switch
    {
        "mute" => [PText(key.Id)],
        "rule" => [P(key.RuleId!.Value)],
        _ => [P(key.ServerId), PText(key.Database), PText(key.Id)],
    };

    private static int Next(RowKey key) => key.Kind == "override" ? 4 : 2;

    private async Task<JsonObject?> ReadCurrentAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Spec spec, RowKey key, CancellationToken cancellationToken)
    {
        var columns = string.Join(", ", spec.Fields.Select(f => f.Type == T.Json ? $"{f.Name}::text" : f.Name));
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
                T.Bool => JsonValue.Create(reader.GetBoolean(i)),
                T.Int => JsonValue.Create(reader.GetInt32(i)),
                T.Double => JsonValue.Create(reader.GetDouble(i)),
                T.Time => JsonValue.Create(Utc(reader.GetDateTime(i)).ToString("o", CultureInfo.InvariantCulture)),
                T.Json => JsonNode.Parse(reader.GetString(i)),
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

    private static bool SameValue(T type, JsonNode? a, JsonNode? b)
    {
        if (a is null || b is null)
            return a is null && b is null;
        switch (type)
        {
            case T.Double:
                return a.GetValue<double>() == b.GetValue<double>();
            case T.Time:
                return DateTime.TryParse(a.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var x)
                    && DateTime.TryParse(b.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var y)
                    && x == y;
            case T.Json:
                return JsonNode.DeepEquals(Structured(a), Structured(b));
            case T.Bool:
                return a.GetValue<bool>() == b.GetValue<bool>();
            case T.Int:
                return a.GetValue<int>() == b.GetValue<int>();
            default:
                return string.Equals(a.GetValue<string>(), b.GetValue<string>(), StringComparison.Ordinal);
        }
    }

    private static JsonNode? Structured(JsonNode node) =>
        node is JsonValue v && v.TryGetValue(out string? s) && s is not null && (s.TrimStart().StartsWith('{') || s.TrimStart().StartsWith('['))
            ? JsonNode.Parse(s) : node;

    private async Task DeleteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Spec spec, RowKey key, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, $"DELETE FROM {spec.Table} WHERE {Where(key)}", KeyParams(key));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private NpgsqlParameter FieldParam(T type, JsonNode? value) => type switch
    {
        T.Bool => new NpgsqlParameter<bool?> { TypedValue = value?.GetValue<bool>(), NpgsqlDbType = NpgsqlDbType.Boolean },
        T.Int => new NpgsqlParameter<int?> { TypedValue = value?.GetValue<int>(), NpgsqlDbType = NpgsqlDbType.Integer },
        T.Double => new NpgsqlParameter<double?> { TypedValue = value?.GetValue<double>(), NpgsqlDbType = NpgsqlDbType.Double },
        T.Time => new NpgsqlParameter<DateTime?>
        {
            TypedValue = value is null ? null : DateTime.SpecifyKind(DateTime.Parse(value.ToString(), CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal), DateTimeKind.Unspecified),
            NpgsqlDbType = NpgsqlDbType.Timestamp,
        },
        T.Json => new NpgsqlParameter<string?> { TypedValue = value is null ? null : (Structured(value) ?? value).ToJsonString(), NpgsqlDbType = NpgsqlDbType.Jsonb },
        _ => PText(value?.GetValue<string>()),
    };

    private async Task UpdateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Spec spec, RowKey key, JsonObject before, string actor, CancellationToken cancellationToken)
    {
        var parameters = KeyParams(key).ToList();
        var sets = new List<string>();
        var n = Next(key);
        foreach (var (name, type) in spec.Fields)
        {
            sets.Add($"{name} = ${n++}");
            parameters.Add(FieldParam(type, before[name]));
        }

        var extra = key.Kind switch
        {
            "rule" => $", version = version + 1, updated_at = (now() AT TIME ZONE 'UTC'), updated_by = ${n}",
            "override" => $", updated_at = now(), set_by = ${n}",
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
            case "mute":
                Add("id", PText(key.Id));
                Add("created_at_utc", FieldParam(T.Time, JsonValue.Create(Now().ToString("o", CultureInfo.InvariantCulture))));
                break;
            case "rule":
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
            Add(name, FieldParam(type, image[name]));
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

    private async Task JournalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string journalEvent, string actor,
        Guid? requestId, int? serverId, string detailJson, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, InvestigationRequestStore.AgentActionJournalSql,
            new NpgsqlParameter<DateTime> { TypedValue = Now(), NpgsqlDbType = NpgsqlDbType.TimestampTz },
            new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid },
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            PText(journalEvent), PText(actor), PText(null), PText(detailJson));
        await command.ExecuteScalarAsync(cancellationToken);
    }

    /// <summary>The journal actor for a person's undo or switch: <c>person:&lt;login&gt;</c>, the login trimmed and held to the identity shape.</summary>
    public static string PersonActor(string login)
    {
        var trimmed = (login ?? "").Trim();
        if (trimmed.Length is 0 or > 64 || trimmed.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-' or '@' or '\\')))
            throw new ArgumentException("The login must be 1 to 64 letters, digits or . _ : - @ \\.", nameof(login));
        return "person:" + trimmed;
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
