/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// What the MCP host serves, as a service parameter a tool can take (#5916): the one <see cref="DarlingMcpWritePolicy.Effective"/>
/// the host started with. A Level 2 action tool needs it to tell its two arms apart.
/// </summary>
public sealed class DarlingMcpServedPolicy
{
    internal DarlingMcpServedPolicy(DarlingMcpWritePolicy.Effective policy, IReadOnlySet<string>? namedAgentNames = null)
    {
        Policy = policy;
        NamedAgentNames = namedAgentNames ?? new HashSet<string>();
    }

    internal DarlingMcpWritePolicy.Effective Policy { get; }

    /// <summary>The names of the named agent tokens this host serves (#6542), empty when it serves none. A legacy-token call may not
    /// claim as one of them: the journal would credit that named agent with a claim it never made.</summary>
    internal IReadOnlySet<string> NamedAgentNames { get; }
}

/// <summary>
/// <c>set_alert_threshold_override</c> (#5916): set or clear a threshold for one server, or one database on it, without
/// touching the global value. Offered under both arms. Under the writes arm it is an ordinary write tool and
/// <c>agent_action</c> is ignored. Under the investigations arm it is a Level 2 action: <c>agent_action</c> is required and
/// the change goes through <see cref="DarlingAgentActionGate"/>, for the request's own server only, attributed to the
/// agent and the request. An agent changes a threshold by override only; <c>update_alert_settings</c> is never offered to it.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpThresholdOverrideTools
{
    internal const string ToolName = "set_alert_threshold_override";
    internal const string Kind = ThresholdOverrideImage.Kind;

    private static readonly Regex SetByShape = new("^[A-Za-z0-9._:-]{1,64}$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The table, schema-qualified; the mcp role holds SELECT, INSERT, UPDATE and DELETE on it (#5920).</summary>
    internal const string Table = ThresholdOverrideStore.Table;

    internal const string PreconditionMessage =
        "The service's mcp store role has no grant on config.config_threshold_overrides. A bring-your-own store needs " +
        DarlingManagedRoles.ProvisionRolesScriptPath + " re-run (it is idempotent; single-table grants only reach a store when the script " +
        "runs again) as a superuser. Managed and compose stores grant it themselves on start. Call again after the script has run.";

    [McpServerTool(Name = "set_alert_threshold_override"), Description(
        "Sets or clears a threshold for ONE server (or one database on it), or for a server TAG, leaving the global value alone. Changes the " +
        "monitoring store only; the service picks it up within its reload interval. <<GUIDE>> " +
        "key is a tunable integer threshold named as get_alert_settings names it (for example cpu.threshold_percent, " +
        "blocking.pg_count_threshold); get_alert_settings with server_name lists the keys, the global value, the effective value " +
        "and where it comes from. value is an integer inside the same bounds the global setting has. clear: true removes the " +
        "override so the global value applies again. tag (a tag name or id, in place of server_name) holds the value for every server under that tag " +
        "and every tag below it: the writes arm only, and no database_name. Precedence is database, then server, then tag (the deepest tag a server " +
        "is under, as a notification route resolves it, the older override winning a same-depth tie), then global. database_name applies only to pvs.* and file_growth.rise_mb / " +
        "file_growth.volume_percent. Refused: a key outside that list, a self_alerts.*, enable, cooldown or route key, an " +
        "out-of-bounds value. To silence an alert, mute it. An agent cannot set a threshold to the value that turns its alert off (the top of the " +
        "key's range, or zero for the low_disk.* keys); that value is refused as threshold_disables_alert. A very high count below " +
        "the top is allowed, and its alert still fires if that count is reached. " +
        "set_by names who made the change (writes arm; default mcp). On an investigation request's agent arm, agent_action is " +
        "REQUIRED and the change is for the request's server only; the writes arm ignores agent_action. " +
        "The answer carries previous_override (null when there was none) and undo, the call that puts the override back as it was (the same call with the previous value, or clear: true). " +
        "status ok, cleared, refused, precondition (the store role lacks the grant; the message names provision-roles.sql).")]
    public static async Task<string> SetAlertThresholdOverride(
        NpgsqlDataSource postgres,
        DarlingMcpServedPolicy served,
        ILogger logger,
        [Description("Server name, or the start of it. Omit when tag is given.")] string? server_name = null,
        [Description("A tunable threshold, as get_alert_settings names it, e.g. cpu.threshold_percent.")] string key = "",
        [Description("The override value (integer).")] int? value = null,
        [Description("Database on that server (pvs.*, file_growth.rise_mb, file_growth.volume_percent). Omit for the whole server.")] string? database_name = null,
        [Description("true removes the override.")] bool clear = false,
        [Description("Who made the change (writes arm). 1-64 of A-Za-z0-9._:-. Default mcp.")] string? set_by = null,
        [Description("Investigations arm only: the agent_action object (request_id, claim_id, agent_id, reason, evidence). Ignored under the writes arm.")] string? agent_action = null,
        [Description("A server tag, by name or id, in place of server_name: the value applies to every server under the tag and the tags below it (#6926). Writes arm only; no database_name.")] string? tag = null,
        StoreOwnerSource? ownerSource = null,
        CancellationToken cancellationToken = default) =>
        await SetAsync(postgres, served.Policy, new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, agentActionSource: ownerSource?.Read()),
            logger, server_name, key, value, database_name, clear, set_by, agent_action, cancellationToken, served.NamedAgentNames, tag);

    /// <summary>The tool body over a given store, so a test drives either arm.</summary>
    internal static async Task<string> SetAsync(
        NpgsqlDataSource postgres, DarlingMcpWritePolicy.Effective policy, InvestigationRequestStore store, ILogger logger,
        string? server_name, string key, int? value, string? database_name, bool clear, string? set_by, string? agent_action,
        CancellationToken cancellationToken, IReadOnlySet<string>? namedAgentNames = null, string? tag = null)
    {
        try
        {
            if (DarlingThresholdKeys.RefusalFor(key) is { } keyRefusal)
                return Refused(keyRefusal.Reason, keyRefusal.Message);
            DarlingThresholdKeys.TryGet(key, out var entry);
            var wire = entry.Wire;
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();
            var hasTag = !string.IsNullOrWhiteSpace(tag);
            if (hasTag && !string.IsNullOrWhiteSpace(server_name))
                return Refused("scope_conflict", "Give a server_name or a tag, not both.");
            if (!hasTag && string.IsNullOrWhiteSpace(server_name))
                return Refused("scope_required", "server_name (or tag) is required.");
            if (hasTag && database is not null)
                return Refused("database_scope", "A tag override holds a server-level value, so it takes no database_name.");
            if (hasTag && !policy.Writes)
                return Refused("tag_writes_arm_only", "A tag override changes every server under the tag, so it is set on the writes arm, not by an investigation request's agent, whose change is for the request's server only.");

            if (!clear)
            {
                if (value is null)
                    return Refused("value_required", "value is required (an integer), or pass clear: true to remove the override.");
                if (DarlingThresholdKeys.ValidateValue(entry, value.Value, database) is { } valueRefusal)
                    return Refused(valueRefusal.Reason, valueRefusal.Message);
            }
            else if (database is not null && entry.Scope != PerformanceMonitor.Alerting.AlertThresholdScope.ServerAndDatabase)
            {
                return Refused("database_scope", $"'{wire}' takes a server override only, not a database_name.");
            }

            string? person = null;
            if (policy.Writes)
            {
                person = string.IsNullOrWhiteSpace(set_by) ? "mcp" : set_by.Trim();
                if (!SetByShape.IsMatch(person))
                    return Refused("set_by_invalid", "set_by must be 1-64 of A-Za-z0-9._:-.");
            }

            if (hasTag)
                return await SetTagAsync(postgres, tag!, wire, value, clear, person!, cancellationToken);

            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name!, cancellationToken, NameClash.Refuse);
            if (error is not null)
                return error;

            if (!await HoldsGrantAsync(postgres, cancellationToken))
                return McpHelpers.Status("precondition", PreconditionMessage);

            var applied = new Applied();
            /* The gate validates the argument itself (check 1); this parse only names the agent and request on the row. */
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            async Task<string?> ReadBefore(NpgsqlConnection c, NpgsqlTransaction t, CancellationToken ct)
            {
                applied.Previous = await ThresholdOverrideStore.ReadValueAsync(c, t, resolved.ServerId, database, wire, ct);
                return applied.Previous is null ? null : JsonSerializer.Serialize(new ThresholdOverrideImage(applied.Previous.Value));
            }

            async Task<string?> Write(NpgsqlConnection c, NpgsqlTransaction t, CancellationToken ct)
            {
                /* Attribution: the writes arm names the person; an agent change names the agent and the request. */
                var by = person ?? "agent:" + action?.AgentId;
                Guid? requestId = person is null ? action?.RequestId : null;
                applied.Previous = await ThresholdOverrideStore.ReadValueAsync(c, t, resolved.ServerId, database, wire, ct);
                if (clear)
                    await ThresholdOverrideStore.DeleteAsync(c, t, resolved.ServerId, database, wire, ct);
                else
                    await ThresholdOverrideStore.UpsertAsync(c, t, resolved.ServerId, database, wire, value!.Value, by, requestId, ct);
                applied.SetBy = by;
                applied.RequestId = requestId;
                return clear ? null : JsonSerializer.Serialize(new ThresholdOverrideImage(value!.Value));
            }

            if (policy.Writes)
            {
                await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                await Write(connection, transaction, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
            }
            else
            {
                var outcome = await DarlingAgentActionGate.RunAsync(
                    policy, postgres, store, logger, ToolName, Kind, agent_action,
                    new AgentActionTarget(resolved.ServerId, resolved.ServerName, wire, database),
                    (_, _, context, _) => Task.FromResult(
                        action is not null && !clear ? AgentKindRule(entry, value!.Value) : null),
                    ReadBefore, Write, cancellationToken, namedAgentNames);
                if (outcome.Refusal is { } refusal)
                    return DarlingAgentActionGate.RefusalAnswer(refusal);
            }

            return JsonSerializer.Serialize(new
            {
                status = clear ? "cleared" : "ok",
                server = resolved.DisplayName,
                server_id = resolved.ServerId,
                key = wire,
                database = database,
                value = clear ? (int?)null : value,
                previous_override = applied.Previous,
                /* #6403: the call that puts the override back as it was: the same tool with the previous value, or clear when there was none. */
                undo = DarlingMcpAlertTools.UndoCall(ToolName, applied.Previous is { } was
                    ? new System.Collections.Generic.Dictionary<string, object?>
                    {
                        ["server_name"] = resolved.DisplayName, ["key"] = wire, ["value"] = was, ["database_name"] = database,
                    }
                    : new System.Collections.Generic.Dictionary<string, object?>
                    {
                        ["server_name"] = resolved.DisplayName, ["key"] = wire, ["clear"] = true, ["database_name"] = database,
                    }),
                set_by = applied.SetBy,
                request_id = applied.RequestId,
                message = clear
                    ? "The override is removed; the global value applies to this server again."
                    : "The override is stored. The service applies it within its reload interval; the global value is unchanged."
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", PreconditionMessage);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ForeignKeyViolation)
        {
            /* The only foreign key on the table is the server's registry row: the server was removed from the configuration and
               the reload that drops it from collect.servers has not run yet, so the name still resolved. */
            return Refused("server_not_registered",
                "That server is no longer in the monitored-server configuration, so it cannot hold an override. Check list_servers; the registry catches up within its reload interval.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    /// <summary>The tag branch of <see cref="SetAsync"/> (#6926), writes arm only: resolve the tag, then set or clear its value in one transaction.
    /// A tag with no servers under it is allowed (servers are tagged later and take the value then).</summary>
    private static async Task<string> SetTagAsync(
        NpgsqlDataSource postgres, string tagText, string wire, int? value, bool clear, string by, CancellationToken cancellationToken)
    {
        var (tagId, tagName, problem) = await ThresholdOverrideStore.ResolveTagAsync(postgres, tagText, cancellationToken);
        /* A clear by id still works for an override whose tag was deleted (it is listed as orphaned): the row is found by its tag id below. */
        var orphanClear = problem is not null && clear
            && int.TryParse(tagText.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var orphanId) && (tagId = orphanId) is not null;
        if (problem is not null && !orphanClear)
            return Refused("tag_not_found", problem);
        if (!await HoldsGrantAsync(postgres, cancellationToken))
            return McpHelpers.Status("precondition", PreconditionMessage);

        double? previous;
        await using (var connection = await postgres.OpenConnectionAsync(cancellationToken))
        await using (var transaction = await connection.BeginTransactionAsync(cancellationToken))
        {
            previous = await ThresholdOverrideStore.ReadTagValueAsync(connection, transaction, tagId!.Value, wire, cancellationToken);
            if (orphanClear && previous is null)
                return Refused("tag_not_found", problem!);
            if (clear)
                await ThresholdOverrideStore.DeleteTagAsync(connection, transaction, tagId.Value, wire, cancellationToken);
            else
                await ThresholdOverrideStore.UpsertTagAsync(connection, transaction, tagId.Value, wire, value!.Value, by, null, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }

        return JsonSerializer.Serialize(new
        {
            status = clear ? "cleared" : "ok",
            tag = tagName,
            tag_id = tagId,
            key = wire,
            value = clear ? (int?)null : value,
            previous_override = previous,
            undo = DarlingMcpAlertTools.UndoCall(ToolName, previous is { } was
                ? new System.Collections.Generic.Dictionary<string, object?> { ["tag"] = tagId!.Value.ToString(CultureInfo.InvariantCulture), ["key"] = wire, ["value"] = was }
                : new System.Collections.Generic.Dictionary<string, object?> { ["tag"] = tagId!.Value.ToString(CultureInfo.InvariantCulture), ["key"] = wire, ["clear"] = true }),
            set_by = by,
            message = clear
                ? "The tag override is removed; servers under the tag fall back to a shallower tag, or the global value."
                : "The tag override is stored. The service applies it within its reload interval to every server under the tag that has no server or database override of its own."
        }, McpHelpers.JsonOptions);
    }

    private sealed class Applied
    {
        internal double? Previous;
        internal string? SetBy;
        internal Guid? RequestId;
    }

    /// <summary>
    /// Check 7, the kind rule: an agent turns an alert off only by a mute, never by a threshold. Every key has one off end
    /// (<see cref="DarlingThresholdKeys.Entry.OffEnd"/>: the top of its range, or zero for the Low Disk bars) and the agent
    /// is refused a value AT that end, whatever the key. There is no cap and no band: a very high count one below the top
    /// still fires when that count is reached, and is allowed.
    /// </summary>
    internal static AgentActionRefusal? AgentKindRule(DarlingThresholdKeys.Entry entry, int value)
    {
        return entry.OffEnd is { } off && value == off
            ? new AgentActionRefusal("threshold_disables_alert",
                $"{value.ToString(CultureInfo.InvariantCulture)} on '{entry.Wire}' is the value that turns the alert off. A threshold override only moves the bar; to silence an alert, create a mute rule.")
            : null;
    }

    private static string Refused(string reason, string message) =>
        JsonSerializer.Serialize(new { status = "refused", reason, message }, McpHelpers.JsonOptions);

    /// <summary>True when the connected role may write the overrides table. A missing grant is the BYO store before the
    /// provisioning script re-run (#5920), which the tool names rather than answering with a raw permission error.</summary>
    internal static async Task<bool> HoldsGrantAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(
            "SELECT COALESCE(has_table_privilege(current_user, to_regclass('" + Table + "'), 'SELECT') " +
            "AND has_table_privilege(current_user, to_regclass('" + Table + "'), 'INSERT') " +
            "AND has_table_privilege(current_user, to_regclass('" + Table + "'), 'UPDATE') " +
            "AND has_table_privilege(current_user, to_regclass('" + Table + "'), 'DELETE'), false)");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    /// <summary>
    /// The <c>get_alert_settings</c> view for one server (#5916): every tunable threshold with its global value, the value in
    /// effect for this server and where that value comes from (<c>global</c>, <c>tag_override</c> naming the tag, or <c>server_override</c>),
    /// and the database overrides the server holds. A read of the overrides table, so it answers what the service will apply after its next reload.
    /// </summary>
    internal static async Task<object> BuildServerViewAsync(
        NpgsqlDataSource postgres, DarlingAlertReader.AlertSettingsReadRow s, int serverId, string serverName,
        CancellationToken cancellationToken)
    {
        var rows = new System.Collections.Generic.List<(string? Database, string Key, double Value, string SetBy, Guid? RequestId, DateTime UpdatedAt)>();
        string? note = null;
        try
        {
            await using var command = postgres.CreateCommand(
                "SELECT database_name, threshold_key, value, set_by, request_id, updated_at FROM " + Table + " WHERE server_id = $1");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetString(1), reader.GetDouble(2), reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetGuid(4), reader.GetDateTime(5)));
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            note = PreconditionMessage;
        }

        /* #6926: the tag overrides that reach this server, resolved by the notification routes' own tag rule (TagThresholdResolver), the same
           one the service loads. A tag read that fails reaches no server: the view then shows what the engine would apply. */
        var tagWinners = new System.Collections.Generic.Dictionary<string, (int TagId, string TagName, double Value, string SetBy, DateTime UpdatedAt)>();
        try
        {
            var candidates = new System.Collections.Generic.List<TagOverrideCandidate>();
            var meta = new System.Collections.Generic.Dictionary<(int Tag, string Key), (string SetBy, DateTime UpdatedAt)>();
            await using (var command = postgres.CreateCommand(
                "SELECT id, scope_tag_id, threshold_key, value, set_by, updated_at FROM " + Table + " WHERE scope_tag_id IS NOT NULL"))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var key = reader.GetString(2);
                    var raw = reader.GetDouble(3);
                    /* The loader's own accept rule (known key, integer, inside the key's bounds), so a row the engine skips never names a tag here. */
                    if (DarlingThresholdOverrides.Accept(key, raw, string.Empty, out _) is not null)
                        continue;
                    candidates.Add(new TagOverrideCandidate(reader.GetInt64(0), reader.GetInt32(1), key, raw));
                    meta[(reader.GetInt32(1), key)] = (reader.GetString(4), reader.GetDateTime(5));
                }
            }

            if (candidates.Count > 0)
            {
                await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
                var inputs = await StoreConfigProvider.ReadRouteScopeInputsAsync(connection, null, cancellationToken);
                var names = new System.Collections.Generic.Dictionary<int, string>();
                await using (var command = postgres.CreateCommand("SELECT id, name FROM config.server_tags"))
                {
                    command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                    await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                        names[reader.GetInt32(0)] = reader.GetString(1);
                }

                foreach (var ((server, key), winner) in TagThresholdResolver.Resolve(
                             candidates, inputs.KnownServerIds, inputs.TagParents, inputs.ServerTags, inputs.TagsReadable))
                {
                    if (server != serverId)
                        continue;
                    var m = meta[(winner.TagId, key)];
                    tagWinners[key] = (winner.TagId, names.GetValueOrDefault(winner.TagId, string.Empty), winner.Value, m.SetBy, m.UpdatedAt);
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            note ??= PreconditionMessage;
        }

        /* One number per key, read the way the engine reads it (#5916): the global value and the value with this server's
           override applied each go through the same pair clamp the enclosing get_alert_settings payload applies, so an
           override that inverts a warning/critical pair shows the pair the engine evaluates, not the stored one. */
        int? ServerOverride(string wire)
        {
            foreach (var r in rows)
            {
                if (r.Database is null && r.Key == wire)
                    return (int)r.Value;
            }

            return null;
        }

        int? TagOverride(string wire) => tagWinners.TryGetValue(wire, out var t) ? (int)t.Value : null;

        var thresholds = new System.Collections.Generic.List<object>();
        foreach (var entry in DarlingThresholdKeys.Entries)
        {
            var global = GlobalValueAsEngineReadsIt(s, entry.Wire);
            var server = ServerOverride(entry.Wire);
            var effective = Evaluated(entry.Wire, w => ServerOverride(w) ?? TagOverride(w) ?? GlobalValue(s, w));
            var found = tagWinners.TryGetValue(entry.Wire, out var tw);
            var tagHit = server is null && found;

            thresholds.Add(new
            {
                key = entry.Wire,
                global_value = global,
                effective_value = effective,
                source = server is not null ? "server_override" : tagHit ? "tag_override" : "global",
                tag = tagHit ? tw.TagName : null,
                tag_id = tagHit ? tw.TagId : (int?)null,
                database_scope = entry.Scope == PerformanceMonitor.Alerting.AlertThresholdScope.ServerAndDatabase
            });
        }

        var databaseOverrides = new System.Collections.Generic.List<object>();
        var serverOverrides = new System.Collections.Generic.List<object>();
        foreach (var r in rows)
        {
            var row = new
            {
                key = r.Key, database = r.Database, value = r.Value, set_by = r.SetBy, request_id = r.RequestId,
                updated_at = r.UpdatedAt.ToString("o", CultureInfo.InvariantCulture)
            };
            (r.Database is null ? serverOverrides : databaseOverrides).Add(row);
        }

        return new
        {
            server = serverName,
            server_id = serverId,
            note = note ?? "effective_value is the global value with this server's override applied, else the deepest server tag's (source tag_override, naming the tag; tag_overrides lists the ones that reach this server), else global; a database override (database_overrides) applies to that database only. Set one with set_alert_threshold_override.",
            thresholds,
            server_overrides = serverOverrides,
            database_overrides = databaseOverrides,
            tag_overrides = tagWinners.Select(t => (object)new
            {
                key = t.Key, tag = t.Value.TagName, tag_id = t.Value.TagId, value = t.Value.Value, set_by = t.Value.SetBy,
                updated_at = t.Value.UpdatedAt.ToString("o", CultureInfo.InvariantCulture)
            }).ToList()
        };
    }

    /// <summary>The global value of <paramref name="wire"/> as the engine evaluates it: the stored column, with the two
    /// warning/critical pairs through their evaluator's clamp. The one definition both the <c>get_alert_settings</c> payload
    /// and the per-server view read.</summary>
    internal static int GlobalValueAsEngineReadsIt(DarlingAlertReader.AlertSettingsReadRow s, string wire) =>
        Evaluated(wire, w => GlobalValue(s, w));

    /// <summary>A bar as the engine evaluates it: <paramref name="read"/> supplies the raw number for a key, and the four
    /// paired bars go through <c>ClampBars</c> (Warning at least 1, Critical never below Warning).</summary>
    private static int Evaluated(string wire, Func<string, int> read) => wire switch
    {
        "runnable_queue.warning_per_scheduler" => RunnableQueueEvaluator.ClampBars(read(wire), read("runnable_queue.critical_per_scheduler")).Warning,
        "runnable_queue.critical_per_scheduler" => RunnableQueueEvaluator.ClampBars(read("runnable_queue.warning_per_scheduler"), read(wire)).Critical,
        "client_timeouts.warning_per_minute" => AttentionEvaluator.ClampBars(read(wire), read("client_timeouts.critical_per_minute")).Warning,
        "client_timeouts.critical_per_minute" => AttentionEvaluator.ClampBars(read("client_timeouts.warning_per_minute"), read(wire)).Critical,
        _ => read(wire),
    };

    /// <summary>The stored column for <paramref name="wire"/>, unclamped.</summary>
    private static int GlobalValue(DarlingAlertReader.AlertSettingsReadRow s, string wire) => wire switch
    {
        "cpu.threshold_percent" => s.CpuThresholdPercent,
        "blocking.count_threshold" => s.BlockingCountThreshold,
        "blocking.wait_threshold_seconds" => s.BlockingWaitSecondsThreshold,
        "deadlocks.count_threshold" => s.DeadlockCountThreshold,
        "runnable_queue.warning_per_scheduler" => s.RunnableQueueWarningPerScheduler,
        "runnable_queue.critical_per_scheduler" => s.RunnableQueueCriticalPerScheduler,
        "client_timeouts.warning_per_minute" => s.ClientTimeoutsWarningPerMinute,
        "client_timeouts.critical_per_minute" => s.ClientTimeoutsCriticalPerMinute,
        "long_running_query.threshold_minutes" => s.LongRunningQueryThresholdMinutes,
        "tempdb_space.threshold_percent" => s.TempDbSpaceThresholdPercent,
        "low_disk.threshold_percent" => s.LowDiskThresholdPercent,
        "low_disk.threshold_gb" => s.LowDiskThresholdGb,
        "low_disk.critical_free_percent" => s.DiskCriticalFreePercent,
        "low_disk.critical_free_gb" => s.DiskCriticalFreeGb,
        "pvs.threshold_percent" => s.PvsThresholdPercent,
        "pvs.floor_gb" => s.PvsFloorGb,
        "file_growth.rise_mb" => s.FileGrowthRiseMb,
        "file_growth.volume_percent" => s.FileGrowthVolumePercent,
        "file_growth.lookback_minutes" => s.FileGrowthLookbackMinutes,
        "long_running_job.multiplier" => s.LongRunningJobMultiplier,
        "failed_job.lookback_minutes" => s.FailedJobLookbackMinutes,
        DarlingThresholdKeys.PgBlockingCount => s.PgBlockingCountThreshold,
        DarlingThresholdKeys.PgDeadlockCount => s.PgDeadlockCountThreshold,
        DarlingThresholdKeys.IdentityCapacityWarning => s.IdentityCapacityWarningPercent,
        DarlingThresholdKeys.IdentityCapacityCritical => s.IdentityCapacityCriticalPercent,
        _ => throw new ArgumentOutOfRangeException(nameof(wire), wire, "Not a tunable threshold."),
    };
}
