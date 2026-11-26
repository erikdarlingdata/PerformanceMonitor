using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #9639: the mute-rule writes, twins of Darling's create_mute_rule, update_mute_rule, delete_mute_rule and set_mute_rule_enabled.
/// Each saves through <see cref="MuteRuleService"/>, the same code Manage Mute Rules runs on Save, so the running app's mute cache
/// follows the change with no restart and the window and the tool cannot disagree. Lite stores a rule's server name, metric, five
/// patterns, reason and expiry, so those are the fields the tools take. Darling's start time, repeat schedule, digest action, server
/// id and external source have no column here and no argument.
/// </summary>
public sealed partial class McpAlertTools
{
    private const string MuteRulesTable = "config_mute_rules";

    /// <summary>Serializes the check-then-insert of a create within this process, so a client retry that arrives while the first call
    /// is in flight sees that call's rule instead of racing it.</summary>
    private static readonly SemaphoreSlim s_muteCreateGate = new(1, 1);

    /// <summary>The wire shape of one rule: what get_mute_rules lists and the write tools echo as the stored rule.</summary>
    internal static object MutePayload(MuteRule r) => new
    {
        id = r.Id,
        enabled = r.Enabled,
        created_at_utc = r.CreatedAtUtc.ToString("o"),
        expires_at_utc = r.ExpiresAtUtc?.ToString("o"),
        reason = r.Reason,
        server_name = r.ServerName,
        metric_name = r.MetricName,
        /* #5654: always null in Lite (no external alert intake); the key keeps one rule shape across products. */
        external_source = (string?)null,
        database_pattern = r.DatabasePattern,
        query_text_pattern = r.QueryTextPattern,
        wait_type_pattern = r.WaitTypePattern,
        job_name_pattern = r.JobNamePattern,
        client_app_pattern = r.ClientAppPattern,
        match_summary = r.Summary
    };

    private static string? Trimmed(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static string Invalid(string parameter, string message) => McpHelpers.Refusal(parameter, message + " Nothing was written.");

    private static string MuteUnavailable(string archivePath) =>
        PreservedTableRestore.IsRestorePendingFor(archivePath, MuteRulesTable)
            ? McpHelpers.Status("unavailable", "Mute rules cannot be changed until Lite restarts: an earlier database reset's saved-settings restore is pending and finishes at the next Lite start.")
            : McpHelpers.Status("unavailable", "The store refused the write, so nothing was saved. Lite's log has the reason.");

    /// <summary>The refusal for a pattern the rule's metric never carries (the window hides that box and drops it on Save), or null.</summary>
    internal static string? PatternScopeRefusal(MuteRule rule, out string parameter)
    {
        var metric = rule.MetricName;
        parameter = "metric_name";
        (string Name, string? Value, bool Allowed)[] fields =
        [
            ("database_pattern", rule.DatabasePattern, MuteRulePatternFields.AllowsDatabase(metric)),
            ("query_text_pattern", rule.QueryTextPattern, MuteRulePatternFields.AllowsQueryText(metric)),
            ("wait_type_pattern", rule.WaitTypePattern, MuteRulePatternFields.AllowsWaitType(metric)),
            ("job_name_pattern", rule.JobNamePattern, MuteRulePatternFields.AllowsJobName(metric)),
            ("client_app_pattern", rule.ClientAppPattern, MuteRulePatternFields.AllowsClientApp(metric)),
        ];
        foreach (var (name, value, allowed) in fields)
        {
            if (value is not null && !allowed)
            {
                parameter = name;
                return $"{name} does not apply to the metric '{metric}': its alerts do not carry that dimension, so the rule would match nothing. Clear {name}, or change metric_name.";
            }
        }

        return null;
    }

    private static bool TryParseUtc(string text, out DateTime utc)
    {
        if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
        {
            utc = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
            return true;
        }

        utc = default;
        return false;
    }

    private static MuteRule? FindMute(MuteRuleService service, string ruleId) =>
        service.GetRules().FirstOrDefault(r => string.Equals(r.Id, ruleId, StringComparison.Ordinal));

    [McpServerTool(Name = "create_mute_rule"), Description(
        "Write: creates an alert mute rule: matching alerts are still logged, just not delivered. Changes alert configuration the app delivers on. Provide any combination of scope/pattern fields; a field left out does not narrow the rule, so a rule with NO fields set matches and mutes EVERY alert on every server. No confirm step: the running app uses the rule at once. Returns the stored rule with its generated id, for delete_mute_rule or set_mute_rule_enabled. Served only when mcp_allow_writes is true.<<GUIDE>>" +
        "Write: creates an alert mute rule, the Manage Mute Rules window's Add. Matching alerts are still logged, just not delivered. Changes alert configuration the app delivers on: a field left out does not narrow the rule, so a rule with NO scope or pattern set mutes EVERY alert on every server (the window asks for confirmation here; this tool does not). " +
        "Served only when mcp_allow_writes is true. server_name and metric_name match the alert's exact values (see get_alert_history, get_mute_rules); the *_pattern fields are case-insensitive substrings, and each applies only to the metrics whose alerts carry that dimension (the window hides the other boxes), so a pattern the metric never carries is refused. " +
        "expires_at is an optional ISO-8601 UTC instant in the future after which the rule stops applying; omit it for a permanent rule. The running app uses the rule at once. " +
        "Returns {status:\"created\", mute_rule:{...} as stored, previous:null, undo (the delete_mute_rule call)}, or {status:\"already_exists\", rule_id, mute_rule} when an enabled, unexpired rule already holds the same scope, patterns and expiry (nothing is written; a retry is safe), {status:\"invalid\"} with nothing written, or {status:\"unavailable\"} while an earlier database reset's saved-settings restore is pending. " +
        "Lite has no start time, repeat schedule, digest action, server_id or external_source for a rule, so this tool takes none of them.")]
    public static Task<string> CreateMuteRule(
        LocalDataService dataService,
        MuteRuleService muteRuleService,
        [Description("Scope the rule to this server (its display name, as get_alert_history reports). Omit for all servers.")] string? server_name = null,
        [Description("Scope to this alert metric (e.g. 'High CPU', 'Blocking Detected', 'Deadlocks Detected'). Omit for all metrics.")] string? metric_name = null,
        [Description("Case-insensitive substring the alert's database name must contain. Omit for any database.")] string? database_pattern = null,
        [Description("Case-insensitive substring the alert's query text must contain. Omit for any query.")] string? query_text_pattern = null,
        [Description("Case-insensitive substring the alert's wait type must contain. Omit for any wait type.")] string? wait_type_pattern = null,
        [Description("Case-insensitive substring the alert's job name must contain. Omit for any job.")] string? job_name_pattern = null,
        [Description("Case-insensitive substring of the alert's client application name (e.g. 'Management Studio'). Omit for any.")] string? client_app_pattern = null,
        [Description("Optional human-readable reason, shown in the mute-rule list.")] string? reason = null,
        [Description("Optional ISO-8601 UTC expiry (e.g. 2027-08-01T00:00:00Z), which must be in the future; after this the rule no longer mutes. Omit for a permanent rule.")] string? expires_at = null) =>
        CreateMuteRuleAsync(muteRuleService, dataService.Database.ArchivePath, server_name, metric_name, database_pattern, query_text_pattern,
            wait_type_pattern, job_name_pattern, client_app_pattern, reason, expires_at);

    internal static async Task<string> CreateMuteRuleAsync(
        MuteRuleService service, string archivePath, string? server_name, string? metric_name, string? database_pattern, string? query_text_pattern,
        string? wait_type_pattern, string? job_name_pattern, string? client_app_pattern, string? reason, string? expires_at, DateTime? nowUtc = null)
    {
        try
        {
            var now = nowUtc ?? DateTime.UtcNow;
            DateTime? expiresAtUtc = null;
            if (!string.IsNullOrWhiteSpace(expires_at))
            {
                if (!TryParseUtc(expires_at, out var parsed))
                {
                    return Invalid("expires_at", $"expires_at '{expires_at}' is not a valid ISO-8601 timestamp (e.g. 2027-08-01T00:00:00Z).");
                }

                if (parsed <= now)
                {
                    return Invalid("expires_at", $"expires_at ({parsed:o}) is already in the past, so the rule would be born expired and mute nothing. Send a future instant, or omit it for a permanent rule.");
                }

                expiresAtUtc = parsed;
            }

            var rule = new MuteRule
            {
                Enabled = true,
                CreatedAtUtc = now,
                ExpiresAtUtc = expiresAtUtc,
                Reason = Trimmed(reason),
                ServerName = Trimmed(server_name),
                MetricName = Trimmed(metric_name),
                DatabasePattern = Trimmed(database_pattern),
                QueryTextPattern = Trimmed(query_text_pattern),
                WaitTypePattern = Trimmed(wait_type_pattern),
                JobNamePattern = Trimmed(job_name_pattern),
                ClientAppPattern = Trimmed(client_app_pattern),
            };

            if (PatternScopeRefusal(rule, out var scopeParameter) is { } scopeProblem)
            {
                return Invalid(scopeParameter, scopeProblem);
            }

            if (PreservedTableRestore.IsRestorePendingFor(archivePath, MuteRulesTable))
            {
                return MuteUnavailable(archivePath);
            }

            await s_muteCreateGate.WaitAsync();
            try
            {
                var existing = service.GetRules().FirstOrDefault(r => r.Enabled && !r.IsExpiredAt(now) && SameScope(r, rule));
                if (existing is not null)
                {
                    return JsonSerializer.Serialize(new
                    {
                        status = "already_exists",
                        message = "An enabled mute rule with the same scope, patterns and expiry is already in force, so nothing was created. " +
                                  "Use its rule_id with set_mute_rule_enabled, update_mute_rule or delete_mute_rule; a different expires_at makes a different rule.",
                        rule_id = existing.Id,
                        mute_rule = MutePayload(existing),
                    }, McpHelpers.JsonOptions);
                }

                if (!await service.AddRuleAsync(rule))
                {
                    return MuteUnavailable(archivePath);
                }
            }
            finally
            {
                s_muteCreateGate.Release();
            }

            return JsonSerializer.Serialize(new
            {
                status = "created",
                mute_rule = MutePayload(FindMute(service, rule.Id) ?? rule),
                previous = (object?)null,
                undo = UndoCall("delete_mute_rule", new { rule_id = rule.Id }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_mute_rule", ex);
        }
    }

    private static bool SameScope(MuteRule a, MuteRule b) =>
        Same(a.ServerName, b.ServerName) && Same(a.MetricName, b.MetricName) && Same(a.DatabasePattern, b.DatabasePattern)
        && Same(a.QueryTextPattern, b.QueryTextPattern) && Same(a.WaitTypePattern, b.WaitTypePattern) && Same(a.JobNamePattern, b.JobNamePattern)
        && Same(a.ClientAppPattern, b.ClientAppPattern) && a.ExpiresAtUtc == b.ExpiresAtUtc;

    private static bool Same(string? a, string? b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

    [McpServerTool(Name = "set_mute_rule_enabled"), Description(
        "Write: enables or disables an existing mute rule by id, WITHOUT deleting it. A disabled rule suppresses nothing but keeps its id, scope, reason and creation date, so this is the reversible change, unlike delete+create. Returns updated (rule as stored), unchanged (already that value, safe to retry), or not_found. Served only when mcp_allow_writes is true.<<GUIDE>>" +
        "Write: enables or disables an existing mute rule by its id (from get_mute_rules or create_mute_rule) WITHOUT deleting it, the checkbox in Manage Mute Rules. A disabled rule suppresses nothing but keeps its id, scope, reason and creation date, so prefer this to delete_mute_rule then create_mute_rule for a reversible change. " +
        "Served only when mcp_allow_writes is true. Returns {status:\"updated\", mute_rule:{...} as stored, previous {enabled}, undo}, {status:\"unchanged\", ...} when the rule already holds that value (a retry is safe and writes nothing), {status:\"not_found\"}, or {status:\"unavailable\"} while an earlier database reset's saved-settings restore is pending. The running app uses the change at once.")]
    public static Task<string> SetMuteRuleEnabled(
        LocalDataService dataService,
        MuteRuleService muteRuleService,
        [Description("The id of the mute rule to enable or disable (from get_mute_rules or create_mute_rule).")] string rule_id,
        [Description("true to put the rule back in force, false to stop it suppressing while keeping the rule.")] bool enabled) =>
        SetMuteRuleEnabledAsync(muteRuleService, dataService.Database.ArchivePath, rule_id, enabled);

    internal static async Task<string> SetMuteRuleEnabledAsync(MuteRuleService service, string archivePath, string rule_id, bool enabled)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(rule_id))
            {
                return Invalid("rule_id", "rule_id is required.");
            }

            var existing = FindMute(service, rule_id);
            if (existing is null)
            {
                return McpHelpers.Status("not_found", $"No mute rule with id '{rule_id}'.");
            }

            /* MuteRuleService flips Enabled on the cached rule object itself, which is the object FindMute returned.
               Read the old value now: after the save, existing.Enabled already holds the new one. */
            var wasEnabled = existing.Enabled;
            var undo = UndoCall("set_mute_rule_enabled", new { rule_id, enabled = wasEnabled });
            if (wasEnabled == enabled)
            {
                return JsonSerializer.Serialize(new { status = "unchanged", mute_rule = MutePayload(existing), previous = new { enabled = wasEnabled }, undo }, McpHelpers.JsonOptions);
            }

            if (PreservedTableRestore.IsRestorePendingFor(archivePath, MuteRulesTable) || !await service.SetRuleEnabledAsync(rule_id, enabled))
            {
                return MuteUnavailable(archivePath);
            }

            var stored = FindMute(service, rule_id);
            return stored is null
                ? McpHelpers.Status("not_found", $"Mute rule '{rule_id}' was set enabled={(enabled ? "true" : "false")} but is no longer in the store: it was deleted concurrently.")
                : JsonSerializer.Serialize(new { status = "updated", mute_rule = MutePayload(stored), previous = new { enabled = wasEnabled }, undo }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("set_mute_rule_enabled", ex);
        }
    }

    [McpServerTool(Name = "delete_mute_rule"), Description(
        "Write: deletes an alert mute rule by its id (from get_mute_rules or create_mute_rule), the Delete button in Manage Mute Rules. Permanent: the alerts it muted are delivered again at once. Prefer set_mute_rule_enabled for a temporary change. " +
        "Served only when mcp_allow_writes is true. Returns {status:\"deleted\", rule_id, previous {mute_rule}, undo (the create_mute_rule call carrying the deleted rule's fields; the re-created rule is a NEW rule with a new id and creation date, and is enabled)}, {status:\"not_found\"}, or {status:\"unavailable\"} while an earlier database reset's saved-settings restore is pending.")]
    public static Task<string> DeleteMuteRule(
        LocalDataService dataService,
        MuteRuleService muteRuleService,
        [Description("The id of the mute rule to delete (from get_mute_rules or create_mute_rule).")] string rule_id) =>
        DeleteMuteRuleAsync(muteRuleService, dataService.Database.ArchivePath, rule_id);

    internal static async Task<string> DeleteMuteRuleAsync(MuteRuleService service, string archivePath, string rule_id)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(rule_id))
            {
                return Invalid("rule_id", "rule_id is required.");
            }

            var existing = FindMute(service, rule_id);
            if (existing is null)
            {
                return McpHelpers.Status("not_found", $"No mute rule with id '{rule_id}'.");
            }

            if (PreservedTableRestore.IsRestorePendingFor(archivePath, MuteRulesTable) || !await service.RemoveRuleAsync(rule_id))
            {
                return MuteUnavailable(archivePath);
            }

            var arguments = new Dictionary<string, object>();
            void Add(string name, object? value)
            {
                if (value is not null)
                {
                    arguments[name] = value;
                }
            }

            Add("server_name", existing.ServerName);
            Add("metric_name", existing.MetricName);
            Add("database_pattern", existing.DatabasePattern);
            Add("query_text_pattern", existing.QueryTextPattern);
            Add("wait_type_pattern", existing.WaitTypePattern);
            Add("job_name_pattern", existing.JobNamePattern);
            Add("client_app_pattern", existing.ClientAppPattern);
            Add("reason", existing.Reason);
            Add("expires_at", existing.ExpiresAtUtc?.ToString("o"));

            return JsonSerializer.Serialize(new
            {
                status = "deleted",
                rule_id,
                previous = new { mute_rule = MutePayload(existing) },
                undo = UndoCall("create_mute_rule", arguments),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_mute_rule", ex);
        }
    }

    private static readonly string[] MuteEditableFields =
        ["server_name", "metric_name", "database_pattern", "query_text_pattern", "wait_type_pattern", "job_name_pattern", "client_app_pattern", "reason", "expires_at_utc"];

    [McpServerTool(Name = "update_mute_rule"), Description(
        "Write: edits an existing alert mute rule IN PLACE by its id. PARTIAL update: send only the fields to change, in the shape get_mute_rules returns; a field you do NOT send stays as stored, and an EXPLICIT JSON null CLEARS that field. enabled is NOT editable here (use set_mute_rule_enabled); created_at_utc never moves. Clearing scope fields WIDENS the rule: one left with none mutes EVERY alert. Returns updated, unchanged, not_found, or invalid (nothing written). Served only when mcp_allow_writes is true.<<GUIDE>>" +
        "Write: edits an existing alert mute rule IN PLACE by its id, the Edit button in Manage Mute Rules. PARTIAL update: send only the fields to change, in the shape get_mute_rules returns; a field you do not send stays as stored, and an explicit JSON null CLEARS it. " +
        "Editable fields: server_name, metric_name, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, client_app_pattern, reason, expires_at_utc (null makes the rule permanent; create_mute_rule's expires_at is accepted as an alias, send only one). " +
        "enabled is not editable here (use set_mute_rule_enabled) and created_at_utc never moves. Clearing scope fields WIDENS the rule: one left with none mutes EVERY alert. A pattern the (resulting) metric's alerts never carry is refused. " +
        "Served only when mcp_allow_writes is true. Returns {status:\"updated\", updated_fields, mute_rule as stored, previous (the sent fields as they were), undo (the update_mute_rule call that sets them back)}, {status:\"unchanged\"} when nothing differs (writes nothing), {status:\"not_found\"}, {status:\"invalid\"} with nothing written, or {status:\"unavailable\"} while an earlier database reset's saved-settings restore is pending. " +
        "Lite has no start time, repeat schedule, digest action, server_id or external_source for a rule, so those fields are refused.")]
    public static Task<string> UpdateMuteRule(
        LocalDataService dataService,
        MuteRuleService muteRuleService,
        [Description("The id of the mute rule to edit (from get_mute_rules or create_mute_rule).")] string rule_id,
        [Description("A JSON object with ONLY the mute-rule fields to change, in the shape get_mute_rules returns (e.g. {\"reason\":\"root cause found\"}). An explicit null clears a field; a field not sent does not change.")] string changes_json) =>
        UpdateMuteRuleAsync(muteRuleService, dataService.Database.ArchivePath, rule_id, changes_json);

    internal static async Task<string> UpdateMuteRuleAsync(MuteRuleService service, string archivePath, string rule_id, string changes_json)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(rule_id))
            {
                return Invalid("rule_id", "rule_id is required.");
            }

            JsonNode? root = null;
            string? parseError = null;
            try
            {
                root = JsonNode.Parse(changes_json);
            }
            catch (JsonException ex)
            {
                parseError = JsonParseError.Describe("changes_json", ex);
            }

            if (parseError is not null)
            {
                return Invalid("changes_json", parseError);
            }

            if (root is not JsonObject body)
            {
                return Invalid("changes_json", "changes_json must be a JSON object of the mute-rule fields to change (see get_mute_rules for the shape).");
            }

            var changes = new List<(string Field, Action<MuteRule> Apply)>();
            string? error = null;
            var errorParameter = "changes_json";

            string? AddText(string name, JsonNode? node, Action<MuteRule, string?> set)
            {
                if (node is null)
                {
                    changes.Add((name, r => set(r, null)));
                    return null;
                }

                if (node is JsonValue v && v.TryGetValue<string>(out var s))
                {
                    var trimmed = s.Trim();
                    if (trimmed.Length == 0)
                    {
                        var blank = $"'{name}' is blank. Pass null to clear the field, or a non-blank value to set it.";
                        return blank;
                    }

                    changes.Add((name, r => set(r, trimmed)));
                    return null;
                }

                var notText = $"'{name}' must be a string, or null to clear it.";
                return notText;
            }

            foreach (var prop in body)
            {
                if (error is not null)
                {
                    break;
                }

                var field = prop.Key;
                errorParameter = field;
                switch (field)
                {
                    case "server_name": error = AddText(field, prop.Value, (r, v) => r.ServerName = v); break;
                    case "metric_name": error = AddText(field, prop.Value, (r, v) => r.MetricName = v); break;
                    case "database_pattern": error = AddText(field, prop.Value, (r, v) => r.DatabasePattern = v); break;
                    case "query_text_pattern": error = AddText(field, prop.Value, (r, v) => r.QueryTextPattern = v); break;
                    case "wait_type_pattern": error = AddText(field, prop.Value, (r, v) => r.WaitTypePattern = v); break;
                    case "job_name_pattern": error = AddText(field, prop.Value, (r, v) => r.JobNamePattern = v); break;
                    case "client_app_pattern": error = AddText(field, prop.Value, (r, v) => r.ClientAppPattern = v); break;
                    case "reason": error = AddText(field, prop.Value, (r, v) => r.Reason = v); break;
                    case "expires_at_utc":
                    case "expires_at":
                        if (prop.Value is null)
                        {
                            changes.Add(("expires_at_utc", r => r.ExpiresAtUtc = null));
                        }
                        else if (prop.Value is JsonValue ev && ev.TryGetValue<string>(out var text) && TryParseUtc(text, out var utc))
                        {
                            changes.Add(("expires_at_utc", r => r.ExpiresAtUtc = utc));
                        }
                        else
                        {
                            error = $"'{field}' must be an ISO-8601 UTC timestamp (e.g. 2027-08-01T00:00:00Z), or null for a permanent rule.";
                        }

                        break;
                    case "enabled":
                        error = "'enabled' is not editable here: use set_mute_rule_enabled, which writes the flag alone.";
                        break;
                    case "id":
                        error = "'id' is the rule's identity and cannot be edited. Pass the rule to edit as rule_id; send only the fields to change.";
                        break;
                    case "created_at_utc":
                        error = "'created_at_utc' never moves: it is the date the rule was authored.";
                        break;
                    case "match_summary":
                    case "summary":
                        error = $"'{field}' is derived from the scope fields and is not stored: edit the fields it summarizes instead.";
                        break;
                    default:
                        error = $"Unknown field '{field}'. Editable fields: {string.Join(", ", MuteEditableFields)}. Lite has no start time, repeat schedule, digest action, server_id or external_source for a rule.";
                        break;
                }
            }

            if (error is null && changes.GroupBy(c => c.Field, StringComparer.Ordinal).Any(g => g.Count() > 1))
            {
                errorParameter = "expires_at";
                error = "'expires_at' and 'expires_at_utc' are two names for the same field; send only one.";
            }

            if (error is not null)
            {
                return Invalid(errorParameter, error);
            }

            if (changes.Count == 0)
            {
                return Invalid("changes_json", "No editable mute-rule fields were provided. Send only the fields to change (e.g. {\"reason\":\"...\"}); pass null to clear one.");
            }

            var existing = FindMute(service, rule_id);
            if (existing is null)
            {
                return McpHelpers.Status("not_found", $"No mute rule with id '{rule_id}'.");
            }

            var merged = existing.Clone();
            foreach (var change in changes)
            {
                change.Apply(merged);
            }

            /* The scope check runs only when the call moves the metric or a pattern, so an old rule's other fields stay editable. */
            if (changes.Any(c => c.Field is "metric_name" || c.Field.EndsWith("_pattern", StringComparison.Ordinal))
                && PatternScopeRefusal(merged, out var scopeParameter) is { } scopeProblem)
            {
                return Invalid(scopeParameter, scopeProblem);
            }

            var previous = MutePreviousFields(existing, changes.Select(c => c.Field));
            var undo = UndoCall("update_mute_rule", new { rule_id, changes_json = previous.ToJsonString() });
            if (SameEditable(existing, merged))
            {
                return JsonSerializer.Serialize(new { status = "unchanged", mute_rule = MutePayload(existing), previous, undo }, McpHelpers.JsonOptions);
            }

            if (PreservedTableRestore.IsRestorePendingFor(archivePath, MuteRulesTable) || !await service.UpdateRuleAsync(merged))
            {
                return MuteUnavailable(archivePath);
            }

            var stored = FindMute(service, rule_id);
            return stored is null
                ? McpHelpers.Status("not_found", $"Mute rule '{rule_id}' was updated but is no longer in the store: it was deleted concurrently.")
                : JsonSerializer.Serialize(new
                {
                    status = "updated",
                    updated_fields = changes.Select(c => c.Field).ToArray(),
                    mute_rule = MutePayload(stored),
                    previous,
                    undo,
                }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_mute_rule", ex);
        }
    }

    private static bool SameEditable(MuteRule a, MuteRule b) =>
        string.Equals(a.ServerName, b.ServerName, StringComparison.Ordinal)
        && string.Equals(a.MetricName, b.MetricName, StringComparison.Ordinal)
        && string.Equals(a.DatabasePattern, b.DatabasePattern, StringComparison.Ordinal)
        && string.Equals(a.QueryTextPattern, b.QueryTextPattern, StringComparison.Ordinal)
        && string.Equals(a.WaitTypePattern, b.WaitTypePattern, StringComparison.Ordinal)
        && string.Equals(a.JobNamePattern, b.JobNamePattern, StringComparison.Ordinal)
        && string.Equals(a.ClientAppPattern, b.ClientAppPattern, StringComparison.Ordinal)
        && string.Equals(a.Reason, b.Reason, StringComparison.Ordinal)
        && a.ExpiresAtUtc == b.ExpiresAtUtc;

    /// <summary>The fields an update took charge of, as the rule held them before it, a field that was empty as an explicit null (which clears it again).</summary>
    private static JsonObject MutePreviousFields(MuteRule before, IEnumerable<string> fields)
    {
        var image = (JsonObject)JsonSerializer.SerializeToNode(MutePayload(before), McpHelpers.JsonOptions)!;
        var previous = new JsonObject();
        foreach (var field in fields)
        {
            previous[field] = image[field]?.DeepClone();
        }

        return previous;
    }
}
