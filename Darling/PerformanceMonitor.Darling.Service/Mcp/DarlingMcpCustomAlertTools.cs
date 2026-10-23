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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The custom-alert-rule MANAGEMENT tools (#3285) - the second WRITE surface on Darling's MCP server (the
/// custom-view tools are the first). They let an MCP client list / read / validate / create / update / delete
/// the user-authored alert rules stored in <c>config.custom_alert_rules</c>, so an agent can stand up a
/// threshold alert ("page me when this metric crosses that bar") conversationally, the same rules the
/// <see cref="CustomAlertEvaluator"/> loads and fires on each sweep.
///
/// <para><b>No divergent second implementation.</b> Every tool routes through the EXISTING authority, never a
/// parallel copy: persistence is <see cref="CustomAlertRuleStore"/> (the same typed CRUD +
/// optimistic-concurrency + duplicate-name detection the web editor and the evaluator's own reload use);
/// definition validation is <see cref="CustomAlertRuleDefinition.TryParse"/> (the SAME parser/validator the
/// evaluator applies when it loads a rule, so a rule that stores here is a rule the evaluator can compile and
/// fire, and a rule that validates here is one the evaluator will not skip as broken). Validation always runs
/// BEFORE persistence on create and on any update that carries a new definition, so a stored rule always
/// parses.</para>
///
/// <para><b>Evaluate-now.</b> <see cref="TestCustomAlertRule"/> (#3299) reports a rule's CURRENT value on each
/// in-scope server and whether it WOULD breach right now, delivering and persisting nothing. It does not fork
/// the evaluator: it runs the SAME shared per-server scalar seam the sweep uses
/// (<see cref="CustomAlertEvaluator.EvaluateScalarNowAsync"/> + <see cref="CustomAlertEvaluator.ClassifyTestValue"/>),
/// so a test value can never diverge from what the running rule sees. What a one-shot cannot reproduce is the
/// rule's hysteresis and per-server streak, so a breach here means "true this instant", not "has fired".</para>
///
/// <para><b>Security.</b> These tools connect (like every MCP tool) as the least-privilege <c>mcp</c> role,
/// which is granted INSERT/UPDATE/DELETE on ONLY <c>config.custom_alert_rules</c> (see
/// <see cref="DarlingManagedRoles"/>, V116) - the same single-table write the web editor carries. A
/// token-holder can therefore create/modify/delete alert rules but still cannot reach the
/// <c>config_command</c> service-credential pivot or the carved secret columns. A rule's <c>definition</c> is
/// a compose panel + a predicate; it carries no secrets and names only <c>collect.*</c> collector tables, so
/// a stored rule is structurally incapable of reading a config control-plane table.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpCustomAlertTools
{
    [McpServerTool(Name = "list_custom_alert_rules"), Description(
        "Lists every saved custom alert rule as a lightweight summary - id, name, description, whether it is " +
        "enabled, version, who/when it was last updated, and when it last fired (last_fired, an ISO-8601 UTC " +
        "instant, or null if the rule has never fired), plus expires_at_utc (an ISO-8601 UTC instant, or null for a rule that never " +
        "expires) and expired (true once that instant has passed: an expired rule is not evaluated and does not fire, and is turned " +
        "back on by moving or clearing its expiry with update_custom_alert_rule). No definition body. Use the id with " +
        "get_custom_alert_rule to fetch a rule's full spec, or with update_custom_alert_rule / " +
        "delete_custom_alert_rule.")]
    public static async Task<string> ListCustomAlertRules(NpgsqlDataSource postgres)
    {
        try
        {
            var store = new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds);
            var rules = await store.ListAsync();
            return DarlingWebEndpoints.BuildRuleSummariesNode(rules).ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("list_custom_alert_rules", ex);
        }
    }

    [McpServerTool(Name = "get_custom_alert_rule"), Description(
        "Gets one saved custom alert rule in full, including its definition JSON (the metric + predicate + " +
        "optional hysteresis/scope/cadence) plus name, description, enabled, version, and expires_at_utc / expired (see " +
        "list_custom_alert_rules). Get the id from " +
        "list_custom_alert_rules. Use the returned version when calling update_custom_alert_rule (optimistic " +
        "concurrency).")]
    public static async Task<string> GetCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The numeric id of the rule to fetch (from list_custom_alert_rules).")] long rule_id)
    {
        try
        {
            var store = new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds);
            var result = await store.GetAsync(rule_id);
            return result is CustomAlertRuleResult.Ok ok && ok.Rule is not null
                ? DarlingWebEndpoints.BuildFullRuleNode(ok.Rule).ToJsonString(McpHelpers.JsonOptions)
                : Outcome("not_found", $"No custom alert rule with id {rule_id}.");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_custom_alert_rule", ex);
        }
    }

    [McpServerTool(Name = "validate_custom_alert_rule"), Description(
        "Dry-run: validates a custom-alert-rule definition JSON WITHOUT persisting anything, returning {valid:true} or " +
        "{valid:false, error} naming the first problem. This is the exact authority create_custom_alert_rule / " +
        "update_custom_alert_rule run before saving, and the evaluator applies when it loads a rule, so a result valid " +
        "here is accepted there too. A count-aggregate predicate that can fire when the count is 0 is always rejected: it " +
        "cannot tell zero events from a stalled collector. " +
        "<<GUIDE>> " +
        "Dry-run: validates a custom-alert-rule definition WITHOUT persisting anything. Returns {valid:true} or " +
        "{valid:false, error:\"...\"} naming the first problem. This is the exact authority " +
        "create_custom_alert_rule / update_custom_alert_rule run before saving AND the evaluator applies when " +
        "it loads a rule, so use it to iterate on a generated definition until it is valid. The definition is a " +
        "JSON object: a 'metric' (a compose SCALAR panel - a 'source' + 'measure'|'ratio' + 'aggregate', plus " +
        "an optional 'hours' window between one minute and 24 hours; no 'timeBucket'/'topN', since the metric " +
        "must reduce to a single value), a 'predicate' (either a SCALAR comparison - 'op' one of gt/ge/lt/le, a " +
        "numeric 'warnThreshold', and an optional 'criticalThreshold' that must be more extreme than warn in the " +
        "operator's direction; or a RANGE band - 'op' 'between' or 'outside', with numeric 'lowerBound' and " +
        "'upperBound' where lowerBound < upperBound, which fires Warning and carries no warn/critical but MAY add " +
        "an optional critical band via 'criticalLowerBound'/'criticalUpperBound' (both or neither) for a second, " +
        "more-severe tier - WIDER than the warn band for 'outside' (criticalLowerBound <= lowerBound and " +
        "upperBound <= criticalUpperBound), NARROWER for 'between' (lowerBound <= criticalLowerBound <= " +
        "criticalUpperBound <= upperBound); the scalar and range shapes being mutually exclusive), an " +
        "optional 'hysteresis' ('breachSamples'/'clearSamples', each an integer >= 1), an optional 'scope' " +
        "('mode' 'all', or 'servers' with a non-empty 'servers' list, or 'tag' with an integer 'tagId' fleet-tag " +
        "id whose directly-assigned servers the rule then evaluates), and an " +
        "optional 'evaluationIntervalSeconds' (>= 30). Build the metric panel from the compose catalog exposed " +
        "by describe_custom_view_catalog. On a count aggregate, a predicate that fires when the count is 0 is " +
        "rejected because it cannot tell zero events from a stalled collector: a scalar '<'/'<=' (use '>=' " +
        "instead), or a 'between'/'outside' band whose firing region includes 0 (set the bounds so a count of 0 " +
        "does not fire).")]
    public static Task<string> ValidateCustomAlertRule(
        [Description("The rule definition JSON to validate (NOT persisted).")] string definition)
    {
        try
        {
            var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition);
            return Task.FromResult(JsonSerializer.Serialize(
                new { valid = parsed is not null && error is null, error }, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("validate_custom_alert_rule", ex));
        }
    }

    [McpServerTool(Name = "create_custom_alert_rule"), Description(
        "Agent arm: needs agent_action, request's server only. Creates a new saved custom alert rule. The definition is VALIDATED first (same as " +
        "validate_custom_alert_rule); an invalid definition returns {status:\"invalid\", ...} and saves " +
        "nothing. On success returns the stored rule (id, version 1, and the definition) plus previous:null and undo (the delete_custom_alert_rule call for the new id). A name collision " +
        "returns {status:\"conflict\", ...}. The rule is stamped as MCP-authored. A rule created enabled starts " +
        "being evaluated on the next sweep; pass enabled=false to stage it paused. With 100 rules already enabled " +
        "fleet-wide, an enabled create is refused as {status:\"invalid\", ...}; paused rules do not count, so " +
        "enabled=false still saves. Optional expires_at_utc (a future ISO-8601 UTC instant) makes it a short-term watch that stops " +
        "evaluating on its own; the answer says when it expires, and a past expiry is refused. " +
        "Server scope is part of the definition, not a separate argument (#6455): with no 'scope' the rule applies to EVERY monitored server; " +
        "to limit it, put {\"scope\":{\"mode\":\"servers\",\"servers\":[\"<server name>\"]}} (or mode 'tag' with a fleet-tag 'tagId') in the definition.")]
    public static async Task<string> CreateCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The rule name - unique across all rules, max 200 characters.")] string name,
        [Description("The rule definition JSON (metric + predicate, plus optional hysteresis/scope/cadence). Validate it with validate_custom_alert_rule first.")] string definition,
        [Description("Optional human-readable description.")] string? description = null,
        [Description("Whether the rule is active. Default true (it evaluates on the next sweep); false stages it paused.")] bool enabled = true,
        [Description("Level 2 only: under mcp.allowInvestigations this change needs agent_action (request_id, claim_id, agent_id, reason, evidence); see the tool guide. Ignored when mcp.allowWrites is on.")] string? agent_action = null,
        DarlingAgentActionSeat? agentSeat = null,
        ILogger? logger = null,
        [Description("Optional expiry: an ISO-8601 UTC instant in the future (for example 2026-08-01T00:00:00Z). After it the rule stops evaluating. Omit for a rule that does not expire.")] string? expires_at_utc = null)
    {
        if (DarlingAgentActionSeat.IsGated(agentSeat))
        {
            return await DarlingMcpAgentActions.CreateCustomRuleAsync(agentSeat!, postgres, logger, agent_action, name, definition, description, enabled, expires_at_utc);
        }

        try
        {
            var (expiry, expiryError) = ResolveCreateExpiry(expires_at_utc);
            if (expiryError is not null)
            {
                return Outcome("invalid", expiryError);
            }

            var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition);
            if (parsed is null || error is not null)
            {
                return Outcome("invalid", error ?? "definition is invalid.");
            }

            var store = new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds);
            var result = await store.CreateAsync(name, description, definition, enabled, DarlingWebEndpoints.McpEditorPrincipal, expiresAtUtc: expiry);
            return result switch
            {
                CustomAlertRuleResult.Ok ok => CreateAnswer(ok.Rule!),
                CustomAlertRuleResult.Conflict conflict => Outcome("conflict", conflict.Message),
                CustomAlertRuleResult.Invalid invalid => Outcome("invalid", invalid.Message),
                _ => Outcome("error", "Could not create the rule."),
            };
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_custom_alert_rule", ex);
        }
    }

    [McpServerTool(Name = "update_custom_alert_rule"), Description(
        "Agent arm: needs agent_action, request's server only. Partial update of an existing rule: send only the fields to change (incl. expires_at_utc); an " +
        "omitted field keeps its current value. description ALSO accepts an empty string \"\" to clear it (vocabulary " +
        "shared with update_custom_view). An invalid definition changes nothing. Pass the " +
        "'version' last read from get_custom_alert_rule: a mismatch returns {status:\"conflict\"} rather than overwriting. " +
        "Setting enabled:true is refused as {status:\"invalid\"} if it would push the fleet-wide enabled-rule count over " +
        "its cap. " +
        "<<GUIDE>> " +
        "Updates an existing custom alert rule in place - a PARTIAL update: send only the fields you want to " +
        "change (name, description, definition, enabled, expires_at_utc), and every field you omit keeps its current value " +
        "(omitting description does NOT clear it). To CLEAR the description, send it as an empty string \"\" - " +
        "the one write vocabulary shared with update_custom_view: omitted = unchanged, empty = cleared. Provide at " +
        "least one field. A new definition is VALIDATED " +
        "first; an invalid one returns {status:\"invalid\", ...} and changes nothing. Pass the 'version' you " +
        "last read via get_custom_alert_rule - if someone else changed the rule since, this returns " +
        "{status:\"conflict\", ...} rather than silently overwriting their edit (reload and re-apply). A " +
        "missing id returns {status:\"not_found\", ...}; a name collision returns {status:\"conflict\", ...}. On " +
        "success returns the stored rule with its bumped version, plus previous and undo. " +
        "Enabling a rule (enabled:true, whether it is a fresh transition or re-saving one already on) is refused with " +
        "{status:\"invalid\"} once the fleet-wide enabled-rule count is at its 100 cap (\"Enabled custom alert rule limit " +
        "(100) reached. Disable or delete another rule before enabling this one.\"); leaving a rule disabled, or setting " +
        "it disabled, never counts against that cap and is never refused for it. " +
        "expires_at_utc is the rule's optional expiry: an ISO-8601 UTC instant sets or moves it (it must be in the future when you " +
        "change it), an empty string \"\" clears it so the rule never expires, and omitting it keeps the current one. A rule past " +
        "its expiry is EXPIRED: it is not evaluated and does not fire, and enabled:true on its own does not revive it. Turn it " +
        "back on by sending a later expires_at_utc, or \"\", with the rule's current version; the answer to any update of an " +
        "expired rule says this and repeats the call. The answer also carries previous (the values before the change) and undo (the update_custom_alert_rule call, at the new version, that restores the fields this call changed). " +
        "Server scope is part of the definition, not a separate argument (#6455): a rule with no 'scope' applies to EVERY monitored server, " +
        "and a new definition replaces the old one whole, so to keep or set a limit include {\"scope\":{\"mode\":\"servers\",\"servers\":[\"<server name>\"]}} " +
        "(or mode 'tag' with a fleet-tag 'tagId') in it.")]
    public static async Task<string> UpdateCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The id of the rule to update (from list_custom_alert_rules).")] long rule_id,
        [Description("The version you last read from get_custom_alert_rule (optimistic concurrency; a mismatch is a conflict, not an overwrite).")] int version,
        [Description("New rule name (unique, max 200 characters). Omit to keep the current name.")] string? name = null,
        [Description("New rule definition JSON. Validate it with validate_custom_alert_rule first. Omit to keep the current definition.")] string? definition = null,
        [Description("New human-readable description. Omit to keep the current description; send an empty string \"\" to clear it.")] string? description = null,
        [Description("Whether the rule is active. Omit to keep the current enabled state; false pauses it, true resumes it.")] bool? enabled = null,
        [Description("Level 2 only: under mcp.allowInvestigations this change needs agent_action (request_id, claim_id, agent_id, reason, evidence); see the tool guide. Ignored when mcp.allowWrites is on.")] string? agent_action = null,
        DarlingAgentActionSeat? agentSeat = null,
        ILogger? logger = null,
        [Description("Expiry: an ISO-8601 UTC instant in the future moves it, \"\" clears it (never expires), omit keeps it. Moving or clearing it turns an expired rule back on.")] string? expires_at_utc = null)
    {
        if (DarlingAgentActionSeat.IsGated(agentSeat))
        {
            return await DarlingMcpAgentActions.UpdateCustomRuleAsync(agentSeat!, postgres, logger, agent_action, rule_id, version, name, definition, description, enabled, expires_at_utc);
        }

        try
        {
            if (name is null && definition is null && description is null && enabled is null && expires_at_utc is null)
            {
                return Outcome("invalid", "Provide at least one field to change (name, description, definition, enabled, or expires_at_utc).");
            }

            /* A new definition is validated BEFORE any store hit (TryParse is pure), so a bad one returns
               'invalid' without opening a connection. A definition that is NOT being changed is passed through
               unchanged and NOT re-validated: it was validated when stored, and a rename or a pause must not be
               blocked by a measure that has since drifted out of the catalog. */
            if (definition is not null)
            {
                var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition);
                if (parsed is null || error is not null)
                {
                    return Outcome("invalid", error ?? "definition is invalid.");
                }
            }

            var store = new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds);
            var current = await store.GetAsync(rule_id);
            if (current is not CustomAlertRuleResult.Ok currentOk || currentOk.Rule is null)
            {
                return Outcome("not_found", $"No custom alert rule with id {rule_id}.");
            }

            var row = currentOk.Rule;
            var (expiry, expiryError) = ResolveUpdateExpiry(expires_at_utc, row.ExpiresAtUtc);
            if (expiryError is not null)
            {
                return Outcome("invalid", expiryError);
            }

            var result = await store.UpdateAsync(
                rule_id,
                name ?? row.Name,
                ResolveOptionalText(description, row.Description),
                definition ?? row.DefinitionJson,
                enabled ?? row.Enabled,
                version,
                DarlingWebEndpoints.McpEditorPrincipal,
                expiry);
            return result switch
            {
                CustomAlertRuleResult.Ok ok => UpdateAnswer(row, ok.Rule!, name, definition, description, enabled, expires_at_utc),
                CustomAlertRuleResult.NotFound => Outcome("not_found", $"No custom alert rule with id {rule_id}."),
                CustomAlertRuleResult.Conflict conflict => Outcome("conflict", conflict.Message),
                CustomAlertRuleResult.Invalid invalid => Outcome("invalid", invalid.Message),
                _ => Outcome("error", "Could not update the rule."),
            };
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_custom_alert_rule", ex);
        }
    }

    [McpServerTool(Name = "delete_custom_alert_rule"), Description(
        "Agent arm: needs agent_action, request's server only. Deletes a saved custom alert rule by id. Returns {status:\"deleted\", rule_id:N, previous (the deleted rule), undo (the create_custom_alert_rule call that recreates it under a new id), undo_note} on success, or " +
        "{status:\"not_found\", ...} when no rule has that id. This is permanent, and it also drops the rule's " +
        "accumulated per-server alert state. Any OPEN incident for the rule is force-resolved first (a recovery " +
        "row is written to alert history) so nothing is left showing as firing forever. To pause a rule without " +
        "deleting it, set enabled=false with update_custom_alert_rule (its open incidents are resolved too).")]
    public static async Task<string> DeleteCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The id of the rule to delete (from list_custom_alert_rules).")] long rule_id,
        [Description("Level 2 only: under mcp.allowInvestigations this change needs agent_action (request_id, claim_id, agent_id, reason, evidence); see the tool guide. Ignored when mcp.allowWrites is on.")] string? agent_action = null,
        DarlingAgentActionSeat? agentSeat = null,
        ILogger? logger = null)
    {
        if (DarlingAgentActionSeat.IsGated(agentSeat))
        {
            return await DarlingMcpAgentActions.DeleteCustomRuleAsync(agentSeat!, postgres, logger, agent_action, rule_id);
        }

        try
        {
            // #3305: resolve any open incident (write the recovery row) BEFORE the delete's FK cascade drops the
            // state that says which (rule, server) pairs were firing. No logger on the MCP surface — the recovery
            // row still writes; only the (optional) service-log line is skipped.
            var before = await new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds).GetAsync(rule_id);
            if (before is not CustomAlertRuleResult.Ok beforeOk || beforeOk.Rule is null)
            {
                return Outcome("not_found", $"No custom alert rule with id {rule_id}.");
            }

            var result = await CustomAlertEvaluator.ResolveAndDeleteRuleAsync(postgres, rule_id, logger: null, CancellationToken.None);
            return result is CustomAlertRuleResult.Ok
                ? DeleteAnswer(beforeOk.Rule)
                : Outcome("not_found", $"No custom alert rule with id {rule_id}.");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_custom_alert_rule", ex);
        }
    }

    /// <summary>
    /// The answer to a delete (#6403): the deleted rule as <c>previous</c>, and the create that recreates it. The recreated rule gets a new
    /// id, and its accumulated per-server alert state and history rows are not restored, so the answer says so in <c>undo_note</c>. An
    /// expiry that has already passed is left out of the create (a past expiry is refused), and the note says that too.
    /// </summary>
    private static string DeleteAnswer(CustomAlertRule rule)
    {
        var arguments = new JsonObject
        {
            ["name"] = rule.Name,
            ["definition"] = rule.DefinitionJson,
            ["enabled"] = rule.Enabled,
        };
        if (rule.Description is not null)
        {
            arguments["description"] = rule.Description;
        }

        var note = "The undo recreates the rule under a NEW id; its per-server alert state is not restored.";
        if (rule.ExpiresAtUtc is not null)
        {
            if (rule.IsExpiredAt(DateTime.UtcNow))
            {
                note += " The rule's expiry had already passed, so the recreated rule has no expiry.";
            }
            else
            {
                arguments["expires_at_utc"] = CustomAlertExpiry.Format(rule.ExpiresAtUtc);
            }
        }

        return new JsonObject
        {
            ["status"] = "deleted",
            ["rule_id"] = rule.Id,
            ["previous"] = DarlingWebEndpoints.BuildFullRuleNode(rule),
            ["undo"] = DarlingMcpUndoShape.Call("create_custom_alert_rule", arguments),
            ["undo_note"] = note,
        }.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The caveat every evaluate-now result carries: the one-shot predicate is only half of "would it
    /// fire" — the running evaluator also gates on hysteresis and per-server streak state.</summary>
    private const string HysteresisNote =
        "This evaluates the metric ONCE and applies the predicate right now; it does NOT apply the rule's " +
        "hysteresis (breachSamples/clearSamples) or per-server streak state, and delivers/persists nothing. " +
        "So breaching:true means the condition is true this instant (it would start counting toward a fire), " +
        "not that the rule has fired. A null current_value is no-data and never breaches.";

    [McpServerTool(Name = "test_custom_alert_rule"), Description(
        "Evaluate-now for a SAVED rule (rule_id) OR a supplied definition (exactly one): compiles the metric and reads " +
        "its CURRENT value on each in-scope server, reporting per-server {server, current_value, breaching, severity} " +
        "WITHOUT delivering a notification, writing history, or touching per-server streak state. breaching:true means " +
        "the predicate is true THIS INSTANT, not that the rule has fired: hysteresis and streak state are not applied. A " +
        "null current_value is no-data and never breaches. Zero in-scope servers returns status:no_in_scope_servers with " +
        "empty results, not an error. " +
        "<<GUIDE>> " +
        "Evaluate-now: for a SAVED rule (rule_id) OR a supplied definition, compiles the rule's metric and reads " +
        "its CURRENT value on each in-scope monitored server, reporting the value and whether it WOULD breach " +
        "(and at which severity tier) right now - WITHOUT delivering a notification, writing history, or touching " +
        "any per-server streak state. Pass EXACTLY ONE of rule_id or definition (definition is validated but not " +
        "saved, same shape as validate_custom_alert_rule). Returns per-server {server, current_value, breaching, " +
        "severity}. This is the INSTANTANEOUS predicate only - a real alert also requires the " +
        "condition to persist across the rule's hysteresis (breachSamples/clearSamples) and per-server streak, " +
        "which a one-shot cannot reproduce, so breaching:true means 'true right now', not 'has fired'. A null " +
        "current_value is no-data (an empty window, or a measure that is NULL for that server) and never breaches. " +
        "When no monitored server is in scope for the rule (an empty tag, or a servers list matching none), the answer is " +
        "{status:\"no_in_scope_servers\", rule_id, name, note, results:[]} instead of the per-server list; a normal run's " +
        "response carries no top-level status field at all, so an empty results array only ever means " +
        "no_in_scope_servers, never a run that quietly skipped every server.")]
    public static async Task<string> TestCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The id of a SAVED rule to test (from list_custom_alert_rules). Provide this OR definition, not both.")] long? rule_id = null,
        [Description("A rule definition JSON to test WITHOUT saving it (same shape as validate_custom_alert_rule). Provide this OR rule_id.")] string? definition = null)
    {
        try
        {
            // Exactly one input: rule_id XOR definition (== is true when both provided or both omitted).
            if (rule_id.HasValue == (definition is not null))
            {
                return Outcome("invalid", "Provide exactly one of rule_id or definition.");
            }

            long? resolvedId = null;
            string? ruleName = null;
            CustomAlertRuleDefinition def;

            if (rule_id.HasValue)
            {
                var store = new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds);
                var got = await store.GetAsync(rule_id.Value);
                if (got is not CustomAlertRuleResult.Ok ok || ok.Rule is null)
                {
                    return Outcome("not_found", $"No custom alert rule with id {rule_id.Value}.");
                }

                var (parsedSaved, savedError) = CustomAlertRuleDefinition.TryParse(ok.Rule.DefinitionJson);
                if (parsedSaved is null || savedError is not null)
                {
                    // A drifted saved rule: surface the validator error rather than throwing (test-now on a
                    // broken rule should say WHY it can't run, the way the health surface flags it).
                    return Outcome("invalid", $"The saved rule no longer validates: {savedError}");
                }

                def = parsedSaved;
                resolvedId = ok.Rule.Id;
                ruleName = ok.Rule.Name;
            }
            else
            {
                var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition!);
                if (parsed is null || error is not null)
                {
                    return Outcome("invalid", error ?? "definition is invalid.");
                }

                def = parsed;
            }

            // In-scope = the enabled monitored servers the rule applies to, via the SAME tag-aware gate the sweep
            // uses (#3350). A tag-scoped rule resolves its tag's current members from config.server_tag_map on
            // this same least-privilege mcp/viewer pool (which carries SELECT on the config tag tables); an
            // All/Servers rule is decided purely from the storage name and never hits the tag tables.
            var servers = await DarlingServerResolver.LoadEnabledAsync(postgres);
            IReadOnlySet<int>? tagServerIds = null;
            if (def.ScopeMode == CustomAlertScopeMode.Tag && def.ScopeTagId is int tagId)
            {
                var members = await new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds).ListTagMembersAsync(new[] { tagId });
                tagServerIds = members.TryGetValue(tagId, out var ids) ? ids : null;
            }

            var inScope = servers
                .Where(s => CustomAlertEvaluator.RuleAppliesToServer(def, tagServerIds, s.ServerId, s.ServerName))
                .ToList();
            if (inScope.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "no_in_scope_servers",
                    rule_id = resolvedId,
                    name = ruleName,
                    note = HysteresisNote,
                    results = Array.Empty<object>(),
                }, McpHelpers.JsonOptions);
            }

            // Resolve the run context ONCE (window availability + the composed-query deadline), then run the
            // SHARED per-server scalar seam the evaluator's sweep uses, so the value can never diverge. The
            // mcp-role `postgres` is the least-privilege pool (statement_timeout + ACL) — never the owner pool.
            var now = DateTime.UtcNow;
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, CancellationToken.None);
            var composedSeconds = await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, CancellationToken.None);

            var results = new List<object>(inScope.Count);
            foreach (var server in inScope)
            {
                var value = await CustomAlertEvaluator.EvaluateScalarNowAsync(
                    postgres, def, server.ServerName, now, rollups, coverage, composedSeconds, logger: null, CancellationToken.None);
                var (breaching, severity) = CustomAlertEvaluator.ClassifyTestValue(def, value);
                results.Add(new
                {
                    server = server.DisplayName ?? server.ServerName,
                    current_value = value,
                    breaching,
                    severity = severity?.ToString(),
                    no_data = value is null,
                });
            }

            return JsonSerializer.Serialize(new
            {
                rule_id = resolvedId,
                name = ruleName,
                note = HysteresisNote,
                results,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("test_custom_alert_rule", ex);
        }
    }

    [McpServerTool(Name = "backtest_custom_alert_rule"), Description(
        "Replays a SAVED rule (rule_id) OR a supplied definition over STORED history and reports per server the fire and clear " +
        "points it WOULD have produced, with the same persistence rule the live engine applies. Writes and delivers nothing. " +
        "Span: default 24 hours, over 168 is refused. At most 10 servers, 2,880 queries and 60 seconds per call: a cap that " +
        "stops the replay answers status:partial with stopped_at. A server with no stored samples is unavailable, never 0 fires. " +
        "<<GUIDE>> " +
        "Backtest: replays the rule over history already in the store, without touching the live rule, its streak state or any " +
        "channel. Pass EXACTLY ONE of rule_id or definition (definition is validated but not saved, same shape as " +
        "validate_custom_alert_rule). servers is a comma-separated list of server names, each resolved through the registry " +
        "(exact, then partial); omitted, the servers the rule is in scope for. hours is the span (default 24, at most 168); " +
        "as_of is the end of the span (default now). " +
        "STEP: the larger of the source's collection cadence on that server (the scheduler's own override, fleet, default " +
        "resolution) and span / 288. At each step the rule's metric is read as of that step over the rule's own window (the " +
        "same scalar seam test_custom_alert_rule and the sweep run), and the value goes through the persistence gate with the " +
        "rule's breachSamples / clearSamples from a fresh streak: a no-data step freezes the streak and is never a breach or a " +
        "clear, as on the live engine. So the fire and clear points are where the live engine would have fired and cleared on " +
        "those samples. What a replay cannot reproduce: a sample the store no longer holds (retention), a streak already open " +
        "before the span, the channel side (mutes, throttles, failed sends), and the streak's clock: the live engine counts " +
        "breachSamples / clearSamples once per evaluation (every evaluationIntervalSeconds, default 60) and a replay counts them " +
        "once per step, so with breachSamples above 1 a sustained breach takes breachSamples x step_seconds here against " +
        "breachSamples x evaluationIntervalSeconds live - read a replay of a multi-sample rule as a floor on the live fires, and " +
        "do the arithmetic from step_seconds and breach_samples in the answer. " +
        "ANSWER: {status, rule_id, name, span_hours, start_utc, end_utc, breach_samples, clear_samples, queries_run, elapsed_ms, servers:[{server, status, reason, " +
        "step_seconds, steps, steps_run, samples_evaluated, no_data_steps, breaching_samples, fire_count, clear_count, fires:[{at_utc, " +
        "value, severity}], clears:[{at_utc, value}], firing_at_end, covered_from_utc, covered_to_utc}]}. status is ok when every " +
        "step of every server ran. partial when a cap stopped it: stopped_at {cap: servers|queries|time|error, at_utc, reason} names " +
        "the cap and the first step not run (at_utc is null for the servers cap, which replays the first 10 servers by name and " +
        "lists the rest in servers_not_covered), and each server's covered_from_utc / covered_to_utc say what it did cover. A " +
        "server whose every step returned no value is status:unavailable with a reason and no counts: the source holds no " +
        "samples for it in the span (or the metric no longer compiles), which is not the same as zero fires. A read that FAILS " +
        "(a statement timeout, a dropped connection) ends only that server's replay and keeps its steps already replayed: that " +
        "server is partial (unavailable when nothing was replayed) with the error as its reason, and when no cap stopped the call " +
        "stopped_at.cap is error. Top-level " +
        "status is unavailable when every server is; empty when no monitored server is in scope; invalid for a bad input or a " +
        "span over 168 hours.")]
    public static async Task<string> BacktestCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The id of a SAVED rule to replay (from list_custom_alert_rules). Provide this OR definition, not both.")] long? rule_id = null,
        [Description("A rule definition JSON to replay WITHOUT saving it (same shape as validate_custom_alert_rule). Provide this OR rule_id.")] string? definition = null,
        [Description("Comma-separated server names (at most 10 are replayed). Omitted: the servers the rule is in scope for.")] string? servers = null,
        [Description("The span to replay, in hours ending at as_of. Default 24; more than 168 (7 days) is refused.")] double? hours = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (rule_id.HasValue == (definition is not null))
            {
                return Outcome("invalid", "Provide exactly one of rule_id or definition.");
            }

            // The number is validated BEFORE TimeSpan.FromHours sees it: FromHours throws on NaN and on anything past
            // ~2.56e8, and a throw lands in the catch below as an `error` envelope, not the documented `invalid` refusal.
            if (hours is double requested && (double.IsNaN(requested) || requested <= 0))
            {
                return Outcome("invalid", "hours must be a positive number of hours (default 24, at most 168).");
            }

            if (hours is double overCap && overCap > CustomAlertBacktest.MaxSpan.TotalHours)
            {
                return Outcome("invalid",
                    $"hours {overCap:0.##} is over the 168-hour (7-day) cap on one backtest; replay a shorter span, or several spans with as_of.");
            }

            var span = hours is double h ? TimeSpan.FromHours(h) : CustomAlertBacktest.DefaultSpan;

            var asOfError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
            if (asOfError is not null)
            {
                return asOfError;
            }

            long? resolvedId = null;
            string? ruleName = null;
            CustomAlertRuleDefinition def;
            if (rule_id.HasValue)
            {
                var got = await new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds).GetAsync(rule_id.Value, cancellationToken);
                if (got is not CustomAlertRuleResult.Ok ok || ok.Rule is null)
                {
                    return Outcome("not_found", $"No custom alert rule with id {rule_id.Value}.");
                }

                var (parsedSaved, savedError) = CustomAlertRuleDefinition.TryParse(ok.Rule.DefinitionJson);
                if (parsedSaved is null || savedError is not null)
                {
                    return Outcome("invalid", $"The saved rule no longer validates: {savedError}");
                }

                def = parsedSaved;
                resolvedId = ok.Rule.Id;
                ruleName = ok.Rule.Name;
            }
            else
            {
                var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition!);
                if (parsed is null || error is not null)
                {
                    return Outcome("invalid", error ?? "definition is invalid.");
                }

                def = parsed;
            }

            // The targets: the named servers through the registry, or the rule's in-scope servers by the sweep's own gate.
            var registered = await DarlingServerResolver.LoadEnabledAsync(postgres, cancellationToken);
            var targets = new List<CustomAlertBacktest.Target>();
            if (!string.IsNullOrWhiteSpace(servers))
            {
                foreach (var name in servers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    var (resolved, resolveError) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, name, cancellationToken);
                    if (resolveError is not null)
                    {
                        return resolveError;
                    }

                    if (targets.All(t => t.ServerId != resolved.ServerId))
                    {
                        var row = registered.FirstOrDefault(r => r.ServerId == resolved.ServerId);
                        targets.Add(new CustomAlertBacktest.Target(resolved.ServerId, resolved.ServerName, row?.DisplayName ?? resolved.ServerName));
                    }
                }
            }
            else
            {
                IReadOnlySet<int>? tagServerIds = null;
                if (def.ScopeMode == CustomAlertScopeMode.Tag && def.ScopeTagId is int tagId)
                {
                    var members = await new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds).ListTagMembersAsync(new[] { tagId }, cancellationToken);
                    tagServerIds = members.TryGetValue(tagId, out var ids) ? ids : null;
                }

                targets.AddRange(registered
                    .Where(s => CustomAlertEvaluator.RuleAppliesToServer(def, tagServerIds, s.ServerId, s.ServerName))
                    .Select(s => new CustomAlertBacktest.Target(s.ServerId, s.ServerName, s.DisplayName ?? s.ServerName)));
            }

            if (targets.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "empty",
                    rule_id = resolvedId,
                    name = ruleName,
                    message = "No monitored server is in scope for this rule, so there is nothing to replay.",
                    servers = Array.Empty<object>(),
                }, McpHelpers.JsonOptions);
            }

            // The servers cap: replay the first ten by name, report the rest as not covered.
            var ordered = targets.OrderBy(t => t.Label, StringComparer.OrdinalIgnoreCase).ToList();
            var covered = ordered.Take(CustomAlertBacktest.MaxServers).ToList();
            var notCovered = ordered.Skip(CustomAlertBacktest.MaxServers).Select(t => t.Label).ToList();

            var overrides = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, serverId: null, cancellationToken);
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
            var composedSeconds = await McpCommandDeadlines.ResolveComposedQuerySecondsAsync(postgres, cancellationToken);

            var replays = new List<CustomAlertBacktest.ServerReplay>(covered.Count);
            var unregisteredByServer = new Dictionary<int, IReadOnlyList<string>>();
            foreach (var target in covered)
            {
                var step = CustomAlertBacktest.StepFor(span, CustomAlertBacktest.CadenceFor(def.Plan.Measure.SourceTable, target.ServerId, overrides));
                replays.Add(new CustomAlertBacktest.ServerReplay(def, target, step, CustomAlertBacktest.StepTimes(windowEnd, span, step)));
                unregisteredByServer[target.ServerId] =
                    await ComposeServerScope.FindUnregisteredAsync(postgres, new[] { target.StorageName }, cancellationToken)
                    ?? Array.Empty<string>();
            }

            // Every step is the evaluator's own scalar seam as of that step, on this least-privilege mcp pool, never the owner's.
            var run = await CustomAlertBacktest.ReplayAsync(
                replays,
                (target, at, ct) => CustomAlertEvaluator.EvaluateScalarNowAsync(
                    postgres, def, target.StorageName, at, rollups, coverage, composedSeconds, logger: null, ct, unregisteredByServer[target.ServerId]),
                CustomAlertBacktest.MaxQueries,
                CustomAlertBacktest.MaxElapsed,
                cancellationToken);

            return FormatBacktest(def, resolvedId, ruleName, span, windowEnd, run, notCovered);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return McpHelpers.FormatError("backtest_custom_alert_rule", ex);
        }
    }

    /// <summary>The backtest's answer (#5911), split out so a test can hold its shape over a run it built itself.</summary>
    internal static string FormatBacktest(
        CustomAlertRuleDefinition def, long? ruleId, string? ruleName, TimeSpan span, DateTime endUtc,
        CustomAlertBacktest.Run run, IReadOnlyList<string> serversNotCovered)
    {
        static string Utc(DateTime? value) => value is DateTime v
            ? DateTime.SpecifyKind(v, DateTimeKind.Utc).ToString("o", System.Globalization.CultureInfo.InvariantCulture)
            : null!;

        var stoppedBy = run.StoppedBy != CustomAlertBacktest.StopCap.None
            ? run.StoppedBy
            : serversNotCovered.Count > 0 ? CustomAlertBacktest.StopCap.Servers : CustomAlertBacktest.StopCap.None;

        var servers = run.Servers.Select(s =>
        {
            var unavailable = s.SamplesEvaluated == 0 && (s.StepsRun > 0 || s.Failed);
            var failure = s.Failed
                ? $"A read failed at {Utc(s.FailedAtUtc)} and this server's replay stopped there ({s.StepsRun} of {s.Steps.Count} steps ran): {s.FailureReason}"
                : null;
            return (object)new
            {
                server = s.Target.Label,
                status = unavailable ? "unavailable" : s.StepsRun == s.Steps.Count && !s.Failed ? "ok" : "partial",
                reason = unavailable
                    ? failure ?? "Every step returned no value: the store holds no samples of this rule's source for this server in the span covered (or the metric no longer compiles). That is no answer, not zero fires."
                    : failure,
                step_seconds = (long)s.Step.TotalSeconds,
                steps = s.Steps.Count,
                steps_run = s.StepsRun,
                samples_evaluated = s.SamplesEvaluated,
                no_data_steps = s.NoDataSteps,
                breaching_samples = unavailable ? (int?)null : s.BreachingSamples,
                fire_count = unavailable ? (int?)null : s.Fires.Count,
                clear_count = unavailable ? (int?)null : s.Clears.Count,
                fires = unavailable ? null : s.Fires.Select(f => new { at_utc = Utc(f.AtUtc), value = f.Value, severity = f.Severity }).ToList(),
                clears = unavailable ? null : s.Clears.Select(c => new { at_utc = Utc(c.AtUtc), value = c.Value }).ToList(),
                firing_at_end = unavailable ? (bool?)null : s.FiringAtEnd,
                covered_from_utc = s.StepsRun > 0 ? Utc(s.Steps[0]) : null,
                covered_to_utc = Utc(s.LastStepRun),
            };
        }).ToList();

        var firstFailed = run.Servers.Where(s => s.Failed).OrderBy(s => s.FailedAtUtc).FirstOrDefault();
        var allUnavailable = run.Servers.Count > 0 && run.Servers.All(s => s.SamplesEvaluated == 0 && (s.StepsRun > 0 || s.Failed));
        var status = stoppedBy != CustomAlertBacktest.StopCap.None || (firstFailed is not null && !allUnavailable)
            ? "partial"
            : allUnavailable ? "unavailable" : "ok";

        object? stoppedAt = stoppedBy switch
        {
            CustomAlertBacktest.StopCap.Servers => new
            {
                cap = "servers",
                at_utc = (string?)null,
                reason = $"{serversNotCovered.Count + run.Servers.Count} servers asked for; one backtest replays at most {CustomAlertBacktest.MaxServers}, so the first {CustomAlertBacktest.MaxServers} by name were replayed.",
            },
            CustomAlertBacktest.StopCap.Queries => new
            {
                cap = "queries",
                at_utc = (string?)Utc(run.StoppedAtUtc),
                reason = $"The replay reached the {CustomAlertBacktest.MaxQueries:N0}-query cap on one backtest.",
            },
            CustomAlertBacktest.StopCap.Time => new
            {
                cap = "time",
                at_utc = (string?)Utc(run.StoppedAtUtc),
                reason = $"The replay reached the {CustomAlertBacktest.MaxElapsed.TotalSeconds:0}-second cap on one backtest; replay a shorter span or fewer servers for the rest.",
            },
            // No cap stopped it but a read did: the replay broke on a server, which is not the same as a cap and not the same
            // as a store with nothing in it.
            _ when firstFailed is not null => new
            {
                cap = "error",
                at_utc = (string?)Utc(firstFailed.FailedAtUtc),
                reason = $"A read failed on {run.Servers.Count(s => s.Failed)} of {run.Servers.Count} replayed servers; the steps already replayed are kept, and each failed server's own reason says why.",
            },
            _ => null,
        };

        return JsonSerializer.Serialize(new
        {
            status,
            message = status == "unavailable" && firstFailed is not null
                ? "No server returned a value before a read failed on it: see each server's reason. That is no answer, not zero fires."
                : status == "unavailable"
                ? "No server returned a value at any step: the store holds no samples of this rule's source for these servers in the span. That is no answer, not zero fires."
                : null,
            rule_id = ruleId,
            name = ruleName,
            span_hours = span.TotalHours,
            start_utc = Utc(endUtc - span),
            end_utc = Utc(endUtc),
            breach_samples = def.BreachSamples,
            clear_samples = def.ClearSamples,
            queries_run = run.QueriesRun,
            elapsed_ms = run.ElapsedMilliseconds,
            stopped_at = stoppedAt,
            servers_not_covered = serversNotCovered.Count > 0 ? serversNotCovered : null,
            servers,
        }, McpHelpers.JsonOptions);
    }

    [McpServerTool(Name = "list_custom_alert_templates"), Description(
        "Lists the built-in STARTER custom-alert-rule templates: curated {key, name, description, definition} entries for " +
        "common PostgreSQL and SQL Server signals, where definition is a ready-to-use rule body. The starters are " +
        "deliberately conservative, not tuned: browse them, adjust thresholds and scope to your fleet, then pass the " +
        "definition to create_custom_alert_rule to save it, or test_custom_alert_rule to see what it would do right now. " +
        "Read-only: lists code-defined templates and touches no store. " +
        "<<GUIDE>> " +
        "Lists the built-in STARTER custom-alert-rule templates - curated {metric, predicate, hysteresis} " +
        "definitions for common signals: PostgreSQL (dead-tuple pile-up / replica replay lag / connection " +
        "count / table bloat / replication-slot WAL retention) and SQL Server (high signal-wait % / sustained " +
        "blocking / long-running query / tempdb space). Each entry is {key, name, description, definition}, " +
        "where definition is a ready-to-use rule body. Browse them, tweak the thresholds and scope to your " +
        "fleet (the starters are deliberately conservative, not tuned), then pass the definition to " +
        "create_custom_alert_rule to save it - or to test_custom_alert_rule to see what it would do right now. " +
        "Read-only: this lists code-defined templates and touches no store.")]
    public static Task<string> ListCustomAlertTemplates()
    {
        var templates = new JsonArray();
        foreach (var template in CustomAlertTemplates.All)
        {
            templates.Add(new JsonObject
            {
                ["key"] = template.Key,
                ["name"] = template.Name,
                ["description"] = template.Description,
                // The definition embedded as a JSON object (NOT an escaped string), so a client can hand it
                // straight to create_custom_alert_rule / test_custom_alert_rule after editing.
                ["definition"] = JsonNode.Parse(template.DefinitionJson),
            });
        }

        return Task.FromResult(new JsonObject { ["templates"] = templates }.ToJsonString(McpHelpers.JsonOptions));
    }

    /// <summary>The expiry of a CREATE (#6235); the rule lives in <see cref="CustomAlertExpiry.ResolveCreateExpiry"/> (Storage).</summary>
    internal static (DateTime? Expiry, string? Error) ResolveCreateExpiry(string? sent) => CustomAlertExpiry.ResolveCreateExpiry(sent);

    /// <summary>The expiry of an UPDATE (#6235); the rule lives in <see cref="CustomAlertExpiry.ResolveUpdateExpiry"/> (Storage).</summary>
    internal static (DateTime? Expiry, string? Error) ResolveUpdateExpiry(string? sent, DateTime? current) => CustomAlertExpiry.ResolveUpdateExpiry(sent, current);

    /// <summary>
    /// The answer to a create or an update (#6235): the stored rule, plus a note when the rule carries an expiry. A rule with a future
    /// expiry says when it expires and the call that keeps it; an EXPIRED rule says it is not evaluated and the exact call that turns it
    /// back on (a later expiry, or an empty one; <c>enabled:true</c> alone does not). On an update that changed the expiry the answer also
    /// gives the previous value and the call that restores it.
    /// </summary>
    internal static string RuleAnswer(CustomAlertRule rule, DateTime? previousExpiry = null, bool expiryTouched = false) =>
        RuleNode(rule, previousExpiry, expiryTouched).ToJsonString(McpHelpers.JsonOptions);

    private static JsonObject RuleNode(CustomAlertRule rule, DateTime? previousExpiry, bool expiryTouched)
    {
        var node = DarlingWebEndpoints.BuildFullRuleNode(rule);
        var now = DateTime.UtcNow;
        var next = rule.Version;
        if (rule.IsExpiredAt(now))
        {
            node["note"] = $"This rule expired at {CustomAlertExpiry.Format(rule.ExpiresAtUtc)}: it is not evaluated and does not fire. enabled:true alone does not turn it back on. " +
                $"To turn it back on, call update_custom_alert_rule with rule_id={rule.Id}, version={next} and expires_at_utc set to a later UTC instant, or \"\" to clear the expiry.";
        }
        else if (rule.ExpiresAtUtc is not null)
        {
            node["note"] = $"This rule expires at {CustomAlertExpiry.Format(rule.ExpiresAtUtc)}; after that it stops evaluating (it is kept, not deleted). " +
                $"To keep it, call update_custom_alert_rule with rule_id={rule.Id}, version={next} and expires_at_utc set to a later UTC instant, or \"\" for no expiry.";
        }

        if (expiryTouched && previousExpiry != rule.ExpiresAtUtc)
        {
            node["previous_expires_at_utc"] = CustomAlertExpiry.Format(previousExpiry);
        }

        return node;
    }

    /// <summary>The answer to a create (#6403): the stored rule, no previous value, and the delete that undoes it.</summary>
    private static string CreateAnswer(CustomAlertRule rule)
    {
        var node = RuleNode(rule, null, false);
        node["previous"] = null;
        node["undo"] = DarlingMcpUndoShape.Call("delete_custom_alert_rule", new JsonObject { ["rule_id"] = rule.Id });
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// The answer to an update (#6403): the stored rule, the values it had before, and the update that puts the changed fields back. The
    /// undo names only the fields this call sent (a field that was not changed is not re-sent, so an undo is never blocked by a
    /// definition that has drifted since), at the new version. An empty string clears a description or an expiry that had none.
    /// </summary>
    private static string UpdateAnswer(CustomAlertRule before, CustomAlertRule after, string? name, string? definition, string? description, bool? enabled, string? expiresAtUtc)
    {
        var node = RuleNode(after, before.ExpiresAtUtc, expiresAtUtc is not null);
        node["previous"] = new JsonObject
        {
            ["name"] = before.Name,
            ["description"] = before.Description,
            ["definition"] = JsonNode.Parse(before.DefinitionJson),
            ["enabled"] = before.Enabled,
            ["expires_at_utc"] = CustomAlertExpiry.Format(before.ExpiresAtUtc),
            ["version"] = before.Version,
        };
        var arguments = new JsonObject { ["rule_id"] = after.Id, ["version"] = after.Version };
        if (name is not null)
        {
            arguments["name"] = before.Name;
        }

        if (definition is not null)
        {
            arguments["definition"] = before.DefinitionJson;
        }

        if (description is not null)
        {
            arguments["description"] = before.Description ?? string.Empty;
        }

        if (enabled is not null)
        {
            arguments["enabled"] = before.Enabled;
        }

        if (expiresAtUtc is not null)
        {
            // A past expiry cannot be sent back (the tool refuses one that is not the rule's current value, and the refusal would
            // drop every other restored field with it), so an already-passed expiry that THIS call changed is restored as "no expiry"
            // and the note says so. A call that resent the rule's own passed expiry unchanged left it as it was: the undo sends the
            // same value back, which the tool accepts because it equals the current one, and there is nothing to note.
            var passed = after.ExpiresAtUtc != before.ExpiresAtUtc && before.ExpiresAtUtc is { } was && was <= DateTime.UtcNow;
            arguments["expires_at_utc"] = passed ? string.Empty : CustomAlertExpiry.Format(before.ExpiresAtUtc) ?? string.Empty;
            if (passed)
            {
                node["undo_note"] = "The rule's expiry had already passed, so the undo leaves the rule with no expiry rather than the passed one.";
            }
        }

        node["undo"] = DarlingMcpUndoShape.Call("update_custom_alert_rule", arguments);
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// The ONE vocabulary for an optional text field on a partial update, shared by every Darling MCP update tool
    /// that has one (this rule's <c>description</c>, and <c>update_custom_view</c>'s): <b>omitted (null) means
    /// unchanged; an empty or whitespace-only string means cleared (stored as NULL); anything else is the new
    /// value.</b>
    ///
    /// <para>Why this shape. Over MCP an omitted argument and an explicit JSON <c>null</c> both arrive as a C#
    /// <c>null</c>, so null cannot carry two meanings and "unchanged" is the one it must carry - a caller who
    /// sends <c>{enabled: false}</c> to pause a rule is not asking to lose its description. That leaves the
    /// empty string as the only in-band way to say "clear", and an empty description is not a value anyone
    /// stores on purpose, so nothing is lost by taking it. Before #3541 A14 the two update tools disagreed -
    /// <c>update_custom_alert_rule</c> preserved an omitted description while <c>update_custom_view</c> treated
    /// the same omission as a clear - and neither offered a clear at all, which is a contract that cannot be
    /// honored on one side and a silent data loss on the other.</para>
    ///
    /// <para>Internal (not private) so the view tool calls this exact method rather than restating the rule; a
    /// census test pins both call sites to it.</para>
    /// </summary>
    internal static string? ResolveOptionalText(string? sent, string? current)
    {
        if (sent is null)
        {
            return current;
        }

        return string.IsNullOrWhiteSpace(sent) ? null : sent;
    }

    /// <summary>A small <c>{status, message}</c> envelope for a non-data write outcome (conflict / invalid /
    /// not_found / error) - the same shape <see cref="DarlingMcpCustomViewTools"/> and
    /// <see cref="DarlingMcpAlertTools"/> use, so an MCP client can branch on the outcome kind. A successful
    /// create/update/get returns the rule object itself, not this.</summary>
    private static string Outcome(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);
}
