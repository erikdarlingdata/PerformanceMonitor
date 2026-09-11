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
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

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
/// <para><b>Evaluate-now is deliberately absent.</b> A faithful test-now would have to reproduce
/// <see cref="CustomAlertEvaluator"/>'s per-server scalar run (its window, rollup routing, and scalar
/// extraction) and then its predicate, which either couples this class to the evaluator or forks a divergent
/// copy of it. Both are ruled out here; the clean home for an evaluate-now is a shared seam on the evaluator
/// itself, tracked as a follow-up. To see a rule's current metric value in the meantime, run its
/// <c>metric</c> panel through the custom-view <c>run_custom_view_panel</c> tool.</para>
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
        "enabled, version, and who/when it was last updated. No definition body. Use the id with " +
        "get_custom_alert_rule to fetch a rule's full spec, or with update_custom_alert_rule / " +
        "delete_custom_alert_rule.")]
    public static async Task<string> ListCustomAlertRules(NpgsqlDataSource postgres)
    {
        try
        {
            var store = new CustomAlertRuleStore(postgres);
            var rules = await store.ListAsync();
            return BuildSummariesNode(rules).ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("list_custom_alert_rules", ex);
        }
    }

    [McpServerTool(Name = "get_custom_alert_rule"), Description(
        "Gets one saved custom alert rule in full, including its definition JSON (the metric + predicate + " +
        "optional hysteresis/scope/cadence) plus name, description, enabled, and version. Get the id from " +
        "list_custom_alert_rules. Use the returned version when calling update_custom_alert_rule (optimistic " +
        "concurrency).")]
    public static async Task<string> GetCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The numeric id of the rule to fetch (from list_custom_alert_rules).")] long rule_id)
    {
        try
        {
            var store = new CustomAlertRuleStore(postgres);
            var result = await store.GetAsync(rule_id);
            return result is CustomAlertRuleResult.Ok ok && ok.Rule is not null
                ? BuildFullRuleNode(ok.Rule).ToJsonString(McpHelpers.JsonOptions)
                : Outcome("not_found", $"No custom alert rule with id {rule_id}.");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_custom_alert_rule", ex);
        }
    }

    [McpServerTool(Name = "validate_custom_alert_rule"), Description(
        "Dry-run: validates a custom-alert-rule definition WITHOUT persisting anything. Returns {valid:true} or " +
        "{valid:false, error:\"...\"} naming the first problem. This is the exact authority " +
        "create_custom_alert_rule / update_custom_alert_rule run before saving AND the evaluator applies when " +
        "it loads a rule, so use it to iterate on a generated definition until it is valid. The definition is a " +
        "JSON object: a 'metric' (a compose SCALAR panel - a 'source' + 'measure'|'ratio' + 'aggregate', plus " +
        "an optional 'hours' window between one minute and 24 hours; no 'timeBucket'/'topN', since the metric " +
        "must reduce to a single value), a 'predicate' ('op' one of gt/ge/lt/le, a numeric 'warnThreshold', and " +
        "an optional 'criticalThreshold' that must be more extreme than warn in the operator's direction), an " +
        "optional 'hysteresis' ('breachSamples'/'clearSamples', each an integer >= 1), an optional 'scope' " +
        "('mode' 'all' or 'servers' with a non-empty 'servers' list; tag scope is not yet supported), and an " +
        "optional 'evaluationIntervalSeconds' (>= 30). Build the metric panel from the compose catalog exposed " +
        "by describe_custom_view_catalog. A '<'/'<=' predicate on a count aggregate is rejected because it " +
        "cannot tell zero events from a stalled collector; use '>=' instead.")]
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
        "Creates a new saved custom alert rule. The definition is VALIDATED first (same as " +
        "validate_custom_alert_rule); an invalid definition returns {status:\"invalid\", ...} and saves " +
        "nothing. On success returns the stored rule (id, version 1, and the definition). A name collision " +
        "returns {status:\"conflict\", ...}. The rule is stamped as MCP-authored. A rule created enabled starts " +
        "being evaluated on the next sweep; pass enabled=false to stage it paused.")]
    public static async Task<string> CreateCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The rule name - unique across all rules, max 200 characters.")] string name,
        [Description("The rule definition JSON (metric + predicate, plus optional hysteresis/scope/cadence). Validate it with validate_custom_alert_rule first.")] string definition,
        [Description("Optional human-readable description.")] string? description = null,
        [Description("Whether the rule is active. Default true (it evaluates on the next sweep); false stages it paused.")] bool enabled = true)
    {
        try
        {
            var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition);
            if (parsed is null || error is not null)
            {
                return Outcome("invalid", error ?? "definition is invalid.");
            }

            var store = new CustomAlertRuleStore(postgres);
            var result = await store.CreateAsync(name, description, definition, enabled, DarlingWebEndpoints.McpEditorPrincipal);
            return result switch
            {
                CustomAlertRuleResult.Ok ok => BuildFullRuleNode(ok.Rule!).ToJsonString(McpHelpers.JsonOptions),
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
        "Updates an existing custom alert rule in place - a PARTIAL update: send only the fields you want to " +
        "change (name, description, definition, enabled), and every field you omit keeps its current value " +
        "(omitting description does NOT clear it). Provide at least one field. A new definition is VALIDATED " +
        "first; an invalid one returns {status:\"invalid\", ...} and changes nothing. Pass the 'version' you " +
        "last read via get_custom_alert_rule - if someone else changed the rule since, this returns " +
        "{status:\"conflict\", ...} rather than silently overwriting their edit (reload and re-apply). A " +
        "missing id returns {status:\"not_found\", ...}; a name collision returns {status:\"conflict\", ...}. On " +
        "success returns the stored rule with its bumped version.")]
    public static async Task<string> UpdateCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The id of the rule to update (from list_custom_alert_rules).")] long rule_id,
        [Description("The version you last read from get_custom_alert_rule (optimistic concurrency; a mismatch is a conflict, not an overwrite).")] int version,
        [Description("New rule name (unique, max 200 characters). Omit to keep the current name.")] string? name = null,
        [Description("New rule definition JSON. Validate it with validate_custom_alert_rule first. Omit to keep the current definition.")] string? definition = null,
        [Description("New human-readable description. Omit to keep the current description (this cannot clear it).")] string? description = null,
        [Description("Whether the rule is active. Omit to keep the current enabled state; false pauses it, true resumes it.")] bool? enabled = null)
    {
        try
        {
            if (name is null && definition is null && description is null && enabled is null)
            {
                return Outcome("invalid", "Provide at least one field to change (name, description, definition, or enabled).");
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

            var store = new CustomAlertRuleStore(postgres);
            var current = await store.GetAsync(rule_id);
            if (current is not CustomAlertRuleResult.Ok currentOk || currentOk.Rule is null)
            {
                return Outcome("not_found", $"No custom alert rule with id {rule_id}.");
            }

            var row = currentOk.Rule;
            var result = await store.UpdateAsync(
                rule_id,
                name ?? row.Name,
                description ?? row.Description,
                definition ?? row.DefinitionJson,
                enabled ?? row.Enabled,
                version,
                DarlingWebEndpoints.McpEditorPrincipal);
            return result switch
            {
                CustomAlertRuleResult.Ok ok => BuildFullRuleNode(ok.Rule!).ToJsonString(McpHelpers.JsonOptions),
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
        "Deletes a saved custom alert rule by id. Returns {status:\"deleted\", rule_id:N} on success, or " +
        "{status:\"not_found\", ...} when no rule has that id. This is permanent, and it also drops the rule's " +
        "accumulated per-server alert state. Any OPEN incident for the rule is force-resolved first (a recovery " +
        "row is written to alert history) so nothing is left showing as firing forever. To pause a rule without " +
        "deleting it, set enabled=false with update_custom_alert_rule (its open incidents are resolved too).")]
    public static async Task<string> DeleteCustomAlertRule(
        NpgsqlDataSource postgres,
        [Description("The id of the rule to delete (from list_custom_alert_rules).")] long rule_id)
    {
        try
        {
            // #3305: resolve any open incident (write the recovery row) BEFORE the delete's FK cascade drops the
            // state that says which (rule, server) pairs were firing. No logger on the MCP surface — the recovery
            // row still writes; only the (optional) service-log line is skipped.
            var result = await CustomAlertEvaluator.ResolveAndDeleteRuleAsync(postgres, rule_id, logger: null, CancellationToken.None);
            return result is CustomAlertRuleResult.Ok
                ? JsonSerializer.Serialize(new { status = "deleted", rule_id }, McpHelpers.JsonOptions)
                : Outcome("not_found", $"No custom alert rule with id {rule_id}.");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_custom_alert_rule", ex);
        }
    }

    /// <summary>The caveat every evaluate-now result carries: the one-shot predicate is only half of "would it
    /// fire" — the running evaluator also gates on hysteresis and per-server streak state.</summary>
    private const string HysteresisNote =
        "This evaluates the metric ONCE and applies the predicate right now; it does NOT apply the rule's " +
        "hysteresis (breachSamples/clearSamples) or per-server streak state, and delivers/persists nothing. " +
        "So breaching:true means the condition is true this instant (it would start counting toward a fire), " +
        "not that the rule has fired. A null current_value is no-data and never breaches.";

    [McpServerTool(Name = "test_custom_alert_rule"), Description(
        "Evaluate-now: for a SAVED rule (rule_id) OR a supplied definition, compiles the rule's metric and reads " +
        "its CURRENT value on each in-scope monitored server, reporting the value and whether it WOULD breach " +
        "(and at which severity tier) right now - WITHOUT delivering a notification, writing history, or touching " +
        "any per-server streak state. Pass EXACTLY ONE of rule_id or definition (definition is validated but not " +
        "saved, same shape as validate_custom_alert_rule). Returns per-server {server, current_value, breaching, " +
        "severity}. IMPORTANT: this is the INSTANTANEOUS predicate only - a real alert also requires the " +
        "condition to persist across the rule's hysteresis (breachSamples/clearSamples) and per-server streak, " +
        "which a one-shot cannot reproduce, so breaching:true means 'true right now', not 'has fired'. A null " +
        "current_value is no-data (an empty window, or a measure that is NULL for that server) and never breaches.")]
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
                var store = new CustomAlertRuleStore(postgres);
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

            // In-scope = the enabled monitored servers the rule applies to (the same AppliesTo the sweep uses).
            var servers = await DarlingServerResolver.LoadEnabledAsync(postgres);
            var inScope = servers.Where(s => def.AppliesTo(s.ServerName)).ToList();
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

    /// <summary>The full single-rule wire shape (definition embedded as JSON, NOT an escaped string) - mirrors
    /// <see cref="DarlingWebEndpoints.BuildFullViewNode"/>, adding the <c>enabled</c> column that alert rules
    /// carry and views do not. Returned by get / create / update.</summary>
    private static JsonObject BuildFullRuleNode(CustomAlertRule rule) => new()
    {
        ["id"] = rule.Id,
        ["name"] = rule.Name,
        ["description"] = rule.Description,
        ["definition"] = JsonNode.Parse(rule.DefinitionJson),
        ["enabled"] = rule.Enabled,
        ["version"] = rule.Version,
        ["created_at"] = rule.CreatedAt,
        ["updated_at"] = rule.UpdatedAt,
        ["updated_by"] = rule.UpdatedBy,
    };

    /// <summary>The bare-array list wire shape (no definition body) - mirrors
    /// <see cref="DarlingWebEndpoints.BuildSummariesNode"/>, carrying <c>enabled</c> so the list can show a
    /// paused rule without fetching each full definition.</summary>
    private static JsonArray BuildSummariesNode(IReadOnlyList<CustomAlertRuleSummary> rules)
    {
        var array = new JsonArray();
        foreach (var rule in rules)
        {
            array.Add(new JsonObject
            {
                ["id"] = rule.Id,
                ["name"] = rule.Name,
                ["description"] = rule.Description,
                ["enabled"] = rule.Enabled,
                ["version"] = rule.Version,
                ["updated_at"] = rule.UpdatedAt,
                ["updated_by"] = rule.UpdatedBy,
            });
        }

        return array;
    }

    /// <summary>A small <c>{status, message}</c> envelope for a non-data write outcome (conflict / invalid /
    /// not_found / error) - the same shape <see cref="DarlingMcpCustomViewTools"/> and
    /// <see cref="DarlingMcpAlertTools"/> use, so an MCP client can branch on the outcome kind. A successful
    /// create/update/get returns the rule object itself, not this.</summary>
    private static string Outcome(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);
}
