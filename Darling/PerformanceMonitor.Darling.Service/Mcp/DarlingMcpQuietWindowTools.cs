/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The quiet window write tools (#6231, part D): no alerts for one server, or for the fleet, between a start and an end, so planned
/// work does not page anyone. A quiet window is NOT a second system: it is a mute rule whose only scope is a server (or nothing, for the
/// fleet) and whose end is set, written through <see cref="DarlingMcpAlertTools.CreateMuteRuleOver"/> and the same
/// <see cref="PgMuteRuleStore"/> the mute tools use, so the validation, the duplicate check and the reload beacon exist once. What the
/// mute rule lacked was a START (V208 <c>starts_at_utc</c>), so a window can be set ahead of the work; until the start passes the rule
/// is listed as <c>upcoming</c> and mutes nothing. The window ends on its own at its end. Served under <c>mcp.allowWrites</c> only, and on
/// <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>: an agent investigating an alert can never silence the fleet.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQuietWindowTools
{
    /// <summary>The prefix every quiet window's reason carries, so the mute list reads as what it is.</summary>
    internal const string ReasonPrefix = "Quiet window";

    /// <summary>
    /// True when <paramref name="rule"/> has the shape of a quiet window: an end, a server scope or none, and NOTHING narrower (no
    /// metric, pattern, source or client application). <c>clear_quiet_window</c> refuses any other rule, so it can never delete an
    /// ordinary mute by id.
    /// </summary>
    internal static bool IsQuietWindow(MuteRule rule) =>
        rule.ExpiresAtUtc.HasValue
        && !rule.IsRecurring
        && rule.MetricName is null
        && rule.DatabasePattern is null
        && rule.QueryTextPattern is null
        && rule.WaitTypePattern is null
        && rule.JobNamePattern is null
        && rule.ClientAppPattern is null
        && rule.ExternalSource is null
        && IsQuietWindowReason(rule.Reason);

    /// <summary>True when <paramref name="reason"/> is the text <c>set_quiet_window</c> writes: the prefix alone, or the prefix, a colon and a note.
    /// The Viewer's own Silence rules carry another reason, so they are not cleared here.</summary>
    internal static bool IsQuietWindowReason(string? reason) =>
        reason is not null
        && (string.Equals(reason, ReasonPrefix, StringComparison.Ordinal) || reason.StartsWith(ReasonPrefix + ": ", StringComparison.Ordinal));

    /// <summary>The longest window <c>set_quiet_window</c> sets: 7 days, the longest finite expiry the Viewer's mute dialog offers (1 hour, 24 hours,
    /// 7 days, or never). A longer mute is made by an operator in the Viewer.</summary>
    internal static readonly TimeSpan MaxWindow = TimeSpan.FromDays(7);

    /// <summary>How far ahead a window may start: 7 days from now, the same horizon as its length.</summary>
    internal static readonly TimeSpan MaxStartAhead = TimeSpan.FromDays(7);

    private static bool SameInstant(DateTime? a, DateTime? b) =>
        a is null || b is null ? a is null && b is null : Math.Abs((a.Value - b.Value).Ticks) < TimeSpan.TicksPerMillisecond;

    [McpServerTool(Name = "set_quiet_window"), Description(
        "Sets a QUIET WINDOW: no alert is delivered for one server (server_name) or for the WHOLE FLEET (server_name omitted) from starts_at (optional, default now) until ends_at (required). Alerts are still logged. Changes shared alert configuration the service delivers on. The window ends on its own. Returns the rule id and the clear_quiet_window call that undoes it.<<GUIDE>>" +
        "A quiet window is a mute rule with only a server scope (or none, for the fleet) and an end time, so get_mute_rules lists it with its " +
        "starts_at_utc and a state of active, upcoming or expired. starts_at is an optional ISO-8601 UTC timestamp: set it ahead of " +
        "planned work and the window mutes nothing until that moment (state upcoming); omit it to start now. ends_at is required, an " +
        "ISO-8601 UTC timestamp that must be in the future and after starts_at; an end that is not after the start is refused and nothing is " +
        "written. A window lasts at most 7 days (ends_at minus the start), the longest finite choice the Viewer offers, and starts at most 7 days from now. A server (or the fleet) can have only one quiet window upcoming or active at a time: a second one answers refused, naming the first, and you clear that one first. server_name scopes the window to one monitored server (its display name; keyed on the server's store id, so a renamed " +
        "server does not escape it); OMIT it and the window mutes EVERY alert on EVERY server, so say so to the person who asked. " +
        "Repeating the same call is safe: an identical window already in force answers already_exists with its id. The running service " +
        "applies the window on its next collection sweep, when the write's config_version bump makes it reload its mute cache. " +
        "Answer: {status:\"created\", rule_id, scope, starts_at_utc, ends_at_utc, state, previous: null, undo:{tool:\"clear_quiet_window\", " +
        "arguments:{rule_id}}}. Needs mcp.allowWrites; an agent investigation arm can never call it.")]
    public static async Task<string> SetQuietWindow(
        NpgsqlDataSource postgres,
        [Description("ISO-8601 UTC end of the window (e.g. 2026-10-21T06:00:00Z). Required; must be in the future.")] string ends_at,
        [Description("The one server to quiet (its display name). Omit to quiet the WHOLE FLEET.")] string? server_name = null,
        [Description("Optional ISO-8601 UTC start. Omit to start now. Before the start the window mutes nothing and shows as upcoming.")] string? starts_at = null,
        [Description("Optional reason, shown in the mute list (for example 'patching window, change 4471').")] string? reason = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            (int ServerId, string DisplayName)? target = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
                if (error is not null)
                {
                    return error;
                }

                target = (resolved.ServerId, resolved.DisplayName);
            }

            var answer = await SetQuietWindowCore(new PgMuteRuleStore(postgres), target, ends_at, starts_at, reason, DateTime.UtcNow);
            LogWrite(logger, "set_quiet_window", answer);
            return answer;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("set_quiet_window", ex);
        }
    }

    /// <summary>
    /// set_quiet_window's body over the <see cref="IMuteRuleStore"/> seam, so a test drives it with the real store or a fake and a fixed
    /// clock. <paramref name="target"/> is the resolved server, or null for the fleet.
    /// </summary>
    internal static async Task<string> SetQuietWindowCore(
        IMuteRuleStore store, (int ServerId, string DisplayName)? target, string? endsAt, string? startsAt, string? reason, DateTime nowUtc)
    {
        if (string.IsNullOrWhiteSpace(endsAt))
        {
            return DarlingMcpAlertTools.Outcome("invalid", "ends_at is required: an ISO-8601 UTC timestamp (e.g. 2026-10-21T06:00:00Z).");
        }

        if (!TryParseUtc(endsAt, out var end))
        {
            return DarlingMcpAlertTools.Outcome("invalid", $"ends_at '{endsAt}' is not a valid ISO-8601 timestamp (e.g. 2026-10-21T06:00:00Z).");
        }

        DateTime? start = null;
        if (!string.IsNullOrWhiteSpace(startsAt))
        {
            if (!TryParseUtc(startsAt, out var parsedStart))
            {
                return DarlingMcpAlertTools.Outcome("invalid", $"starts_at '{startsAt}' is not a valid ISO-8601 timestamp (e.g. 2026-10-21T02:00:00Z).");
            }

            start = parsedStart;
        }

        if (start.HasValue && end <= start.Value)
        {
            return DarlingMcpAlertTools.Outcome("invalid",
                $"ends_at ({Iso(end)}) must be after starts_at ({Iso(start.Value)}). Nothing was written.");
        }

        if (end <= nowUtc)
        {
            return DarlingMcpAlertTools.Outcome("invalid", $"ends_at ({Iso(end)}) is already past, so the window would never apply. Nothing was written.");
        }

        if (end - (start ?? nowUtc) > MaxWindow)
        {
            return DarlingMcpAlertTools.Outcome("invalid",
                $"A quiet window can last at most 7 days, the longest finite choice the Viewer's mute dialog offers (ends_at {Iso(end)} is longer). Set a shorter window and set another when it ends. Nothing was written.");
        }

        // Both limits count from now, like the Viewer's choices (1 hour, 24 hours, 7 days): a start a year ahead, or one window
        // set after another, would make a long mute out of short ones. (#6231)
        if (start.HasValue && start.Value - nowUtc > MaxStartAhead)
        {
            return DarlingMcpAlertTools.Outcome("invalid",
                $"starts_at ({Iso(start.Value)}) is more than 7 days from now. A quiet window can start at most 7 days ahead; set it closer to the work. Nothing was written.");
        }

        var sameScope = (await store.LoadAllAsync()).Where(r => r is not null && r.Enabled && IsQuietWindow(r) && !r.IsExpiredAt(nowUtc)
            && (target is { } scoped ? r.ServerId == scoped.ServerId : r.ServerId is null && r.ServerName is null)).ToList();
        if (sameScope.Count > 0 && !sameScope.Any(r => SameInstant(r.ExpiresAtUtc, end) && SameInstant(r.StartsAtUtc, start)))
        {
            var held = sameScope.OrderBy(r => r.CreatedAtUtc).First();
            var who = target is { } named ? $"Server {named.DisplayName}" : "The fleet";
            var heldState = held.StartsAtUtc is { } heldStart && heldStart > nowUtc ? "upcoming" : "active";
            return DarlingMcpAlertTools.Outcome("refused",
                $"{who} already has a quiet window that is {heldState} (rule {held.Id}, ends {Iso(held.ExpiresAtUtc!.Value)}). " +
                "Only one quiet window can be waiting or running for the same scope: clear it with clear_quiet_window first, then set the new one. Nothing was written.");
        }

        var text = string.IsNullOrWhiteSpace(reason) ? ReasonPrefix : $"{ReasonPrefix}: {reason.Trim()}";
        var answer = await DarlingMcpAlertTools.CreateMuteRuleOver(
            store, target?.DisplayName, null, null, null, null, null, text, Iso(end), target?.ServerId,
            target is { } t ? _ => Task.FromResult<string?>(t.DisplayName) : null, null, null, start.HasValue ? Iso(start.Value) : null,
            /* #6465: the same instant the scope check above used, so the duplicate check and CreatedAtUtc don't read the real clock. */
            nowUtc: nowUtc);

        var node = JsonNode.Parse(answer) as JsonObject;
        var status = node?["status"]?.GetValue<string>();
        if (node is null || status is not ("created" or "already_exists"))
        {
            return answer;
        }

        var rule = (JsonObject?)node["mute_rule"];
        var id = status == "created" ? rule?["id"]?.GetValue<string>() : node["rule_id"]?.GetValue<string>() ?? rule?["id"]?.GetValue<string>();
        var stored = id is null ? null : await DarlingMcpAlertTools.FindRuleAsync(store, id);
        return JsonSerializer.Serialize(new
        {
            status,
            rule_id = id,
            scope = target is { } s ? $"server {s.DisplayName} (#{s.ServerId})" : "the whole fleet (every alert on every server)",
            starts_at_utc = stored?.StartsAtUtc is { } sa ? Iso(sa) : null,
            ends_at_utc = stored?.ExpiresAtUtc is { } ea ? Iso(ea) : Iso(end),
            state = stored?.StateAt(nowUtc),
            previous = (object?)null,
            message = status == "created"
                ? "The window is stored. The service applies it on its next collection sweep."
                : "An identical window is already in force; nothing was written.",
            undo = new { tool = "clear_quiet_window", arguments = new { rule_id = id } },
        }, McpHelpers.JsonOptions);
    }

    [McpServerTool(Name = "clear_quiet_window"), Description(
        "Ends a quiet window NOW by its rule id (from set_quiet_window or get_mute_rules): the window's mute rule is deleted, so alerts it was holding back are delivered again. Changes shared alert configuration the service delivers on. Refuses any rule that is not a quiet window (an ordinary mute with a metric or pattern is not touched). Returns the window as it was and the set_quiet_window call that restores it.<<GUIDE>>" +
        "Clears one quiet window by rule_id, whether it is active or still upcoming. Only a rule with the shape of a quiet window that set_quiet_window wrote (the \"Quiet window\" reason, an end " +
        "time, a server scope or none, and no metric, pattern, source or client application) is cleared; any other rule id answers " +
        "invalid and is left alone, so this cannot delete an ordinary mute. Answer: {status:\"cleared\", rule_id, previous:{scope, " +
        "starts_at_utc, ends_at_utc, state, reason}, undo:{tool:\"set_quiet_window\", arguments:{...}}}; the undo carries the same server, " +
        "start and end and is only offered while the end is still ahead. {status:\"not_found\"} when no rule has that id. The running " +
        "service applies the change on its next collection sweep. Needs mcp.allowWrites; an agent investigation arm can never call it.")]
    public static async Task<string> ClearQuietWindow(
        NpgsqlDataSource postgres,
        [Description("The id of the quiet window's rule (from set_quiet_window or get_mute_rules).")] string rule_id,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var answer = await ClearQuietWindowCore(new PgMuteRuleStore(postgres), rule_id, DateTime.UtcNow);
            LogWrite(logger, "clear_quiet_window", answer);
            return answer;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("clear_quiet_window", ex);
        }
    }

    /// <summary>clear_quiet_window's body over the <see cref="IMuteRuleStore"/> seam.</summary>
    internal static async Task<string> ClearQuietWindowCore(IMuteRuleStore store, string? ruleId, DateTime nowUtc)
    {
        if (string.IsNullOrWhiteSpace(ruleId))
        {
            return DarlingMcpAlertTools.Outcome("invalid", "rule_id is required (from set_quiet_window or get_mute_rules).");
        }

        var rule = await DarlingMcpAlertTools.FindRuleAsync(store, ruleId);
        if (rule is null)
        {
            return DarlingMcpAlertTools.Outcome("not_found", $"No mute rule with id '{ruleId}'.");
        }

        if (!IsQuietWindow(rule))
        {
            return DarlingMcpAlertTools.Outcome("invalid",
                $"Mute rule '{ruleId}' is not a quiet window (it has no end time, it is narrowed by a metric, pattern, source or client application, or it was not set by set_quiet_window), so clear_quiet_window leaves it alone. Use delete_mute_rule or set_mute_rule_enabled for an ordinary mute.");
        }

        await store.DeleteAsync(rule.Id);

        var scope = rule.ServerId.HasValue || rule.ServerName is not null
            ? $"server {rule.ServerName} (#{rule.ServerId?.ToString(CultureInfo.InvariantCulture) ?? "name-keyed"})"
            : "the whole fleet (every alert on every server)";
        object? undo = null;
        if (rule.ExpiresAtUtc!.Value > nowUtc)
        {
            undo = new
            {
                tool = "set_quiet_window",
                arguments = new
                {
                    ends_at = Iso(rule.ExpiresAtUtc.Value),
                    server_name = rule.ServerName,
                    starts_at = rule.StartsAtUtc is { } sa ? Iso(sa) : null,
                    reason = rule.Reason is { } r && r.StartsWith(ReasonPrefix + ": ", StringComparison.Ordinal) ? r[(ReasonPrefix.Length + 2)..] : null,
                },
            };
        }

        return JsonSerializer.Serialize(new
        {
            status = "cleared",
            rule_id = rule.Id,
            previous = new
            {
                scope,
                starts_at_utc = rule.StartsAtUtc is { } s ? Iso(s) : null,
                ends_at_utc = Iso(rule.ExpiresAtUtc.Value),
                state = rule.StateAt(nowUtc),
                reason = rule.Reason,
            },
            message = "The window is deleted. The service stops applying it on its next collection sweep.",
            undo,
        }, McpHelpers.JsonOptions);
    }

    /// <summary>One Information line per call that wrote: the tool, who called, the rule id. A refusal wrote nothing and is not logged here.</summary>
    private static void LogWrite(ILogger? logger, string tool, string answer)
    {
        if (logger is null)
        {
            return;
        }

        var node = JsonNode.Parse(answer) as JsonObject;
        var status = node?["status"]?.GetValue<string>();
        if (status is not ("created" or "already_exists" or "cleared"))
        {
            return;
        }

        logger.LogInformation("MCP write {Tool} by {Identity}: {Status}, rule {RuleId}",
            tool, ReadCaller.WriteIdentity(), status, node?["rule_id"]?.GetValue<string>());
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

    private static string Iso(DateTime utc) => utc.ToString("o", CultureInfo.InvariantCulture);
}
