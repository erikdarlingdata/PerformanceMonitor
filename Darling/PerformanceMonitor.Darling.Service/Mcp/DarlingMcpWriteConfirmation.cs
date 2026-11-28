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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Asks the user to confirm Darling's destructive write tools before they run (#9562), through the SDK's multi round-trip flow
/// (the 2026-07-28 revision, SEP-2322): the call answers "input required" with one yes or no question, the client asks its user and
/// sends the same call again with the answer, and only then does the tool run. The endpoint stays stateless, so nothing is held
/// between the two calls and no session is needed. A plain server-to-client elicitation call cannot work on a stateless server.
///
/// <para><b>One filter, every listed tool.</b> <see cref="Filter"/> is registered once, so a listed tool needs no change of its
/// own and a tool added to <see cref="ToolNames"/> is covered the day it is listed. It sits after the unknown-argument guard (a call
/// the guard refuses is never asked about) and outside the latency record (a question is not a failed read).</para>
///
/// <para><b>When it asks.</b> The tool is served to this caller, the client negotiated the multi round-trip flow
/// (<see cref="McpServer.IsMrtrSupported"/>) and declares the elicitation capability, the call carries no answer yet, and the call is
/// not an agent's: a call with <c>agent_action</c>, or any call from the investigations arm's token, has no user to ask and runs as
/// before. Every other client runs as before too. The gates (<c>mcp.allowWrites</c>, <c>mcp.allowServerAdmin</c>, the bearer token)
/// still decide whether the tool is served at all, and a tool that is not served never asks.</para>
///
/// <para><b>The answer.</b> Accept with the box left on is yes and the call runs. Decline, cancel, or an accept with the box cleared
/// is no: nothing runs, and the call answers <c>status: declined</c> as a normal result, not an error. For <c>purge_now</c>, which
/// has its own show-then-confirm step, a yes issues that step's confirm value for the plan the question showed, so one yes queues
/// the purge.</para>
/// </summary>
internal static class DarlingMcpWriteConfirmation
{
    /// <summary>The key of the one input request, and of its answer when the client sends the call again.</summary>
    internal const string InputKey = "confirm_write";

    private const string AnswerField = "confirm";

    /// <summary>
    /// The listed tools, each with the sentence that names what the call would change. The sentence is read from the call's own
    /// arguments, so it names exactly what the call names: a name where the call carries one (a server, a plan guide) and an id where
    /// it does not. A describer returns null when this call changes nothing (<c>drop_plan_guide</c> with its default dry run), and
    /// the call then runs without a question.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, Func<IDictionary<string, JsonElement>, string?>> Describers =
        new Dictionary<string, Func<IDictionary<string, JsonElement>, string?>>(StringComparer.Ordinal)
        {
            ["remove_server"] = a => $"Remove the monitored server {Q(a, "server_name")} from the central store's server list? The service stops collecting from it within one sweep.",
            ["remove_webhook_signing_key"] = a => Has(a, "route_id")
                ? $"Remove the webhook signing key of notification route {Q(a, "route_id")}, and its previous key?"
                : "Remove the default webhook destination's signing key, and its previous key?",
            ["delete_custom_alert_rule"] = a => $"Delete custom alert rule {Q(a, "rule_id")}? This is permanent.",
            ["delete_custom_view"] = a => $"Delete custom view {Q(a, "view_id")}? This is permanent.",
            ["delete_known_pattern"] = a => $"Delete known pattern {Q(a, "id")}?",
            ["delete_mute_rule"] = a => $"Delete mute rule {Q(a, "rule_id")}? Alerts it held back are delivered again.",
            ["delete_notification_route"] = a => $"Delete notification route {Q(a, "route_id")}? Its alerts fall to the next matching route or the parent channels.",
            ["delete_query_tag"] = a => $"Delete query tag {Q(a, "tag_id")} and its child tags?",
            ["delete_query_tag_rule"] = a => $"Delete query tag rule {Q(a, "rule_id")}? Queries it tagged lose that tag at the next collector pass.",
            ["delete_server_tag"] = a => $"Delete server tag {Q(a, "tag_id")} and its child tags?",
            ["drop_plan_guide"] = a => IsFalse(a, "dry_run")
                ? $"Drop plan guide {Q(a, "name")} in database {Q(a, "database_name")}{(Has(a, "server_name") ? " on server " + Q(a, "server_name") : string.Empty)}?"
                : null,
            ["purge_expired_mute_rules"] = _ => "Delete every mute rule that has already expired?",
            ["reset_collector_schedules"] = a => Has(a, "server_name")
                ? $"Reset the collector schedules of server {Q(a, "server_name")} back to the defaults?"
                : "Reset the fleet-wide collector schedules back to the defaults?",
            ["clear_change_window"] = a => $"Clear change window {Q(a, "window_id")} now? Every request it still holds is released at once.",
            ["clear_quiet_window"] = a => $"End the quiet window of rule {Q(a, "rule_id")} now? Alerts it held back are delivered again.",
        };

    /// <summary>The tools that ask, by name. <c>purge_now</c> is listed here and described separately, because its question reads the store.</summary>
    internal static IReadOnlyCollection<string> ToolNames { get; } =
        Describers.Keys.Append("purge_now").OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>The call-tool filter.</summary>
    internal static McpRequestFilter<CallToolRequestParams, CallToolResult> Filter =>
        next => async (request, cancellationToken) =>
        {
            var name = request.Params?.Name;
            if (name is null || !ToolNames.Contains(name) || !ClientCanBeAsked(request) || IsAgentCall(request))
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            var arguments = request.Params!.Arguments ?? new Dictionary<string, JsonElement>();
            if (request.Params.InputResponses is { } responses && responses.TryGetValue(InputKey, out var response))
            {
                if (!IsYes(response))
                {
                    return Declined(name);
                }

                if (name == "purge_now")
                {
                    await SeatPurgeConfirmAsync(request, arguments, cancellationToken).ConfigureAwait(false);
                }

                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            var question = name == "purge_now"
                ? await PurgeQuestionAsync(request, arguments, cancellationToken).ConfigureAwait(false)
                : Describers[name](arguments);
            if (question is null)
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            throw new InputRequiredException(
                new Dictionary<string, InputRequest> { [InputKey] = InputRequest.ForElicitation(Ask(question)) },
                requestState: null);
        };

    /// <summary>The question as an elicitation form: one box, on, that Accept leaves on and Decline never sends.</summary>
    internal static ElicitRequestParams Ask(string question) => new()
    {
        Message = question,
        RequestedSchema = new ElicitRequestParams.RequestSchema
        {
            Properties = new Dictionary<string, ElicitRequestParams.PrimitiveSchemaDefinition>
            {
                [AnswerField] = new ElicitRequestParams.BooleanSchema { Title = "Go ahead", Default = true },
            },
            Required = [AnswerField],
        },
    };

    /// <summary>What a call answers when the user said no: a normal result that says nothing was changed.</summary>
    internal static CallToolResult Declined(string tool) => new()
    {
        Content =
        [
            new TextContentBlock
            {
                Text = new JsonObject
                {
                    ["status"] = "declined",
                    ["tool"] = tool,
                    ["changed"] = false,
                    ["message"] = "The user declined, so nothing was changed.",
                }.ToJsonString(),
            },
        ],
    };

    private static bool ClientCanBeAsked(RequestContext<CallToolRequestParams> request)
    {
        var server = request.Server;
        if (server is null || !server.IsMrtrSupported || server.ClientCapabilities?.Elicitation is null)
        {
            return false;
        }

        // A tool this caller is not served is unknown to it, and asking about it would only end in the unknown-tool error.
        return server.ServerOptions?.ToolCollection is { } tools && tools.TryGetPrimitive(request.Params!.Name, out _);
    }

    /// <summary>An agent has no user to ask: the investigations arm's token (a gated seat), and any call that carries an <c>agent_action</c>.</summary>
    private static bool IsAgentCall(RequestContext<CallToolRequestParams> request)
    {
        if (request.Params?.Arguments is { } arguments
            && arguments.TryGetValue("agent_action", out var action)
            && action.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined))
        {
            return true;
        }

        return DarlingAgentActionSeat.IsGated(request.Services?.GetService<DarlingAgentActionSeat>());
    }

    private static bool IsYes(InputResponse response)
    {
        var raw = response.RawValue;
        if (raw.ValueKind != JsonValueKind.Object
            || !raw.TryGetProperty("action", out var action)
            || !string.Equals(action.GetString(), "accept", StringComparison.Ordinal))
        {
            return false;
        }

        return !(raw.TryGetProperty("content", out var content)
            && content.ValueKind == JsonValueKind.Object
            && content.TryGetProperty(AnswerField, out var box)
            && box.ValueKind == JsonValueKind.False);
    }

    private static async Task<string?> PurgeQuestionAsync(
        RequestContext<CallToolRequestParams> request, IDictionary<string, JsonElement> arguments, CancellationToken cancellationToken)
    {
        var retention = IntOf(arguments, "retention_days");
        var postgres = request.Services?.GetService<NpgsqlDataSource>();
        if (postgres is null)
        {
            return retention is int days
                ? $"Purge the whole store's collected history to {days.ToString(CultureInfo.InvariantCulture)} days now? Deleted rows cannot be brought back."
                : "Purge the whole store's collected history to each collector's configured retention now? Deleted rows cannot be brought back.";
        }

        var preview = await DarlingMcpIncidentActionTools.PreviewPurgeAsync(postgres, retention, cancellationToken).ConfigureAwait(false);
        var groups = preview.Groups
            .Select(g => $"{(int)g!["retention_days"]!} days for {(int)g["collector_count"]!} collectors")
            .ToList();
        return "Purge the whole store's collected history now? It deletes rows older than the retention that applies to each collector ("
            + string.Join(", ", groups) + "), for every server, and the deleted rows cannot be brought back.";
    }

    /// <summary>
    /// A yes to <c>purge_now</c> is the user's confirmation of the plan the question showed, so the call carries the confirm value its
    /// own show-then-confirm step would have asked for. The plan is read again here, so a retention that changed while the user
    /// answered is purged to what it is now, exactly as a second call by hand would.
    /// </summary>
    private static async Task SeatPurgeConfirmAsync(
        RequestContext<CallToolRequestParams> request, IDictionary<string, JsonElement> arguments, CancellationToken cancellationToken)
    {
        if (request.Services?.GetService<NpgsqlDataSource>() is not { } postgres)
        {
            return;
        }

        var preview = await DarlingMcpIncidentActionTools.PreviewPurgeAsync(postgres, IntOf(arguments, "retention_days"), cancellationToken).ConfigureAwait(false);
        var confirm = McpConfirmTokens.Shared.Issue("purge", DarlingMcpIncidentActionTools.PurgeScope, preview.Plan);
        request.Params!.Arguments = new Dictionary<string, JsonElement>(arguments)
        {
            ["confirm"] = JsonSerializer.SerializeToElement(confirm),
        };
    }

    private static int? IntOf(IDictionary<string, JsonElement> arguments, string key) =>
        arguments.TryGetValue(key, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var number) ? number : null;

    private static bool Has(IDictionary<string, JsonElement> arguments, string key) =>
        arguments.TryGetValue(key, out var value) && value.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined);

    private static bool IsFalse(IDictionary<string, JsonElement> arguments, string key) =>
        arguments.TryGetValue(key, out var value) && value.ValueKind == JsonValueKind.False;

    /// <summary>An argument's value as the question quotes it: a string in single quotes, anything else as written, capped at 80 characters.</summary>
    private static string Q(IDictionary<string, JsonElement> arguments, string key)
    {
        if (!arguments.TryGetValue(key, out var value))
        {
            return "(not given)";
        }

        var text = value.ValueKind == JsonValueKind.String ? value.GetString() ?? string.Empty : value.ToString();
        if (text.Length > 80)
        {
            text = text[..80] + "...";
        }

        return value.ValueKind == JsonValueKind.String ? "'" + text + "'" : text;
    }
}
