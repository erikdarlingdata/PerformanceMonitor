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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Common;

/// <summary>
/// The core of the confirmation both products' destructive write tools share (#9562): the call answers "input required" with one
/// yes or no question, the client asks its user and sends the same call again with the answer, and only then does the tool run. It
/// uses the SDK's multi round-trip flow (the 2026-07-28 revision, SEP-2322), so both stateless endpoints stay stateless: nothing is
/// held between the two calls and no session is needed.
///
/// <para>Each host passes what is its own: the tools that ask, the question each one asks (read from the call's own arguments), and
/// whether a call is an agent's. Darling's is <c>DarlingMcpWriteConfirmation</c>, Lite's is <c>McpWriteConfirmationLite</c>.</para>
///
/// <para><b>When it asks.</b> The tool is listed and served to this caller, the client negotiated the multi round-trip flow
/// (<see cref="McpServer.IsMrtrSupported"/>) and declares the elicitation capability, the call carries no answer yet, and the host
/// says the call is not an agent's. Every other client runs as before.</para>
///
/// <para><b>The answer.</b> Accept with the box left on is yes and the call runs. Decline, cancel, or an accept with the box cleared
/// is no: nothing runs, and the call answers <c>status: declined</c> as a normal result, not an error.</para>
///
/// <para><b>A yes counts only for the question that was asked, once.</b> Every question carries a <c>requestState</c>, which the client
/// echoes when it sends the call again (<see cref="McpConfirmationStates"/>). An answer counts only on a call that echoes a state this
/// process issued for this same tool and these same arguments, less than ten minutes ago, that no earlier answer has used. A call that
/// carries an answer with no state, or a state for another tool or other arguments, or an altered, expired or already used state, is
/// treated as having no answer yet: it is asked again and nothing runs. A host may issue its own state for a tool, as Darling's
/// <c>purge_now</c> does with the confirm value its own two-step flow issues (which is single use and expires on its own).</para>
/// </summary>
public static class McpWriteConfirmation
{
    /// <summary>The key of the one input request, and of its answer when the client sends the call again.</summary>
    public const string InputKey = "confirm_write";

    private const string AnswerField = "confirm";

    /// <summary>One question: its text, and the <c>requestState</c> the host issued for it (null for a state <see cref="McpConfirmationStates"/> issues).</summary>
    public sealed record Question(string Text, string? State = null);

    /// <summary>The question for a listed call, or null when this call changes nothing, so it runs without one.</summary>
    public delegate Task<Question?> QuestionOf(
        RequestContext<CallToolRequestParams> request, IDictionary<string, JsonElement> arguments, CancellationToken cancellationToken);

    /// <summary>Builds the call-tool filter.</summary>
    /// <param name="toolNames">The tools that ask.</param>
    /// <param name="question">The question for a listed call.</param>
    /// <param name="isAgentCall">Whether the call is an agent's: it has no user to ask and runs as before.</param>
    /// <param name="onYes">Runs after a yes and before the tool, or null for nothing.</param>
    /// <param name="hostState">Whether a state is one the host issued for the tool itself (a <see cref="Question"/> that carried its own
    /// state). The host's own flow then spends it. Null when the host issues none.</param>
    /// <param name="hostDeclined">Called with the tool and the state when the answer to a host-issued state is no, so the host spends it
    /// (a yes is spent by the host's own flow when the tool runs). Null when the host issues no state.</param>
    /// <param name="states">The states this class issues, or null for the process-wide <see cref="McpConfirmationStates.Shared"/>.</param>
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Create(
        IReadOnlyCollection<string> toolNames,
        QuestionOf question,
        Func<RequestContext<CallToolRequestParams>, bool> isAgentCall,
        Func<RequestContext<CallToolRequestParams>, string, IDictionary<string, JsonElement>, CancellationToken, Task>? onYes = null,
        Func<string, string, bool>? hostState = null,
        Action<string, string>? hostDeclined = null,
        McpConfirmationStates? states = null) =>
        next => async (request, cancellationToken) =>
        {
            var name = request.Params?.Name;
            if (name is null || !toolNames.Contains(name) || !ClientCanBeAsked(request) || isAgentCall(request))
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            var arguments = request.Params!.Arguments ?? new Dictionary<string, JsonElement>();
            var issuer = states ?? McpConfirmationStates.Shared;
            if (request.Params.InputResponses is { } responses && responses.TryGetValue(InputKey, out var response)
                && !string.IsNullOrEmpty(request.Params.RequestState))
            {
                var state = request.Params.RequestState;
                var hostIssued = hostState is not null && hostState(name, state);
                var ticket = hostIssued ? null : issuer.Check(name, arguments, state);

                /* The first answer to a state spends it, a no included, so a no cannot be followed by a yes on the same state. */
                if (hostIssued || (ticket is { } valid && issuer.TrySpend(valid)))
                {
                    if (!IsYes(response))
                    {
                        if (hostIssued)
                        {
                            hostDeclined?.Invoke(name, state);
                        }

                        return Declined(name);
                    }

                    if (onYes is not null)
                    {
                        await onYes(request, name, arguments, cancellationToken).ConfigureAwait(false);
                    }

                    return await next(request, cancellationToken).ConfigureAwait(false);
                }
            }

            /* No answer yet, or an answer that is not for a question this process asked of this call, or one already used: ask (again). */
            var asked = await question(request, arguments, cancellationToken).ConfigureAwait(false);
            if (asked is null)
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            throw new InputRequiredException(
                new Dictionary<string, InputRequest> { [InputKey] = InputRequest.ForElicitation(Ask(asked.Text)) },
                requestState: asked.State ?? issuer.Issue(name, arguments));
        };

    /// <summary>The question as an elicitation form: one box, on, that Accept leaves on and Decline never sends.</summary>
    public static ElicitRequestParams Ask(string question) => new()
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
    public static CallToolResult Declined(string tool) => new()
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

    /// <summary>Whether the call carries <paramref name="key"/> with a value.</summary>
    public static bool Has(IDictionary<string, JsonElement> arguments, string key) =>
        arguments.TryGetValue(key, out var value) && value.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined);

    /// <summary>Whether the call carries <paramref name="key"/> as the literal <c>false</c>.</summary>
    public static bool IsFalse(IDictionary<string, JsonElement> arguments, string key) =>
        arguments.TryGetValue(key, out var value) && value.ValueKind == JsonValueKind.False;

    /// <summary>Whether the call carries <paramref name="key"/> as the literal <c>true</c>.</summary>
    public static bool IsTrue(IDictionary<string, JsonElement> arguments, string key) =>
        arguments.TryGetValue(key, out var value) && value.ValueKind == JsonValueKind.True;

    /// <summary>An argument's value as a question quotes it: a string in single quotes, anything else as written, capped at 80 characters.</summary>
    public static string Q(IDictionary<string, JsonElement> arguments, string key)
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
