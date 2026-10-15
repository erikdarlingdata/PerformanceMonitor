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
using System.Threading.Tasks;
using Microsoft.AspNetCore.TestHost;

namespace Darling.Tests.AgentContract;

/// <summary>
/// A Level 1 investigation agent in test code only (#5632 Phase A, L8): the loop a real agent runs, driven against the real
/// MCP host over HTTP with the bearer token. It lists, claims (the claim composes the brief), calls every <c>next_calls</c>
/// entry the brief names, and reports in report format 1 with the evidence of the calls it made and a proposed note. It
/// only reads and reports: the four investigation tools and the read tools a brief names are all it calls. Darling ships
/// no agent; a census (<c>AgentContractNamespaceCensusTests</c>) fails if a shipped project names this namespace.
/// </summary>
internal sealed class FakeInvestigationAgent
{
    private readonly TestServer _host;
    private readonly string _token;

    /// <summary>An agent named <paramref name="agentId"/> on <paramref name="host"/>, sending <paramref name="token"/>.</summary>
    internal FakeInvestigationAgent(TestServer host, string token, string agentId)
    {
        _host = host;
        _token = token;
        AgentId = agentId;
    }

    /// <summary>The agent id it claims under.</summary>
    internal string AgentId { get; }

    /// <summary>The <c>list_investigation_requests</c> answer for <paramref name="state"/> (all when null).</summary>
    internal async Task<JsonElement> ListAsync(string? state = "open")
    {
        var arguments = new JsonObject();
        if (state is not null)
        {
            arguments["state"] = state;
        }

        return await CallAsync("list_investigation_requests", arguments);
    }

    /// <summary>The <c>claim_investigation_request</c> answer for <paramref name="requestId"/>.</summary>
    internal Task<JsonElement> ClaimAsync(string requestId) =>
        CallAsync("claim_investigation_request", new JsonObject { ["request_id"] = requestId, ["agent_id"] = AgentId });

    /// <summary>
    /// One whole investigation: claim <paramref name="requestId"/>, call each <c>next_calls</c> entry of the brief, and
    /// submit a report built from those calls. Throws when the claim does not answer <c>claimed</c>.
    /// </summary>
    internal async Task<InvestigationRun> InvestigateAsync(string requestId)
    {
        var claim = await ClaimAsync(requestId);
        if (claim.GetProperty("status").GetString() != "claimed")
        {
            throw new InvalidOperationException("The claim did not answer claimed: " + claim.GetRawText());
        }

        string claimId = claim.GetProperty("claim_id").GetString()!;
        var brief = claim.GetProperty("brief");

        var calls = new List<AgentCall>();
        var evidence = new JsonArray();
        foreach (var next in brief.GetProperty("next_calls").EnumerateArray())
        {
            string tool = next.GetProperty("tool").GetString()!;
            var arguments = JsonNode.Parse(next.GetProperty("arguments").GetRawText())!.AsObject();
            var answer = await CallAsync(tool, arguments);
            string status = answer.ValueKind == JsonValueKind.Object && answer.TryGetProperty("status", out var s)
                ? s.GetString() ?? "" : "ok";
            string message = answer.ValueKind == JsonValueKind.Object && answer.TryGetProperty("message", out var m) ? m.GetString() ?? "" : "";
            calls.Add(new AgentCall(tool, arguments.ToJsonString(), status, message));
            evidence.Add(new JsonObject
            {
                ["tool"] = tool,
                ["arguments"] = JsonNode.Parse(arguments.ToJsonString()),
                ["key_numbers"] = "status " + status,
            });
        }

        string server = brief.GetProperty("request").GetProperty("server").GetString() ?? "";
        string family = brief.GetProperty("request").GetProperty("family").GetString() ?? "";
        var submit = await CallAsync("submit_investigation_report", new JsonObject
        {
            ["request_id"] = requestId,
            ["claim_id"] = claimId,
            ["report_format"] = 1,
            ["verdict"] = "real",
            ["root_cause"] = "The " + family + " alert on " + server + " matches what the read tools show.",
            ["evidence"] = evidence.ToJsonString(),
            ["summary"] = "Read " + calls.Count + " tool(s) the brief named; no change was made.",
            ["proposed_note"] = "Contract-test note: the " + family + " alerts on this server were investigated by a test agent.",
        });

        return new InvestigationRun(claimId, brief.GetRawText(), calls, submit);
    }

    /// <summary>A <c>tools/call</c> through the real host with the token; the answer's text parsed as JSON.</summary>
    internal async Task<JsonElement> CallAsync(string tool, JsonObject arguments)
    {
        var (text, _) = await StatementFilterCensus.CallRealToolAsync(_host, tool, arguments, bearerToken: _token);
        using var doc = JsonDocument.Parse(text);
        return doc.RootElement.Clone();
    }
}

/// <summary>One read the agent made: its arguments, and the status word and message it answered.</summary>
internal sealed record AgentCall(string Tool, string Arguments, string Status, string Message);

/// <summary>What one investigation did: its claim id, the brief it read, the calls it made and the submit answer.</summary>
internal sealed record InvestigationRun(string ClaimId, string BriefJson, IReadOnlyList<AgentCall> Calls, JsonElement Submit)
{
    /// <summary>The tools the agent called, in order.</summary>
    internal IEnumerable<string> ToolsCalled => Calls.Select(c => c.Tool);
}
